import io
import json
import time
from datetime import date, datetime, timedelta
from urllib import parse

import dagster as dg
import pandas as pd
import requests
from dagster_duckdb import DuckDBResource
from pykrx import stock as krx

from .cd_constants import DATE_FORMAT, EXCHANGES, get_today, get_today_date
from .resources import PostgresResource

DAYS = 42
BATCH_SIZE = 1000000 # Used in digest_historical_prices

# Helper functions (will be used by other historical files too)
def _adjust_to_business_day(day: date, holidays: set) -> date:
    """
    주어진 날짜(day)가 주말(토,일)이나 휴일이면,
    유효한 영업일이 될 때까지 하루씩 이전 날짜로 보정합니다.
    """
    while day.weekday() >= 5 or day in holidays:
        day -= timedelta(days=1)
    return day

def _getHoliday(year: int) -> list:
    """
    주어진 연도의 공휴일 정보를 반환합니다.
    반환값은 date 객체의 리스트입니다.
    """
    url = (
        "http://apis.data.go.kr/B090041/openapi/service/SpcdeInfoService/getRestDeInfo"
    )
    api_key_utf8 = "H4xFVKT1c5kElJbwG%2B5NtUbAG25YOtp2eohbfrMfuXb5fzm2e3JmWlLFMvzRS2LHduyLq823elhR3JR7KvzTLg%3D%3D"
    api_key_decode = parse.unquote(api_key_utf8)
    params = {
        "ServiceKey": api_key_decode,
        "solYear": year,
        "numOfRows": 200,
        "_type": "json",
    }
    response = requests.get(url, params=params)
    text = response.content.decode("utf-8")
    data_dict = json.loads(text)
    items = data_dict["response"]["body"]["items"]["item"]
    holidays = []
    for item in items:
        holiday_date = datetime.strptime(str(item.get("locdate")), "%Y%m%d").date()
        holidays.append(holiday_date)
    return holidays

def _get_valid_days(start_year: int, end_year: int, reference_date: date) -> list:
    """
    시작 연도부터 종료 연도까지 각 월의 마지막 영업일(주말/휴일 제외)을 구합니다.
    단, 기준일(reference_date)로부터 약 43일(두 달 전) 이내의 날짜는 제외합니다.
    """
    valid_days = []
    target_date = reference_date - timedelta(days=43)

    for year in range(start_year, end_year + 1):
        holidays_set = set(_getHoliday(year))
        if year == 2006: # 2006년 지방선거일
            holidays_set.add(date(2006, 5, 31))

        for month in range(1, 13):
            if month == 12:
                last_day = date(year + 1, 1, 1) - timedelta(days=1)
            else:
                last_day = date(year, month + 1, 1) - timedelta(days=1)

            if year == reference_date.year and last_day > target_date:
                continue

            last_day = _adjust_to_business_day(last_day, holidays_set)

            if last_day < reference_date:
                if last_day.month == 12: # 12월 추가 보정
                    last_day = _adjust_to_business_day(
                        last_day - timedelta(days=1), holidays_set
                    )
                valid_days.append(last_day)
    return valid_days


@dg.asset(
    kinds={"source", "duckdb"},
    group_name="CD_HISTORY",
    tags={"data_tier": "bronze", "data_source": "pykrx", "data_type": "prices"},
)
def historical_prices(
    context: dg.AssetExecutionContext, cd_duckdb: DuckDBResource
) -> dg.MaterializeResult:
    """
    오늘 기준 42일 전부터 오늘까지, 모든 거래소의 일일 거래소 가격 정보를 가져와
    수정주가(티커, 시가, 고가, 저가, 종가, 거래량, 거래금액, 등락률)를 cd_duckdb에 저장합니다.
    휴장일이나 가격이 모두 0인 데이터는 제외합니다.
    """
    today = datetime.today()
    start_date = today - timedelta(days=DAYS)

    COLUMNS = [
        "date", "ticker", "open", "high", "low", "close",
        "volume", "transaction", "rate", "exchange",
    ]
    PRICE_COLUMNS = ["open", "high", "low", "close", "volume", "transaction", "rate"]

    all_data = []

    for days_offset in range(DAYS + 1):
        current_date = start_date + timedelta(days=days_offset)
        date_str = current_date.strftime(DATE_FORMAT)

        for exchange in EXCHANGES:
            context.log.info(f"Fetching price data for {exchange} on {date_str}")
            try:
                df = krx.get_market_ohlcv(date=date_str, market=exchange).reset_index()
                if not df.empty:
                    df.columns = [
                        "ticker", "open", "high", "low", "close", "volume",
                        "transaction", "rate", "marketcap"
                    ]
                    df["date"] = pd.to_datetime(date_str, format=DATE_FORMAT)
                    df["exchange"] = exchange

                    invalid_mask = (df[PRICE_COLUMNS] == 0).all(axis=1)
                    valid_df = df[~invalid_mask].copy()

                    if len(valid_df) < len(df):
                        context.log.info(
                            f"Filtered out {len(df) - len(valid_df)} rows with all zero price for {exchange} on {date_str}"
                        )

                    if not valid_df.empty:
                        valid_df = valid_df[COLUMNS]
                        all_data.append(valid_df)
                    else:
                        context.log.info(
                            f"No valid data for {exchange} on {date_str} after filtering"
                        )
                else:
                    context.log.info(f"No data for {exchange} on {date_str}")
            except Exception as e:
                context.log.error(
                    f"Error fetching data for {exchange} on {date_str}: {e}"
                )
                continue
            time.sleep(0.3)

    if not all_data:
        context.log.info("휴장일 등의 이유로 가격 데이터가 없습니다.")
        return dg.MaterializeResult(
            metadata={
                "Date": dg.MetadataValue.text(get_today()),
                "Result": dg.MetadataValue.text("휴장일 등의 이유로 가격 데이터가 없습니다."),
            }
        )

    price_df = pd.concat(all_data, ignore_index=True)
    context.log.info(f"Total records fetched for prices: {len(price_df)}")
    # print(price_df.head()) # Replaced with context.log if needed for debugging

    with cd_duckdb.get_connection() as conn:
        conn.register("price_df_duckdb", price_df) # Changed temp table name
        conn.execute("CREATE OR REPLACE TABLE price AS SELECT * FROM price_df_duckdb")
        preview_df = conn.execute("SELECT * FROM price LIMIT 10").fetchdf()
        count = conn.execute("SELECT COUNT(*) FROM price").fetchone()[0]
        date_counts = conn.execute(
            "SELECT date, COUNT(*) as count FROM price GROUP BY date ORDER BY date"
        ).fetchdf()

    return dg.MaterializeResult(
        metadata={
            "Number of records": dg.MetadataValue.int(count),
            "Date range": dg.MetadataValue.text(f"{start_date.strftime(DATE_FORMAT)} to {today.strftime(DATE_FORMAT)}"),
            "Unique dates": dg.MetadataValue.int(date_counts.shape[0]),
            "Preview": dg.MetadataValue.md(preview_df.to_markdown(index=False)),
            "Date distribution": dg.MetadataValue.md(date_counts.to_markdown(index=False)),
            "dagster/column_schema": dg.TableSchema(
                columns=[
                    dg.TableColumn("date", "date", description="Data date"),
                    dg.TableColumn("ticker", "string", description="Ticker symbol"),
                    dg.TableColumn("open", "int", description="Opening price"),
                    dg.TableColumn("high", "int", description="High price"),
                    dg.TableColumn("low", "int", description="Low price"),
                    dg.TableColumn("close", "int", description="Closing price"),
                    dg.TableColumn("volume", "bigint", description="Trading volume"),
                    dg.TableColumn("transaction", "bigint", description="Transaction amount"),
                    dg.TableColumn("rate", "float", description="Rate of change"),
                    dg.TableColumn("exchange", "string", description="Exchange identifier"),
                ]
            ),
        }
    )

@dg.asset(
    group_name="CD_HISTORY",
    kinds={"duckdb", "postgres"},
    tags={"data_tier": "silver", "data_source": "pykrx", "data_type": "prices"},
    deps=[historical_prices], # Dependency on the DuckDB asset
)
def digest_historical_prices(
    context: dg.AssetExecutionContext,
    cd_duckdb: DuckDBResource,
    cd_postgres: PostgresResource,
) -> dg.MaterializeResult:
    """
    DuckDB의 price 테이블을 읽어 유효한 가격 데이터를 선별한 후,
    날짜, 연도, 월 등의 데이터를 처리하여 PostgreSQL DB의 price 테이블에 bulk insert 합니다.
    """
    context.log.info("Loading price from CD DuckDB for digesting to PostgreSQL")
    with cd_duckdb.get_connection() as conn:
        # Ensure this table name matches what historical_prices creates
        price_df = conn.execute("SELECT * FROM price").fetchdf() 
    context.log.info(f"Loaded {len(price_df)} rows from DuckDB price table")

    price_columns_filter = ["open", "high", "low", "close", "volume", "transaction", "rate"]
    invalid_mask = (price_df[price_columns_filter] == 0).all(axis=1)
    valid_df = price_df[~invalid_mask].copy()

    valid_df["year"] = pd.to_datetime(valid_df["date"]).dt.year
    valid_df["month"] = pd.to_datetime(valid_df["date"]).dt.month
    # Ensure date is string in ISO format for PostgreSQL
    valid_df["date"] = pd.to_datetime(valid_df["date"]).dt.strftime("%Y-%m-%dT%H:%M:%S")


    final_df = valid_df[[
        "date", "exchange", "ticker", "open", "high", "low", "close",
        "volume", "transaction", "rate", "year", "month",
    ]]

    with cd_postgres.get_connection() as pg_conn:
        cursor = pg_conn.cursor()
        unique_date_exchanges = final_df[["date", "exchange"]].drop_duplicates()
        context.log.info(
            f"Deleting records for {len(unique_date_exchanges)} date-exchange combinations from PostgreSQL price table"
        )
        for _, row in unique_date_exchanges.iterrows():
            cursor.execute(
                'DELETE FROM price WHERE date = %s AND exchange = %s',
                (row["date"], row["exchange"]),
            )

        context.log.info(f"Inserting {len(final_df)} records into PostgreSQL price table")
        
        for i in range(0, len(final_df), BATCH_SIZE):
            batch_df = final_df.iloc[i : i + BATCH_SIZE]
            if i % (BATCH_SIZE * 10) == 0: # Log progress every 10 batches
                 context.log.info(
                    f"Processing batch for insert: {i // BATCH_SIZE + 1} / { (len(final_df) -1) // BATCH_SIZE + 1 } ({len(batch_df)} records)"
                )
            output = io.StringIO()
            batch_df.to_csv(output, index=False, header=False, sep="\t")
            output.seek(0)
            cursor.copy_from(
                output,
                "price", # PostgreSQL table name
                sep="\t",
                columns=(
                    "date", "exchange", "ticker", "open", "high", "low", "close",
                    "volume", "transaction", "rate", "year", "month",
                ),
            )
        pg_conn.commit()

    summary_df = pd.DataFrame({
        "날짜 수": [unique_date_exchanges["date"].nunique()],
        "거래소 수": [unique_date_exchanges["exchange"].nunique()],
        "총 레코드 수": [len(final_df)],
        "첫 날짜": [unique_date_exchanges["date"].min()],
        "마지막 날짜": [unique_date_exchanges["date"].max()],
    })

    return dg.MaterializeResult(
        metadata={
            "처리 날짜": dg.MetadataValue.text(get_today()),
            "처리된 레코드 수": dg.MetadataValue.int(len(final_df)),
            "요약": dg.MetadataValue.md(summary_df.to_markdown(index=False)),
            "미리보기 (PostgreSQL)": dg.MetadataValue.md(final_df.head(5).to_markdown(index=False)),
        }
    )

@dg.asset(
    group_name="CD_HISTORY",
    kinds={"postgres"},
    tags={"data_tier": "gold", "data_source": "pykrx", "data_type": "prices"},
    deps=[digest_historical_prices], # Depends on the silver layer asset
)
def sync_historical_price_to_security(
    context: dg.AssetExecutionContext, cd_postgres: PostgresResource
) -> dg.MaterializeResult:
    """
    한국 시장 대상으로, PostgreSQL price 테이블의 레코드에 대해 security 정보를 업데이트합니다.
    """
    date_str = get_today()
    exchange_list_filter = EXCHANGES # Assuming EXCHANGES is defined and appropriate

    context.log.info("Starting sync of historical price data to security in PostgreSQL")
    update_count = 0
    total_records_affected = 0 # To count how many price records are updated

    with cd_postgres.get_connection() as conn:
        with conn.cursor() as cursor:
            context.log.info("Loading securities from PostgreSQL database...")
            cursor.execute(
                """
                SELECT ticker, exchange, security_id, name, kor_name
                FROM security
                WHERE exchange = ANY(%s) AND delisting_date IS NULL
                """,
                (exchange_list_filter,),
            )
            securities_map = {
                (row[0], row[1]): {"security_id": row[2], "name": row[3], "kor_name": row[4]}
                for row in cursor.fetchall()
            }
            context.log.info(f"Loaded {len(securities_map)} securities into memory for mapping")

            context.log.info("Finding unlinked price records in PostgreSQL...")
            # Select distinct ticker, exchange pairs that need updating
            cursor.execute(
                """
                SELECT DISTINCT ticker, exchange
                FROM price
                WHERE security_id IS NULL AND exchange = ANY(%s)
                """,
                (exchange_list_filter,),
            )
            unlinked_price_groups = cursor.fetchall()
            total_groups_to_update = len(unlinked_price_groups)
            context.log.info(f"Found {total_groups_to_update} unlinked (ticker, exchange) groups in price table")

            if not unlinked_price_groups:
                context.log.info("No price records to update with security_id in PostgreSQL.")
                return dg.MaterializeResult(
                    metadata={
                        "Date": dg.MetadataValue.text(date_str),
                        "Exchange Filter": dg.MetadataValue.text(", ".join(exchange_list_filter)),
                        "Result": dg.MetadataValue.text("모든 price 레코드가 이미 security 정보와 연결되어 있습니다."),
                    }
                )
            
            updated_security_links_count = 0
            
            # Iterate through distinct (ticker, exchange) pairs
            for i, (ticker, exchange) in enumerate(unlinked_price_groups):
                if (i + 1) % 500 == 0: # Log progress every 500 groups
                    context.log.info(f"Processing group {i+1}/{total_groups_to_update}: Ticker {ticker}, Exchange {exchange}")

                security_info = securities_map.get((ticker, exchange))
                if security_info:
                    try:
                        cursor.execute(
                            """
                            UPDATE price
                            SET security_id = %s, name = %s, kor_name = %s
                            WHERE ticker = %s AND exchange = %s AND security_id IS NULL
                            RETURNING id;
                            """,
                            (
                                security_info["security_id"],
                                security_info["name"],
                                security_info["kor_name"],
                                ticker,
                                exchange,
                            ),
                        )
                        num_rows_updated_for_group = cursor.rowcount
                        total_records_affected += num_rows_updated_for_group
                        if num_rows_updated_for_group > 0:
                            updated_security_links_count +=1 # Count distinct (ticker,exchange) pairs updated
                        # Commit per group or less frequently if performance allows and atomicity per group is desired
                        # For now, committing at the end.
                    except Exception as e:
                        context.log.error(f"Error updating price for ticker {ticker}, exchange {exchange}: {e}")
                        conn.rollback() # Rollback on error for this group
                        # Decide if to raise or continue
                        continue # Continue with next group
                else:
                    context.log.warning(f"No security found for ticker {ticker}, exchange {exchange}. Skipping update for this group.")
            
            conn.commit() # Commit all successful updates

    context.log.info(
        f"Security sync completed. {updated_security_links_count} (ticker, exchange) groups updated, affecting {total_records_affected} price records."
    )

    metadata = {
        "Date": dg.MetadataValue.text(date_str),
        "Exchange Filter": dg.MetadataValue.text(", ".join(exchange_list_filter)),
        "Updated (Ticker, Exchange) Groups": dg.MetadataValue.int(updated_security_links_count),
        "Total Price Records Updated": dg.MetadataValue.int(total_records_affected),
    }
    if total_records_affected > 0 :
        # Potentially add a preview of updated records if feasible and useful
        # For example, fetch a few updated rows.
        pass


    return dg.MaterializeResult(metadata=metadata)

