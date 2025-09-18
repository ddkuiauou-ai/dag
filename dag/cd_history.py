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
BEFORE_YEARS = 20
KONEX_START_DATE = "20130701"
BATCH_SIZE = 1000000


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
        # locdate를 date 객체로 변환
        holiday_date = datetime.strptime(str(item.get("locdate")), "%Y%m%d").date()
        holidays.append(holiday_date)
    return holidays


def _get_valid_days(start_year: int, end_year: int, reference_date: date) -> list:
    """
    시작 연도부터 종료 연도까지 각 월의 마지막 영업일(주말/휴일 제외)을 구합니다.
    단, 기준일(reference_date)로부터 약 43일(두 달 전) 이내의 날짜는 제외합니다.

    반환값은 date 객체 리스트입니다.
    """
    valid_days = []
    # 기준일로부터 약 43일 전 날짜 (최근 2개월 제외)
    target_date = reference_date - timedelta(days=43)

    for year in range(start_year, end_year + 1):
        # 휴일 리스트를 집합으로 변환하여 조회 속도 개선
        holidays = set(_getHoliday(year))
        # 2006년 특별 처리: 지방선거일
        if year == 2006:
            holidays.add(date(2006, 5, 31))

        for month in range(1, 13):
            # 각 월의 마지막 날 계산
            if month == 12:
                last_day = date(year + 1, 1, 1) - timedelta(days=1)
            else:
                last_day = date(year, month + 1, 1) - timedelta(days=1)

            # 기준 연도인 경우, 최근 43일 이내의 날짜는 건너뜁니다.
            if year == reference_date.year and last_day > target_date:
                continue

            # 주말 및 휴일 보정
            last_day = _adjust_to_business_day(last_day, holidays)

            # 기준일 이전의 날짜만 선택
            if last_day < reference_date:
                # 원본 코드에서 12월에 한해 하루 추가 보정한 이유가 있다면 그대로 유지
                if last_day.month == 12:
                    last_day = _adjust_to_business_day(
                        last_day - timedelta(days=1), holidays
                    )
                valid_days.append(last_day)

    return valid_days


@dg.asset(
    kinds={"source", "duckdb"},
    group_name="CD",
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

    # 컬럼명 정의
    COLUMNS = [
        "date",
        "ticker",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "transaction",
        "rate",
        "exchange",
    ]
    # 유효성 검사용 가격 관련 컬럼
    PRICE_COLUMNS = ["open", "high", "low", "close", "volume", "transaction", "rate"]

    all_data = []  # 각 날짜와 거래소별 DataFrame 저장 리스트

    for days_offset in range(DAYS + 1):
        current_date = start_date + timedelta(days=days_offset)
        date_str = current_date.strftime(DATE_FORMAT)

        for exchange in EXCHANGES:
            context.log.info(f"Fetching price data for {exchange} on {date_str}")
            try:
                # pykrx를 통해 OHLCV 데이터 조회
                df = krx.get_market_ohlcv(date=date_str, market=exchange).reset_index()                
                if not df.empty:
                    df.columns = [
                        "ticker",
                        "open",
                        "high",
                        "low",
                        "close",
                        "volume",
                        "transaction",
                        "rate",
                        "marketcap"
                    ]
                    df["date"] = pd.to_datetime(date_str, format=DATE_FORMAT)
                    df["exchange"] = exchange

                    # 모든 가격 관련 컬럼이 0인 행 필터링
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

            # API 호출 간 간격 (과도한 호출 방지)
            time.sleep(0.3)

    if not all_data:
        context.log.info("휴장일 등의 이유로 가격 데이터가 없습니다.")
        return dg.MaterializeResult(
            metadata={
                "Date": dg.MetadataValue.text(get_today()),
                "Result": dg.MetadataValue.text(
                    "휴장일 등의 이유로 가격 데이터가 없습니다."
                ),
            }
        )

    price_df = pd.concat(all_data, ignore_index=True)
    
    print(f"Total records fetched: {len(price_df)}")
    print(price_df.head())

    # cd_duckdb에 데이터 저장
    with cd_duckdb.get_connection() as conn:
        conn.register("price_df", price_df)
        conn.execute("CREATE OR REPLACE TABLE price AS SELECT * FROM price_df")
        preview_df = conn.execute("SELECT * FROM price LIMIT 10").fetchdf()
        count = conn.execute("SELECT COUNT(*) FROM price").fetchone()[0]
        date_counts = conn.execute(
            "SELECT date, COUNT(*) as count FROM price GROUP BY date ORDER BY date"
        ).fetchdf()

    return dg.MaterializeResult(
        metadata={
            "Number of records": dg.MetadataValue.int(count),
            "Date range": dg.MetadataValue.text(f"{start_date} to {today}"),
            "Unique dates": dg.MetadataValue.int(date_counts.shape[0]),
            "Preview": dg.MetadataValue.md(preview_df.to_markdown(index=False)),
            "Date distribution": dg.MetadataValue.md(
                date_counts.to_markdown(index=False)
            ),
            "dagster/column_schema": dg.TableSchema(
                columns=[
                    dg.TableColumn("date", "date", description="Data date"),
                    dg.TableColumn("ticker", "string", description="Ticker symbol"),
                    dg.TableColumn("open", "int", description="Opening price"),
                    dg.TableColumn("high", "int", description="High price"),
                    dg.TableColumn("low", "int", description="Low price"),
                    dg.TableColumn("close", "int", description="Closing price"),
                    dg.TableColumn("volume", "bigint", description="Trading volume"),
                    dg.TableColumn(
                        "transaction", "bigint", description="Transaction amount"
                    ),
                    dg.TableColumn("rate", "float", description="Rate of change"),
                    dg.TableColumn(
                        "exchange", "string", description="Exchange identifier"
                    ),
                ]
            ),
        }
    )


@dg.asset(
    group_name="CD",
    kinds={"duckdb", "postgres"},
    deps=[historical_prices],
)
def digest_historical_prices(
    context: dg.AssetExecutionContext,
    cd_duckdb: DuckDBResource,
    cd_postgres: PostgresResource,
) -> dg.MaterializeResult:
    """
    의 price 테이블을 읽어 유효한 가격 데이터를 선별한 후,
    날짜, 연도, 월 등의 데이터를 처리하여 PostgreSQL DB의 price 테이블에 bulk insert 합니다.
    임시 테이블을 사용하지 않고, 직접 COPY 명령어를 통해 데이터를 삽입합니다.
    """
    context.log.info("Loading price from CD DuckDB")
    with cd_duckdb.get_connection() as conn:
        price_df = conn.execute("SELECT * FROM price").fetchdf()
    context.log.info(f"Loaded {len(price_df)} rows from price table")

    # 모든 가격 관련 컬럼이 0인 행 필터링 (휴일 데이터)
    price_columns = ["open", "high", "low", "close", "volume", "transaction", "rate"]
    invalid_mask = (price_df[price_columns] == 0).all(axis=1)
    valid_df = price_df[~invalid_mask].copy()

    # 날짜 및 연도/월 데이터 처리
    valid_df["year"] = pd.to_datetime(valid_df["date"]).dt.year
    valid_df["month"] = pd.to_datetime(valid_df["date"]).dt.month
    valid_df["date"] = pd.to_datetime(valid_df["date"]).dt.strftime("%Y-%m-%dT%H:%M:%S")

    # 필요한 컬럼 순서로 재정렬
    final_df = valid_df[
        [
            "date",
            "exchange",
            "ticker",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "transaction",
            "rate",
            "year",
            "month",
        ]
    ]

    with cd_postgres.get_connection() as pg_conn:
        cursor = pg_conn.cursor()

        # 기존 데이터 삭제: 고유한 (date, exchange) 조합별로 삭제
        unique_date_exchanges = final_df[["date", "exchange"]].drop_duplicates()
        context.log.info(
            f"Deleting records for {len(unique_date_exchanges)} date-exchange combinations"
        )
        for _, row in unique_date_exchanges.iterrows():
            cursor.execute(
                'DELETE FROM price WHERE date = %s AND exchange = %s',
                (row["date"], row["exchange"]),
            )

        # 신규 데이터 삽입: 임시 테이블 없이 직접 COPY 명령어를 사용
        context.log.info(f"Inserting {len(final_df)} records into price table")
        import io

        for i in range(0, len(final_df), BATCH_SIZE):
            batch_df = final_df.iloc[i : i + BATCH_SIZE]
            if i % (BATCH_SIZE * 10) == 0:
                context.log.info(
                    f"Processing batch: {i} to {i + len(batch_df)} of {len(final_df)} records"
                )
            output = io.StringIO()
            batch_df.to_csv(output, index=False, header=False, sep="\t")
            output.seek(0)
            cursor.copy_from(
                output,
                "price",
                sep="\t",
                columns=(
                    "date",
                    "exchange",
                    "ticker",
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                    "transaction",
                    "rate",
                    "year",
                    "month",
                ),
            )

        pg_conn.commit()

    # 메타데이터 요약 작성
    summary_df = pd.DataFrame(
        {
            "날짜 수": [unique_date_exchanges["date"].nunique()],
            "거래소 수": [unique_date_exchanges["exchange"].nunique()],
            "총 레코드 수": [len(final_df)],
            "첫 날짜": [unique_date_exchanges["date"].min()],
            "마지막 날짜": [unique_date_exchanges["date"].max()],
        }
    )

    return dg.MaterializeResult(
        metadata={
            "처리 날짜": dg.MetadataValue.text(get_today()),
            "처리된 레코드 수": dg.MetadataValue.int(len(final_df)),
            "요약": dg.MetadataValue.md(summary_df.to_markdown(index=False)),
            "미리보기": dg.MetadataValue.md(final_df.head(5).to_markdown(index=False)),
        }
    )


@dg.asset(
    group_name="CD",
    kinds={"postgres"},
    deps=[digest_historical_prices],
)
def sync_historical_price_to_security(
    context: dg.AssetExecutionContext, cd_postgres: PostgresResource
) -> dg.MaterializeResult:
    """
    한국 시장 대상으로, price 테이블의 모든 레코드에 대해 security 정보를 직접 업데이트하여 연결합니다.
    미리 security 데이터를 조회한 후, 한 번에 배치 업데이트를 수행합니다.
    """
    date_str = get_today()
    exchange_list = EXCHANGES  # 한국 시장의 거래소 리스트

    context.log.info("Starting sync of historical price data to security")
    update_count = 0
    total_records = 0

    with cd_postgres.get_connection() as conn:
        with conn.cursor() as cursor:
            # 1. Security 정보를 메모리에 로드
            context.log.info("Loading securities from database...")
            cursor.execute(
                """
                SELECT 
                    ticker, 
                    exchange, 
                    security_id, 
                    name, 
                    kor_name
                FROM 
                    security
                WHERE 
                    exchange = ANY(%s) AND 
                    delisting_date IS NULL
                """,
                (exchange_list,),
            )
            securities = {
                (row[0], row[1]): {
                    "security_id": row[2],
                    "name": row[3],
                    "kor_name": row[4],
                }
                for row in cursor.fetchall()
            }
            context.log.info(f"Loaded {len(securities)} securities into memory")

            # 2. 연결되지 않은 Price 레코드 찾기
            context.log.info("Finding unlinked price records...")
            cursor.execute(
                """
                SELECT 
                    ticker, 
                    exchange, 
                    COUNT(*) AS cnt
                FROM 
                    price
                WHERE 
                    security_id IS NULL AND 
                    exchange = ANY(%s)
                GROUP BY 
                    ticker, exchange
                """,
                (exchange_list,),
            )
            price_groups = cursor.fetchall()
            total_groups = len(price_groups)
            context.log.info(f"Found {total_groups} unlinked price groups")

            if not price_groups:
                context.log.info("No price records to update")
                return dg.MaterializeResult(
                    metadata={
                        "Date": dg.MetadataValue.text(date_str),
                        "Exchange": dg.MetadataValue.text(", ".join(exchange_list)),
                        "Result": dg.MetadataValue.text(
                            "모든 주식과 price가 이미 연결되어 있습니다."
                        ),
                    }
                )

            update_batch = []
            metadata_rows = []

            for i, (ticker, exchange, cnt) in enumerate(price_groups):
                key = (ticker, exchange)
                if key in securities:
                    sec = securities[key]
                    update_batch.append(
                        (
                            sec["security_id"],
                            sec["name"],
                            sec["kor_name"],
                            ticker,
                            exchange,
                        )
                    )
                    metadata_rows.append(
                        [
                            date_str,
                            ticker,
                            exchange,
                            sec["security_id"],
                            sec["name"],
                            cnt,
                        ]
                    )
                    total_records += cnt

                    if (i + 1) % 1000 == 0 or (i + 1) == total_groups:
                        context.log.info(
                            f"Progress: {i+1}/{total_groups} groups processed"
                        )

                    if len(update_batch) >= BATCH_SIZE:
                        try:
                            # Sort by ticker and exchange for consistent lock ordering
                            update_batch.sort(key=lambda x: (x[3], x[4]))  # ticker, exchange
                            
                            # Use smaller batches to reduce lock contention
                            mini_batch_size = 100  # Adjust based on your data volume
                            for j in range(0, len(update_batch), mini_batch_size):
                                mini_batch = update_batch[j:j + mini_batch_size]
                                cursor.executemany(
                                    """
                                    UPDATE price
                                    SET 
                                        security_id = %s, 
                                        name = %s, 
                                        kor_name = %s
                                    WHERE 
                                        ticker = %s AND 
                                        exchange = %s
                                    """,
                                    mini_batch,
                                )
                                conn.commit()
                                update_count += len(mini_batch)
                                context.log.info(
                                    f"Mini-batch update: {len(mini_batch)} groups updated"
                                )
                            update_batch = []
                        except Exception as e:
                            context.log.error(f"Error in batch update: {e}")
                            conn.rollback()
                            raise

            if update_batch:
                context.log.info(
                    f"Processing final batch of {len(update_batch)} groups"
                )
                try:
                    # Sort by ticker and exchange for consistent lock ordering
                    update_batch.sort(key=lambda x: (x[3], x[4]))  # ticker, exchange
                    
                    # Use smaller batches to reduce lock contention
                    mini_batch_size = 100  # Adjust based on your data volume
                    for j in range(0, len(update_batch), mini_batch_size):
                        mini_batch = update_batch[j:j + mini_batch_size]
                        cursor.executemany(
                            """
                            UPDATE price
                            SET 
                                security_id = %s, 
                                name = %s, 
                                kor_name = %s
                            WHERE 
                                ticker = %s AND 
                                exchange = %s
                            """,
                            mini_batch,
                        )
                        conn.commit()
                        update_count += len(mini_batch)
                        context.log.info(
                            f"Final mini-batch update: {len(mini_batch)} groups updated"
                        )
                except Exception as e:
                    context.log.error(f"Error in final batch update: {e}")
                    conn.rollback()
                    raise

    context.log.info(
        f"Sync completed: {update_count} groups ({total_records} records) updated"
    )

    if metadata_rows:
        result_df = pd.DataFrame(
            metadata_rows,
            columns=["date", "ticker", "exchange", "security_id", "name", "count"],
        )
        metadata = {
            "Date": dg.MetadataValue.text(date_str),
            "Exchange": dg.MetadataValue.text(", ".join(exchange_list)),
            "# of syncing to security": dg.MetadataValue.int(total_records),
            "Preview head": dg.MetadataValue.md(
                result_df.head().to_markdown(index=False)
            ),
            "Preview tail": dg.MetadataValue.md(
                result_df.tail().to_markdown(index=False)
            ),
        }
    else:
        metadata = {
            "Date": dg.MetadataValue.text(date_str),
            "Exchange": dg.MetadataValue.text(", ".join(exchange_list)),
            "Result": dg.MetadataValue.text(
                "모든 주식과 price가 이미 연결되어 있습니다."
            ),
        }

    return dg.MaterializeResult(metadata=metadata)


@dg.asset(
    group_name="CD",
    kinds={"source", "duckdb"},
)
def historical_marketcaps(
    context: dg.AssetExecutionContext, cd_duckdb: DuckDBResource
) -> dg.MaterializeResult:
    """
    지난 20년 동안 월말의 거래소 시가총액 데이터를 pykrx로 수집하고,
    cd_duckdb에 저장합니다.
    (티커, 종가, 시가총액, 거래량, 거래대금, 상장주식수)
    """
    # krx.get_market_cap의 컬럼 순서에 맞춘 컬럼명
    COLUMN_NAMES = ["ticker", "close", "marketcap", "volume", "transaction", "shares"]

    current_date = get_today_date()  # 현재 날짜 (date 객체)
    start_year = current_date.year - BEFORE_YEARS
    dates = _get_valid_days(start_year, current_date.year, current_date)

    # KONEX 시작일 이후 날짜만 사용
    konex_start = datetime.strptime(KONEX_START_DATE, DATE_FORMAT).date()
    konex_dates = [d for d in dates if d >= konex_start]

    marketcaps_list = []

    for exchange in EXCHANGES:
        applicable_dates = konex_dates if exchange == "KONEX" else dates
        for d in applicable_dates:
            context.log.info(f"Fetching market cap data for {exchange} on {d}")
            marketcaps = krx.get_market_cap(date=d.strftime("%Y%m%d"), market=exchange)
            time.sleep(0.4)  # API 호출 간 간격 조정

            marketcaps.reset_index(inplace=True)
            marketcaps.columns = COLUMN_NAMES
            marketcaps["ticker"] = marketcaps["ticker"].astype(str)
            marketcaps["date"] = d
            marketcaps["exchange"] = exchange

            if marketcaps["marketcap"].lt(0).any():
                error_msg = (
                    f"Negative marketcap found: {exchange} on {d}\n"
                    f"{marketcaps[marketcaps['marketcap'].lt(0)]}"
                )
                context.log.error(error_msg)
                raise ValueError(f"Negative marketcap detected for {exchange} on {d}")

            context.log.info(
                f"Total records: {marketcaps.shape[0]} for {exchange} on {d}"
            )
            if not marketcaps.empty:
                marketcaps_list.append(marketcaps)

    if not marketcaps_list:
        raise ValueError("No market capitalization data collected.")

    historical_df = pd.concat(marketcaps_list, ignore_index=True)

    with cd_duckdb.get_connection() as conn:
        conn.register("historical_marketcaps_df", historical_df)
        conn.execute(
            'CREATE OR REPLACE TABLE "historical_marketcaps" AS SELECT * FROM historical_marketcaps_df'
        )
        preview_df = conn.execute(
            'SELECT * FROM "historical_marketcaps" LIMIT 10'
        ).fetchdf()
        count = conn.execute('SELECT COUNT(*) FROM "historical_marketcaps"').fetchone()[
            0
        ]

    return dg.MaterializeResult(
        metadata={
            "Number of records": dg.MetadataValue.int(count),
            "Preview": dg.MetadataValue.md(preview_df.to_markdown(index=False)),
            "dagster/column_schema": dg.TableSchema(
                columns=[
                    dg.TableColumn("date", "date", description="Data date"),
                    dg.TableColumn("ticker", "string", description="Ticker symbol"),
                    dg.TableColumn("close", "int", description="Closing price"),
                    dg.TableColumn(
                        "marketcap", "int", description="Market capitalization"
                    ),
                    dg.TableColumn("volume", "bigint", description="Trading volume"),
                    dg.TableColumn(
                        "transaction", "bigint", description="Transaction amount"
                    ),
                    dg.TableColumn("shares", "bigint", description="Number of shares"),
                    dg.TableColumn(
                        "exchange", "string", description="Exchange identifier"
                    ),
                ]
            ),
        }
    )


@dg.asset(
    group_name="CD",
    kinds={"duckdb", "postgres"},
    deps=[historical_marketcaps],
)
def digest_historical_marketcaps(
    context: dg.AssetExecutionContext,
    cd_duckdb: DuckDBResource,
    cd_postgres: PostgresResource,
) -> dg.MaterializeResult:
    """
    cd_duckdb의 historical_marketcaps 테이블 데이터를 읽어,
    유효한 데이터( close, marketcap, volume, transaction 컬럼이 모두 0이 아닌 경우)만
    bulk 로 PostgreSQL의 marketcap 테이블에 입력합니다.
    """
    context.log.info("Querying historical_marketcaps data from CD_DuckDB")
    query = "SELECT * FROM historical_marketcaps"
    with cd_duckdb.get_connection() as dconn:
        raw_df = dconn.execute(query).fetchdf()

    context.log.info(f"Retrieved {raw_df.shape[0]} records from historical_marketcaps")

    # 유효성 체크: 모든 (close, marketcap, volume, transaction) 값이 0인 경우 유효하지 않음
    cols = ["close", "marketcap", "volume", "transaction"]
    valid_mask = (raw_df[cols] == 0).all(axis=1)
    valid_df = raw_df[~valid_mask].copy()

    context.log.info(
        f"Found {valid_df.shape[0]} valid records out of {raw_df.shape[0]}"
    )

    # 만약 일부만 유효하다면 오류 발생 (혼재된 데이터가 존재하는 경우)
    if not valid_df.empty and valid_df.shape[0] != raw_df.shape[0]:
        context.log.error(
            f"Validity check failed: {valid_df.shape[0]} valid vs {raw_df.shape[0]} total"
        )
        raise ValueError(
            f"Some market cap records are invalid. Valid rows: {valid_df.shape[0]}, Total rows: {raw_df.shape[0]}"
        )

    today_str = get_today()
    if valid_df.empty:
        msg = "No market cap data available (holiday or non-trading day)."
        context.log.info(msg)
        metadata = {
            "Date": dg.MetadataValue.text(today_str),
            "Result": dg.MetadataValue.text(msg),
        }
        context.add_output_metadata(metadata)
        return dg.MaterializeResult(metadata=metadata)

    # 메타데이터 집계
    unique_dates = valid_df["date"].nunique()
    exchange_counts = valid_df["exchange"].value_counts()
    kospi_count = exchange_counts.get("KOSPI", 0)
    kosdaq_count = exchange_counts.get("KOSDAQ", 0)
    konex_count = exchange_counts.get("KONEX", 0)

    context.log.info(
        f"Unique dates: {unique_dates}, KOSPI: {kospi_count}, KOSDAQ: {kosdaq_count}, KONEX: {konex_count}"
    )

    preview_df = pd.DataFrame(
        {
            "date": [today_str],
            "# of Days": [unique_dates],
            "# of KOSPI": [kospi_count],
            "# of KOSDAQ": [kosdaq_count],
            "# of KONEX": [konex_count],
            "count": [valid_df.shape[0]],
        }
    )

    context.log.info("Processing date and additional columns")
    valid_df["date"] = pd.to_datetime(valid_df["date"])
    valid_df = valid_df.assign(
        year=valid_df["date"].dt.year,
        month=valid_df["date"].dt.month,
    )

    expected_columns = [
        "date",
        "exchange",
        "ticker",
        "marketcap",
        "volume",
        "transaction",
        "shares",
        "year",
        "month",
    ]
    df_to_insert = valid_df[expected_columns].copy()
    insert_tuples = list(df_to_insert.itertuples(index=False, name=None))

    context.log.info(f"Prepared {len(insert_tuples)} rows for insert")

    with cd_postgres.get_connection() as pg_conn:
        cursor = pg_conn.cursor()
        # 각 (date, exchange) 조합별 기존 데이터 삭제
        date_exchange_pairs = df_to_insert[["date", "exchange"]].drop_duplicates()
        context.log.info(
            f"Deleting existing data for {len(date_exchange_pairs)} date-exchange pairs"
        )
        for idx, (d, exch) in enumerate(
            date_exchange_pairs.itertuples(index=False, name=None)
        ):
            if idx % 10 == 0:
                context.log.info(
                    f"Deleting data: {idx+1}/{len(date_exchange_pairs)} pairs processed"
                )
            cursor.execute(
                'DELETE FROM marketcap WHERE date = %s AND exchange = %s',
                (d.isoformat(), exch),
            )

        context.log.info(f"Inserting {len(insert_tuples)} new records")
        insert_df = pd.DataFrame(insert_tuples, columns=expected_columns)
        insert_df["date"] = insert_df["date"].dt.strftime("%Y-%m-%dT%H:%M:%S")

        import io

        for i in range(0, len(insert_df), BATCH_SIZE):
            batch_df = insert_df.iloc[i : i + BATCH_SIZE]
            context.log.info(
                f"Inserting batch {i//BATCH_SIZE + 1}/{(len(insert_df)-1)//BATCH_SIZE + 1} ({len(batch_df)} records)"
            )
            output = io.StringIO()
            batch_df.to_csv(output, index=False, header=False, sep="\t")
            output.seek(0)
            cursor.copy_from(
                output,
                "marketcap",
                sep="\t",
                columns=(
                    "date",
                    "exchange",
                    "ticker",
                    "marketcap",
                    "volume",
                    "transaction",
                    "shares",
                    "year",
                    "month",
                ),
            )

        context.log.info("Committing changes to PostgreSQL")
        pg_conn.commit()

    metadata = {
        "Date": dg.MetadataValue.text(today_str),
        "Rows": dg.MetadataValue.int(int(df_to_insert.shape[0])),
        "# of Days": dg.MetadataValue.int(int(unique_dates)),
        "# of KOSPI": dg.MetadataValue.int(int(kospi_count)),
        "# of KOSDAQ": dg.MetadataValue.int(int(kosdaq_count)),
        "# of KONEX": dg.MetadataValue.int(int(konex_count)),
        "Preview1": dg.MetadataValue.md(preview_df.head(10).to_markdown(index=False)),
        "Preview2": dg.MetadataValue.md(preview_df.tail(10).to_markdown(index=False)),
    }
    context.log.info("Digest historical marketcaps completed successfully")
    return dg.MaterializeResult(metadata=metadata)


@dg.asset(
    group_name="CD",
    kinds={"postgres"},
    deps=["digest_historical_marketcaps"],
)
def sync_historical_marketcap_to_security(context, cd_postgres: PostgresResource):
    """
    marketcap 테이블의 security_id, name, kor_name 컬럼을
    security 테이블의 정보로 업데이트합니다.
    (한국 시장 대상)
    """
    date_str = get_today()
    context.log.info("Starting sync of historical marketcap to security")

    update_query = """
    UPDATE marketcap m
    SET security_id = s.security_id,
        name = s.name,
        kor_name = s.kor_name
    FROM security s
    WHERE m.ticker = s.ticker
      AND m.exchange = s.exchange
      AND m.security_id IS NULL
      AND s.delisting_date IS NULL
      AND s.exchange = ANY(%s)
    """
    with cd_postgres.get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(update_query, (EXCHANGES,))
            updated_count = cursor.rowcount
            conn.commit()

    context.log.info(f"Updated {updated_count} records in marketcap table")
    metadata = {
        "Date": dg.MetadataValue.text(date_str),
        "Updated Records": dg.MetadataValue.int(updated_count),
        "Result": dg.MetadataValue.text(
            "marketcap table updated with security information."
        ),
    }
    return dg.MaterializeResult(metadata=metadata)


@dg.asset(
    group_name="CD",
    kinds={"source", "duckdb"},
)
def historical_bppedds(
    context: dg.AssetExecutionContext, cd_duckdb: DuckDBResource
) -> dg.MaterializeResult:
    """
    20년간 월말 BPPEDD 데이터를 pykrx를 통해 수집하여 cd_duckdb에 저장합니다.
    (티커, BPS, PER, PBR, EPS, DIV, DPS)
    """
    # pykrx에서 제공하는 컬럼 순서에 맞춘 컬럼명
    COLUMN_NAMES = ["ticker", "bps", "per", "pbr", "eps", "div", "dps"]

    current = get_today_date()  # 현재 날짜 (date 객체)
    # 기준 연도: (현재 연도 - BEFORE_YEARS) 부터 현재 연도까지의 유효 월말 영업일 목록 생성
    dates = _get_valid_days(current.year - BEFORE_YEARS, current.year, current)
    # KONEX 시작일 이후 날짜만 필터링 (KONEX 데이터만 적용)
    konex_start_date = datetime.strptime(KONEX_START_DATE, DATE_FORMAT).date()
    konex_dates = [d for d in dates if d >= konex_start_date]

    bppedds_list = []
    # KONEX 제외한 거래소 목록
    exchanges = list(set(EXCHANGES) - {"KONEX"})

    for exchange in exchanges:
        # KONEX 제외이므로 모든 날짜 사용
        for date in dates:
            context.log.info(f"Fetching BPPEDD data for {exchange} on {date}")
            bppedd = krx.get_market_fundamental_by_ticker(
                date=date.strftime("%Y%m%d"), market=exchange
            )
            time.sleep(0.4)
            bppedd.reset_index(inplace=True)

            bppedd.columns = COLUMN_NAMES
            bppedd["ticker"] = bppedd["ticker"].astype(str)
            bppedd["date"] = date
            bppedd["exchange"] = exchange

            # 모든 값(bps, pbr, eps, div, dps)이 0인 행 필터링 (우선주 등)
            value_columns = ["bps", "pbr", "eps", "div", "dps"]
            is_all_zero = (bppedd[value_columns] == 0).all(axis=1)
            valid_bppedd = bppedd[~is_all_zero].copy()

            if len(valid_bppedd) < len(bppedd):
                context.log.info(
                    f"Filtered out {len(bppedd) - len(valid_bppedd)} records with all zeros for {exchange} on {date}"
                )

            # 컬럼 순서 재정렬: date, ticker, bps, per, pbr, eps, div, dps, exchange
            valid_bppedd = valid_bppedd[
                ["date", "ticker", "bps", "per", "pbr", "eps", "div", "dps", "exchange"]
            ]
            context.log.info(
                f"Total valid records: {valid_bppedd.shape[0]} for {exchange} on {date}"
            )

            if not valid_bppedd.empty:
                bppedds_list.append(valid_bppedd)

    if not bppedds_list:
        raise ValueError("수집된 BPPEDD 데이터가 없습니다.")

    historical_df = pd.concat(bppedds_list, ignore_index=True)

    with cd_duckdb.get_connection() as conn:
        conn.register("historical_bppedds_df", historical_df)
        conn.execute(
            'CREATE OR REPLACE TABLE "historical_bppedds" AS SELECT * FROM historical_bppedds_df'
        )
        preview_df = conn.execute(
            'SELECT * FROM "historical_bppedds" LIMIT 10'
        ).fetchdf()
        count = conn.execute('SELECT COUNT(*) FROM "historical_bppedds"').fetchone()[0]

    return dg.MaterializeResult(
        metadata={
            "Number of records": dg.MetadataValue.int(count),
            "Preview": dg.MetadataValue.md(preview_df.to_markdown(index=False)),
            "dagster/column_schema": dg.TableSchema(
                columns=[
                    dg.TableColumn("date", "date", description="Data date"),
                    dg.TableColumn("ticker", "string", description="Ticker symbol"),
                    dg.TableColumn("bps", "float", description="Book value per share"),
                    dg.TableColumn(
                        "per", "float", description="Price-to-earnings ratio"
                    ),
                    dg.TableColumn("pbr", "float", description="Price-to-book ratio"),
                    dg.TableColumn("eps", "float", description="Earnings per share"),
                    dg.TableColumn("div", "float", description="Dividend yield"),
                    dg.TableColumn("dps", "float", description="Dividend per share"),
                    dg.TableColumn(
                        "exchange", "string", description="Exchange identifier"
                    ),
                ]
            ),
        }
    )


@dg.asset(
    group_name="CD",
    kinds={"duckdb", "postgres"},
    deps=[historical_bppedds],
)
def digest_historical_bppedds(
    context: dg.AssetExecutionContext,
    cd_duckdb: DuckDBResource,
    cd_postgres: PostgresResource,
) -> dg.MaterializeResult:
    """
    CD_DuckDB에 로드된 historical_bppedds 테이블에서 데이터를 조회하여,
    PostgreSQL의 bppedd 테이블을 bulk 방식으로 업데이트합니다.
    - bps, pbr, eps, div, dps 컬럼이 모두 0인 경우는 유효하지 않은 데이터로 판단합니다.
    - ticker 컬럼은 그대로 사용합니다.
    - 임시 테이블 사용 없이, 그리고 다른 코드와의 통일성을 위해 copy_from 방식을 사용합니다.
    """
    context.log.info("Querying historical_bppedds data from CD_DuckDB")
    query = "SELECT * FROM historical_bppedds"
    with cd_duckdb.get_connection() as dconn:
        raw_df = dconn.execute(query).fetchdf()
    context.log.info(
        f"Retrieved total {raw_df.shape[0]} records from historical_bppedds"
    )

    # 데이터 유효성 체크
    context.log.info("Checking data validity")
    valid_mask = (raw_df[["bps", "pbr", "eps", "div", "dps"]] == 0).all(axis=1)
    valid_df = raw_df[~valid_mask].copy()
    context.log.info(
        f"Found {valid_df.shape[0]} valid records out of {raw_df.shape[0]} total records"
    )

    if not valid_df.empty and valid_df.shape[0] != raw_df.shape[0]:
        context.log.error(
            f"Validity check failed: {valid_df.shape[0]} valid vs {raw_df.shape[0]} total"
        )
        raise ValueError(
            f"휴장일이 아님에도 시총 데이터의 일부가 유효하지 않습니다. "
            f"유효한 행: {valid_df.shape[0]}, 전체 행: {raw_df.shape[0]}"
        )

    today_str = get_today()
    if valid_df.empty:
        msg = "휴장일 등의 이유로 시총 데이터가 없습니다."
        context.log.info(msg)
        metadata = {
            "Date": dg.MetadataValue.text(today_str),
            "Result": dg.MetadataValue.text(msg),
        }
        context.add_output_metadata(metadata)
        return dg.MaterializeResult(metadata=metadata)

    # 메타데이터 집계
    context.log.info("Aggregating metadata")
    unique_dates = valid_df["date"].nunique()
    exchange_counts = valid_df["exchange"].value_counts()
    kospi_count = exchange_counts.get("KOSPI", 0)
    kosdaq_count = exchange_counts.get("KOSDAQ", 0)
    konex_count = exchange_counts.get("KONEX", 0)
    context.log.info(
        f"Unique dates: {unique_dates}, KOSPI: {kospi_count}, KOSDAQ: {kosdaq_count}, KONEX: {konex_count}"
    )

    preview_df = pd.DataFrame(
        {
            "date": [today_str],
            "# of Days": [unique_dates],
            "# of KOSPI": [kospi_count],
            "# of KOSDAQ": [kosdaq_count],
            "# of KONEX": [konex_count],
            "count": [valid_df.shape[0]],
        }
    )

    # 날짜 및 추가 컬럼 처리
    context.log.info("Processing date and additional columns")
    valid_df["date"] = pd.to_datetime(valid_df["date"])
    valid_df = valid_df.assign(
        year=valid_df["date"].dt.year,
        month=valid_df["date"].dt.month,
    )
    expected_columns = [
        "date",
        "exchange",
        "ticker",
        "bps",
        "per",
        "pbr",
        "eps",
        "div",
        "dps",
        "year",
        "month",
    ]
    df_to_insert = valid_df[expected_columns].copy()
    # ISO 포맷으로 날짜 변환
    df_to_insert["date"] = df_to_insert["date"].dt.strftime("%Y-%m-%dT%H:%M:%S")
    context.log.info(f"Prepared {df_to_insert.shape[0]} rows for insert")

    with cd_postgres.get_connection() as pg_conn:
        cursor = pg_conn.cursor()

        # (date, exchange) 조합을 한 번에 삭제
        unique_date_exchange = df_to_insert[["date", "exchange"]].drop_duplicates()
        context.log.info(
            f"Deleting existing data for {len(unique_date_exchange)} date-exchange pairs"
        )
        del_tuples = list(unique_date_exchange.itertuples(index=False, name=None))
        values_placeholder = ", ".join(["(%s, %s)"] * len(del_tuples))
        params = [item for tup in del_tuples for item in tup]
        delete_query = (
            f'DELETE FROM bppedd WHERE (date, exchange) IN ({values_placeholder})'
        )
        cursor.execute(delete_query, params)

        # Bulk insert using copy_from 방식 (batch 단위)
        total_rows = df_to_insert.shape[0]
        for i in range(0, total_rows, BATCH_SIZE):
            batch_df = df_to_insert.iloc[i : i + BATCH_SIZE]
            if i % (BATCH_SIZE * 10) == 0:
                context.log.info(
                    f"Inserting batch {i//BATCH_SIZE + 1}/{(total_rows-1)//BATCH_SIZE + 1} ({batch_df.shape[0]} records)"
                )
            output = io.StringIO()
            batch_df.to_csv(output, index=False, header=False, sep="\t")
            output.seek(0)
            cursor.copy_from(
                output,
                "bppedd",
                sep="\t",
                columns=(
                    "date",
                    "exchange",
                    "ticker",
                    "bps",
                    "per",
                    "pbr",
                    "eps",
                    "div",
                    "dps",
                    "year",
                    "month",
                ),
            )
        pg_conn.commit()

    metadata = {
        "Date": dg.MetadataValue.text(today_str),
        "Rows": dg.MetadataValue.int(df_to_insert.shape[0]),
        "# of Days": dg.MetadataValue.int(int(unique_dates)),
        "# of KOSPI": dg.MetadataValue.int(int(kospi_count)),
        "# of KOSDAQ": dg.MetadataValue.int(int(kosdaq_count)),
        "# of KONEX": dg.MetadataValue.int(int(konex_count)),
        "Preview1": dg.MetadataValue.md(preview_df.head(10).to_markdown(index=False)),
        "Preview2": dg.MetadataValue.md(preview_df.tail(10).to_markdown(index=False)),
    }
    context.log.info("Digest historical bppedds completed successfully")
    return dg.MaterializeResult(metadata=metadata)


@dg.asset(
    group_name="CD",
    kinds={"postgres"},
    deps=["digest_historical_bppedds"],
)
def sync_historical_bppedd_to_security(context, cd_postgres: PostgresResource):
    """
    Historical bppedd 데이터를 security 테이블과 연결합니다.
    (한국 시장 대상 - KONEX 제외)
    """
    date_str = get_today()
    exchange_list = list(set(EXCHANGES) - {"KONEX"})

    context.log.info("Starting sync of historical bppedd to security")
    with cd_postgres.get_connection() as conn:
        cursor = conn.cursor()
        update_query = """
            UPDATE bppedd b
            SET security_id = s.security_id, name = s.name, kor_name = s.kor_name
            FROM security s
            WHERE b.ticker = s.ticker
              AND b.exchange = s.exchange
              AND b.security_id IS NULL
              AND b.exchange = ANY(%s)
            RETURNING b.ticker, b.exchange, b.security_id, b.name, b.kor_name
        """
        cursor.execute(update_query, (exchange_list,))
        updated_rows = cursor.fetchall()
        conn.commit()

    if updated_rows:
        result_df = pd.DataFrame(
            updated_rows,
            columns=["ticker", "exchange", "security_id", "name", "kor_name"],
        )
        total_count = len(updated_rows)
        context.log.info(f"Updated {total_count} bppedd records in total")
        metadata = {
            "Date": dg.MetadataValue.text(date_str),
            "Exchange": dg.MetadataValue.text(", ".join(exchange_list)),
            "# of syncing to security": dg.MetadataValue.int(total_count),
            "Preview head": dg.MetadataValue.md(
                result_df.head().to_markdown(index=False)
            ),
            "Preview tail": dg.MetadataValue.md(
                result_df.tail().to_markdown(index=False)
            ),
        }
    else:
        context.log.info("No bppedd records were updated - all already linked")
        metadata = {
            "Date": dg.MetadataValue.text(date_str),
            "Exchange": dg.MetadataValue.text(", ".join(exchange_list)),
            "Result": dg.MetadataValue.text(
                "모든 주식과 bppedd가 이미 연결되어 있습니다"
            ),
        }

    return dg.MaterializeResult(metadata=metadata)
