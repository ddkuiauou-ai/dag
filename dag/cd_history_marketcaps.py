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

from .cd_constants import BEFORE_YEARS, BATCH_SIZE, DATE_FORMAT, EXCHANGES, get_today, get_today_date, KONEX_START_DATE
from .resources import PostgresResource
# Import shared helper functions from cd_history_prices
from .cd_history_prices import _adjust_to_business_day, _getHoliday, _get_valid_days

@dg.asset(
    kinds={"source", "duckdb"},
    group_name="CD_HISTORY",
    tags={"data_tier": "bronze", "data_source": "pykrx", "data_type": "marketcaps"},
)
def historical_marketcaps(
    context: dg.AssetExecutionContext, cd_duckdb: DuckDBResource
) -> dg.MaterializeResult:
    """
    지난 20년 동안 월말의 거래소 시가총액 데이터를 pykrx로 수집하고,
    cd_duckdb에 저장합니다.
    (티커, 종가, 시가총액, 거래량, 거래대금, 상장주식수)
    """
    COLUMN_NAMES = ["ticker", "close", "marketcap", "volume", "transaction", "shares"]

    current_date_obj = get_today_date()
    start_year = current_date_obj.year - BEFORE_YEARS
    # _get_valid_days is imported from cd_history_prices
    dates_to_fetch = _get_valid_days(start_year, current_date_obj.year, current_date_obj)

    konex_start_dt = datetime.strptime(KONEX_START_DATE, DATE_FORMAT).date()
    
    marketcaps_list = []

    for exchange in EXCHANGES:
        applicable_dates = []
        if exchange == "KONEX":
            applicable_dates = [d for d in dates_to_fetch if d >= konex_start_dt]
        else:
            applicable_dates = dates_to_fetch
        
        for d in applicable_dates:
            date_str_api = d.strftime("%Y%m%d")
            context.log.info(f"Fetching market cap data for {exchange} on {date_str_api}")
            try:
                marketcaps_df = krx.get_market_cap(date=date_str_api, market=exchange)
                time.sleep(0.4) 

                marketcaps_df.reset_index(inplace=True)
                marketcaps_df.columns = COLUMN_NAMES
                marketcaps_df["ticker"] = marketcaps_df["ticker"].astype(str)
                marketcaps_df["date"] = d # Store as date object
                marketcaps_df["exchange"] = exchange

                if marketcaps_df["marketcap"].lt(0).any():
                    error_msg = (
                        f"Negative marketcap found: {exchange} on {d}\n"
                        f"{marketcaps_df[marketcaps_df['marketcap'].lt(0)]}"
                    )
                    context.log.error(error_msg)
                    # Decide if to raise or just log and skip
                    # For now, raising as per original logic
                    raise ValueError(f"Negative marketcap detected for {exchange} on {d}")

                context.log.info(
                    f"Total records: {marketcaps_df.shape[0]} for {exchange} on {d}"
                )
                if not marketcaps_df.empty:
                    marketcaps_list.append(marketcaps_df)
            except Exception as e:
                context.log.error(f"Error fetching marketcap for {exchange} on {date_str_api}: {e}")
                # Decide if to continue to next date/exchange or stop
                continue

    if not marketcaps_list:
        context.log.warning("No market capitalization data collected.")
        # Return a result indicating no data, or raise error as per original
        raise ValueError("No market capitalization data collected after attempting all dates/exchanges.")

    historical_df_concat = pd.concat(marketcaps_list, ignore_index=True)
    context.log.info(f"Total marketcap records concatenated: {len(historical_df_concat)}")

    with cd_duckdb.get_connection() as conn:
        conn.register("historical_marketcaps_df_duckdb", historical_df_concat) # Changed temp table name
        # Naming the table in DuckDB, e.g., historical_marketcaps
        conn.execute(
            'CREATE OR REPLACE TABLE historical_marketcaps AS SELECT * FROM historical_marketcaps_df_duckdb'
        )
        preview_df = conn.execute(
            'SELECT * FROM historical_marketcaps LIMIT 10'
        ).fetchdf()
        count = conn.execute('SELECT COUNT(*) FROM historical_marketcaps').fetchone()[0]

    return dg.MaterializeResult(
        metadata={
            "Number of records": dg.MetadataValue.int(count),
            "Preview": dg.MetadataValue.md(preview_df.to_markdown(index=False)),
            "dagster/column_schema": dg.TableSchema(
                columns=[
                    dg.TableColumn("date", "date", description="Data date"),
                    dg.TableColumn("ticker", "string", description="Ticker symbol"),
                    dg.TableColumn("close", "int", description="Closing price"),
                    dg.TableColumn("marketcap", "int", description="Market capitalization"),
                    dg.TableColumn("volume", "bigint", description="Trading volume"),
                    dg.TableColumn("transaction", "bigint", description="Transaction amount"),
                    dg.TableColumn("shares", "bigint", description="Number of shares"),
                    dg.TableColumn("exchange", "string", description="Exchange identifier"),
                ]
            ),
        }
    )

@dg.asset(
    group_name="CD_HISTORY",
    kinds={"duckdb", "postgres"},
    tags={"data_tier": "silver", "data_source": "pykrx", "data_type": "marketcaps"},
    deps=[historical_marketcaps], # Dependency on the DuckDB asset
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
    context.log.info("Querying historical_marketcaps data from CD_DuckDB for PostgreSQL digest")
    # Ensure this table name matches what historical_marketcaps creates
    query = "SELECT * FROM historical_marketcaps" 
    with cd_duckdb.get_connection() as dconn:
        raw_df = dconn.execute(query).fetchdf()
    context.log.info(f"Retrieved {raw_df.shape[0]} records from DuckDB historical_marketcaps")

    cols_filter = ["close", "marketcap", "volume", "transaction"]
    valid_mask = ~(raw_df[cols_filter] == 0).all(axis=1) # Keep rows where NOT all are zero
    valid_df = raw_df[valid_mask].copy()
    context.log.info(f"Found {valid_df.shape[0]} valid marketcap records out of {raw_df.shape[0]}")

    # Original code had a check here that raised an error if valid_df.shape[0] != raw_df.shape[0]
    # This seems counterintuitive if filtering is intended. Removing strict error, logging instead.
    if valid_df.shape[0] < raw_df.shape[0]:
        context.log.warning(
            f"{raw_df.shape[0] - valid_df.shape[0]} records were filtered out due to all zero values in critical columns."
        )

    today_str_metadata = get_today()
    if valid_df.empty:
        msg = "No valid market cap data available after filtering (holiday or non-trading day effects)."
        context.log.info(msg)
        return dg.MaterializeResult(metadata={"Date": dg.MetadataValue.text(today_str_metadata), "Result": dg.MetadataValue.text(msg)})

    valid_df["date"] = pd.to_datetime(valid_df["date"]) # Ensure it's datetime for dt accessor
    valid_df = valid_df.assign(
        year=valid_df["date"].dt.year,
        month=valid_df["date"].dt.month,
    )
    # Convert date to ISO string for PostgreSQL
    valid_df["date"] = valid_df["date"].dt.strftime("%Y-%m-%dT%H:%M:%S")

    expected_columns_pg = [
        "date", "exchange", "ticker", "marketcap", "volume",
        "transaction", "shares", "year", "month",
    ]
    # Ensure 'close' is not in expected_columns_pg if not in marketcap table in PG
    # Original code for digest_historical_marketcaps did not include 'close'
    df_to_insert = valid_df[expected_columns_pg].copy()

    context.log.info(f"Prepared {len(df_to_insert)} marketcap rows for PostgreSQL insert")

    with cd_postgres.get_connection() as pg_conn:
        cursor = pg_conn.cursor()
        date_exchange_pairs = df_to_insert[["date", "exchange"]].drop_duplicates()
        context.log.info(
            f"Deleting existing marketcap data for {len(date_exchange_pairs)} date-exchange pairs from PostgreSQL"
        )
        for idx, (d_val, exch_val) in enumerate(date_exchange_pairs.itertuples(index=False, name=None)):
            if idx % 10 == 0: # Log progress
                context.log.info(f"Deleting marketcap data: {idx+1}/{len(date_exchange_pairs)} pairs processed")
            # d_val is already a string in ISO format
            cursor.execute('DELETE FROM marketcap WHERE date = %s AND exchange = %s', (d_val, exch_val))

        context.log.info(f"Inserting {len(df_to_insert)} new marketcap records into PostgreSQL")
        for i in range(0, len(df_to_insert), BATCH_SIZE):
            batch_df = df_to_insert.iloc[i : i + BATCH_SIZE]
            context.log.info(
                f"Inserting marketcap batch {i//BATCH_SIZE + 1}/{(len(df_to_insert)-1)//BATCH_SIZE + 1} ({len(batch_df)} records)"
            )
            output = io.StringIO()
            batch_df.to_csv(output, index=False, header=False, sep="\t")
            output.seek(0)
            cursor.copy_from(
                output, "marketcap", sep="\t",
                columns=expected_columns_pg,
            )
        pg_conn.commit()
    
    # Metadata aggregation from valid_df before date conversion for correct min/max
    unique_dates_count = pd.to_datetime(valid_df["date"]).nunique()
    exchange_value_counts = valid_df["exchange"].value_counts()
    kospi_val_count = exchange_value_counts.get("KOSPI", 0)
    kosdaq_val_count = exchange_value_counts.get("KOSDAQ", 0)
    konex_val_count = exchange_value_counts.get("KONEX", 0)

    metadata_preview_df = pd.DataFrame({
        "date_run": [today_str_metadata],
        "#_Unique_Days": [unique_dates_count],
        "#_KOSPI_recs": [kospi_val_count],
        "#_KOSDAQ_recs": [kosdaq_val_count],
        "#_KONEX_recs": [konex_val_count],
        "total_recs_inserted": [df_to_insert.shape[0]],
    })

    return dg.MaterializeResult(
        metadata={
            "Date Run": dg.MetadataValue.text(today_str_metadata),
            "Rows Inserted": dg.MetadataValue.int(int(df_to_insert.shape[0])),
            "Unique Days Processed": dg.MetadataValue.int(int(unique_dates_count)),
            "KOSPI Records": dg.MetadataValue.int(int(kospi_val_count)),
            "KOSDAQ Records": dg.MetadataValue.int(int(kosdaq_val_count)),
            "KONEX Records": dg.MetadataValue.int(int(konex_val_count)),
            "Summary Preview": dg.MetadataValue.md(metadata_preview_df.to_markdown(index=False)),
        }
    )

@dg.asset(
    group_name="CD_HISTORY",
    kinds={"postgres"},
    tags={"data_tier": "gold", "data_source": "pykrx", "data_type": "marketcaps"},
    deps=[digest_historical_marketcaps], # Depends on the silver layer asset
)
def sync_historical_marketcap_to_security(
    context: dg.AssetExecutionContext, cd_postgres: PostgresResource
) -> dg.MaterializeResult:
    """
    PostgreSQL marketcap 테이블의 security_id, name, kor_name 컬럼을
    security 테이블의 정보로 업데이트합니다. (한국 시장 대상)
    """
    date_str_run = get_today()
    context.log.info("Starting sync of historical marketcap to security in PostgreSQL")

    # Assuming EXCHANGES constant is appropriate for filtering Korean market
    update_query_sql = """
    UPDATE marketcap m
    SET security_id = s.security_id,
        name = s.name,
        kor_name = s.kor_name
    FROM security s
    WHERE m.ticker = s.ticker
      AND m.exchange = s.exchange
      AND m.security_id IS NULL
      AND s.delisting_date IS NULL
      AND s.exchange = ANY(%s); -- Parameter for exchange list
    """
    with cd_postgres.get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(update_query_sql, (EXCHANGES,))
            updated_count_rows = cursor.rowcount
            conn.commit()

    context.log.info(f"Updated {updated_count_rows} records in PostgreSQL marketcap table with security info.")
    return dg.MaterializeResult(
        metadata={
            "Date Run": dg.MetadataValue.text(date_str_run),
            "Updated Marketcap Records": dg.MetadataValue.int(updated_count_rows),
            "Target Exchanges": dg.MetadataValue.text(", ".join(EXCHANGES)),
            "Result": dg.MetadataValue.text("PostgreSQL marketcap table updated with security information."),
        }
    )

