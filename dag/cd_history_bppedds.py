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
    tags={"data_tier": "bronze", "data_source": "pykrx", "data_type": "bppedds"},
)
def historical_bppedds(
    context: dg.AssetExecutionContext, cd_duckdb: DuckDBResource
) -> dg.MaterializeResult:
    """
    20년간 월말 BPPEDD 데이터를 pykrx를 통해 수집하여 cd_duckdb에 저장합니다.
    (티커, BPS, PER, PBR, EPS, DIV, DPS)
    KONEX 데이터는 제외합니다.
    """
    COLUMN_NAMES = ["ticker", "bps", "per", "pbr", "eps", "div", "dps"]
    current_date_obj = get_today_date()
    start_year = current_date_obj.year - BEFORE_YEARS
    # _get_valid_days is imported from cd_history_prices
    dates_to_fetch = _get_valid_days(start_year, current_date_obj.year, current_date_obj)

    bppedds_list = []
    # KONEX 제외한 거래소 목록
    exchanges_to_process = list(set(EXCHANGES) - {"KONEX"})

    for exchange in exchanges_to_process:
        for d in dates_to_fetch: # KONEX 제외이므로 모든 applicable_dates는 dates_to_fetch와 동일
            date_str_api = d.strftime("%Y%m%d")
            context.log.info(f"Fetching BPPEDD data for {exchange} on {date_str_api}")
            try:
                # pykrx.get_market_fundamental_by_ticker is used for BPPEDD
                bppedd_df = krx.get_market_fundamental_by_ticker(date=date_str_api, market=exchange)
                time.sleep(0.4) 

                bppedd_df.reset_index(inplace=True)
                bppedd_df.columns = COLUMN_NAMES
                bppedd_df["ticker"] = bppedd_df["ticker"].astype(str)
                bppedd_df["date"] = d # Store as date object
                bppedd_df["exchange"] = exchange

                value_columns_filter = ["bps", "pbr", "eps", "div", "dps"]
                is_all_zero_mask = (bppedd_df[value_columns_filter] == 0).all(axis=1)
                valid_bppedd_df = bppedd_df[~is_all_zero_mask].copy()

                if len(valid_bppedd_df) < len(bppedd_df):
                    context.log.info(
                        f"Filtered out {len(bppedd_df) - len(valid_bppedd_df)} BPPEDD records with all zeros for {exchange} on {date_str_api}"
                    )
                
                # Ensure correct column order as per original logic for the final df
                final_columns_order = ["date", "ticker", "bps", "per", "pbr", "eps", "div", "dps", "exchange"]
                valid_bppedd_df = valid_bppedd_df[final_columns_order]

                context.log.info(
                    f"Total valid BPPEDD records: {valid_bppedd_df.shape[0]} for {exchange} on {date_str_api}"
                )
                if not valid_bppedd_df.empty:
                    bppedds_list.append(valid_bppedd_df)
            except Exception as e:
                context.log.error(f"Error fetching BPPEDD for {exchange} on {date_str_api}: {e}")
                continue

    if not bppedds_list:
        context.log.warning("No BPPEDD data collected.")
        raise ValueError("수집된 BPPEDD 데이터가 없습니다.") # As per original

    historical_df_concat = pd.concat(bppedds_list, ignore_index=True)
    context.log.info(f"Total BPPEDD records concatenated: {len(historical_df_concat)}")

    with cd_duckdb.get_connection() as conn:
        conn.register("historical_bppedds_df_duckdb", historical_df_concat) # Changed temp table name
        # Naming the table in DuckDB, e.g., historical_bppedds
        conn.execute(
            'CREATE OR REPLACE TABLE historical_bppedds AS SELECT * FROM historical_bppedds_df_duckdb'
        )
        preview_df = conn.execute(
            'SELECT * FROM historical_bppedds LIMIT 10'
        ).fetchdf()
        count = conn.execute('SELECT COUNT(*) FROM historical_bppedds').fetchone()[0]

    return dg.MaterializeResult(
        metadata={
            "Number of records": dg.MetadataValue.int(count),
            "Preview": dg.MetadataValue.md(preview_df.to_markdown(index=False)),
            "dagster/column_schema": dg.TableSchema(
                columns=[
                    dg.TableColumn("date", "date", description="Data date"),
                    dg.TableColumn("ticker", "string", description="Ticker symbol"),
                    dg.TableColumn("bps", "float", description="Book value per share"),
                    dg.TableColumn("per", "float", description="Price-to-earnings ratio"),
                    dg.TableColumn("pbr", "float", description="Price-to-book ratio"),
                    dg.TableColumn("eps", "float", description="Earnings per share"),
                    dg.TableColumn("div", "float", description="Dividend yield"),
                    dg.TableColumn("dps", "float", description="Dividend per share"),
                    dg.TableColumn("exchange", "string", description="Exchange identifier"),
                ]
            ),
        }
    )

@dg.asset(
    group_name="CD_HISTORY",
    kinds={"duckdb", "postgres"},
    tags={"data_tier": "silver", "data_source": "pykrx", "data_type": "bppedds"},
    deps=[historical_bppedds], # Dependency on the DuckDB asset
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
    """
    context.log.info("Querying historical_bppedds data from CD_DuckDB for PostgreSQL digest")
    # Ensure this table name matches what historical_bppedds creates
    query = "SELECT * FROM historical_bppedds"
    with cd_duckdb.get_connection() as dconn:
        raw_df = dconn.execute(query).fetchdf()
    context.log.info(f"Retrieved total {raw_df.shape[0]} BPPEDD records from DuckDB")

    valid_mask_cols = ["bps", "pbr", "eps", "div", "dps"]
    valid_mask = ~(raw_df[valid_mask_cols] == 0).all(axis=1) # Keep rows where NOT all are zero
    valid_df = raw_df[valid_mask].copy()
    context.log.info(f"Found {valid_df.shape[0]} valid BPPEDD records out of {raw_df.shape[0]}")

    # Original code had a check here that raised an error if valid_df.shape[0] != raw_df.shape[0]
    # Logging warning instead as filtering is intended.
    if valid_df.shape[0] < raw_df.shape[0]:
        context.log.warning(
            f"{raw_df.shape[0] - valid_df.shape[0]} BPPEDD records were filtered out due to all zero values in critical columns."
        )

    today_str_metadata = get_today()
    if valid_df.empty:
        msg = "No valid BPPEDD data available after filtering."
        context.log.info(msg)
        return dg.MaterializeResult(metadata={"Date": dg.MetadataValue.text(today_str_metadata), "Result": dg.MetadataValue.text(msg)})

    valid_df["date"] = pd.to_datetime(valid_df["date"]) # Ensure datetime for dt accessor
    valid_df = valid_df.assign(
        year=valid_df["date"].dt.year,
        month=valid_df["date"].dt.month,
    )
    # Convert date to ISO string for PostgreSQL
    valid_df["date"] = valid_df["date"].dt.strftime("%Y-%m-%dT%H:%M:%S")

    expected_columns_pg = [
        "date", "exchange", "ticker", "bps", "per", "pbr",
        "eps", "div", "dps", "year", "month",
    ]
    df_to_insert = valid_df[expected_columns_pg].copy()
    context.log.info(f"Prepared {df_to_insert.shape[0]} BPPEDD rows for PostgreSQL insert")

    with cd_postgres.get_connection() as pg_conn:
        cursor = pg_conn.cursor()
        unique_date_exchange_pg = df_to_insert[["date", "exchange"]].drop_duplicates()
        context.log.info(
            f"Deleting existing BPPEDD data for {len(unique_date_exchange_pg)} date-exchange pairs from PostgreSQL"
        )
        
        # Optimized delete for many pairs
        if not unique_date_exchange_pg.empty:
            del_tuples = list(unique_date_exchange_pg.itertuples(index=False, name=None))
            # Ensure date is string for query
            # params = [(str(date_val), str(exch_val)) for date_val, exch_val in del_tuples]
            # values_placeholder = ", ".join(["(%s, %s)"] * len(params))
            # delete_query = f'DELETE FROM bppedd WHERE (date, exchange) IN ({values_placeholder})'
            # cursor.execute(delete_query, [item for tup in params for item in tup])
            # Simpler loop for delete as per original structure, assuming not too many unique pairs for performance hit
            for d_val, exch_val in unique_date_exchange_pg.itertuples(index=False, name=None):
                 cursor.execute('DELETE FROM bppedd WHERE date = %s AND exchange = %s', (d_val, exch_val))

        context.log.info(f"Inserting {len(df_to_insert)} new BPPEDD records into PostgreSQL")
        for i in range(0, len(df_to_insert), BATCH_SIZE):
            batch_df = df_to_insert.iloc[i : i + BATCH_SIZE]
            context.log.info(
                f"Inserting BPPEDD batch {i//BATCH_SIZE + 1}/{(len(df_to_insert)-1)//BATCH_SIZE + 1} ({len(batch_df)} records)"
            )
            output = io.StringIO()
            batch_df.to_csv(output, index=False, header=False, sep="\t")
            output.seek(0)
            cursor.copy_from(
                output, "bppedd", sep="\t",
                columns=expected_columns_pg,
            )
        pg_conn.commit()

    unique_dates_count = pd.to_datetime(valid_df["date"]).nunique() # Use valid_df before date string conversion for nunique
    exchange_value_counts = valid_df["exchange"].value_counts()
    kospi_val_count = exchange_value_counts.get("KOSPI", 0)
    kosdaq_val_count = exchange_value_counts.get("KOSDAQ", 0)
    # KONEX is excluded, so this should be 0
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
    tags={"data_tier": "gold", "data_source": "pykrx", "data_type": "bppedds"},
    deps=[digest_historical_bppedds], # Depends on the silver layer asset
)
def sync_historical_bppedd_to_security(
    context: dg.AssetExecutionContext, cd_postgres: PostgresResource
) -> dg.MaterializeResult:
    """
    PostgreSQL bppedd 테이블의 security_id, name, kor_name 컬럼을
    security 테이블의 정보로 업데이트합니다. (한국 시장 대상 - KONEX 제외)
    """
    date_str_run = get_today()
    # KONEX 제외한 거래소 리스트
    exchange_list_filter = list(set(EXCHANGES) - {"KONEX"})

    context.log.info("Starting sync of historical BPPEDD to security in PostgreSQL")
    update_query_sql = """
        UPDATE bppedd b
        SET security_id = s.security_id, name = s.name, kor_name = s.kor_name
        FROM security s
        WHERE b.ticker = s.ticker
          AND b.exchange = s.exchange
          AND b.security_id IS NULL
          AND s.delisting_date IS NULL
          AND b.exchange = ANY(%s) -- Ensure b.exchange is in the list
        RETURNING b.ticker, b.exchange, b.security_id, b.name, b.kor_name;
    """
    with cd_postgres.get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(update_query_sql, (exchange_list_filter,))
            updated_rows_fetchall = cursor.fetchall()
            conn.commit()
            updated_count_rows = len(updated_rows_fetchall) # Count after fetchall

    context.log.info(f"Updated {updated_count_rows} BPPEDD records in PostgreSQL with security info.")

    if updated_rows_fetchall:
        result_preview_df = pd.DataFrame(
            updated_rows_fetchall,
            columns=["ticker", "exchange", "security_id", "name", "kor_name"],
        )
        metadata = {
            "Date Run": dg.MetadataValue.text(date_str_run),
            "Target Exchanges": dg.MetadataValue.text(", ".join(exchange_list_filter)),
            "Updated BPPEDD Records": dg.MetadataValue.int(updated_count_rows),
            "Preview Head": dg.MetadataValue.md(result_preview_df.head().to_markdown(index=False)),
            "Preview Tail": dg.MetadataValue.md(result_preview_df.tail().to_markdown(index=False)),
        }
    else:
        metadata = {
            "Date Run": dg.MetadataValue.text(date_str_run),
            "Target Exchanges": dg.MetadataValue.text(", ".join(exchange_list_filter)),
            "Updated BPPEDD Records": dg.MetadataValue.int(0),
            "Result": dg.MetadataValue.text("No BPPEDD records needed updating or all were already linked."),
        }
    return dg.MaterializeResult(metadata=metadata)

