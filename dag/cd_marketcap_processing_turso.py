import time
import traceback  # Added for error logging
from datetime import datetime

import dagster as dg
import pandas as pd
from pykrx import stock as krx
import libsql_experimental as libsql  # Added for Turso

from .cd_constants import DATE_FORMAT
from .partitions import daily_exchange_category_partition
from .resources import TursoResource  # Changed from PostgresResource


# Helper function to convert cursor results to a list of dictionaries
def _cursor_to_list_of_dicts(cursor):
    columns = [col[0] for col in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


# Helper function to safely quote SQL values
def sql_quote(value):
    if value is None:
        return "NULL"
    elif isinstance(value, (int, float, bool)):
        return str(value)
    else:
        return f"'{str(value).replace("'", "''")}'"


@dg.asset(
    partitions_def=daily_exchange_category_partition,
    group_name="CD_TURSO",  # Changed group_name
    kinds={"turso"},  # Changed kinds
    tags={"data_tier": "bronze", "domain": "finance", "source": "pykrx"},
    deps=["cd_upsert_company_by_security_turso"],  # Updated dependency
)
def cd_marketcaps_turso(  # Renamed asset
    context: dg.AssetExecutionContext, cd_turso: TursoResource  # Changed resource
) -> dg.MaterializeResult:
    """
    일일 거래소 시총 정보를 가져옵니다. (TursoDB 버전)
    (티커, 시가총액, 거래대금, 상장주식수, 외국인보유수)
    """
    keys = context.partition_key.keys_by_dimension
    date_str_yyyymmdd = keys["date"].replace("-", "")  # YYYYMMDD for pykrx
    date_str_iso = keys["date"]  # YYYY-MM-DD for SQL and DataFrame
    exchange = keys["exchange"]

    df = krx.get_market_cap(date=date_str_yyyymmdd, market=exchange)
    time.sleep(0.2)
    df.reset_index(inplace=True)

    COLUMN_NAMES = ["ticker", "close", "marketcap", "volume", "transaction", "shares"]
    df.columns = COLUMN_NAMES
    df["ticker"] = df["ticker"].astype(str)
    df["date"] = date_str_iso  # Use YYYY-MM-DD
    df["exchange"] = exchange

    df = df[
        [
            "date",
            "ticker",
            "close",
            "marketcap",
            "volume",
            "transaction",
            "shares",
            "exchange",
        ]
    ]
    # Convert NaNs to None for SQLite compatibility
    df = df.astype(object).where(pd.notnull(df), None)

    column_schema = dg.TableSchema(
        columns=[
            dg.TableColumn("date", "string", description="Data date (YYYY-MM-DD)"),  # Date as string for SQLite
            dg.TableColumn("ticker", "string", description="Ticker symbol"),
            dg.TableColumn("close", "float", description="Closing price"),
            dg.TableColumn("marketcap", "integer", description="Market capitalization"),  # SQLite uses INTEGER for bigint
            dg.TableColumn("volume", "integer", description="Trading volume"),
            dg.TableColumn("transaction", "integer", description="Transaction amount"),
            dg.TableColumn("shares", "integer", description="Number of shares"),
            dg.TableColumn("exchange", "string", description="Exchange identifier"),
        ]
    )

    with cd_turso.get_connection() as conn:
        try:
            delete_query = f"DELETE FROM tmp_marketcaps WHERE date = {sql_quote(date_str_iso)} AND exchange = {sql_quote(exchange)};"
            conn.execute(delete_query)
            context.log.info(f"Executed delete from tmp_marketcaps: {delete_query}")

            if not df.empty:
                insert_statements = []
                for row in df.itertuples(index=False):
                    values = f"({sql_quote(row.date)}, {sql_quote(row.ticker)}, {sql_quote(row.close)}, {sql_quote(row.marketcap)}, {sql_quote(row.volume)}, {sql_quote(row.transaction)}, {sql_quote(row.shares)}, {sql_quote(row.exchange)})"
                    insert_statements.append(f"INSERT INTO tmp_marketcaps (date, ticker, close, marketcap, volume, transaction, shares, exchange) VALUES {values};")

                if insert_statements:
                    batch_sql = "BEGIN;\\n" + "\\n".join(insert_statements) + "\\nCOMMIT;"
                    conn.executescript(batch_sql)
                    context.log.info(f"Successfully inserted {len(df)} records into tmp_marketcaps for {date_str_iso} {exchange}. Batch SQL executed.")
        except libsql.LibsqlError as e:
            context.log.error(f"TursoDB Error in cd_marketcaps_turso: {e}\\nTraceback: {traceback.format_exc()}")
            raise
        except Exception as e:
            context.log.error(f"General Error in cd_marketcaps_turso: {e}\\nTraceback: {traceback.format_exc()}")
            raise

    record_count = len(df)
    preview_df = df.head(10)
    return dg.MaterializeResult(
        metadata={
            "Number of records": dg.MetadataValue.int(record_count),
            "Preview": dg.MetadataValue.md(preview_df.to_markdown(index=False)),
            "dagster/column_schema": column_schema,
        }
    )


@dg.asset(
    partitions_def=daily_exchange_category_partition,
    group_name="CD_TURSO",  # Changed group_name
    kinds={"turso"},  # Changed kinds
    tags={"data_tier": "silver", "domain": "finance", "source": "pykrx"},
    deps=[cd_marketcaps_turso],  # Updated dependency
)
def cd_digest_marketcaps_turso(  # Renamed asset
    context: dg.AssetExecutionContext, cd_turso: TursoResource  # Changed resource
) -> dg.MaterializeResult:
    """
    매일 업데이트 된 시총 데이터를 TursoDB의 "Marketcap" 테이블에 업데이트합니다. (TursoDB 버전)
    처리 완료 후 tmp_marketcaps의 해당 파티션 데이터를 삭제하여 용량을 절약합니다.
    """
    keys = context.partition_key.keys_by_dimension
    date_iso = keys["date"]  # YYYY-MM-DD
    exchange = keys["exchange"]

    date_dt = datetime.strptime(date_iso, "%Y-%m-%d")

    marketcaps_df = pd.DataFrame()  # Initialize
    df_to_insert = pd.DataFrame()  # Initialize to ensure it's defined for metadata

    with cd_turso.get_connection() as conn:
        try:
            select_query = f"SELECT date, ticker, close, marketcap, volume, transaction, shares, exchange FROM tmp_marketcaps WHERE date = {sql_quote(date_iso)} AND exchange = {sql_quote(exchange)}"
            cursor = conn.execute(select_query)
            context.log.info(f"Executed select from tmp_marketcaps: {select_query}")
            rows = _cursor_to_list_of_dicts(cursor)
            marketcaps_df = pd.DataFrame(rows)

            if marketcaps_df.empty:
                return dg.MaterializeResult(
                    metadata={
                        "Date": dg.MetadataValue.text(date_iso),
                        "Result": dg.MetadataValue.text(
                            "marketcaps_df is empty. No data from tmp_marketcaps for this partition."
                        ),
                        "row_count": dg.MetadataValue.int(0),
                        "preview": dg.MetadataValue.md("No data to preview."),
                    }
                )

            cols_to_check = ["close", "marketcap", "volume", "transaction"]
            existing_cols_to_check = [col for col in cols_to_check if col in marketcaps_df.columns]
            if not existing_cols_to_check:
                context.log.warning(f"None of the columns for validity check ({cols_to_check}) exist in marketcaps_df. Skipping validity check.")
                valid_df = marketcaps_df.copy()
            else:
                for col in existing_cols_to_check:
                    marketcaps_df[col] = pd.to_numeric(marketcaps_df[col], errors='coerce').fillna(0)
                valid_mask = (marketcaps_df[existing_cols_to_check] == 0).all(axis=1)
                valid_df = marketcaps_df[~valid_mask]

            if valid_df.empty:
                return dg.MaterializeResult(
                    metadata={
                        "Date": dg.MetadataValue.text(date_iso),
                        "Result": dg.MetadataValue.text(
                            "휴장일 등의 이유로 시총 데이터가 없습니다 (all rows invalid). tmp_marketcaps not cleaned by this asset."
                        ),
                        "row_count": dg.MetadataValue.int(0),
                        "preview": dg.MetadataValue.md("No valid data to preview."),
                    }
                )

            if valid_df.shape[0] != marketcaps_df.shape[0]:
                error_msg = (
                    f"휴장일이 아님에도 시총 데이터의 일부가 유효하지 않습니다. "
                    f"유효한 행: {valid_df.shape[0]}, 전체 행: {marketcaps_df.shape[0]}, "
                    f"exchange: {exchange}, date: {date_iso}. "
                    f"tmp_marketcaps not cleaned by this asset. Upstream should manage its tmp table."
                )
                context.log.error(error_msg)
                raise ValueError(error_msg)

            if "ticker" in valid_df.columns:
                valid_df["ticker"] = valid_df["ticker"].astype(str).str.zfill(6)

            valid_df = valid_df.assign(
                date=date_iso,
                exchange=exchange,
                year=date_dt.year,
                month=date_dt.month,
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
            # Ensure all expected columns are present, add if missing and fill with None/NULL compatible value
            for col in expected_columns:
                if col not in valid_df.columns:
                    valid_df[col] = None

            df_to_insert = valid_df[expected_columns].copy()
            df_to_insert = df_to_insert.astype(object).where(pd.notnull(df_to_insert), None)
            df_to_insert.sort_values(by="ticker", inplace=True)

            statements = []
            statements.append(f"DELETE FROM marketcap WHERE date = {sql_quote(date_iso)} AND exchange = {sql_quote(exchange)};")

            for row_tuple in df_to_insert.itertuples(index=False):
                values = f"({sql_quote(row_tuple.date)}, {sql_quote(row_tuple.exchange)}, {sql_quote(row_tuple.ticker)}, {sql_quote(row_tuple.marketcap)}, {sql_quote(row_tuple.volume)}, {sql_quote(row_tuple.transaction)}, {sql_quote(row_tuple.shares)}, {sql_quote(row_tuple.year)}, {sql_quote(row_tuple.month)})"
                statements.append(f"INSERT INTO marketcap (date, exchange, ticker, marketcap, volume, transaction, shares, year, month) VALUES {values};")

            if statements:
                batch_sql = "BEGIN;\\n" + "\\n".join(statements) + "\\nCOMMIT;"
                conn.executescript(batch_sql)
                context.log.info(f"Successfully processed marketcap for {date_iso} {exchange}. Batch SQL executed.")

        except libsql.LibsqlError as e:
            context.log.error(f"TursoDB Error in cd_digest_marketcaps_turso: {e}\\nTraceback: {traceback.format_exc()}")
            raise
        except Exception as e:
            context.log.error(f"General Error in cd_digest_marketcaps_turso: {e}\\nTraceback: {traceback.format_exc()}")
            raise

    preview_df = df_to_insert.head(10) if not df_to_insert.empty else pd.DataFrame()
    metadata = {
        "row_count": dg.MetadataValue.int(df_to_insert.shape[0]),
        "preview": dg.MetadataValue.md(preview_df.to_markdown(index=False)),
        "date": dg.MetadataValue.text(date_iso),
        "exchange": dg.MetadataValue.text(exchange),
    }
    return dg.MaterializeResult(metadata=metadata)


@dg.asset(
    partitions_def=daily_exchange_category_partition,
    group_name="CD_TURSO", 
    kinds={"turso"}, 
    tags={"data_tier": "silver", "domain": "finance", "source": "internal"}, 
    deps=[cd_digest_marketcaps_turso], 
)
def cd_sync_marketcaps_to_security_turso( 
    context: dg.AssetExecutionContext,
    cd_turso: TursoResource, 
):
    """
    marketcap 테이블의 security_id, name, kor_name 컬럼을
    해당하는 security 테이블의 정보로 업데이트합니다. (TursoDB 버전)
    """
    partition_keys = context.partition_key.keys_by_dimension
    date_iso = partition_keys["date"] 
    exchange_str = partition_keys["exchange"]

    columns_odf = ["date", "ticker", "exchange", "security_id", "name", "count"]
    metadata_rows_for_odf = []

    with cd_turso.get_connection() as conn:
        try:
            marketcap_groups_query = f'''\
                SELECT ticker, exchange, COUNT(*) AS cnt
                FROM marketcap
                WHERE security_id IS NULL 
                  AND exchange = {sql_quote(exchange_str)} 
                  AND date = {sql_quote(date_iso)}
                GROUP BY ticker, exchange
                ORDER BY ticker'''
            cursor = conn.execute(marketcap_groups_query)
            context.log.info(f"Executed marketcap_groups_query: {marketcap_groups_query}")
            marketcap_groups_tuples = cursor.fetchall()

            if not marketcap_groups_tuples:
                return dg.MaterializeResult(metadata={
                    "Date": dg.MetadataValue.text(date_iso),
                    "Exchange": dg.MetadataValue.text(exchange_str),
                    "Result": dg.MetadataValue.text("No marketcap records to sync with security."),
                })

            securities_query = f'''\
                SELECT ticker, exchange, security_id, name, kor_name
                FROM security
                WHERE exchange = {sql_quote(exchange_str)} AND delisting_date IS NULL
                ORDER BY ticker'''
            cursor = conn.execute(securities_query)
            context.log.info(f"Executed securities_query: {securities_query}")
            securities_map = {
                (r[0], r[1]): {"security_id": r[2], "name": r[3], "kor_name": r[4]}
                for r in cursor.fetchall()
            }

            update_statements = []
            for ticker, ex_val, cnt in marketcap_groups_tuples:
                key = (ticker, ex_val)
                if key in securities_map:
                    sec = securities_map[key]
                    update_stmt = f"UPDATE marketcap SET security_id = {sql_quote(sec["security_id"])}, name = {sql_quote(sec["name"])}, kor_name = {sql_quote(sec["kor_name"])} WHERE ticker = {sql_quote(ticker)} AND exchange = {sql_quote(ex_val)} AND date = {sql_quote(date_iso)};"
                    update_statements.append(update_stmt)
                    metadata_rows_for_odf.append(
                        [date_iso, ticker, ex_val, sec["security_id"], sec["name"], cnt]
                    )
            
            if update_statements:
                batch_sql = "BEGIN;\\n" + "\\n".join(update_statements) + "\\nCOMMIT;"
                conn.executescript(batch_sql)
                context.log.info(f"Successfully synced {len(update_statements)} marketcap records to security for {date_iso} {exchange_str}. Batch SQL executed.")

        except libsql.LibsqlError as e:
            context.log.error(f"TursoDB Error in cd_sync_marketcaps_to_security_turso: {e}\\nTraceback: {traceback.format_exc()}")
            raise
        except Exception as e:
            context.log.error(f"General Error in cd_sync_marketcaps_to_security_turso: {e}\\nTraceback: {traceback.format_exc()}")
            raise

    if metadata_rows_for_odf:
        odf = pd.DataFrame(metadata_rows_for_odf, columns=columns_odf)
        total_count = odf["count"].sum()
        metadata = {
            "Date": dg.MetadataValue.text(date_iso),
            "Exchange": dg.MetadataValue.text(exchange_str),
            "# of synced records": dg.MetadataValue.text(str(total_count)),
            "Preview head": dg.MetadataValue.md(odf.head().to_markdown(index=False)),
            "Preview tail": dg.MetadataValue.md(odf.tail().to_markdown(index=False)),
        }
    else:
        metadata = {
            "Date": dg.MetadataValue.text(date_iso),
            "Exchange": dg.MetadataValue.text(exchange_str),
            "Result": dg.MetadataValue.text(
                "No records were synced or found for creating the output dataframe."
            ),
        }

    return dg.MaterializeResult(metadata=metadata)
