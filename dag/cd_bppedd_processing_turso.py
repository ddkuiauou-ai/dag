import time
import traceback
from datetime import datetime

import dagster as dg
import pandas as pd
from pykrx import stock as krx
import libsql_experimental as libsql # Ensure this import is present for libsql.LibsqlError

from .cd_constants import DATE_FORMAT
from .partitions import daily_exchange_category_partition
from .resources import TursoResource # Changed from PostgresResource


# Helper function to convert cursor results to a list of dictionaries
def _cursor_to_list_of_dicts(cursor):
    columns = [col[0] for col in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]

# Helper function to safely quote SQL values
def sql_quote(value):
    if value is None:
        return "NULL"
    elif isinstance(value, (int, float, bool)): # Added bool
        return str(value)
    else:
        # Correctly escape single quotes for SQL
        return f"'{str(value).replace("'", "''")}'"


@dg.asset(
    description="CD 프로젝트 BPPEDD 데이터 처리 - Silver Tier (TursoDB)", # Updated description
    partitions_def=daily_exchange_category_partition,
    group_name="CD_TURSO", # Changed group_name
    kinds={"turso"}, # Changed kinds
    tags={
        "domain": "finance",
        "data_tier": "silver",
        "source": "pykrx"
    },
    deps=["cd_upsert_company_by_security_turso"], # Updated dependency
)
def cd_bppedds_turso( # Renamed asset
    context: dg.AssetExecutionContext, cd_turso: TursoResource # Changed resource
) -> dg.MaterializeResult:
    """
    일일 bppedd 데이터를 가져옵니다. (티커, BPS, PER, PBR, EPS, DIV, DPS) - TursoDB 버전
    """
    keys = context.partition_key.keys_by_dimension
    date_str_yyyymmdd = keys["date"].replace("-", "") # YYYYMMDD for pykrx
    date_str_iso = keys["date"] # YYYY-MM-DD for SQL
    exchange = keys["exchange"]

    final_columns = [
        "date", "ticker", "bps", "per", "pbr", "eps", "div", "dps", "exchange",
    ]
    column_schema = dg.TableSchema(
        columns=[
            dg.TableColumn("date", "string", description="Data date (YYYY-MM-DD)"), # Changed to string for SQLite
            dg.TableColumn("ticker", "string", description="Ticker symbol"),
            dg.TableColumn("bps", "float", description="BPS value"),
            dg.TableColumn("per", "float", description="PER value"),
            dg.TableColumn("pbr", "float", description="PBR value"),
            dg.TableColumn("eps", "float", description="EPS value"),
            dg.TableColumn("div", "float", description="DIV value"),
            dg.TableColumn("dps", "float", description="DPS value"),
            dg.TableColumn("exchange", "string", description="Exchange identifier"),
        ]
    )

    if exchange == "KONEX":
        df = pd.DataFrame(columns=final_columns)
    else:
        df = krx.get_market_fundamental_by_ticker(date=date_str_yyyymmdd, market=exchange)
        time.sleep(0.2)
        df.reset_index(inplace=True)
        context.log.info(f"Fetching market fundamental data for {exchange} on {date_str_yyyymmdd}")
        df.columns = ["ticker", "bps", "per", "pbr", "eps", "div", "dps"]
        df["date"] = date_str_iso # Use YYYY-MM-DD for DataFrame and DB
        df["exchange"] = exchange
        df["ticker"] = df["ticker"].astype(str)
        df = df[final_columns]

    if df.empty:
        metadata = {
            "Number of records": dg.MetadataValue.int(0),
            "Preview": dg.MetadataValue.md("Empty DataFrame"), # Simplified preview
            "dagster/column_schema": column_schema,
        }
        return dg.MaterializeResult(metadata=metadata)

    # Convert DataFrame dtypes for SQLite compatibility (especially NaN to None/NULL)
    df = df.astype(object).where(pd.notnull(df), None)


    with cd_turso.get_connection() as conn:
        try:
            # Delete existing partition data
            delete_query = f"DELETE FROM tmp_bppedds WHERE date = {sql_quote(date_str_iso)} AND exchange = {sql_quote(exchange)};"
            conn.execute(delete_query)
            context.log.info(f"Executed delete: {delete_query}")

            # Batch INSERT
            if not df.empty:
                insert_statements = []
                for row in df.itertuples(index=False):
                    values = f"({sql_quote(row.date)}, {sql_quote(row.ticker)}, {sql_quote(row.bps)}, {sql_quote(row.per)}, {sql_quote(row.pbr)}, {sql_quote(row.eps)}, {sql_quote(row.div)}, {sql_quote(row.dps)}, {sql_quote(row.exchange)})"
                    insert_statements.append(f"INSERT INTO tmp_bppedds (date, ticker, bps, per, pbr, eps, div, dps, exchange) VALUES {values};")
                
                if insert_statements:
                    batch_sql = "BEGIN;\\n" + "\\n".join(insert_statements) + "\\nCOMMIT;"
                    conn.executescript(batch_sql)
                    context.log.info(f"Successfully inserted {len(df)} records into tmp_bppedds for {date_str_iso} {exchange}.")
            # conn.commit() # Though executescript handles BEGIN/COMMIT, an explicit commit here ensures outer transaction (if any by resource) is completed.
            # For Turso with executescript, explicit commit outside BEGIN;...COMMIT; is not needed and can cause issues.
        except libsql.LibsqlError as e:
            context.log.error(f"TursoDB Error in cd_bppedds_turso: {e}\\nQuery: BEGIN;...COMMIT; (batch insert)\\nTraceback: {traceback.format_exc()}")
            # conn.rollback() # Rollback on error - executescript handles atomicity.
            raise
        except Exception as e:
            context.log.error(f"General Error in cd_bppedds_turso: {e}\\nTraceback: {traceback.format_exc()}")
            # conn.rollback() # Rollback on error
            raise


    metadata = {
        "Number of records": dg.MetadataValue.int(len(df)),
        "Preview": dg.MetadataValue.md(df.head(10).to_markdown(index=False)),
        "dagster/column_schema": column_schema,
    }
    return dg.MaterializeResult(metadata=metadata)


@dg.asset(
    description="CD 프로젝트 BPPEDD 데이터 요약 - Gold Tier (TursoDB)", # Updated description
    partitions_def=daily_exchange_category_partition,
    group_name="CD_TURSO", # Changed group_name
    kinds={"turso"}, # Changed kinds
    tags={
        "domain": "finance",
        "data_tier": "gold",
        "source": "internal"
    },
    deps=[cd_bppedds_turso], # Updated dependency
)
def cd_digest_bppedds_turso( # Renamed asset
    context: dg.AssetExecutionContext,
    cd_turso: TursoResource, # Changed resource
) -> dg.MaterializeResult:
    """
    매일 업데이트 된 BPPEDD DB로 업데이트합니다. - TursoDB 버전
    - 처리 완료 후 tmp_bppedds의 해당 파티션 데이터를 삭제하여 용량을 절약합니다.
    """
    keys = context.partition_key.keys_by_dimension
    date_iso = keys["date"] # YYYY-MM-DD
    exchange = keys["exchange"]

    date_dt = datetime.strptime(date_iso, "%Y-%m-%d") # Use date_iso directly

    if exchange == "KONEX":
        return dg.MaterializeResult(
            metadata={
                "Date": dg.MetadataValue.text(date_iso),
                "Result": dg.MetadataValue.text("KONEX 제외 - 데이터 없음"),
            }
        )

    df = pd.DataFrame() # Initialize df
    with cd_turso.get_connection() as conn:
        try:
            select_query = f"""
                SELECT date, ticker, bps, per, pbr, eps, div, dps, exchange
                FROM tmp_bppedds
                WHERE date = {sql_quote(date_iso)} AND exchange = {sql_quote(exchange)}
                  AND NOT (bps = 0 AND pbr = 0 AND eps = 0 AND div = 0 AND dps = 0)
            """
            cursor = conn.execute(select_query)
            context.log.info(f"Executed select: {select_query}")
            rows = _cursor_to_list_of_dicts(cursor)
            df = pd.DataFrame(rows)

            if df.empty:
                # 데이터가 없어도 tmp_bppedds에서 해당 파티션 삭제
                delete_tmp_query = f"DELETE FROM tmp_bppedds WHERE date = {sql_quote(date_iso)} AND exchange = {sql_quote(exchange)};"
                conn.execute(delete_tmp_query)
                # conn.commit() # Not needed if autocommit is on or part of a larger transaction handled by executescript later
                context.log.info(f"Cleaned tmp_bppedds for empty data: {date_iso} {exchange}")
                return dg.MaterializeResult(
                    metadata={
                        "Date": dg.MetadataValue.text(date_iso),
                        "Result": dg.MetadataValue.text(
                            "휴장일 등의 이유로 bppedd 데이터가 없습니다. tmp_bppedds 정리 완료."
                        ),
                    }
                )

            if "ticker" in df.columns:
                df["ticker"] = df["ticker"].astype(str).str.zfill(6)

            df = df.assign(
                date=date_iso, # Already YYYY-MM-DD
                exchange=exchange,
                year=date_dt.year,
                month=date_dt.month,
            )
            # Convert NaN to None for SQLite
            df = df.astype(object).where(pd.notnull(df), None)


            expected_columns = [
                "date", "exchange", "ticker", "bps", "per", "pbr", "eps", "div", "dps", "year", "month",
            ]
            df = df[expected_columns]
            
            # Sort by ticker for consistent locking order (less critical for SQLite but good practice)
            df.sort_values(by="ticker", inplace=True)
            
            statements = [] # Renamed from insert_statements to avoid confusion
            # Delete existing records for the date/exchange in bppedd table
            statements.append(f"DELETE FROM bppedd WHERE date = {sql_quote(date_iso)} AND exchange = {sql_quote(exchange)};")

            # Prepare bulk insert statements
            for row in df.itertuples(index=False):
                values = f"({sql_quote(row.date)}, {sql_quote(row.exchange)}, {sql_quote(row.ticker)}, {sql_quote(row.bps)}, {sql_quote(row.per)}, {sql_quote(row.pbr)}, {sql_quote(row.eps)}, {sql_quote(row.div)}, {sql_quote(row.dps)}, {sql_quote(row.year)}, {sql_quote(row.month)})"
                statements.append(f"INSERT INTO bppedd (date, exchange, ticker, bps, per, pbr, eps, div, dps, year, month) VALUES {values};")
            
            # Clean up tmp_bppedds for this partition
            statements.append(f"DELETE FROM tmp_bppedds WHERE date = {sql_quote(date_iso)} AND exchange = {sql_quote(exchange)};")

            if statements: # Check if there's anything to execute
                batch_sql = "BEGIN;\\n" + "\\n".join(statements) + "\\nCOMMIT;"
                conn.executescript(batch_sql)
                context.log.info(f"Successfully processed bppedd and cleaned up tmp_bppedds for {date_iso} {exchange}.")
            # No explicit conn.commit() needed here as executescript handles it.

        except libsql.LibsqlError as e:
            context.log.error(f"TursoDB Error in cd_digest_bppedds_turso: {e}\\nTraceback: {traceback.format_exc()}")
            raise
        except Exception as e:
            context.log.error(f"General Error in cd_digest_bppedds_turso: {e}\\nTraceback: {traceback.format_exc()}")
            raise

    preview_df = df.head(10)
    metadata = {
        "row_count": dg.MetadataValue.int(df.shape[0]),
        "preview": dg.MetadataValue.md(preview_df.to_markdown(index=False)),
        "Date": dg.MetadataValue.text(date_iso),
        "Exchange": dg.MetadataValue.text(exchange),
        "tmp_cleanup": dg.MetadataValue.text("tmp_bppedds 파티션 데이터 삭제 완료 (as part of main transaction)"),
    }
    return dg.MaterializeResult(metadata=metadata)


@dg.asset(
    description="CD 프로젝트 BPPEDD-Security 동기화 - Gold Tier (TursoDB)", # Updated description
    partitions_def=daily_exchange_category_partition,
    group_name="CD_TURSO", # Changed group_name
    kinds={"turso"}, # Changed kinds
    tags={
        "domain": "finance",
        "data_tier": "gold",
        "source": "internal"
    },
    deps=[cd_digest_bppedds_turso], # Updated dependency
)
def cd_sync_bppedds_to_security_turso( # Renamed asset
    context: dg.AssetExecutionContext,
    cd_turso: TursoResource, # Changed resource
):
    """
    bppedd에 security 테이블과 연결 작업을 수행합니다. (TursoDB 버전)
    """
    partition_keys = context.partition_key.keys_by_dimension
    partition_date_iso = partition_keys["date"] # YYYY-MM-DD
    exchange_str = partition_keys["exchange"]

    if exchange_str == "KONEX":
        metadata = {
            "Date": dg.MetadataValue.text(partition_date_iso),
            "Exchange": dg.MetadataValue.text(exchange_str),
            "Result": dg.MetadataValue.text("KONEX는 제외되었습니다"),
        }
        return dg.MaterializeResult(metadata=metadata)

    data_for_odf = [] 
    final_columns_odf = ["date", "ticker", "exchange", "security_id", "name", "count"]


    with cd_turso.get_connection() as conn:
        try:
            # First query unlinked records to update
            unlinked_query = f"""
                SELECT b.ticker, b.exchange 
                FROM bppedd b
                LEFT JOIN security s ON b.ticker = s.ticker AND b.exchange = s.exchange
                WHERE b.date = {sql_quote(partition_date_iso)}
                AND b.exchange = {sql_quote(exchange_str)}
                AND b.security_id IS NULL
                AND s.security_id IS NOT NULL
                AND s.delisting_date IS NULL
                ORDER BY b.ticker
            """
            cursor = conn.execute(unlinked_query)
            context.log.info(f"Executed unlinked_query: {unlinked_query}")
            unlinked_records_tuples = cursor.fetchall() 
            
            if not unlinked_records_tuples:
                metadata = {
                    "Date": dg.MetadataValue.text(partition_date_iso),
                    "Exchange": dg.MetadataValue.text(exchange_str),
                    "Result": dg.MetadataValue.text("모든 bppedd 레코드가 이미 security와 연결되어 있거나 업데이트할 대상이 없습니다."),
                }
                return dg.MaterializeResult(metadata=metadata)
            
            # Get securities data
            securities_query = f"""
                SELECT ticker, exchange, security_id, name, kor_name
                FROM security
                WHERE exchange = {sql_quote(exchange_str)} AND delisting_date IS NULL
                ORDER BY ticker
            """
            cursor = conn.execute(securities_query)
            context.log.info(f"Executed securities_query: {securities_query}")
            
            securities_map = {
                (row[0], row[1]): {
                    "security_id": row[2],
                    "name": row[3],
                    "kor_name": row[4],
                }
                for row in cursor.fetchall()
            }
            
            update_statements = []
            for ticker, exchange_val in unlinked_records_tuples: 
                key = (ticker, exchange_val)
                if key in securities_map:
                    sec = securities_map[key]
                    update_stmt = f"""
                        UPDATE bppedd
                        SET security_id = {sql_quote(sec["security_id"])}, 
                            name = {sql_quote(sec["name"])}, 
                            kor_name = {sql_quote(sec["kor_name"])}
                        WHERE ticker = {sql_quote(ticker)} 
                          AND exchange = {sql_quote(exchange_val)} 
                          AND date = {sql_quote(partition_date_iso)};
                    """
                    update_statements.append(update_stmt)
            
            if update_statements:
                batch_sql = "BEGIN;\\n" + "\\n".join(update_statements) + "\\nCOMMIT;"
                conn.executescript(batch_sql)
                context.log.info(f"Successfully updated {len(update_statements)} bppedd records for {partition_date_iso} {exchange_str}.")

            # Query updated data for metadata
            results_query = f"""
                SELECT b.ticker, b.exchange, b.security_id, s.name
                FROM bppedd b
                JOIN security s ON b.security_id = s.security_id
                WHERE b.date = {sql_quote(partition_date_iso)} AND b.exchange = {sql_quote(exchange_str)}
                ORDER BY b.ticker
            """
            cursor = conn.execute(results_query)
            context.log.info(f"Executed results_query: {results_query}")
            result_rows_tuples = cursor.fetchall()
            
            for row_tuple in result_rows_tuples:
                data_for_odf.append(
                    [
                        partition_date_iso,
                        row_tuple[0],    # ticker
                        row_tuple[1],    # exchange
                        row_tuple[2],    # security_id
                        row_tuple[3],    # name
                        1,               # count
                    ]
                )
        except libsql.LibsqlError as e:
            context.log.error(f"TursoDB Error in cd_sync_bppedds_to_security_turso: {e}\\nTraceback: {traceback.format_exc()}")
            raise
        except Exception as e:
            context.log.error(f"General Error in cd_sync_bppedds_to_security_turso: {e}\\nTraceback: {traceback.format_exc()}")
            raise

    odf = pd.DataFrame(data_for_odf, columns=final_columns_odf)

    if not odf.empty:
        total_count = odf["count"].sum()
        metadata = {
            "Date": dg.MetadataValue.text(partition_date_iso),
            "Exchange": dg.MetadataValue.text(exchange_str),
            "# of synced records": dg.MetadataValue.text(str(total_count)), 
            "Preview head": dg.MetadataValue.md(odf.head().to_markdown(index=False)),
            "Preview tail": dg.MetadataValue.md(odf.tail().to_markdown(index=False)),
        }
    else:
        metadata = {
            "Date": dg.MetadataValue.text(partition_date_iso),
            "Exchange": dg.MetadataValue.text(exchange_str),
            "Result": dg.MetadataValue.text(
                "No records were synced or found for creating the output dataframe."
            ),
        }

    return dg.MaterializeResult(metadata=metadata)
