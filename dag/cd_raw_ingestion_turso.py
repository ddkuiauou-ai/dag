import re
import time
import uuid
import traceback # Added
import datetime # Added

import dagster as dg
import pandas as pd
from pykrx import stock as krx
# from psycopg2.extras import execute_batch # Removed

from .cd_constants import DATE_FORMAT
from .partitions import daily_exchange_category_partition
from .resources import TursoResource # Changed from PostgresResource

COUNTRY = "KR"


PATTERN_COMPANY_NAME = re.compile(r"(.+?)([0-9]*우[A-Za-z]*)?(\(전환\))?$")

PATTERNS_SECURITY_TYPE = {
    "스팩": re.compile(r"스팩(\d+호)?$"),
    "리츠": re.compile(r"리츠$"),
    "펀드": re.compile(r"(맥쿼리인프라|맵스리얼티1|유전)$"),
    "전환우선주": re.compile(r"\(전환\)$"),
    "우선주": re.compile(r"[0-9]*우[A-Za-z]*$"),
}


def _parse_security_type(s):
    for type_name, pattern in PATTERNS_SECURITY_TYPE.items(): # Renamed type to type_name
        if pattern.search(s):
            return type_name
    return "보통주"


def _parse_company_name(s):
    match = PATTERN_COMPANY_NAME.match(s)
    return match.group(1).strip() if match else s


def _get_company_name(s):
    sec_type = _parse_security_type(s) # Renamed type to sec_type
    name = s if sec_type in ["스팩", "리츠"] else _parse_company_name(s)
    return name

# Helper function to convert cursor output to a list of dictionaries
def _cursor_to_list_of_dicts(cursor):
    columns = [col[0] for col in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]

# Helper function to safely quote values for SQL queries
def sql_quote(value):
    if value is None:
        return "NULL"
    elif isinstance(value, (int, float)):
        return str(value)
    elif isinstance(value, bool): # SQLite uses 0 and 1 for boolean
        return str(int(value))
    elif isinstance(value, (datetime.date, datetime.datetime)):
        return f"'{value.strftime('%Y-%m-%d')}'" # Standard date format
    else:
        escaped_value = str(value).replace("'", "''")
        return f"'{escaped_value}'"


@dg.asset(
    description="한국 거래소별 일일 상장종목 티커 목록 수집 - Bronze Tier - TursoDB", # Updated
    partitions_def=daily_exchange_category_partition,
    kinds={"source", "turso"}, # Updated
    group_name="CD_TURSO", # Updated
    tags={
        "domain": "finance",
        "data_tier": "bronze",
        "source": "pykrx"
    }
)
def cd_stockcode_turso( # Renamed
    context: dg.AssetExecutionContext, cd_turso: TursoResource # Updated
) -> dg.MaterializeResult:
    """날자별 거래소별 상장종목 리스트를 가져옴. only 티커만 - TursoDB"""

    partition_str = context.partition_key.keys_by_dimension
    date_obj = pd.to_datetime(partition_str["date"]) # Keep as datetime object for strftime
    exchange = partition_str["exchange"]
    date_sql_str = date_obj.strftime("%Y-%m-%d") # For SQL queries

    stock_codes = krx.get_market_ticker_list(date=date_obj.strftime("%Y%m%d"), market=exchange)
    time.sleep(0.2)

    df = pd.DataFrame(stock_codes, columns=["ticker"])
    df["date"] = date_sql_str # Store as YYYY-MM-DD string
    df["exchange"] = exchange

    delete_query = "DELETE FROM stockcodename WHERE date = ? AND exchange = ?"
    insert_query = "INSERT INTO stockcodename (date, ticker, exchange) VALUES (?, ?, ?)"
    
    with cd_turso.get_connection() as conn:
        cur = conn.cursor()
        try:
            conn.execute("BEGIN IMMEDIATE")
            cur.execute(delete_query, (date_sql_str, exchange))
            data_to_insert = [(row.date, row.ticker, row.exchange) for row in df.itertuples(index=False)]
            if data_to_insert:
                cur.executemany(insert_query, data_to_insert)
            conn.commit()
        except Exception as e:
            conn.rollback()
            context.log.error(f"Error in cd_stockcode_turso: {str(e)}\n{traceback.format_exc()}")
            raise
        finally:
            cur.close()

    return dg.MaterializeResult(
        metadata={
            "dagster/row_count": dg.MetadataValue.int(len(df)),
            "preview": dg.MetadataValue.md(df.head(10).to_markdown(index=False)),
            "dagster/column_schema": dg.TableSchema(
                columns=[
                    dg.TableColumn("ticker", "string", description="Ticker symbol"),
                    dg.TableColumn("date", "string", description="Data date (YYYY-MM-DD)"),
                    dg.TableColumn("exchange", "string", description="Exchange identifier"),
                ]
            ),
        }
    )


@dg.asset(
    description="한국 거래소별 일일 상장종목 이름 정보 수집 - Bronze Tier - TursoDB", # Updated
    partitions_def=daily_exchange_category_partition,
    kinds={"source", "turso"}, # Updated
    group_name="CD_TURSO", # Updated
    tags={
        "domain": "finance",
        "data_tier": "bronze",
        "source": "pykrx"
    },
    deps=[cd_stockcode_turso], # Updated
)
def cd_stockcodenames_turso( # Renamed
    context: dg.AssetExecutionContext, cd_turso: TursoResource # Updated
) -> dg.MaterializeResult:
    """날짜별 거래소별 상장종목 리스트를 가져옴. (티커, 이름) - TursoDB"""

    partition_str = context.partition_key.keys_by_dimension
    date_obj = pd.to_datetime(partition_str["date"])
    exchange = partition_str["exchange"]
    date_sql_str = date_obj.strftime("%Y-%m-%d")

    select_query = "SELECT ticker FROM stockcodename WHERE date = ? AND exchange = ?"
    tickers = []
    with cd_turso.get_connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute(select_query, (date_sql_str, exchange))
            tickers = [row[0] for row in cur.fetchall()]
        except Exception as e:
            context.log.error(f"Error selecting tickers in cd_stockcodenames_turso: {str(e)}\n{traceback.format_exc()}")
            raise
        finally:
            cur.close()
            
    if not tickers:
        context.log.info(f"No tickers found for {date_sql_str}, {exchange} in cd_stockcodenames_turso.")
        df = pd.DataFrame(columns=["ticker", "name", "date", "exchange"])
    else:
        df = pd.DataFrame(tickers, columns=["ticker"])
        df = df.assign(
            ticker=df["ticker"].astype(str).str.zfill(6),
            name=df["ticker"].apply(lambda t: krx.get_market_ticker_name(t) or ""), # Ensure name is not None
            date=date_sql_str,
            exchange=exchange
        )

    update_query = "UPDATE stockcodename SET name = ? WHERE date = ? AND exchange = ? AND ticker = ?"
    if not df.empty:
        with cd_turso.get_connection() as conn:
            cur = conn.cursor()
            try:
                data_to_update = [
                    (row.name, row.date, row.exchange, row.ticker)
                    for row in df.itertuples(index=False)
                ]
                if data_to_update:
                    cur.executemany(update_query, data_to_update)
                conn.commit()
            except Exception as e:
                conn.rollback()
                context.log.error(f"Error updating stockcodenames in cd_stockcodenames_turso: {str(e)}\n{traceback.format_exc()}")
                raise
            finally:
                cur.close()

    return dg.MaterializeResult(
        metadata={
            "dagster/row_count": dg.MetadataValue.int(len(df)), # Corrected key
            "preview": dg.MetadataValue.md(df.head(10).to_markdown(index=False)),
            "dagster/column_schema": dg.TableSchema(
                columns=[
                    dg.TableColumn("ticker", "string", description="Ticker symbol"),
                    dg.TableColumn("name", "string", description="Ticker name"),
                    dg.TableColumn("date", "string", description="Data date (YYYY-MM-DD)"),
                    dg.TableColumn("exchange", "string", description="Exchange identifier"),
                ]
            ),
        }
    )


@dg.asset(
    description="종목 정보 통합 처리 - 신규상장, 상장폐지, 이름변경 관리 - Bronze Tier - TursoDB", # Updated
    partitions_def=daily_exchange_category_partition,
    group_name="CD_TURSO", # Updated
    kinds={"turso"}, # Updated
    tags={
        "domain": "finance",
        "data_tier": "bronze",
        "source": "pykrx" # Source is pykrx, but processing is computed
    },
    deps=[cd_stockcodenames_turso], # Updated
)
def cd_ingest_stockcodenames_turso( # Renamed
    context: dg.AssetExecutionContext,
    cd_turso: TursoResource, # Updated
) -> dg.MaterializeResult:
    """
    신규상장, 상장폐지, 이름변경을 처리합니다. - TursoDB
    """
    partition_keys = context.partition_key.keys_by_dimension
    date_input_str = partition_keys["date"] # YYYY-MM-DD from partition
    exchange = partition_keys["exchange"]

    date_dt = pd.to_datetime(date_input_str)
    date_for_sql = date_dt.strftime("%Y-%m-%d") # YYYY-MM-DD for SQL

    select_stock_query = "SELECT ticker, name, exchange, date FROM stockcodename WHERE date = ? AND exchange = ?"
    stock_df_data = []
    with cd_turso.get_connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute(select_stock_query, (date_for_sql, exchange))
            stock_df_data = _cursor_to_list_of_dicts(cur)
        except Exception as e:
            context.log.error(f"Error fetching stockcodenames in cd_ingest_stockcodenames_turso: {str(e)}\n{traceback.format_exc()}")
            raise
        finally:
            cur.close()
    
    stock_df = pd.DataFrame.from_records(stock_df_data)

    if stock_df.empty:
        context.log.info("조회된 stockcodenames 데이터가 없습니다.")
        # ... (metadata for empty result)
        listed_md = delisted_md = name_change_md = "No data available."
        undo_count = name_change_count = listed_count = delisted_count = 0
        result_df = pd.DataFrame(columns=["date", "ticker", "name", "type", "exchange"])

    else:
        stock_df["ticker"] = stock_df["ticker"].astype(str).str.zfill(6)
        # exchange = stock_df["exchange"].iloc[0] # exchange is already known

        select_security_query = "SELECT security_id, ticker, name, kor_name, exchange, delisting_date FROM security WHERE exchange = ?"
        existed_df_data = []
        with cd_turso.get_connection() as conn:
            cur = conn.cursor()
            try:
                cur.execute(select_security_query, (exchange,))
                existed_df_data = _cursor_to_list_of_dicts(cur)
            except Exception as e:
                context.log.error(f"Error fetching existing securities in cd_ingest_stockcodenames_turso: {str(e)}\n{traceback.format_exc()}")
                raise
            finally:
                cur.close()

        existed_df = pd.DataFrame.from_records(existed_df_data)
        if existed_df.empty:
            existed_df = pd.DataFrame(columns=["security_id", "ticker", "name", "kor_name", "exchange", "delisting_date"])

        merged_df = pd.merge(stock_df, existed_df, how="outer", on=["ticker", "exchange"], indicator=True, suffixes=('_new', '_old'))
        
        new_df = merged_df[merged_df['_merge'] == 'left_only'].copy()
        new_df["name"] = new_df["name_new"]

        delisted_df = merged_df[merged_df['_merge'] == 'right_only'].copy()
        delisted_df["name"] = delisted_df["name_old"]
        
        both_df = merged_df[merged_df['_merge'] == 'both'].copy()
        name_change_df = both_df[both_df["name_new"] != both_df["name_old"]].copy()
        name_change_df["name"] = name_change_df["name_new"]

        update_name_statements = []
        if not name_change_df.empty:
            for _, row in name_change_df.iterrows():
                update_name_statements.append(f"UPDATE security SET name = {sql_quote(row['name_new'])}, kor_name = {sql_quote(row['name_new'])} WHERE security_id = {sql_quote(row['security_id'])};")
            context.log.info(f"이름 변경 업데이트 준비: {len(name_change_df)}건")

        undo_delisting_statements = []
        undo_df = both_df[both_df["delisting_date"].notnull()] # Assuming delisting_date is YYYY-MM-DD string or None
        if not undo_df.empty:
            for sec_id in undo_df["security_id"].tolist():
                 undo_delisting_statements.append(f"UPDATE security SET delisting_date = NULL WHERE security_id = {sql_quote(sec_id)};")
            context.log.info(f"상장폐지 취소 업데이트 준비: {len(undo_df)}건")

        insert_new_statements = []
        if not new_df.empty:
            for _, row in new_df.iterrows():
                sec_id = str(uuid.uuid4())
                insert_new_statements.append(
                    f"INSERT INTO security (security_id, ticker, name, exchange, kor_name, country, listing_date) VALUES ("
                    f"{sql_quote(sec_id)}, {sql_quote(row['ticker'])}, {sql_quote(row['name_new'])}, {sql_quote(row['exchange'])}, "
                    f"{sql_quote(row['name_new'])}, {sql_quote(COUNTRY)}, {sql_quote(date_for_sql)});"
                )
            context.log.info(f"신규 상장 INSERT 준비: {len(new_df)}건")

        update_delisted_statements = []
        if not delisted_df.empty:
            delisted_filtered = delisted_df[delisted_df["delisting_date"].isnull()]
            if not delisted_filtered.empty:
                for sec_id in delisted_filtered["security_id"].tolist():
                    update_delisted_statements.append(f"UPDATE security SET delisting_date = {sql_quote(date_for_sql)} WHERE security_id = {sql_quote(sec_id)};")
                context.log.info(f"상장폐지 업데이트 준비: {len(delisted_filtered)}건")
        
        all_statements = update_name_statements + undo_delisting_statements + insert_new_statements + update_delisted_statements
        if all_statements:
            with cd_turso.get_connection() as conn:
                cur = conn.cursor()
                try:
                    script = "BEGIN;\n" + "\n".join(all_statements) + "\nCOMMIT;"
                    cur.executescript(script)
                    context.log.info(f"Total DML statements executed: {len(all_statements)}")
                except Exception as e:
                    # executescript rolls back automatically on error
                    context.log.error(f"Error executing DML script in cd_ingest_stockcodenames_turso: {str(e)}\n{traceback.format_exc()}")
                    raise
                finally:
                    cur.close()
        
        summary_frames = []
        if not new_df.empty: summary_frames.append(new_df[["ticker", "name", "exchange"]].assign(type="listed"))
        if not delisted_df.empty: summary_frames.append(delisted_df[["ticker", "name", "exchange"]].assign(type="delisted"))
        if not name_change_df.empty: summary_frames.append(name_change_df[["ticker", "name", "exchange"]].assign(type="name_change"))

        if summary_frames:
            result_df = pd.concat(summary_frames, ignore_index=True)
            result_df["date"] = date_for_sql # Use YYYY-MM-DD
        else:
            result_df = pd.DataFrame(columns=["date", "ticker", "name", "type", "exchange"])

        undo_count = len(undo_df)
        name_change_count = len(name_change_df)
        listed_count = len(new_df)
        delisted_count = len(delisted_df) # This should be delisted_filtered if only those are updated
        delisted_count = len(delisted_filtered) if not delisted_df.empty and 'delisted_filtered' in locals() else len(delisted_df)


        listed_md = new_df.to_markdown(index=False) if not new_df.empty else "No new listings."
        delisted_md = delisted_df.to_markdown(index=False) if not delisted_df.empty else "No delistings."
        name_change_md = name_change_df.to_markdown(index=False) if not name_change_df.empty else "No name changes."

    preview_df = result_df.head(10)
    row_count = len(result_df)
    metadata = {
        "row_count": dg.MetadataValue.int(row_count),
        "preview": dg.MetadataValue.md(preview_df.to_markdown(index=False)),
        "# Of Undo Delisting": dg.MetadataValue.int(undo_count),
        "# Of Name Change": dg.MetadataValue.int(name_change_count),
        "# Of Listed": dg.MetadataValue.int(listed_count),
        "# Of Delisted": dg.MetadataValue.int(delisted_count),
        "Listed": dg.MetadataValue.md(listed_md),
        "Delisted": dg.MetadataValue.md(delisted_md),
        "Name Change": dg.MetadataValue.md(name_change_md),
        "dagster/column_schema": dg.TableSchema(
            columns=[
                dg.TableColumn("ticker", "string", description="Ticker symbol"),
                dg.TableColumn("name", "string", description="Ticker name"),
                dg.TableColumn("date", "string", description="Data date (YYYY-MM-DD)"),
                dg.TableColumn("exchange", "string", description="Exchange identifier"),
                dg.TableColumn("type", "string", description="Change type (listed/delisted/name_change)"),
            ]
        ),
    }
    return dg.MaterializeResult(metadata=metadata)


@dg.asset(
    description="종목 보안 타입 분류 - Silver Tier - TursoDB", # Updated
    partitions_def=daily_exchange_category_partition,
    group_name="CD_TURSO", # Updated
    kinds={"turso"}, # Updated
    tags={
        "domain": "finance",
        "data_tier": "silver",
        "source": "computed"
    },
    deps=[cd_ingest_stockcodenames_turso], # Updated
)
def cd_update_securitytypebyname_turso( # Renamed
    context: dg.AssetExecutionContext,
    cd_turso: TursoResource, # Updated
) -> dg.MaterializeResult:
    """
    보통주, 우선주, 전환우선주, 리츠, 스팩, 펀드 security type 업데이트. - TursoDB
    """
    partition_keys = context.partition_key.keys_by_dimension
    date_input_str = partition_keys["date"] # YYYY-MM-DD
    exchange = partition_keys["exchange"]
    date_for_sql = pd.to_datetime(date_input_str).strftime("%Y-%m-%d")


    select_query = "SELECT security_id, name, exchange FROM security WHERE delisting_date IS NULL AND exchange = ? AND type IS NULL"
    rows_data = []
    with cd_turso.get_connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute(select_query, (exchange,))
            rows_data = _cursor_to_list_of_dicts(cur)
        except Exception as e:
            context.log.error(f"Error selecting securities in cd_update_securitytypebyname_turso: {str(e)}\n{traceback.format_exc()}")
            raise
        finally:
            cur.close()

    if not rows_data:
        odf = pd.DataFrame(columns=["date", "exchange", "security_id", "name", "type"]).astype(str)
        metadata = {
            "Date": dg.MetadataValue.text(date_for_sql),
            "Exchange": dg.MetadataValue.text(exchange),
            "Result": dg.MetadataValue.text("모든 종목의 타입이 이미 업데이트 되었습니다"),
             "dagster/row_count": dg.MetadataValue.int(0),
        }
        return dg.MaterializeResult(metadata=metadata)

    odf = pd.DataFrame.from_records(rows_data)
    odf["type"] = odf["name"].apply(_parse_security_type)
    odf["date"] = date_for_sql

    update_statements = []
    if not odf.empty:
        for _, row in odf.iterrows():
            update_statements.append(f"UPDATE security SET type = {sql_quote(row['type'])} WHERE security_id = {sql_quote(row['security_id'])};")

    if update_statements:
        with cd_turso.get_connection() as conn:
            cur = conn.cursor()
            try:
                script = "BEGIN;\n" + "\n".join(update_statements) + "\nCOMMIT;"
                cur.executescript(script)
                context.log.info(f"Successfully updated types for {len(odf)} securities.")
            except Exception as e:
                context.log.error(f"Error updating security types in cd_update_securitytypebyname_turso: {str(e)}\n{traceback.format_exc()}")
                raise
            finally:
                cur.close()
    
    type_counts = odf["type"].value_counts().to_dict()
    metadata = {
        "Date": dg.MetadataValue.text(date_for_sql),
        "Exchange": dg.MetadataValue.text(exchange),
        "# of 보통주": dg.MetadataValue.int(type_counts.get("보통주", 0)),
        "# of 우선주": dg.MetadataValue.int(type_counts.get("우선주", 0)),
        "# of 전환우선주": dg.MetadataValue.int(type_counts.get("전환우선주", 0)),
        "# of 리츠": dg.MetadataValue.int(type_counts.get("리츠", 0)),
        "# of 스팩": dg.MetadataValue.int(type_counts.get("스팩", 0)),
        "# of 펀드": dg.MetadataValue.int(type_counts.get("펀드", 0)),
        "Preview": dg.MetadataValue.md(odf.to_markdown(index=False)), # Added index=False
        "dagster/row_count": dg.MetadataValue.int(len(odf)),
    }
    return dg.MaterializeResult(metadata=metadata)


@dg.asset(
    description="회사 엔티티 생성 및 종목-회사 연결 관리 - Silver Tier - TursoDB", # Updated
    partitions_def=daily_exchange_category_partition,
    group_name="CD_TURSO", # Updated
    kinds={"turso"}, # Updated
    tags={
        "domain": "finance",
        "data_tier": "silver",
        "source": "computed"
    },
    deps=[cd_update_securitytypebyname_turso], # Updated
)
def cd_upsert_company_by_security_turso( # Renamed
    context: dg.AssetExecutionContext, cd_turso: TursoResource # Updated
) -> dg.MaterializeResult:
    """
    신규 종목의 경우, company를 생성하고 company와 security를 연결합니다. - TursoDB
    """
    partition_info = context.partition_key.keys_by_dimension
    date_input_str = partition_info["date"] # YYYY-MM-DD
    exchange = partition_info["exchange"]
    date_for_sql = pd.to_datetime(date_input_str).strftime("%Y-%m-%d")


    columns = ["date", "exchange", "company_id", "name", "ticker", "security_name", "action_type"]
    odf_data = []
    
    EXCLUDES = ("스팩", "리츠", "펀드")
    excludes_placeholders = ','.join(['?'] * len(EXCLUDES))

    select_securities_query = f"""
    SELECT security_id, name, ticker, type, exchange
    FROM security
    WHERE company_id IS NULL
      AND delisting_date IS NULL
      AND exchange = ?
      AND (type IS NULL OR type NOT IN ({excludes_placeholders}))
    """
    securities_data = []
    with cd_turso.get_connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute(select_securities_query, (exchange,) + EXCLUDES)
            securities_data = _cursor_to_list_of_dicts(cur)
        except Exception as e:
            context.log.error(f"Error selecting securities in cd_upsert_company_by_security_turso: {str(e)}\n{traceback.format_exc()}")
            raise
        finally:
            cur.close()

    securities_df = pd.DataFrame.from_records(securities_data)

    if securities_df.empty:
        empty_df = pd.DataFrame(columns=columns)
        metadata = {
            "Date": dg.MetadataValue.text(date_for_sql),
            "Exchange": dg.MetadataValue.text(exchange),
            "Result": dg.MetadataValue.text("모든 증권이 이미 회사에 연결되어 있거나, 업데이트 대상이 없습니다."),
            "Preview": dg.MetadataValue.md(empty_df.to_markdown(index=False)), # Added index=False
             "dagster/row_count": dg.MetadataValue.int(0),
        }
        return dg.MaterializeResult(metadata=metadata)

    select_companies_query = f"""
    SELECT DISTINCT c.company_id, c.name
    FROM company c
    JOIN security s ON s.company_id = c.company_id
    WHERE s.exchange = ?
      AND (s.type IS NULL OR s.type NOT IN ({excludes_placeholders}))
    """
    companies_data = []
    with cd_turso.get_connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute(select_companies_query, (exchange,) + EXCLUDES)
            companies_data = _cursor_to_list_of_dicts(cur)
        except Exception as e:
            context.log.error(f"Error selecting companies in cd_upsert_company_by_security_turso: {str(e)}\n{traceback.format_exc()}")
            raise
        finally:
            cur.close()
            
    existing_companies = {row["name"]: {"company_id": row["company_id"], "name": row["name"]} for row in companies_data}

    num_create = 0
    num_link = 0
    
    insert_company_statements = []
    update_security_statements = []

    for _, sec in securities_df.iterrows():
        company_name = _get_company_name(sec["name"])
        action_type = ""
        company_id_to_link = None

        if company_name in existing_companies:
            company = existing_companies[company_name]
            company_id_to_link = company["company_id"]
            action_type = "link"
            num_link += 1
        else:
            new_company_id = str(uuid.uuid4())
            # kor_name, country, type are fixed for new companies here
            insert_company_statements.append(
                f"INSERT INTO company (company_id, name, kor_name, country, type, created_at, updated_at) VALUES ("
                f"{sql_quote(new_company_id)}, {sql_quote(company_name)}, {sql_quote(company_name)}, "
                f"{sql_quote('대한민국')}, {sql_quote('상장법인')}, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);"
            )
            existing_companies[company_name] = {"company_id": new_company_id, "name": company_name} # Add to known companies
            company_id_to_link = new_company_id
            action_type = "create"
            num_create += 1
        
        if company_id_to_link:
            update_security_statements.append(
                f"UPDATE security SET company_id = {sql_quote(company_id_to_link)}, updated_at = CURRENT_TIMESTAMP "
                f"WHERE security_id = {sql_quote(sec['security_id'])};"
            )
        
        odf_data.append([
            date_for_sql, exchange, company_id_to_link, company_name,
            sec["ticker"], sec["name"], action_type,
        ])

    all_statements = insert_company_statements + update_security_statements
    if all_statements:
        with cd_turso.get_connection() as conn:
            cur = conn.cursor()
            try:
                script = "BEGIN;\n" + "\n".join(all_statements) + "\nCOMMIT;"
                cur.executescript(script)
                context.log.info(f"Executed {len(insert_company_statements)} company inserts and {len(update_security_statements)} security updates.")
            except Exception as e:
                context.log.error(f"Error in cd_upsert_company_by_security_turso DML execution: {str(e)}\n{traceback.format_exc()}")
                raise
            finally:
                cur.close()
        
    odf = pd.DataFrame(odf_data, columns=columns)
    metadata = {
        "Date": dg.MetadataValue.text(date_for_sql),
        "Exchange": dg.MetadataValue.text(exchange),
        "# of new com": dg.MetadataValue.int(num_create),
        "# of linking to com": dg.MetadataValue.int(num_link),
        "Preview": dg.MetadataValue.md(odf.head(10).to_markdown(index=False)), # Added head(10) and index=False
        "dagster/row_count": dg.MetadataValue.int(len(odf)),
    }
    
    return dg.MaterializeResult(metadata=metadata)
