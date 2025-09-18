import time
from datetime import datetime

import dagster as dg
import pandas as pd
from pykrx import stock as krx

from .cd_constants import DATE_FORMAT
from .partitions import daily_exchange_category_partition
from .resources import TursoResource


# Helper functions for TursoDB
def sql_quote(value):
    """ 작은따옴표를 SQL 문자열에 맞게 이스케이프합니다. """
    if value is None:
        return "NULL"
    return f"'{str(value).replace("'", "''")}'"

def _cursor_to_list_of_dicts(cursor):
    """Converts a database cursor result to a list of dictionaries."""
    if cursor.description is None:
        return []
    columns = [desc[0] for desc in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


@dg.asset(
    description="한국거래소(KRX)에서 일별 OHLCV 가격 데이터 수집 - CD 주식 분석 파이프라인을 위한 Bronze 계층 종합 거래 데이터 (TursoDB)",
    partitions_def=daily_exchange_category_partition,
    kinds={"source", "turso"}, # postgres -> turso
    group_name="CD_TURSO", # CD -> CD_TURSO
    tags={
        "domain": "finance",
        "data_tier": "bronze",
        "source": "pykrx",
        "project": "cd",
        "region": "korea",
        "update_frequency": "daily",
        "asset_type": "ingestion"
    },
    deps=["cd_upsert_company_by_security_turso"], # Dependency updated to Turso version
)
def cd_prices_turso( # Function name updated
    context: dg.AssetExecutionContext, cd_turso: TursoResource # cd_postgres -> cd_turso
) -> dg.MaterializeResult:
    """
    일일 거래소 가격 정보를 가져와 수정주가(티커, 시가, 고가, 저가, 종가, 거래량, 거래금액, 등락률)를 TursoDB에 저장합니다.
    """
    # 파티션 키 추출 및 포맷팅
    keys = context.partition_key.keys_by_dimension
    date_str_nodash = keys["date"].replace("-", "") # 예: 20250608
    exchange = keys["exchange"]
    
    # TursoDB는 YYYY-MM-DD 형식의 텍스트로 날짜 저장
    date_for_db = keys["date"] # 예: 2025-06-08

    # OHLCV 데이터 조회 및 전처리
    df = krx.get_market_ohlcv(date=date_str_nodash, market=exchange).reset_index()
    time.sleep(0.2) # KRX API 호출 간격
    context.log.info(f"Retrieved {len(df)} rows from KRX for {date_str_nodash} {exchange}")

    if df.empty:
        context.log.info(f"No data retrieved from KRX for {date_str_nodash} {exchange}. Skipping DB operations.")
        return dg.MaterializeResult(
            metadata={
                "dagster/row_count": dg.MetadataValue.int(0),
                "처리일자": dg.MetadataValue.text(date_for_db),
                "거래소": dg.MetadataValue.text(exchange),
                "result": dg.MetadataValue.text("KRX에서 조회된 데이터가 없습니다."),
            }
        )

    df.columns = [
        "ticker", "open", "high", "low", "close", "volume", "transaction", "rate", "marketcap"
    ]
    df["date"] = date_for_db # YYYY-MM-DD 형식
    df["exchange"] = exchange
    df["ticker"] = df["ticker"].astype(str).str.zfill(6) # 티커 포맷팅

    df = df[
        ["date", "ticker", "open", "high", "low", "close", "volume", "transaction", "rate", "exchange"]
    ]

    # TursoDB tmp_prices 테이블에 데이터 저장
    column_schema = dg.TableSchema(
        columns=[
            dg.TableColumn("date", "string", description="Data date (YYYY-MM-DD)"), # date -> string
            dg.TableColumn("ticker", "string", description="Ticker symbol"),
            dg.TableColumn("open", "integer", description="Opening price"), # float -> integer (KRX 가격은 정수)
            dg.TableColumn("high", "integer", description="High price"),
            dg.TableColumn("low", "integer", description="Low price"),
            dg.TableColumn("close", "integer", description="Closing price"),
            dg.TableColumn("volume", "integer", description="Trading volume"), # bigint -> integer
            dg.TableColumn("transaction", "integer", description="Transaction amount"), # bigint -> integer
            dg.TableColumn("rate", "float", description="Rate of change"),
            dg.TableColumn("exchange", "string", description="Exchange identifier"),
        ]
    )
    with cd_turso.get_connection() as conn: # cd_postgres -> cd_turso
        cur = conn.cursor()
        # TursoDB는 %s 대신 ? 사용, 날짜는 문자열로
        delete_query = f"DELETE FROM tmp_prices WHERE date = {sql_quote(date_for_db)} AND exchange = {sql_quote(exchange)}"
        cur.execute(delete_query)
        
        data_tuples = [
            (
                row.date, row.ticker, int(row.open), int(row.high), int(row.low), int(row.close),
                int(row.volume), int(row.transaction), float(row.rate), row.exchange,
            )
            for row in df.itertuples(index=False)
        ]
        
        if data_tuples:
            # executemany 사용
            insert_query = """
            INSERT INTO tmp_prices
            (date, ticker, open, high, low, close, volume, transaction, rate, exchange)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            cur.executemany(insert_query, data_tuples)
        conn.commit()

    record_count = len(df)
    preview_df = df.head(10)
    return dg.MaterializeResult(
        metadata={
            "dagster/row_count": dg.MetadataValue.int(record_count),
            "처리일자": dg.MetadataValue.text(date_for_db),
            "거래소": dg.MetadataValue.text(exchange),
            "preview": dg.MetadataValue.md(preview_df.to_markdown(index=False)),
            "dagster/column_schema": column_schema,
        }
    )


@dg.asset(
    description="가격 데이터 검증 및 임시 테이블에서 영구 테이블로 저장 - OHLCV 데이터를 검증하고 tmp_prices에서 price 테이블로 이동하는 Silver 계층 데이터 처리 (TursoDB)",
    partitions_def=daily_exchange_category_partition,
    group_name="CD_TURSO", # CD -> CD_TURSO
    kinds={"turso"}, # postgres -> turso
    tags={
        "domain": "finance",
        "data_tier": "silver", 
        "source": "computed",
        "project": "cd",
        "region": "korea",
        "update_frequency": "daily",
        "asset_type": "processing"
    },
    deps=[cd_prices_turso], # cd_prices -> cd_prices_turso
)
def cd_digest_price_turso( # Function name updated
    context: dg.AssetExecutionContext, cd_turso: TursoResource # cd_postgres -> cd_turso
) -> dg.MaterializeResult:
    """
    매일 업데이트 된 가격 데이터를 TursoDB의 price 테이블에 업데이트합니다.
    처리 완료 후 tmp_prices의 해당 파티션 데이터를 삭제하여 용량을 절약합니다.
    """
    # ──── 파티션 키 및 날짜 처리 ────
    partition_keys = context.partition_key.keys_by_dimension
    # date_str_nodash = partition_keys["date"].replace("-", "") # 사용 안함
    exchange = partition_keys["exchange"]
    
    # TursoDB는 YYYY-MM-DD 형식의 텍스트로 날짜 저장
    date_for_db = partition_keys["date"] # 예: 2025-06-08
    date_dt = pd.to_datetime(date_for_db) # 연, 월 추출용

    # tmp_prices 테이블에서 데이터 조회
    # %s 대신 f-string과 sql_quote 사용
    select_query = f"""
        SELECT date, ticker, open, high, low, close, volume, transaction, rate, exchange
        FROM tmp_prices
        WHERE date = {sql_quote(date_for_db)} AND exchange = {sql_quote(exchange)}
    """
    
    prices_df_list = []
    with cd_turso.get_connection() as conn: # cd_postgres -> cd_turso
        cur = conn.cursor()
        cur.execute(select_query)
        prices_df_list = _cursor_to_list_of_dicts(cur)
    
    prices_df = pd.DataFrame(prices_df_list)

    # 데이터가 없는 경우 tmp_prices 정리 후 반환
    if prices_df.empty:
        with cd_turso.get_connection() as conn: # cd_postgres -> cd_turso
            cur = conn.cursor()
            # %s 대신 f-string과 sql_quote 사용
            delete_tmp_query = f"DELETE FROM tmp_prices WHERE date = {sql_quote(date_for_db)} AND exchange = {sql_quote(exchange)}"
            cur.execute(delete_tmp_query)
            conn.commit()
            
        return dg.MaterializeResult(
            metadata={
                "Date": dg.MetadataValue.text(date_for_db),
                "Result": dg.MetadataValue.text(
                    "prices 테이블에서 데이터를 조회할 수 없습니다. tmp_prices 정리 완료."
                ),
                 "dagster/row_count": dg.MetadataValue.int(0),
            }
        )

    # ──── 데이터 유효성 체크 ────
    cols_to_check = ["open", "high", "low", "close", "volume"] # transaction, rate는 0일 수 있음
    # 숫자형으로 변환 시도, 오류 발생 시 NaN으로 처리 후 0으로 채움
    for col in cols_to_check:
        prices_df[col] = pd.to_numeric(prices_df[col], errors='coerce').fillna(0)

    valid_mask = (prices_df[cols_to_check] == 0).all(axis=1)
    valid_df = prices_df[~valid_mask].copy() # .copy() 추가


    # 유효한 데이터가 전혀 없는 경우 (예: 휴장일) - tmp_prices 정리 후 반환
    if valid_df.empty:
        with cd_turso.get_connection() as conn: # cd_postgres -> cd_turso
            cur = conn.cursor()
            delete_tmp_query = f"DELETE FROM tmp_prices WHERE date = {sql_quote(date_for_db)} AND exchange = {sql_quote(exchange)}"
            cur.execute(delete_tmp_query)
            conn.commit()
            
        return dg.MaterializeResult(
            metadata={
                "Date": dg.MetadataValue.text(date_for_db),
                "Result": dg.MetadataValue.text(
                    "휴장일 등의 이유로 가격이 없습니다. tmp_prices 정리 완료."
                ),
                "dagster/row_count": dg.MetadataValue.int(0),
            }
        )

    # 일부만 유효한 경우 tmp_prices 정리 후 에러 발생 (기존 로직 유지)
    if valid_df.shape[0] != prices_df.shape[0]:
        with cd_turso.get_connection() as conn: # cd_postgres -> cd_turso
            cur = conn.cursor()
            delete_tmp_query = f"DELETE FROM tmp_prices WHERE date = {sql_quote(date_for_db)} AND exchange = {sql_quote(exchange)}"
            cur.execute(delete_tmp_query)
            conn.commit()
            
        error_msg = (
            f"휴장일이 아님에도 가격 정보가 일부만 유효합니다. "
            f"유효한 행: {valid_df.shape[0]}, 전체 행: {prices_df.shape[0]}, "
            f"exchange: {exchange}, date: {date_for_db}. " # prices_df에서 직접 가져오도록 수정
            f"tmp_prices 정리 완료."
        )
        context.log.error(error_msg)
        raise ValueError(error_msg)

    df_to_insert = valid_df.copy()

    # ──── 데이터 전처리 ────
    if "ticker" in df_to_insert.columns:
        df_to_insert["ticker"] = df_to_insert["ticker"].astype(str).str.zfill(6)

    df_to_insert = df_to_insert.assign(
        date=date_for_db, # formatted_date_iso -> date_for_db
        exchange=exchange,
        year=date_dt.year,
        month=date_dt.month,
    )
    # rate 컬럼을 float으로 명시적 변환
    if 'rate' in df_to_insert.columns:
        df_to_insert['rate'] = pd.to_numeric(df_to_insert['rate'], errors='coerce').fillna(0.0)


    expected_columns = [
        "date", "exchange", "ticker", "open", "high", "low", "close", 
        "volume", "transaction", "rate", "year", "month",
    ]
    df_to_insert = df_to_insert[expected_columns]

    # ──── TursoDB price 테이블 업데이트 ────
    # 숫자형 컬럼들을 int 또는 float으로 변환
    int_cols = ["open", "high", "low", "close", "volume", "transaction", "year", "month"]
    for col in int_cols:
        if col in df_to_insert.columns:
            df_to_insert[col] = pd.to_numeric(df_to_insert[col], errors='coerce').fillna(0).astype(int)
    
    insert_tuples = [tuple(row) for row in df_to_insert.to_numpy()]
    
    # Sort tuples by ticker to ensure consistent locking order (SQLite에서는 덜 중요하지만 유지)
    insert_tuples.sort(key=lambda x: x[2])  # ticker is at index 2

    with cd_turso.get_connection() as conn: # cd_postgres -> cd_turso
        cur = conn.cursor()
        # sql_script_parts = [] # executescript 대신 개별 실행으로 변경
        try:
            # Delete existing records for the date/exchange
            delete_price_query = f"DELETE FROM price WHERE date = {sql_quote(date_for_db)} AND exchange = {sql_quote(exchange)};"
            # sql_script_parts.append(delete_price_query)
            cur.execute(delete_price_query) # 직접 실행
            
            # Bulk insert using executemany
            if insert_tuples:
                insert_query_template = """
                INSERT INTO price
                (date, exchange, ticker, open, high, low, close, volume, transaction, rate, year, month)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                """
                cur.executemany(insert_query_template, insert_tuples) # 직접 실행

            # ──── 성공적으로 처리 완료 후 tmp_prices에서 해당 파티션 삭제 ────
            delete_tmp_query = f"DELETE FROM tmp_prices WHERE date = {sql_quote(date_for_db)} AND exchange = {sql_quote(exchange)};"
            # sql_script_parts.append(delete_tmp_query)
            cur.execute(delete_tmp_query) # 직접 실행

            conn.commit()
            context.log.info(f"Successfully processed and cleaned up tmp_prices for {date_for_db} {exchange}")
            
        except Exception as e:
            conn.rollback() # SQLite도 rollback 지원
            context.log.error(f"Error processing price data: {str(e)}")
            # 에러 발생 시에도 tmp_prices 정리 시도
            try:
                # 새 커서 사용 또는 기존 커서 재사용 (상태에 따라)
                # 여기서는 같은 트랜잭션 내에서 실패했으므로, rollback 후 새 작업으로 간주하거나
                # conn.cursor()를 다시 호출하여 새 커서를 얻는 것이 안전할 수 있음.
                # 다만, 현재 로직은 같은 커서(cur)를 사용.
                cur.execute(f"DELETE FROM tmp_prices WHERE date = {sql_quote(date_for_db)} AND exchange = {sql_quote(exchange)}")
                conn.commit()
                context.log.info(f"Cleaned up tmp_prices after error for {date_for_db} {exchange}")
            except Exception as cleanup_error:
                context.log.error(f"Failed to cleanup tmp_prices: {str(cleanup_error)}")
            raise

    # ──── 결과 요약 및 MaterializeResult 반환 ────
    preview_df = df_to_insert.head(10)
    row_count = len(df_to_insert)
    metadata = {
        "dagster/row_count": dg.MetadataValue.int(row_count),
        "preview": dg.MetadataValue.md(preview_df.to_markdown(index=False)),
        "처리일자": dg.MetadataValue.text(date_for_db),
        "거래소": dg.MetadataValue.text(exchange),
        "tmp_cleanup": dg.MetadataValue.text("tmp_prices 파티션 데이터 삭제 완료"),
    }
    return dg.MaterializeResult(metadata=metadata)


@dg.asset(
    description="가격 데이터를 위한 증권 메타데이터 통합 - 가격 기록을 증권 정보(이름 및 ID 포함)와 연결하는 Silver 계층 프로세스 (TursoDB)",
    partitions_def=daily_exchange_category_partition,
    group_name="CD_TURSO", # CD -> CD_TURSO
    kinds={"turso"}, # postgres -> turso
    tags={
        "domain": "finance",
        "data_tier": "silver",
        "source": "computed", 
        "project": "cd",
        "region": "korea",
        "update_frequency": "daily",
        "asset_type": "integration"
    },
    deps=[cd_digest_price_turso], # cd_digest_price -> cd_digest_price_turso
)
def cd_sync_price_to_security_turso( # Function name updated
    context: dg.AssetExecutionContext, cd_turso: TursoResource # cd_postgres -> cd_turso
) -> dg.MaterializeResult:
    """
    price 테이블의 security_id, name, kor_name 컬럼을
    해당하는 security 테이블의 정보로 업데이트합니다. (TursoDB 버전)
    """

    # ──── 파티션 키 처리 ────
    partition_keys = context.partition_key.keys_by_dimension
    # raw_date_nodash = partition_keys["date"].replace("-", "")  # 예: "20250323" -> date_for_db 사용
    exchange = partition_keys["exchange"]
    date_for_db = partition_keys["date"] # YYYY-MM-DD 형식

    columns = ["date", "ticker", "exchange", "security_id", "name", "count"]
    update_statements_for_executescript = [] # SQL 문 저장 (executescript용)
    metadata_rows = []  # 결과 메타데이터에 사용할 데이터 저장

    with cd_turso.get_connection() as conn: # cd_postgres -> cd_turso
        cur = conn.cursor()
        # ──── security_id가 연결되지 않은 price 레코드를 그룹별로 조회 ────
        price_groups_query = f"""
            SELECT ticker, exchange, COUNT(*) AS cnt
            FROM price
            WHERE security_id IS NULL AND exchange = {sql_quote(exchange)} AND date = {sql_quote(date_for_db)}
            GROUP BY ticker, exchange
            ORDER BY ticker
            """
        cur.execute(price_groups_query)
        price_groups_list = _cursor_to_list_of_dicts(cur)

        # ──── 해당 Exchange의 Security 정보 조회 ────
        securities_query = f"""
            SELECT ticker, exchange, security_id, name, kor_name
            FROM security
            WHERE exchange = {sql_quote(exchange)} AND delisting_date IS NULL
            ORDER BY ticker
            """
        cur.execute(securities_query)
        security_rows_list = _cursor_to_list_of_dicts(cur)
        
        securities_map = { 
            row["ticker"] + row["exchange"]: {
                "ticker": row["ticker"],
                "exchange": row["exchange"],
                "security_id": row["security_id"],
                "name": row["name"],
                "kor_name": row["kor_name"],
            }
            for row in security_rows_list
        }

        # ──── 각 Price 그룹별로 Security 존재 시 업데이트 SQL 문 준비 ────
        for group_row in price_groups_list: 
            ticker = group_row["ticker"]
            ex_val = group_row["exchange"] # ex -> ex_val (내장 함수 충돌 방지)
            cnt = group_row["cnt"]
            key = ticker + ex_val
            
            if key in securities_map: 
                sec = securities_map[key] 
                
                update_sql = (
                    f"UPDATE price "
                    f"SET security_id = {sql_quote(sec['security_id'])}, "
                    f"name = {sql_quote(sec['name'])}, "
                    f"kor_name = {sql_quote(sec['kor_name'])} "
                    f"WHERE ticker = {sql_quote(ticker)} AND exchange = {sql_quote(ex_val)} AND date = {sql_quote(date_for_db)};"
                )
                update_statements_for_executescript.append(update_sql)
                
                metadata_rows.append(
                    [
                        date_for_db, 
                        sec["ticker"],
                        exchange, 
                        sec["security_id"],
                        sec["name"],
                        cnt,
                    ]
                )

        # ──── 배치 업데이트 실행 (executescript) ────
        if update_statements_for_executescript:
            full_script = "BEGIN;\\n" + "\\n".join(update_statements_for_executescript) + "\\nCOMMIT;"
            try:
                cur.executescript(full_script)
                context.log.info(f"TursoDB에 총 {len(update_statements_for_executescript)}개의 가격-증권 연결 업데이트 실행 완료.")
            except Exception as e:
                context.log.error(f"TursoDB executescript 실행 중 오류: {e}")
                raise 
            
    odf = pd.DataFrame(metadata_rows, columns=columns)

    metadata = {
        "dagster/row_count": dg.MetadataValue.int(len(odf)),
        "처리일자": dg.MetadataValue.text(date_for_db), 
        "거래소": dg.MetadataValue.text(exchange),
    }

    if not odf.empty:
        metadata["업데이트된증권수"] = dg.MetadataValue.int(len(odf))
        metadata["head"] = dg.MetadataValue.md(odf.head().to_markdown(index=False))
        metadata["tail"] = dg.MetadataValue.md(odf.tail().to_markdown(index=False))
    else:
        metadata["result"] = dg.MetadataValue.text("업데이트된 데이터가 없습니다")

    return dg.MaterializeResult(metadata=metadata)
