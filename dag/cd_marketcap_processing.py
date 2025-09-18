import time
from datetime import datetime

import dagster as dg
import pandas as pd
from pykrx import stock as krx

from .cd_constants import DATE_FORMAT
from .partitions import daily_exchange_category_partition
from .resources import PostgresResource
from psycopg2.extras import execute_batch


@dg.asset(
    partitions_def=daily_exchange_category_partition,
    group_name="CD",
    kinds={"source", "postgres"},
    tags={"data_tier": "bronze", "domain": "finance", "source": "pykrx"},
    deps=["cd_upsert_company_by_security"],
)
def cd_marketcaps(
    context: dg.AssetExecutionContext, cd_postgres: PostgresResource
) -> dg.MaterializeResult:
    """
    일일 거래소 시총 정보를 가져옵니다.
    (티커, 시가총액, 거래대금, 상장주식수, 외국인보유수)
    """
    # 파티션 키 추출 및 포맷팅 (YYYYMMDD)
    keys = context.partition_key.keys_by_dimension
    date = keys["date"].replace("-", "")
    exchange = keys["exchange"]

    # 시총 데이터 조회 및 전처리
    df = krx.get_market_cap(date=date, market=exchange)
    time.sleep(0.2)
    df.reset_index(inplace=True)

    # 컬럼명 재지정 및 추가 컬럼 생성
    COLUMN_NAMES = ["ticker", "close", "marketcap", "volume", "transaction", "shares"]
    df.columns = COLUMN_NAMES
    df["ticker"] = df["ticker"].astype(str)
    df["date"] = pd.to_datetime(date, format=DATE_FORMAT)
    df["exchange"] = exchange

    # 최종 컬럼 순서 설정
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

    # PostgreSQL tmp_marketcaps 테이블에 데이터 저장
    column_schema = dg.TableSchema(
        columns=[
            dg.TableColumn("date", "date", description="Data date"),
            dg.TableColumn("ticker", "string", description="Ticker symbol"),
            dg.TableColumn("close", "float", description="Closing price"),
            dg.TableColumn("marketcap", "bigint", description="Market capitalization"),
            dg.TableColumn("volume", "bigint", description="Trading volume"),
            dg.TableColumn("transaction", "bigint", description="Transaction amount"),
            dg.TableColumn("shares", "bigint", description="Number of shares"),
            dg.TableColumn("exchange", "string", description="Exchange identifier"),
        ]
    )
    with cd_postgres.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM tmp_marketcaps WHERE date = %s AND exchange = %s",
                (df["date"].iloc[0], exchange),
            )
            data = [
                (
                    row.date,
                    row.ticker,
                    row.close,
                    row.marketcap,
                    row.volume,
                    row.transaction,
                    row.shares,
                    row.exchange,
                )
                for row in df.itertuples(index=False)
            ]
            execute_batch(
                cur,
                """
                INSERT INTO tmp_marketcaps
                (date, ticker, close, marketcap, volume, transaction, shares, exchange)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                data,
                page_size=500,
            )
        conn.commit()
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
    group_name="CD",
    kinds={"postgres"},
    tags={"data_tier": "silver", "domain": "finance", "source": "pykrx"},
    deps=[cd_marketcaps],
)
def cd_digest_marketcaps(
    context: dg.AssetExecutionContext, cd_postgres: PostgresResource
) -> dg.MaterializeResult:
    """
    매일 업데이트 된 시총 데이터를 PostgreSQL의 "Marketcap" 테이블에 업데이트합니다.
    처리 완료 후 tmp_marketcaps의 해당 파티션 데이터를 삭제하여 용량을 절약합니다.
    """
    # ──── 파티션 키 및 날짜 처리 ────
    keys = context.partition_key.keys_by_dimension
    date = keys["date"].replace("-", "")
    exchange = keys["exchange"]

    date_dt = pd.to_datetime(date, format="%Y%m%d")
    formatted_date = date_dt.strftime("%Y-%m-%d")
    formatted_date_iso = date_dt.isoformat()

    # tmp_marketcaps 테이블에서 데이터 조회 (SQLAlchemy 엔진 사용)
    select_query = """
        SELECT date, ticker, close, marketcap, volume, transaction, shares, exchange
        FROM tmp_marketcaps
        WHERE date = %s AND exchange = %s
    """
    engine = cd_postgres.get_sqlalchemy_engine()
    marketcaps_df = pd.read_sql_query(select_query, engine, params=(formatted_date_iso, exchange))

    # 데이터가 없는 경우 tmp_marketcaps 정리 후 반환
    if marketcaps_df.empty:
        with cd_postgres.get_connection() as pg_conn:
            cursor = pg_conn.cursor()
            cursor.execute(
                """
                DELETE FROM tmp_marketcaps
                WHERE date = %s AND exchange = %s
                """,
                (formatted_date_iso, exchange),
            )
            pg_conn.commit()
            
        return dg.MaterializeResult(
            metadata={
                "Date": dg.MetadataValue.text(formatted_date),
                "Result": dg.MetadataValue.text(
                    "marketcaps 테이블에서 데이터를 조회할 수 없습니다. tmp_marketcaps 정리 완료."
                ),
            }
        )

    # ──── 데이터 유효성 체크 ────
    # close, marketcap, volume, transaction 컬럼이 모두 0인 경우를 유효하지 않은 데이터로 판단
    cols = ["close", "marketcap", "volume", "transaction"]
    valid_mask = (marketcaps_df[cols] == 0).all(axis=1)
    valid_df = marketcaps_df[~valid_mask]

    # 유효한 데이터가 전혀 없는 경우 (예: 휴장일) - tmp_marketcaps 정리 후 반환
    if valid_df.empty:
        with cd_postgres.get_connection() as pg_conn:
            cursor = pg_conn.cursor()
            cursor.execute(
                """
                DELETE FROM tmp_marketcaps
                WHERE date = %s AND exchange = %s
                """,
                (formatted_date_iso, exchange),
            )
            pg_conn.commit()
            
        return dg.MaterializeResult(
            metadata={
                "Date": dg.MetadataValue.text(formatted_date),
                "Result": dg.MetadataValue.text(
                    "휴장일 등의 이유로 시총 데이터가 없습니다. tmp_marketcaps 정리 완료."
                ),
            }
        )

    # 일부만 유효한 경우 tmp_marketcaps 정리 후 에러 발생 (정상 거래일에 일부만 데이터가 유효하지 않음)
    if valid_df.shape[0] != marketcaps_df.shape[0]:
        with cd_postgres.get_connection() as pg_conn:
            cursor = pg_conn.cursor()
            cursor.execute(
                """
                DELETE FROM tmp_marketcaps
                WHERE date = %s AND exchange = %s
                """,
                (formatted_date_iso, exchange),
            )
            pg_conn.commit()
            
        error_msg = (
            f"휴장일이 아님에도 시총 데이터의 일부가 유효하지 않습니다. "
            f"유효한 행: {valid_df.shape[0]}, 전체 행: {marketcaps_df.shape[0]}, "
            f"exchange: {marketcaps_df['exchange'].iloc[0]}, date: {marketcaps_df['date'].iloc[0]}. "
            f"tmp_marketcaps 정리 완료."
        )
        context.log.error(error_msg)
        raise ValueError(error_msg)

    # ──── 데이터 전처리 ────
    # ticker 컬럼이 존재하면 문자열로 변환 후 6자리 포맷 적용
    if "ticker" in valid_df.columns:
        valid_df.loc[:, "ticker"] = valid_df["ticker"].astype(str).str.zfill(6)

    # 날짜 및 추가 컬럼 할당
    valid_df = valid_df.assign(
        date=formatted_date_iso,
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
    df_to_insert = valid_df[expected_columns].copy()
    insert_tuples = [tuple(row) for row in df_to_insert.to_numpy()]

    # Sort tuples by ticker for consistent locking order
    insert_tuples.sort(key=lambda x: x[2])  # ticker is at index 2

    # ──── PostgreSQL "marketcap" 테이블 업데이트 및 tmp_marketcaps 정리 ────
    with cd_postgres.get_connection() as pg_conn:
        cursor = pg_conn.cursor()
        try:
            # 1. Delete existing records from the main 'marketcap' table for the current partition
            delete_query_main = "DELETE FROM marketcap WHERE date = %s AND exchange = %s"
            cursor.execute(delete_query_main, (formatted_date_iso, exchange))
            context.log.info(
                f"Deleted existing records from marketcap for {formatted_date_iso}, {exchange}"
            )

            # 2. Insert new records into the main 'marketcap' table
            if insert_tuples:
                insert_query_main = """
                    INSERT INTO marketcap (date, exchange, ticker, marketcap, volume, transaction, shares, year, month)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                execute_batch( # Ensure psycopg2.extras.execute_batch is imported
                    cursor, insert_query_main, insert_tuples, page_size=500
                )
                context.log.info(
                    f"Inserted {len(insert_tuples)} records into marketcap for {formatted_date_iso}, {exchange}"
                )

            # 3. Clean up tmp_marketcaps for the current partition
            delete_query_tmp = (
                "DELETE FROM tmp_marketcaps WHERE date = %s AND exchange = %s"
            )
            cursor.execute(delete_query_tmp, (formatted_date_iso, exchange))
            context.log.info(
                f"Cleaned up tmp_marketcaps for {formatted_date_iso}, {exchange}"
            )

            pg_conn.commit()  # Commit all changes as a single transaction

        except Exception as e:
            context.log.error(f"Error during marketcap table update and cleanup: {e}")
            pg_conn.rollback()
            raise # Re-raise the exception for Dagster to handle
        finally:
            cursor.close() # Ensure cursor is closed

    # ──── 결과 메타데이터 생성 ────
    record_count = len(valid_df)  # Use valid_df for record count
    preview_df = valid_df.head(10) # Use valid_df for preview

    metadata = {
        "Date": dg.MetadataValue.text(formatted_date), # Original date format for display
        "Exchange": dg.MetadataValue.text(exchange),
        "Number of records processed": dg.MetadataValue.int(record_count),
        "Preview": dg.MetadataValue.md(preview_df.to_markdown(index=False)),
    }
    return dg.MaterializeResult(metadata=metadata)


@dg.asset(
    partitions_def=daily_exchange_category_partition,
    group_name="CD",
    kinds={"postgres"},
    tags={"data_tier": "silver", "domain": "finance", "source": "pykrx"},
    deps=[cd_digest_marketcaps],
)
def cd_sync_marketcaps_to_security(
    context: dg.AssetExecutionContext,
    cd_postgres: PostgresResource,
):
    """
    marketcap 테이블의 security_id, name, kor_name 컬럼을
    해당하는 security 테이블의 정보로 업데이트합니다.
    (한국 시장 대상)
    """

    # ──── 파티션 키 처리 ────
    partition_keys = context.partition_key.keys_by_dimension
    raw_date = partition_keys["date"].replace("-", "")  # 예: "20250323"
    exchange = partition_keys["exchange"]
    partition_date = datetime.strptime(raw_date, "%Y%m%d")

    # ──── 초기 변수 설정 ────
    columns = ["date", "ticker", "exchange", "security_id", "name", "count"]
    update_params = []  # 업데이트 쿼리 파라미터 저장
    metadata_rows = []  # 결과 메타데이터에 사용할 데이터 저장

    with cd_postgres.get_connection() as conn:
        with conn.cursor() as cursor:
            # ──── security_id 미연결 marketcap 레코드 그룹 조회 ────
            cursor.execute(
                """
                SELECT ticker, exchange, COUNT(*) AS cnt
                FROM marketcap
                WHERE security_id IS NULL 
                  AND exchange = %s 
                  AND date = %s
                GROUP BY ticker, exchange
                ORDER BY ticker  -- Add consistent ordering to prevent deadlocks
                """,
                (exchange, partition_date),
            )
            marketcap_groups = cursor.fetchall()  # 각 튜플: (ticker, exchange, cnt)

            # ──── 해당 Exchange의 Security 정보 조회 ────
            cursor.execute(
                """
                SELECT ticker, exchange, security_id, name, kor_name
                FROM security
                WHERE exchange = %s AND delisting_date IS NULL
                ORDER BY ticker  -- Add consistent ordering to prevent deadlocks
                """,
                (exchange,),
            )
            security_rows = cursor.fetchall()
            # (ticker, exchange)를 키로 Security 정보를 딕셔너리 컴프리헨션으로 구성
            securities = {
                (row[0], row[1]): {
                    "security_id": row[2],
                    "name": row[3],
                    "kor_name": row[4],
                }
                for row in security_rows
            }

            # ──── 업데이트 파라미터 준비 ────
            for ticker, ex, cnt in marketcap_groups:
                key = (ticker, ex)
                if key in securities:
                    sec = securities[key]
                    update_params.append(
                        (
                            sec["security_id"],
                            sec["name"],
                            sec["kor_name"],
                            ticker,
                            ex,
                            partition_date,
                        )
                    )
                    metadata_rows.append(
                        [raw_date, ticker, ex, sec["security_id"], sec["name"], cnt]
                    )

            # ──── 배치 업데이트 실행 ────
            if update_params:
                # Sort by ticker and exchange for consistent lock ordering
                update_params.sort(key=lambda x: (x[3], x[4]))  # ticker, exchange
                # Bulk update using execute_batch for better performance
                execute_batch(
                    cursor,
                    """
                    UPDATE marketcap
                    SET security_id = %s, name = %s, kor_name = %s
                    WHERE ticker = %s AND exchange = %s AND date = %s
                    """,
                    update_params,
                    page_size=500,
                )
                conn.commit()


    # ──── 결과 및 메타데이터 구성 ────
    if metadata_rows:
        odf = pd.DataFrame(metadata_rows, columns=columns)
        total_count = odf["count"].sum()
        metadata = {
            "Date": dg.MetadataValue.text(raw_date),
            "Exchange": dg.MetadataValue.text(exchange),
            "# of syncing to security": dg.MetadataValue.text(str(total_count)),
            "Preview head": dg.MetadataValue.md(odf.head().to_markdown(index=False)),
            "Preview tail": dg.MetadataValue.md(odf.tail().to_markdown(index=False)),
        }
    else:
        metadata = {
            "Date": dg.MetadataValue.text(raw_date),
            "Exchange": dg.MetadataValue.text(exchange),
            "Result": dg.MetadataValue.text(
                "모든 주식과 시가총액이 이미 연결 되어 있습니다"
            ),
        }

    return dg.MaterializeResult(metadata=metadata)
