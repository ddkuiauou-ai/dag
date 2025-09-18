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
    description="CD 프로젝트 BPPEDD 데이터 처리 - Silver Tier",
    partitions_def=daily_exchange_category_partition,
    group_name="CD",
    kinds={"source", "postgres"},
    tags={
        "domain": "finance",
        "data_tier": "silver",
        "source": "pykrx"
    },
    deps=["cd_upsert_company_by_security"],
)
def cd_bppedds(
    context: dg.AssetExecutionContext, cd_postgres: PostgresResource
) -> dg.MaterializeResult:
    """
    일일 bppedd 데이터를 가져옵니다.
    (티커, BPS, PER, PBR, EPS, DIV, DPS)
    """
    # 파티션 키 추출 및 포맷팅 (YYYYMMDD)
    keys = context.partition_key.keys_by_dimension
    date = keys["date"].replace("-", "")
    exchange = keys["exchange"]

    # 최종 컬럼 순서 정의
    final_columns = [
        "date",
        "ticker",
        "bps",
        "per",
        "pbr",
        "eps",
        "div",
        "dps",
        "exchange",
    ]

    # 공통 테이블 스키마 정의
    column_schema = dg.TableSchema(
        columns=[
            dg.TableColumn("date", "date", description="Data date"),
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

    # KONEX는 데이터를 제공하지 않으므로 빈 DataFrame 준비
    if exchange == "KONEX":
        df = pd.DataFrame(columns=final_columns)
    else:
        # pyKRX를 통해 일일 bppedd 데이터 조회
        df = krx.get_market_fundamental_by_ticker(date=date, market=exchange)
        time.sleep(0.2)
        df.reset_index(inplace=True)
        context.log.info(f"Fetching market fundamental data for {exchange} on {date}")

        # pykrx가 제공하는 컬럼 순서에 맞춰 재정의
        df.columns = ["ticker", "bps", "per", "pbr", "eps", "div", "dps"]

        # 날짜, 거래소 컬럼 추가 및 정리
        df["date"] = pd.to_datetime(date, format=DATE_FORMAT)
        df["exchange"] = exchange
        df["ticker"] = df["ticker"].astype(str)
        df = df[final_columns]

    # ──── 빈 데이터 처리 ────
    if df.empty:
        metadata = {
            "Number of records": dg.MetadataValue.int(0),
            "Preview": dg.MetadataValue.md(df.head(10).to_markdown(index=False)),
            "dagster/column_schema": column_schema,
        }
        return dg.MaterializeResult(metadata=metadata)

    # ──── PostgreSQL tmp_bppedds 테이블에 데이터 저장 ────
    with cd_postgres.get_connection() as conn:
        with conn.cursor() as cur:
            # 기존 파티션 데이터 삭제
            cur.execute(
                "DELETE FROM tmp_bppedds WHERE date = %s AND exchange = %s",
                (df["date"].iloc[0], exchange),
            )
            # 일괄 INSERT
            data = [
                (
                    row.date,
                    row.ticker,
                    row.bps,
                    row.per,
                    row.pbr,
                    row.eps,
                    row.div,
                    row.dps,
                    row.exchange,
                )
                for row in df.itertuples(index=False)
            ]
            execute_batch(
                cur,
                """
                INSERT INTO tmp_bppedds
                (date, ticker, bps, per, pbr, eps, div, dps, exchange)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                data,
                page_size=500,
            )
        conn.commit()

    metadata = {
        "Number of records": dg.MetadataValue.int(len(df)),
        "Preview": dg.MetadataValue.md(df.head(10).to_markdown(index=False)),
        "dagster/column_schema": column_schema,
    }
    return dg.MaterializeResult(metadata=metadata)


@dg.asset(
    description="CD 프로젝트 BPPEDD 데이터 요약 - Gold Tier",
    partitions_def=daily_exchange_category_partition,
    group_name="CD",
    kinds={"postgres"},
    tags={
        "domain": "finance",
        "data_tier": "gold",
        "source": "internal"
    },
    deps=[cd_bppedds],
)
def cd_digest_bppedds(
    context: dg.AssetExecutionContext,
    cd_postgres: PostgresResource,
) -> dg.MaterializeResult:
    """
    매일 업데이트 된 BPPEDD DB로 업데이트합니다.
    - 주식 이름은 없으며, sync에서 주식 기준, security에 이름 업데이트 후
      여기서는 security 기준으로 업데이트합니다.
    - 처리 완료 후 tmp_bppedds의 해당 파티션 데이터를 삭제하여 용량을 절약합니다.
    """
    # ──── 파티션 키 및 날짜 처리 ────
    keys = context.partition_key.keys_by_dimension
    date = keys["date"].replace("-", "")
    exchange = keys["exchange"]

    date_dt = pd.to_datetime(date, format="%Y%m%d")
    formatted_date = date_dt.strftime("%Y-%m-%d")
    formatted_date_iso = date_dt.isoformat()

    # KONEX 거래소는 처리하지 않음
    if exchange == "KONEX":
        return dg.MaterializeResult(
            metadata={
                "Date": dg.MetadataValue.text(formatted_date),
                "Result": dg.MetadataValue.text("KONEX 제외 - 데이터 없음"),
            }
        )

    # ──── PostgreSQL tmp_bppedds에서 데이터 조회 (SQLAlchemy 엔진 사용) ────
    select_query = """
        SELECT date, ticker, bps, per, pbr, eps, div, dps, exchange
        FROM tmp_bppedds
        WHERE date = %s AND exchange = %s
          AND NOT (bps = 0 AND pbr = 0 AND eps = 0 AND div = 0 AND dps = 0)
    """
    engine = cd_postgres.get_sqlalchemy_engine()
    df = pd.read_sql_query(select_query, engine, params=(formatted_date_iso, exchange))

    if df.empty:
        # 데이터가 없어도 tmp_bppedds에서 해당 파티션 삭제
        with cd_postgres.get_connection() as pg_conn:
            cursor = pg_conn.cursor()
            cursor.execute(
                """
                DELETE FROM tmp_bppedds
                WHERE date = %s AND exchange = %s
                """,
                (formatted_date_iso, exchange),
            )
            pg_conn.commit()
            
        return dg.MaterializeResult(
            metadata={
                "Date": dg.MetadataValue.text(formatted_date),
                "Result": dg.MetadataValue.text(
                    "휴장일 등의 이유로 bppedd 데이터가 없습니다. tmp_bppedds 정리 완료."
                ),
            }
        )

    # ──── ticker 컬럼 처리: 문자열 변환 후 6자리 포맷 (ticker는 그대로 유지) ────
    if "ticker" in df.columns:
        df["ticker"] = df["ticker"].astype(str).str.zfill(6)

    # ──── 파티션 정보 컬럼 할당 ────
    df = df.assign(
        date=formatted_date_iso,
        exchange=exchange,
        year=date_dt.year,
        month=date_dt.month,
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
    df = df[expected_columns]
    insert_tuples = [tuple(row) for row in df.to_numpy()]

    # ──── PostgreSQL "bppedd" 테이블 업데이트 ────
    # Sort tuples by ticker for consistent locking order
    insert_tuples.sort(key=lambda x: x[2])  # ticker is at index 2

    with cd_postgres.get_connection() as pg_conn:
        cursor = pg_conn.cursor()
        try:
            # 1. Delete existing records from the main 'bppedd' table for the current partition
            # Assuming the target table is named 'bppedd'
            delete_query_main = "DELETE FROM bppedd WHERE date = %s AND exchange = %s"
            cursor.execute(delete_query_main, (formatted_date_iso, exchange))
            context.log.info(
                f"Deleted existing records from bppedd for {formatted_date_iso}, {exchange}"
            )

            # 2. Insert new records into the main 'bppedd' table
            if insert_tuples:
                insert_query_main = """
                    INSERT INTO bppedd (date, exchange, ticker, bps, per, pbr, eps, div, dps, year, month)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                execute_batch(
                    cursor, insert_query_main, insert_tuples, page_size=500
                )  # psycopg2.extras.execute_batch
                context.log.info(
                    f"Inserted {len(insert_tuples)} records into bppedd for {formatted_date_iso}, {exchange}"
                )

            # 3. Clean up tmp_bppedds for the current partition
            delete_query_tmp = (
                "DELETE FROM tmp_bppedds WHERE date = %s AND exchange = %s"
            )
            cursor.execute(delete_query_tmp, (formatted_date_iso, exchange))
            context.log.info(
                f"Cleaned up tmp_bppedds for {formatted_date_iso}, {exchange}"
            )

            pg_conn.commit()  # Commit all changes (delete from bppedd, insert into bppedd, delete from tmp_bppedds)

        except Exception as e:
            context.log.error(f"Error during Bppedd table update and cleanup: {e}")
            pg_conn.rollback()
            raise # Re-raise the exception for Dagster to handle
        finally:
            cursor.close()

    # ──── 결과 메타데이터 생성 ────
    metadata = {
        "Date": dg.MetadataValue.text(formatted_date),
        "Exchange": dg.MetadataValue.text(exchange),
        "Number of records processed": dg.MetadataValue.int(len(df)),
        "Preview": dg.MetadataValue.md(df.head(10).to_markdown(index=False)),
    }
    return dg.MaterializeResult(metadata=metadata)


@dg.asset(
    description="CD 프로젝트 BPPEDD-Security 동기화 - Gold Tier",
    partitions_def=daily_exchange_category_partition,
    group_name="CD",
    kinds={"postgres"},
    tags={
        "domain": "finance",
        "data_tier": "gold",
        "source": "internal"
    },
    deps=[cd_digest_bppedds],
)
def cd_sync_bppedds_to_security(
    context: dg.AssetExecutionContext,
    cd_postgres: PostgresResource,
):
    """
    bppedd에 security 테이블과 연결 작업을 수행합니다.
    (한국 시장 대상)
    """

    # ──── 파티션 키 처리 ────
    partition_keys = context.partition_key.keys_by_dimension
    raw_date = partition_keys["date"].replace("-", "")  # 예: "20250323"
    exchange_str = partition_keys["exchange"]
    partition_date = datetime.strptime(raw_date, "%Y%m%d")

    # ──── KONEX 제외 ────
    if exchange_str == "KONEX":
        metadata = {
            "Date": dg.MetadataValue.text(raw_date),
            "Exchange": dg.MetadataValue.text(exchange_str),
            "Result": dg.MetadataValue.text("KONEX는 제외되었습니다"),
        }
        return dg.MaterializeResult(metadata=metadata)

    # ──── 초기 변수 설정 ────
    columns = ["date", "ticker", "exchange", "security_id", "name", "count"]
    data = []

    with cd_postgres.get_connection() as conn:
        with conn.cursor() as cursor:
            # First query unlinked records to update
            cursor.execute(
                """
                SELECT b.ticker, b.exchange 
                FROM bppedd b
                LEFT JOIN security s ON b.ticker = s.ticker AND b.exchange = s.exchange
                WHERE b.date = %s
                AND b.exchange = %s
                AND b.security_id IS NULL
                AND s.security_id IS NOT NULL
                AND s.delisting_date IS NULL
                ORDER BY b.ticker  -- Add consistent ordering to prevent deadlocks
                """,
                (partition_date, exchange_str),
            )
            
            unlinked_records = cursor.fetchall()
            
            if not unlinked_records:
                # No records to update
                metadata = {
                    "Date": dg.MetadataValue.text(raw_date),
                    "Exchange": dg.MetadataValue.text(exchange_str),
                    "Result": dg.MetadataValue.text(
                        "모든 주식과 시가총액이 이미 연결 되어 있습니다"
                    ),
                }
                return dg.MaterializeResult(metadata=metadata)
            
            # Get securities data
            cursor.execute(
                """
                SELECT ticker, exchange, security_id, name, kor_name
                FROM security
                WHERE exchange = %s AND delisting_date IS NULL
                ORDER BY ticker  -- Add consistent ordering to prevent deadlocks
                """,
                (exchange_str,),
            )
            
            securities = {
                (row[0], row[1]): {
                    "security_id": row[2],
                    "name": row[3],
                    "kor_name": row[4],
                }
                for row in cursor.fetchall()
            }
            
            # Prepare update parameters
            update_params = []
            
            for ticker, exchange in unlinked_records:
                key = (ticker, exchange)
                if key in securities:
                    sec = securities[key]
                    update_params.append(
                        (
                            sec["security_id"],
                            sec["name"],
                            sec["kor_name"],
                            ticker,
                            exchange,
                            partition_date,
                        )
                    )
            
            # ──── 배치 업데이트 실행 ────
            if update_params:
                # Sort by ticker and exchange for consistent lock ordering
                update_params.sort(key=lambda x: (x[3], x[4]))  # ticker, exchange
                # Bulk update using execute_batch for better performance
                execute_batch(
                    cursor,
                    """
                    UPDATE bppedd
                    SET security_id = %s, name = %s, kor_name = %s
                    WHERE ticker = %s AND exchange = %s AND date = %s
                    """,
                    update_params,
                    page_size=500,
                )
                conn.commit()
            
            # Query updated data for metadata
            cursor.execute(
                """
                SELECT b.ticker, b.exchange, b.security_id, s.name
                FROM bppedd b
                JOIN security s ON b.security_id = s.security_id
                WHERE b.date = %s AND b.exchange = %s
                ORDER BY b.ticker
                """,
                (partition_date, exchange_str),
            )
            
            result_rows = cursor.fetchall()
            
            for row in result_rows:
                data.append(
                    [
                        raw_date,  # 원본 날짜
                        row[0],    # 티커
                        row[1],    # exchange
                        row[2],    # security_id
                        row[3],    # name
                        1,         # each row gets a count of 1
                    ]
                )


    odf = pd.DataFrame(data, columns=columns)

    # ──── 결과 메타데이터 구성 ────
    if not odf.empty:
        total_count = odf["count"].sum()
        metadata = {
            "Date": dg.MetadataValue.text(raw_date),
            "Exchange": dg.MetadataValue.text(exchange_str),
            "# of syncing to security": dg.MetadataValue.text(str(total_count)),
            "Preview head": dg.MetadataValue.md(odf.head().to_markdown(index=False)),
            "Preview tail": dg.MetadataValue.md(odf.tail().to_markdown(index=False)),
        }
    else:
        metadata = {
            "Date": dg.MetadataValue.text(raw_date),
            "Exchange": dg.MetadataValue.text(exchange_str),
            "Result": dg.MetadataValue.text(
                "모든 주식과 시가총액이 이미 연결 되어 있습니다"
            ),
        }

    return dg.MaterializeResult(metadata=metadata)
