import time
from datetime import datetime
import io

import dagster as dg
import pandas as pd
from pykrx import stock as krx
from psycopg2.extras import execute_batch

from .cd_constants import DATE_FORMAT
from .partitions import daily_exchange_category_partition
from .resources import PostgresResource


@dg.asset(
    description="한국거래소(KRX)에서 일별 OHLCV 가격 데이터 수집 - CD 주식 분석 파이프라인을 위한 Bronze 계층 종합 거래 데이터",
    partitions_def=daily_exchange_category_partition,
    kinds={"source", "postgres"},
    group_name="CD",
    tags={
        "domain": "finance",
        "data_tier": "bronze",
        "source": "pykrx",
        "project": "cd",
        "region": "korea",
        "update_frequency": "daily",
        "asset_type": "ingestion"
    },
    deps=["cd_upsert_company_by_security"],
)
def cd_prices(
    context: dg.AssetExecutionContext, cd_postgres: PostgresResource
) -> dg.MaterializeResult:
    """
    일일 거래소 가격 정보를 가져와 수정주가(티커, 시가, 고가, 저가, 종가, 거래량, 거래금액, 등락률)를 PostgreSQL에 저장합니다.
    """
    # 파티션 키 추출 및 포맷팅
    keys = context.partition_key.keys_by_dimension
    date = keys["date"].replace("-", "")
    exchange = keys["exchange"]

    # OHLCV 데이터 조회 및 전처리
    df = krx.get_market_ohlcv(date=date, market=exchange).reset_index()
    time.sleep(0.2)
    context.log.info(f"Retrieved {len(df)} rows from KRX for {date} {exchange}")
    context.log.info(f"df.head():\n{df.head()}")
    context.log.info(f"df.columns: {df.tail()}")

    df.columns = [
        "ticker",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "transaction",
        "rate",
        "marketcap" # marketcap은 사용하지 않지만, KRX API에서 반환되는 컬럼. 2025-05-30 기준.
    ]
    df["date"] = pd.to_datetime(date, format=DATE_FORMAT)
    df["exchange"] = exchange
    df = df[
        [
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
    ]

    # PostgreSQL tmp_prices 테이블에 데이터 저장
    column_schema = dg.TableSchema(
        columns=[
            dg.TableColumn("date", "date", description="Data date"),
            dg.TableColumn("ticker", "string", description="Ticker symbol"),
            dg.TableColumn("open", "float", description="Opening price"),
            dg.TableColumn("high", "float", description="High price"),
            dg.TableColumn("low", "float", description="Low price"),
            dg.TableColumn("close", "float", description="Closing price"),
            dg.TableColumn("volume", "bigint", description="Trading volume"),
            dg.TableColumn("transaction", "bigint", description="Transaction amount"),
            dg.TableColumn("rate", "float", description="Rate of change"),
            dg.TableColumn("exchange", "string", description="Exchange identifier"),
        ]
    )
    with cd_postgres.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM tmp_prices WHERE date = %s AND exchange = %s",
                (df["date"].iloc[0], exchange),
            )
            data = [
                (
                    row.date,
                    row.ticker,
                    row.open,
                    row.high,
                    row.low,
                    row.close,
                    row.volume,
                    row.transaction,
                    row.rate,
                    row.exchange,
                )
                for row in df.itertuples(index=False)
            ]
            execute_batch(
                cur,
                """
                INSERT INTO tmp_prices
                (date, ticker, open, high, low, close, volume, transaction, rate, exchange)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                data,
                page_size=500,
            )
        conn.commit()
    record_count = len(df)
    preview_df = df.head(10)
    return dg.MaterializeResult(
        metadata={
            "dagster/row_count": dg.MetadataValue.int(record_count),
            "처리일자": dg.MetadataValue.text(pd.to_datetime(date, format="%Y%m%d").strftime("%Y-%m-%d")),
            "거래소": dg.MetadataValue.text(exchange),
            "preview": dg.MetadataValue.md(preview_df.to_markdown(index=False)),
            "dagster/column_schema": column_schema,
        }
    )


@dg.asset(
    description="가격 데이터 검증 및 임시 테이블에서 영구 테이블로 저장 - OHLCV 데이터를 검증하고 tmp_prices에서 price 테이블로 이동하는 Silver 계층 데이터 처리",
    partitions_def=daily_exchange_category_partition,
    group_name="CD",
    kinds={"postgres"},
    tags={
        "domain": "finance",
        "data_tier": "silver", 
        "source": "computed",
        "project": "cd",
        "region": "korea",
        "update_frequency": "daily",
        "asset_type": "processing"
    },
    deps=[cd_prices],
)
def cd_digest_price(
    context: dg.AssetExecutionContext, cd_postgres: PostgresResource
) -> dg.MaterializeResult:
    """
    매일 업데이트 된 가격 데이터를 PostgreSQL의 price 테이블에 업데이트합니다.
    처리 완료 후 tmp_prices의 해당 파티션 데이터를 삭제하여 용량을 절약합니다.
    """
    # ──── 파티션 키 및 날짜 처리 ────
    partition_keys = context.partition_key.keys_by_dimension
    date = partition_keys["date"].replace("-", "")
    exchange = partition_keys["exchange"]

    date_dt = pd.to_datetime(date, format="%Y%m%d")
    # PostgreSQL에 저장된 날짜 형식과 비교하기 위한 문자열 (예: "2025-03-23")
    formatted_date = date_dt.strftime("%Y-%m-%d")
    # PostgreSQL 업데이트용 ISO 형식 (예: "2025-03-23T00:00:00")
    formatted_date_iso = date_dt.isoformat()

    # tmp_prices 테이블에서 데이터 조회 (SQLAlchemy 엔진 사용)
    select_query = """
        SELECT date, ticker, open, high, low, close, volume, transaction, rate, exchange
        FROM tmp_prices
        WHERE date = %s AND exchange = %s
    """
    engine = cd_postgres.get_sqlalchemy_engine()
    prices_df = pd.read_sql_query(select_query, engine, params=(formatted_date_iso, exchange))

    # 데이터가 없는 경우 tmp_prices 정리 후 반환
    if prices_df.empty:
        with cd_postgres.get_connection() as pg_conn:
            cursor = pg_conn.cursor()
            cursor.execute(
                """
                DELETE FROM tmp_prices
                WHERE date = %s AND exchange = %s
                """,
                (formatted_date_iso, exchange),
            )
            pg_conn.commit()
            
        return dg.MaterializeResult(
            metadata={
                "Date": dg.MetadataValue.text(formatted_date),
                "Result": dg.MetadataValue.text(
                    "prices 테이블에서 데이터를 조회할 수 없습니다. tmp_prices 정리 완료."
                ),
            }
        )

    # ──── 데이터 유효성 체크 ────
    # 검증 대상 컬럼
    cols = ["open", "high", "low", "close", "volume", "transaction", "rate"]
    valid_mask = (prices_df[cols] == 0).all(axis=1)
    valid_df = prices_df[~valid_mask]

    # 유효한 데이터가 전혀 없는 경우 (예: 휴장일) - tmp_prices 정리 후 반환
    if valid_df.empty:
        with cd_postgres.get_connection() as pg_conn:
            cursor = pg_conn.cursor()
            cursor.execute(
                """
                DELETE FROM tmp_prices
                WHERE date = %s AND exchange = %s
                """,
                (formatted_date_iso, exchange),
            )
            pg_conn.commit()
            
        return dg.MaterializeResult(
            metadata={
                "Date": dg.MetadataValue.text(formatted_date),
                "Result": dg.MetadataValue.text(
                    "휴장일 등의 이유로 가격이 없습니다. tmp_prices 정리 완료."
                ),
            }
        )

    # 일부만 유효한 경우 tmp_prices 정리 후 에러 발생
    if valid_df.shape[0] != prices_df.shape[0]:
        with cd_postgres.get_connection() as pg_conn:
            cursor = pg_conn.cursor()
            cursor.execute(
                """
                DELETE FROM tmp_prices
                WHERE date = %s AND exchange = %s
                """,
                (formatted_date_iso, exchange),
            )
            pg_conn.commit()
            
        error_msg = (
            f"휴장일이 아님에도 가격 정보가 일부만 유효합니다. "
            f"유효한 행: {valid_df.shape[0]}, 전체 행: {prices_df.shape[0]}, "
            f"exchange: {prices_df['exchange'].iloc[0]}, date: {prices_df['date'].iloc[0]}. "
            f"tmp_prices 정리 완료."
        )
        context.log.error(error_msg)
        raise ValueError(error_msg)

    df_to_insert = valid_df.copy()

    # ──── 데이터 전처리 ────
    if "ticker" in df_to_insert.columns:
        df_to_insert["ticker"] = df_to_insert["ticker"].astype(str).str.zfill(6)

    df_to_insert = df_to_insert.assign(
        date=formatted_date_iso,
        exchange=exchange,
        year=date_dt.year,
        month=date_dt.month,
    )
    expected_columns = [
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
    df_to_insert = df_to_insert[expected_columns]

    # ──── PostgreSQL price 테이블 업데이트 ────
    insert_tuples = [tuple(row) for row in df_to_insert.to_numpy()]
    
    # Sort tuples by ticker to ensure consistent locking order
    insert_tuples.sort(key=lambda x: x[2])  # ticker is at index 2

    with cd_postgres.get_connection() as pg_conn:
        cursor = pg_conn.cursor()
        try:
            # 1. Delete existing records for the date/exchange
            cursor.execute(
                """
                DELETE FROM price
                WHERE date = %s AND exchange = %s
                """,
                (formatted_date_iso, exchange),
            )
            context.log.info(f"Deleted existing records from price for {formatted_date_iso}, {exchange}")

            # 2. Bulk insert using COPY FROM STDIN for optimal performance
            if insert_tuples:
                # Prepare data in CSV format for COPY
                csv_buffer = io.StringIO()
                # Construct CSV string, ensuring correct order and handling of None for numeric types if necessary
                for row_tuple in insert_tuples:
                    # Ensure all elements are strings for join, handle None for numeric if they can occur
                    # and should be represented as NULL in DB. For now, assuming all values are present.
                    csv_buffer.write(",".join(map(str, row_tuple)) + "\n")
                csv_buffer.seek(0) # Rewind buffer to the beginning

                # Columns in the order they appear in insert_tuples / expected_columns
                # (date, exchange, ticker, open, high, low, close, volume, transaction, rate, year, month)
                copy_query = """
                    COPY price (date, exchange, ticker, open, high, low, close, volume, transaction, rate, year, month)
                    FROM STDIN WITH (FORMAT CSV, DELIMITER ',')
                """
                cursor.copy_expert(sql=copy_query, file=csv_buffer)
                context.log.info(f"Bulk inserted {len(insert_tuples)} records into price using COPY for {formatted_date_iso}, {exchange}")
                
            # 3. Successfully processed, now delete from tmp_prices
            cursor.execute(
                """
                DELETE FROM tmp_prices
                WHERE date = %s AND exchange = %s
                """,
                (formatted_date_iso, exchange),
            )
            context.log.info(f"Cleaned up tmp_prices for {formatted_date_iso}, {exchange}")
            
            pg_conn.commit()
            context.log.info(f"Successfully processed and committed changes for {formatted_date} {exchange}")
            
        except Exception as e:
            pg_conn.rollback()
            context.log.error(f"Error processing price data: {str(e)}")
            # 에러 발생 시에도 tmp_prices 정리
            try:
                cursor.execute(
                    """
                    DELETE FROM tmp_prices
                    WHERE date = %s AND exchange = %s
                    """,
                    (formatted_date_iso, exchange),
                )
                pg_conn.commit()
                context.log.info(f"Cleaned up tmp_prices after error for {formatted_date} {exchange}")
            except Exception as cleanup_error:
                context.log.error(f"Failed to cleanup tmp_prices: {str(cleanup_error)}")
            raise

    # ──── 결과 요약 및 MaterializeResult 반환 ────
    preview_df = df_to_insert.head(10)
    row_count = len(df_to_insert)
    metadata = {
        "dagster/row_count": dg.MetadataValue.int(row_count),
        "preview": dg.MetadataValue.md(preview_df.to_markdown(index=False)),
        "처리일자": dg.MetadataValue.text(formatted_date),
        "거래소": dg.MetadataValue.text(exchange),
        "tmp_cleanup": dg.MetadataValue.text("tmp_prices 파티션 데이터 삭제 완료"),
    }
    return dg.MaterializeResult(metadata=metadata)


@dg.asset(
    description="가격 데이터를 위한 증권 메타데이터 통합 - 가격 기록을 증권 정보(이름 및 ID 포함)와 연결하는 Silver 계층 프로세스",
    partitions_def=daily_exchange_category_partition,
    group_name="CD",
    kinds={"postgres"},
    tags={
        "domain": "finance",
        "data_tier": "silver",
        "source": "computed", 
        "project": "cd",
        "region": "korea",
        "update_frequency": "daily",
        "asset_type": "integration"
    },
    deps=[cd_digest_price],
)
def cd_sync_price_to_security(
    context: dg.AssetExecutionContext, cd_postgres: PostgresResource
) -> dg.MaterializeResult:
    """
    price 테이블의 security_id, name, kor_name 컬럼을
    해당하는 security 테이블의 정보로 업데이트합니다.
    (한국 시장 대상)
    """

    # ──── 파티션 키 처리 ────
    partition_keys = context.partition_key.keys_by_dimension
    raw_date = partition_keys["date"].replace("-", "")  # 예: "20250323"
    exchange = partition_keys["exchange"]

    columns = ["date", "ticker", "exchange", "security_id", "name", "count"]
    update_params = []  # 업데이트 쿼리 파라미터 저장
    metadata_rows = []  # 결과 메타데이터에 사용할 데이터 저장

    with cd_postgres.get_connection() as conn:
        with conn.cursor() as cursor:
            # ──── security_id가 연결되지 않은 price 레코드를 그룹별로 조회 ────
            cursor.execute(
                """
                SELECT ticker, exchange, COUNT(*) AS cnt
                FROM price
                WHERE security_id IS NULL AND exchange = %s
                GROUP BY ticker, exchange
                ORDER BY ticker  -- Add consistent ordering to prevent deadlocks
                """,
                (exchange,),
            )
            price_groups = cursor.fetchall()  # 각 튜플: (ticker, exchange, cnt)

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
            # ticker+exchange를 키로 Security 정보를 딕셔너리 컴프리헨션으로 구성
            securities = {
                row[0]
                + row[1]: {
                    "ticker": row[0],
                    "exchange": row[1],
                    "security_id": row[2],
                    "name": row[3],
                    "kor_name": row[4],
                }
                for row in security_rows
            }

            # ──── 각 Price 그룹별로 Security 존재 시 업데이트 파라미터 준비 ────
            for ticker, ex, cnt in price_groups:
                key = ticker + ex
                if key in securities:
                    sec = securities[key]
                    update_params.append(
                        (
                            sec["security_id"],
                            sec["name"],
                            sec["kor_name"],
                            ticker,
                            ex,
                            pd.to_datetime(raw_date, format="%Y%m%d").isoformat(),
                        )
                    )
                    metadata_rows.append(
                        [
                            raw_date,
                            sec["ticker"],
                            exchange,
                            sec["security_id"],
                            sec["name"],
                            cnt,
                        ]
                    )

            # ──── 배치 업데이트 실행 ────
            if update_params:
                # Sort by ticker and exchange for consistent lock ordering
                update_params.sort(key=lambda x: (x[3], x[4]))  # ticker, exchange
                # Bulk update using execute_batch for better performance
                execute_batch(
                    cursor,
                    """
                    UPDATE price
                    SET security_id = %s, name = %s, kor_name = %s
                    WHERE ticker = %s AND exchange = %s AND date = %s
                    """,
                    update_params,
                    page_size=500,
                )
                conn.commit()

    odf = pd.DataFrame(metadata_rows, columns=columns)

    metadata = {
        "dagster/row_count": dg.MetadataValue.int(len(odf)),
        "처리일자": dg.MetadataValue.text(raw_date),
        "거래소": dg.MetadataValue.text(exchange),
    }

    if not odf.empty:
        metadata["업데이트된증권수"] = dg.MetadataValue.int(len(odf))
        metadata["head"] = dg.MetadataValue.md(odf.head().to_markdown(index=False))
        metadata["tail"] = dg.MetadataValue.md(odf.tail().to_markdown(index=False))
    else:
        metadata["result"] = dg.MetadataValue.text("업데이트된 데이터가 없습니다")

    return dg.MaterializeResult(metadata=metadata)
