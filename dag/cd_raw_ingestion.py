import re
import time
import uuid

import dagster as dg
import pandas as pd
from pykrx import stock as krx
from psycopg2.extras import execute_batch

from .cd_constants import DATE_FORMAT
from .partitions import daily_exchange_category_partition
from .resources import PostgresResource

COUNTRY = "KR"


PATTERN_COMPANY_NAME = re.compile(r"(.+?)([0-9]*우[A-Za-z]*)?(\(전환\))?$")

PATTERNS_SECURITY_TYPE = {
    "스팩": re.compile(r"스팩(\d+호)?$"),
    "리츠": re.compile(r"리츠$"),
    "펀드": re.compile(r"(맥쿼리인프라|맵스리얼티1|유전)$"),
    "전환우선주": re.compile(r"\(전환\)$"),
    "우선주": re.compile(r"[0-9]*우[A-Za-z]*$"),
    # The default pattern for "보통주" is not needed as it's the fallback option
}


def _parse_security_type(s):
    for type, pattern in PATTERNS_SECURITY_TYPE.items():
        if pattern.search(s):
            return type
    return "보통주"


def _parse_company_name(s):
    match = PATTERN_COMPANY_NAME.match(s)
    return match.group(1).strip() if match else s


def _get_company_name(s):
    type = _parse_security_type(s)
    name = s if type in ["스팩", "리츠"] else _parse_company_name(s)
    return name


@dg.asset(
    description="한국 거래소별 일일 상장종목 티커 목록 수집 - Bronze Tier",
    partitions_def=daily_exchange_category_partition,
    kinds={"source", "postgres"},
    group_name="CD",
    tags={
        "domain": "finance",
        "data_tier": "bronze",
        "source": "pykrx"
    }
)
def cd_stockcode(
    context: dg.AssetExecutionContext, cd_postgres: PostgresResource
) -> dg.MaterializeResult:
    """날자별 거래소별 상장종목 리스트를 가져옴. only 티커만"""

    # Extract partition keys
    partition_str = context.partition_key.keys_by_dimension
    date = pd.to_datetime(partition_str["date"])
    exchange = partition_str["exchange"]

    # Fetch tickers from KRX
    stock_codes = krx.get_market_ticker_list(date=date.strftime("%Y%m%d"), market=exchange)
    time.sleep(0.2)

    # Prepare DataFrame
    df = pd.DataFrame(stock_codes, columns=["ticker"])
    df["date"] = date
    df["exchange"] = exchange

    # Delete existing rows for this partition
    delete_query = "DELETE FROM stockcodename WHERE date = %s AND exchange = %s"
    # Bulk insert into PostgreSQL
    insert_query = """
        INSERT INTO stockcodename (date, ticker, exchange)
        VALUES (%s, %s, %s)
    """
    with cd_postgres.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(delete_query, (date, exchange))
            data = [(row.date, row.ticker, row.exchange) for row in df.itertuples(index=False)]
            execute_batch(cur, insert_query, data, page_size=500)
        conn.commit()
    # Return metadata
    return dg.MaterializeResult(
        metadata={
            "dagster/row_count": dg.MetadataValue.int(len(df)),
            "preview": dg.MetadataValue.md(df.head(10).to_markdown(index=False)),
            "dagster/column_schema": dg.TableSchema(
                columns=[
                    dg.TableColumn("ticker", "string", description="Ticker symbol"),
                    dg.TableColumn("date", "date", description="Data date"),
                    dg.TableColumn("exchange", "string", description="Exchange identifier"),
                ]
            ),
        }
    )


@dg.asset(
    description="한국 거래소별 일일 상장종목 이름 정보 수집 - Bronze Tier",
    partitions_def=daily_exchange_category_partition,
    kinds={"source", "postgres"},
    group_name="CD",
    tags={
        "domain": "finance",
        "data_tier": "bronze",
        "source": "pykrx"
    },
    deps=[cd_stockcode],
)
def cd_stockcodenames(
    context: dg.AssetExecutionContext, cd_postgres: PostgresResource
) -> dg.MaterializeResult:
    """날짜별 거래소별 상장종목 리스트를 가져옴. (티커, 이름)"""

    # Extract partition keys
    partition_str = context.partition_key.keys_by_dimension
    date = pd.to_datetime(partition_str["date"])
    exchange = partition_str["exchange"]

    # Fetch base tickers from stockcodename
    select_query = """
        SELECT ticker FROM stockcodename WHERE date = %s AND exchange = %s
    """
    with cd_postgres.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(select_query, (date, exchange))
            tickers = [row[0] for row in cur.fetchall()]
    # Enrich with names
    df = pd.DataFrame(tickers, columns=["ticker"])
    df = df.assign(
        ticker=df["ticker"].astype(str).str.zfill(6),
        name=df["ticker"].apply(krx.get_market_ticker_name),
        date=date,
        exchange=exchange
    )

    # Update existing records with enriched names
    update_query = """
        UPDATE stockcodename
        SET name = %s
        WHERE date = %s AND exchange = %s AND ticker = %s
    """
    with cd_postgres.get_connection() as conn:
        with conn.cursor() as cur:
            data = [
                (row.name, row.date, row.exchange, row.ticker)
                for row in df.itertuples(index=False)
            ]
            execute_batch(cur, update_query, data, page_size=500)
        conn.commit()

    return dg.MaterializeResult(
        metadata={
            "dagter/row_count": dg.MetadataValue.int(len(df)),
            "preview": dg.MetadataValue.md(df.head(10).to_markdown(index=False)),
            "dagster/column_schema": dg.TableSchema(
                columns=[
                    dg.TableColumn("ticker", "string", description="Ticker symbol"),
                    dg.TableColumn("name", "string", description="Ticker name"),
                    dg.TableColumn("date", "date", description="Data date"),
                    dg.TableColumn("exchange", "string", description="Exchange identifier"),
                ]
            ),
        }
    )


@dg.asset(
    description="종목 정보 통합 처리 - 신규상장, 상장폐지, 이름변경 관리 - Bronze Tier",
    partitions_def=daily_exchange_category_partition,
    group_name="CD",
    kinds={"postgres"},
    tags={
        "domain": "finance",
        "data_tier": "bronze",
        "source": "pykrx"
    },
    deps=[cd_stockcodenames],
)
def cd_ingest_stockcodenames(
    context: dg.AssetExecutionContext,
    cd_postgres: PostgresResource,
) -> dg.MaterializeResult:
    """
    신규상장, 상장폐지, 이름변경을 처리합니다.
    - 기존 PostgreSQL의 security 테이블과 병합하여 신규 상장, 상장폐지, 이름변경 여부를 판별합니다.
    - 각 케이스별로 PostgreSQL에 INSERT/UPDATE 쿼리를 실행합니다.
    - 최종 작업 내역(preview, 건수, column_schema 등)을 MaterializeResult로 반환합니다.
    """
    # ────────────── 파티션 키 및 날짜 처리 ──────────────
    partition_keys = context.partition_key.keys_by_dimension
    date = partition_keys["date"].replace("-", "")
    exchange = partition_keys["exchange"]

    # 날짜 포맷팅: 여러 형식의 날짜 문자열 미리 계산 (재사용)
    date_dt = pd.to_datetime(date, format=DATE_FORMAT)
    date_query = date_dt.strftime("%Y-%m-%d")  # SQL 쿼리용 (예: '2023-01-01')
    formatted_date_str = date_dt.strftime(DATE_FORMAT)  # 원래 포맷 (예: '20230101')
    formatted_date = date_dt.isoformat()  # ISO 포맷 (예: '2023-01-01T00:00:00')

    # Fetch enriched stockcodenames from PostgreSQL using SQLAlchemy engine
    select_query = """
        SELECT ticker, name, exchange, date
        FROM stockcodename
        WHERE date = %s AND exchange = %s
    """
    engine = cd_postgres.get_sqlalchemy_engine()
    stock_df = pd.read_sql_query(select_query, engine, params=(date_query, exchange))

    # 데이터가 없는 경우 빈 결과 반환
    if stock_df.empty:
        context.log.info("조회된 stockcodenames 데이터가 없습니다.")
        result_df = pd.DataFrame(columns=["date", "ticker", "name", "type", "exchange"])
        undo_count = name_change_count = listed_count = delisted_count = 0
        listed_md = delisted_md = name_change_md = "No data available."
    else:
        # ────────────── 데이터 전처리 ──────────────
        # ticker를 6자리 문자열로 통일 (예: '1' -> '000001')
        stock_df["ticker"] = stock_df["ticker"].astype(str).str.zfill(6)
        exchange = stock_df["exchange"].iloc[0]

        # ────────────── PostgreSQL에서 기존 security 데이터 조회 ──────────────
        # 필요한 컬럼만 효율적으로 조회
        select_query = f"""
            SELECT 
                security_id, ticker, name, kor_name, exchange, delisting_date
            FROM security
            WHERE exchange = '{exchange}'
        """
        existed_df = pd.read_sql_query(select_query, engine)

        # 결과가 없는 경우 빈 DataFrame 생성 (에러 방지)
        if existed_df.empty:
            existed_df = pd.DataFrame(
                columns=[
                    "security_id",
                    "ticker",
                    "name",
                    "kor_name",
                    "exchange",
                    "delisting_date",
                ]
            )

        # ────────────── 데이터 병합 및 케이스 분리 ──────────────
        # outer join으로 신규/삭제/변경 케이스 식별 (indicator=True)
        merged_df = pd.merge(
            stock_df,
            existed_df,
            how="outer",
            on=["ticker", "exchange"],
            indicator=True,
        )

        # 신규 상장: stockcodename에만 존재하는 레코드
        new_df = merged_df.query('_merge == "left_only"').copy()
        new_df["name"] = new_df["name_x"]

        # 상장폐지: 기존 security에만 존재하는 레코드
        delisted_df = merged_df.query('_merge == "right_only"').copy()
        delisted_df["name"] = delisted_df["name_y"]

        # 양쪽에 모두 존재하는 경우
        both_df = merged_df.query('_merge == "both"').copy()

        # 이름 변경: 동일 ticker이지만 이름이 다른 경우
        name_change_df = both_df[both_df["name_x"] != both_df["name_y"]].copy()
        name_change_df["name"] = name_change_df["name_x"]  # 새 이름으로 업데이트

        # ────────────── PostgreSQL 업데이트 (단일 트랜잭션) ──────────────
        with cd_postgres.get_connection() as pg_conn:
            cursor = pg_conn.cursor()

            # 1. 이름 변경 업데이트 (batch 처리 개선)
            if not name_change_df.empty:
                # 변경할 데이터 리스트 생성 (새 이름, 새 한글 이름, security ID)
                update_data = [
                    (new_name, new_name, sec_id)  # kor_name도 함께 업데이트
                    for sec_id, new_name in zip(
                        name_change_df["security_id"], name_change_df["name"]
                    )
                ]
                
                # Sort by security_id for consistent lock ordering
                update_data.sort(key=lambda x: x[2])  # security_id is at index 2
                
                # Process in smaller batches to reduce lock contention
                batch_size = 300  # Reduced batch size to minimize lock contention
                
                for i in range(0, len(update_data), batch_size):
                    batch = update_data[i:i + batch_size]
                    cursor.executemany(
                        """
                        UPDATE security 
                        SET name = %s, kor_name = %s 
                        WHERE security_id = %s
                        """,
                        batch,
                    )
                    # pg_conn.commit()  # Commit each batch separately

                context.log.info(f"이름 변경 업데이트: {len(update_data)}건")

            # 2. Undo delisting: 기존에 상장폐지된 종목이 다시 등장한 경우
            undo_df = both_df[both_df["delisting_date"].notnull()]
            if not undo_df.empty:
                # ANY 연산자를 사용한 효율적인 업데이트
                cursor.execute(
                    'UPDATE security SET delisting_date = NULL WHERE security_id = ANY(%s)',
                    (undo_df["security_id"].tolist(),),
                )
                context.log.info(f"상장폐지 취소 업데이트: {len(undo_df)}건")

            # 3. 신규 상장 INSERT 처리
            if not new_df.empty:
                # 필요한 컬럼만 선택하고 추가 정보 할당
                # Assumes 'import uuid' is present at the top of the file.
                new_records = new_df[["ticker", "name", "exchange"]].copy()
                new_records = new_records.assign(
                    kor_name=new_records["name"],  # 초기값으로 한글 이름 설정
                    country=COUNTRY,
                    listing_date=formatted_date,
                )

                # Prepare data for executemany
                insert_tuples = [
                    (
                        str(uuid.uuid4()),  # Generate UUID
                        rec["ticker"],
                        rec["name"],
                        rec["exchange"],
                        rec["kor_name"],
                        COUNTRY,  # Global constant
                        rec["listing_date"],  # From .assign()
                    )
                    for rec in new_records.to_dict("records")
                ]
                
                if insert_tuples:
                    cursor.executemany(
                        """
                        INSERT INTO security (security_id, ticker, name, exchange, kor_name, country, listing_date)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """,
                        insert_tuples,
                    )
                context.log.info(f"신규 상장 INSERT: {len(new_records)}건")

            # 4. 상장폐지 업데이트 (이미 폐지된 종목 제외)
            if not delisted_df.empty:
                # delisting_date가 NULL인 경우만 필터링 (중복 업데이트 방지)
                delisted_filtered = delisted_df[delisted_df["delisting_date"].isnull()]
                if not delisted_filtered.empty:
                    cursor.execute(
                        'UPDATE security SET delisting_date = %s WHERE security_id = ANY(%s)',
                        (formatted_date, delisted_filtered["security_id"].tolist()),
                    )
                    context.log.info(f"상장폐지 업데이트: {len(delisted_filtered)}건")

            # 모든 변경 사항 커밋 (단일 트랜잭션)
            pg_conn.commit()

        # ────────────── 결과 DataFrame 및 마크다운 생성 ──────────────
        # 각 작업 유형별 결과를 합쳐서 최종 결과 DataFrame 생성
        summary_frames = []
        if not new_df.empty:
            new_df["type"] = "listed"
            summary_frames.append(new_df[["ticker", "name", "exchange", "type"]])
        if not delisted_df.empty:
            delisted_df["type"] = "delisted"
            summary_frames.append(delisted_df[["ticker", "name", "exchange", "type"]])
        if not name_change_df.empty:
            name_change_df["type"] = "name_change"
            summary_frames.append(
                name_change_df[["ticker", "name", "exchange", "type"]]
            )

        # 결과 데이터프레임 생성
        if summary_frames:
            result_df = pd.concat(summary_frames, ignore_index=True)
            result_df["date"] = pd.to_datetime(formatted_date_str, format=DATE_FORMAT)
        else:
            result_df = pd.DataFrame(
                columns=["date", "ticker", "name", "type", "exchange"]
            )

        # 작업 건수 및 미리보기 마크다운 계산
        undo_count = len(undo_df) if not undo_df.empty else 0
        name_change_count = len(name_change_df) if not name_change_df.empty else 0
        listed_count = len(new_df) if not new_df.empty else 0
        delisted_count = len(delisted_df) if not delisted_df.empty else 0

        # 마크다운 미리보기 생성 (결과가 있는 경우만)
        listed_md = (
            new_df.to_markdown(index=False) if not new_df.empty else "No new listings."
        )
        delisted_md = (
            delisted_df.to_markdown(index=False)
            if not delisted_df.empty
            else "No delistings."
        )
        name_change_md = (
            name_change_df.to_markdown(index=False)
            if not name_change_df.empty
            else "No name changes."
        )

    # ────────────── 최종 MaterializeResult 생성 ──────────────
    preview_df = result_df.head(10)  # 상위 10개 레코드만 미리보기
    row_count = len(result_df)

    # 결과 메타데이터 구성
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
                dg.TableColumn("date", "date", description="Data date"),
                dg.TableColumn("exchange", "string", description="Exchange identifier"),
                dg.TableColumn(
                    "type",
                    "string",
                    description="Change type (listed/delisted/name_change)",
                ),
            ]
        ),
    }
    return dg.MaterializeResult(metadata=metadata)


@dg.asset(
    description="종목 보안 타입 분류 - 보통주/우선주/전환우선주/리츠/스팩/펀드 - Silver Tier",
    partitions_def=daily_exchange_category_partition,
    group_name="CD",
    kinds={"postgres"},
    tags={
        "domain": "finance",
        "data_tier": "silver",
        "source": "computed"
    },
    deps=[cd_ingest_stockcodenames],
)
def cd_update_securitytypebyname(
    context: dg.AssetExecutionContext,
    cd_postgres: PostgresResource,
) -> dg.MaterializeResult:
    """
    보통주, 우선주, 전환우선주, 리츠, 스팩, 펀드 security type 업데이트.
    sync 중 가장 먼저 실행되어야 함. 한국 시장만 대상으로 함.
    """

    # ────────────── 파티션 정보 처리 ──────────────
    partition_keys = context.partition_key.keys_by_dimension
    date = partition_keys["date"].replace("-", "")
    exchange = partition_keys["exchange"]

    # ────────────── 타입이 아직 설정되지 않은 증권 데이터 조회 ──────────────
    select_query = """
    SELECT security_id, name, exchange
    FROM security
    WHERE delisting_date IS NULL
      AND exchange = %s
      AND type IS NULL
    """
    with cd_postgres.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(select_query, (exchange,))
            # 더 효율적인 fetchall() 방식 사용
            rows = cur.fetchall()

    # 데이터가 없으면 빈 결과 반환
    if not rows:
        odf = pd.DataFrame(columns=["date", "exchange", "security_id", "name", "type"]).astype(str)
        metadata = {
            "Date": dg.MetadataValue.text(date),
            "Exchange": dg.MetadataValue.text(exchange),
            "Result": dg.MetadataValue.text(
                "모든 종목의 타입(보통주, 우선주, 전환우선주, 리츠, 스팩, 펀드)이 이미 업데이트 되었습니다"
            ),
        }
        return dg.MaterializeResult(metadata=metadata)

    # 결과를 DataFrame으로 변환
    odf = pd.DataFrame(rows, columns=["security_id", "name", "exchange"])

    # ────────────── security type 산출 및 DataFrame 구성 ──────────────
    # 병렬 처리를 위해 벡터화된 연산으로 변경 가능하지만 _parse_security_type이 정규식 패턴을 사용하므로 apply 유지
    odf["type"] = odf["name"].apply(_parse_security_type)
    odf["date"] = date

    # ────────────── 배치 업데이트 실행 (executemany) ──────────────
    update_query = 'UPDATE security SET type = %s WHERE security_id = %s'
    update_values = list(odf[["type", "security_id"]].itertuples(index=False, name=None))
    
    # Sort by security_id to ensure consistent lock ordering
    update_values.sort(key=lambda x: x[1])  # Sort by security_id (second element)

    with cd_postgres.get_connection() as conn:
        with conn.cursor() as cur:
            if not update_values:
                context.log.info("No security types to update for this partition.")
            else:
                context.log.info(f"Starting update for {len(update_values)} security types for exchange '{exchange}' on date '{date}'. Committing once at the end.")
                # Process updates in smaller batches for executemany, but commit once.
                batch_size = 300  # Retain existing batch size for executemany calls
                updated_count = 0
                for i in range(0, len(update_values), batch_size):
                    batch = update_values[i:i + batch_size]
                    cur.executemany(update_query, batch)
                    updated_count += len(batch)
                    # context.log.debug(f"Executed batch of {len(batch)}. Total processed so far: {updated_count}")

                conn.commit()  # Commit once after all batches are executed
                context.log.info(f"Successfully updated {updated_count} security types for exchange '{exchange}' on date '{date}' in a single transaction.")

    # ────────────── 통계 데이터 계산 ──────────────
    # 각 타입별 카운트 한 번에 계산 (효율성 향상)
    type_counts = odf["type"].value_counts().to_dict()

    metadata = {
        "Date": dg.MetadataValue.text(date),
        "Exchange": dg.MetadataValue.text(exchange),
        "# of 보통주": dg.MetadataValue.int(type_counts.get("보통주", 0)),
        "# of 우선주": dg.MetadataValue.int(type_counts.get("우선주", 0)),
        "# of 전환우선주": dg.MetadataValue.int(type_counts.get("전환우선주", 0)),
        "# of 리츠": dg.MetadataValue.int(type_counts.get("리츠", 0)),
        "# of 스팩": dg.MetadataValue.int(type_counts.get("스팩", 0)),
        "# of 펀드": dg.MetadataValue.int(type_counts.get("펀드", 0)),
        "Preview": dg.MetadataValue.md(odf.to_markdown()),
    }

    return dg.MaterializeResult(metadata=metadata)


@dg.asset(
    description="회사 엔티티 생성 및 종목-회사 연결 관리 - Silver Tier",
    partitions_def=daily_exchange_category_partition,
    group_name="CD",
    kinds={"postgres"},
    tags={
        "domain": "finance",
        "data_tier": "silver",
        "source": "computed"
    },
    deps=[cd_update_securitytypebyname],
)
def cd_upsert_company_by_security(
    context: dg.AssetExecutionContext, cd_postgres: PostgresResource
) -> dg.MaterializeResult:
    """
    신규 종목의 경우, company를 생성하고 company와 security를 연결합니다.
    (한국 시장만 대상)
    """
    # 파라미터 추출 (YYYYMMDD 형식, 문자열 형식)
    partition_info = context.partition_key.keys_by_dimension
    date = partition_info["date"].replace("-", "")
    exchange = partition_info["exchange"]

    columns = [
        "date",
        "exchange",
        "company_id",
        "name",
        "ticker",
        "security_name",
        "action_type",
    ]
    odf_data = []  # 결과 DataFrame에 담을 데이터
    update_values = []  # security 업데이트용 (company_id, security_id) 튜플 리스트

    # 제외할 security type
    EXCLUDES = ("스팩", "리츠", "펀드")
    excludes_tuple = "(" + ",".join(f"'{e}'" for e in EXCLUDES) + ")"

    try:
        # ────────── 업데이트 대상 security 조회 ──────────
        select_securities_query = f"""
        SELECT security_id, name, ticker, type, exchange
        FROM security
        WHERE company_id IS NULL
          AND delisting_date IS NULL
          AND exchange = '{exchange}'
          AND (type IS NULL OR type NOT IN {excludes_tuple})
        """
        engine = cd_postgres.get_sqlalchemy_engine()
        securities_df = pd.read_sql_query(select_securities_query, engine)

        if securities_df.empty:
            empty_df = pd.DataFrame(columns=columns)
            metadata = {
                "Date": dg.MetadataValue.text(date),
                "Exchange": dg.MetadataValue.text(exchange),
                "Result": dg.MetadataValue.text(
                    "모든 증권이 이미 회사에 연결되어 있습니다."
                ),
                "Preview": dg.MetadataValue.md(empty_df.to_markdown()),
            }
            return dg.MaterializeResult(metadata=metadata)

        # ────────── 기존에 연결된 회사 조회 ──────────
        # 성능 최적화: 한 번의 쿼리로 모든 관련 회사 가져오기
        select_companies_query = f"""
        SELECT DISTINCT c.company_id, c.name
        FROM company c
        JOIN security s ON s.company_id = c.company_id
        WHERE s.exchange = '{exchange}'
          AND (s.type IS NULL OR s.type NOT IN {excludes_tuple})
        """
        companies_df = pd.read_sql_query(select_companies_query, engine)
        existing_companies = {
            row["name"]: {"company_id": row["company_id"], "name": row["name"]}
            for _, row in companies_df.iterrows()
        }

        num_create = 0
        num_link = 0
        
        # Properly use connection as context manager
        with cd_postgres.get_connection() as conn:
            # Use a cursor within the connection context
            with conn.cursor() as cur:
                # 회사 생성 쿼리 - created_at, updated_at 필드 포함
                insert_company_query = """
                INSERT INTO company (company_id, name, kor_name, country, type, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                RETURNING company_id, name
                """

                # ────────── 각 security 처리 (회사 생성 및 연결) ──────────
                for _, sec in securities_df.iterrows():
                    # _get_company_name()는 security의 name을 기반으로 회사명을 산출하는 함수
                    company_name = _get_company_name(sec["name"])
                    
                    if company_name in existing_companies:
                        company = existing_companies[company_name]
                        action_type = "link"
                        num_link += 1
                    else:
                        # UUID 생성으로 고유 ID 보장
                        company_id = str(uuid.uuid4())
                        # values 튜플을 SQL 쿼리의 %s 개수와 일치시킴
                        values = (company_id, company_name, company_name, "대한민국", "상장법인")
                        cur.execute(insert_company_query, values)
                        company_row = cur.fetchone()
                        
                        if company_row is None:
                            context.log.error(f"회사 생성 실패: {company_name}")
                            continue
                            
                        company = {"company_id": company_row[0], "name": company_row[1]}
                        existing_companies[company_name] = company
                        action_type = "create"
                        num_create += 1
                    
                    # 이 부분이 루프 내부에 있어야 함 (모든 security에 대해 수행)
                    update_values.append((company["company_id"], sec["security_id"]))
                    odf_data.append(
                        [
                            date,
                            exchange,
                            company["company_id"],
                            company["name"],
                            sec["ticker"],
                            sec["name"],
                            action_type,
                        ]
                    )

                # 성능 최적화: 모든 security를 일괄 업데이트
                if update_values:
                    # ────────── security 업데이트 (회사 연결) ──────────
                    update_query = """
                    UPDATE security
                       SET company_id = %s,
                           updated_at = CURRENT_TIMESTAMP
                     WHERE security_id = %s
                    """
                    # Sort updates by security_id for consistent lock ordering
                    update_values.sort(key=lambda x: x[1])
                    # Process in batches to reduce lock contention
                    batch_size = 300
                    for i in range(0, len(update_values), batch_size):
                        batch = update_values[i : i + batch_size]
                        cur.executemany(update_query, batch)
                        conn.commit()
            
            # 결과 DataFrame 생성
            odf = pd.DataFrame(odf_data, columns=columns)
            metadata = {
                "Date": dg.MetadataValue.text(date),
                "Exchange": dg.MetadataValue.text(exchange),
                "# of new com": dg.MetadataValue.int(num_create),
                "# of linking to com": dg.MetadataValue.int(num_link),
                "Preview": dg.MetadataValue.md(odf.to_markdown()),
            }
            
            return dg.MaterializeResult(metadata=metadata)
            
    except Exception as e:
        # 오류 발생시 로깅
        context.log.error(f"upsert_company_by_security 처리 중 오류 발생: {str(e)}")
        raise
