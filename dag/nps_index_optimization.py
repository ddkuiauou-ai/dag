# NPS PostgreSQL 전환 보조 인덱스 생성 자산 (for SaaS layout)
# 목적: 거대한 raw 테이블(public.pension)에서 dim/fact 스키마로 전환 시
#       매칭/정규화/집계 작업을 빠르게 수행하도록 전환 보조용 인덱스/함수/확장을 생성한다.
# 변경점:
#  - 기존 trigram 인덱스 재생성 및 성능 측정 로직 전부 제거
#  - EXTENSION/정규화 함수/전환 보조 인덱스 생성 + 간단한 체크만 남김
#  - CONCURRENTLY 미사용(요구사항)
#  - 사업자등록번호는 6자리만 제공 → 고유키로 사용하지 않음을 주석 및 체크에 반영

import dagster as dg
import time
import psycopg2
import psycopg2.extensions
from typing import Dict, Any, List
from .resources import PostgresResource

# ------------------------------
# 상수
# ------------------------------
POSTGRES_NPS_TABLE = "public.pension"
SIMILARITY_THRESHOLD = 0.5  # 전환 시 후보 축소를 위해 기본 0.5 권장(데이터에 맞춰 조정 가능)

# 튜닝 스위치/파라미터 (필요 시 조정)
# - CREATE_GIST_TRGM: kNN 유사도 정렬(ORDER BY <->)이 꼭 필요한 경우에만 True 유지
# - WORK_MEM/MAINTENANCE_WORK_MEM: 여유 메모리 많을수록 인덱스 생성 빨라짐(주의: 세션 단위 설정)
# - SYNCHRONOUS_COMMIT_OFF_DURING_BUILD: 인덱스 생성 중 WAL 동기 커밋 비활성화로 쓰기 지연 감소
# - USE_C_COLLATE_PREFIX: 접두어(btree text_pattern_ops) 인덱스에 C collation 적용(정렬·비교 단순화)
# - BRIN_PAGES_PER_RANGE: BRIN 인덱스 범위 크기(작게 할수록 세밀하지만 크기 증가)
CREATE_GIST_TRGM = True
WORK_MEM = '256MB'
MAINTENANCE_WORK_MEM = '1GB'
SYNCHRONOUS_COMMIT_OFF_DURING_BUILD = True
USE_C_COLLATE_PREFIX = False
BRIN_PAGES_PER_RANGE = None  # 예: 32 또는 64 등으로 조정

# 인덱스/오브젝트 이름들
IDX_NAME_NORM_TRGM_GIN = "idx_pension_name_norm_trgm_gin"
IDX_NAME_NORM_TRGM_GIST = "idx_pension_name_norm_trgm_gist"
IDX_NAME_NORM_PREFIX   = "idx_pension_name_norm_prefix"
IDX_NAME_BRN           = "idx_pension_brn"                 # 6자리 BRN → 비고유, 블로킹/필터용
IDX_NAME_BLOCK_ADDRIND = "idx_pension_block_addr_ind"       # (addr_sigungu_code, industry_code)
IDX_NAME_YM_BRIN       = "idx_pension_ym_brin"              # data_created_ym 범위 탐색용

# ------------------------------
# 헬퍼
# ------------------------------
def execute_sql_command(cursor, command: str, description: str, context: dg.OpExecutionContext) -> Dict[str, Any]:
    context.log.debug(f"Executing SQL command [{description}]: {command}")
    cursor.execute(command)
    return {"status": "success", "description": description, "rowcount": cursor.rowcount}

def execute_sql_command_autocommit(conn, command: str, description: str, context: dg.OpExecutionContext) -> Dict[str, Any]:
    context.log.debug(f"Executing SQL command in autocommit mode [{description}]: {command}")
    original_isolation_level = conn.isolation_level
    conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    try:
        with conn.cursor() as cursor:
            cursor.execute(command)
        return {"status": "success", "description": description}
    finally:
        conn.set_isolation_level(original_isolation_level)

# ------------------------------
# 자산 1: 전환 보조 인덱스/함수/확장 생성
# ------------------------------
@dg.asset(
    group_name="NPS",
    tags={"data_tier": "infrastructure", "domain": "optimization", "process": "indexing"},
    description=(
        "SaaS 전환을 빠르게 하기 위한 전환 보조 인덱스/함수/확장 생성.\n"
        "- pg_trgm/unaccent 확장\n"
        "- 회사명 정규화 함수(nps_normalize_company_name)\n"
        "- 정규화 이름 기반 GIN/GiST/text_pattern_ops 인덱스\n"
        "- 6자리 사업자번호(business_reg_num) 필터용 btree 인덱스(비고유)\n"
        "- 주소/업종 블로킹용 복합 인덱스\n"
        "- data_created_ym BRIN 인덱스 + VACUUM ANALYZE\n"
        "(모든 DDL에서 CONCURRENTLY는 사용하지 않음)"
    ),
    deps=["nps_to_postgres_simple"],
)
def nps_pension_staging_indexes(
    context: dg.AssetExecutionContext,
    nps_postgres: PostgresResource,
) -> dg.MaterializeResult:
    start_time = time.time()
    actions: List[Dict[str, Any]] = []

    context.log.info("🚀 전환 보조 인덱스/함수/확장 생성 시작")

    try:
        with nps_postgres.get_connection() as conn:
            with conn.cursor() as cursor:
                # 세션 기본 GUC(보수적으로 최소만 설정)
                session_settings = [
                    (f"SET work_mem = '{WORK_MEM}'", f"세션 작업 메모리 {WORK_MEM}로 설정"),
                    (f"SET maintenance_work_mem = '{MAINTENANCE_WORK_MEM}'", f"인덱스 생성용 작업 메모리 {MAINTENANCE_WORK_MEM}로 설정"),
                    (f"SET pg_trgm.similarity_threshold = {SIMILARITY_THRESHOLD}", f"트라이그램 유사도 임계값 {SIMILARITY_THRESHOLD}로 설정"),
                ]
                if SYNCHRONOUS_COMMIT_OFF_DURING_BUILD:
                    session_settings.append(("SET synchronous_commit = off", "인덱스 생성 동안 synchronous_commit 비활성화"))

                for sql, desc in session_settings:
                    try:
                        actions.append(execute_sql_command(cursor, sql, desc, context))
                        context.log.info(f"✅ {desc}")
                    except Exception as e:
                        context.log.warning(f"⚠️ {desc} 실패: {e}")

                # 1) 확장
                actions.append(execute_sql_command(cursor, "CREATE EXTENSION IF NOT EXISTS pg_trgm;", "pg_trgm 확장 설치/확인", context))
                actions.append(execute_sql_command(cursor, "CREATE EXTENSION IF NOT EXISTS unaccent WITH SCHEMA public;", "unaccent 확장 설치/확인", context))
                context.log.info("✅ 확장 설치/확인 완료")

                # 2) 정규화 함수
                normalize_fn_sql = (
                    """
                    -- 회사명 정규화: 전처리(전각→반각 등) → 대소문자/악센트 제거 → 법인표기 제거 → 괄호 내용 제거 → 불필요 문자 제거
                    CREATE OR REPLACE FUNCTION nps_normalize_company_name(txt text)
                    RETURNS text LANGUAGE sql IMMUTABLE
                    SET search_path = pg_catalog, public
                    AS $$
                      WITH raw AS (
                        SELECT coalesce($1,'') AS t
                      ),
                      -- 전각 괄호/점/공백을 반각으로 변환: （）．　 → () .
                      trans AS (
                        SELECT translate(t, '（）．，　', '()., ') AS t FROM raw
                      ),
                      lower_unaccent AS (
                        SELECT lower(public.unaccent(t)) AS t FROM trans
                      ),
                      protect AS (
                        SELECT 
                          regexp_replace(
                            regexp_replace(
                              regexp_replace(
                                regexp_replace(
                                  regexp_replace(t, '새마을금고', '___KEEP_SMG___', 'g'),
                                  '신용협동조합', '___KEEP_SHC___', 'g'
                                ),
                                '농업협동조합', '___KEEP_NH_AGR___', 'g'
                              ),
                              '(^|\\s)농협(\\s|$)', '___KEEP_NH___', 'g'
                            ),
                            '(^|\\s)신협(\\s|$)', '___KEEP_SHY___', 'g'
                          ) AS t
                        FROM lower_unaccent
                      ),
                      -- (주)/(유)/(사)/(재) 및 각종 법인표기 제거 + ㈜ 포함
                      rm_legal AS (
                        SELECT regexp_replace(
                          t,
                          '('
                            || '주식\\s*회사'   || '|' || '\\(\\s*주\\s*\\)' || '|' || '주\\s*\\)' || '|' || '㈜' || '|'
                            || '유한\\s*회사'   || '|' || '유한법인' || '|' || '\\(\\s*유\\s*\\)' || '|' || '유\\s*\\)' || '|'
                            || '유한책임회사'   || '|'
                            || '합자\\s*회사'   || '|' || '합명\\s*회사' || '|' || '\\(\\s*합\\s*\\)' || '|' || '합\\s*\\)' || '|'
                            || '사단법인'       || '|' || '재단법인' || '|' || '\\(\\s*사\\s*\\)' || '|' || '사\\s*\\)' || '|' || '\\(\\s*재\\s*\\)' || '|' || '재\\s*\\)' || '|'
                            || '학교법인'       || '|' || '사회복지법인' || '|' || '세무법인' || '|' || '의료법인' || '|' || '법무법인' || '|'
                            || '영농조합법인'   || '|' || '영어조합법인' || '|'
                            || '신용협동조합'   || '|' || '농업협동조합' || '|' || '수산업협동조합' || '|' || '새마을금고' || '|'
                            || '사회적?협동조합' || '|' || '협동조합' || '|'
                            || '농업회사법인'
                          || ')',
                          '', 'g'
                        ) AS t
                        FROM protect
                      ),
                      -- 괄호 안 부가 표기(영문/코드 등) 제거: ( ... )
                      rm_paren AS (
                        SELECT regexp_replace(t, '\\([^)]+\\)', '', 'g') AS t FROM rm_legal
                      ),
                      -- 보호어 복원
                      unprotect AS (
                        SELECT 
                          replace(
                            replace(
                              replace(
                                replace(t, '___KEEP_SMG___', '새마을금고'),
                                '___KEEP_SHC___', '신용협동조합'
                              ),
                              '___KEEP_NH_AGR___', '농업협동조합'
                            ),
                            '___KEEP_NH___', '농협'
                          ) AS t
                          FROM rm_paren
                      ),
                      -- 최종: 한글/영문/숫자만 남김
                      cleaned AS (
                        SELECT regexp_replace(t, '[^0-9a-z가-힣]+', '', 'g') AS t FROM unprotect
                      )
                      SELECT t FROM cleaned;
                    $$;
                    """
                )
                actions.append(execute_sql_command(cursor, normalize_fn_sql, "정규화 함수 생성/갱신", context))
                context.log.info("✅ 정규화 함수 생성/갱신 완료")

                # 회사 형태 추출 함수 추가 (주식회사/유한회사 등)
                company_type_fn_sql = (
                    """
                    CREATE OR REPLACE FUNCTION nps_extract_company_type(txt text)
                    RETURNS text LANGUAGE sql IMMUTABLE
                    SET search_path = pg_catalog, public
                    AS $$
                      WITH s0 AS (
                        SELECT coalesce($1,'') AS t
                      ), s AS (
                        -- 전각 → 반각 변환 후 소문자/악센트 제거
                        SELECT lower(public.unaccent(translate(t, '（）．，　', '()., '))) AS t FROM s0
                      )
                      SELECT CASE
                        WHEN t ~ '(주식\\s*회사|\\(\\s*주\\s*\\)|주\\s*\\)|㈜)' THEN '주식회사'
                        WHEN t ~ '(유한\\s*회사|유한법인|\\(\\s*유\\s*\\)|유\\s*\\))' THEN '유한법인'
                        WHEN t ~ '유한책임회사' THEN '유한책임회사'
                        WHEN t ~ '(합자\\s*회사|\\(\\s*합\\s*\\)|합\\s*\\))' THEN '합자회사'
                        WHEN t ~ '(합명\\s*회사|\\(\\s*합명\\s*\\)|\\(\\s*명\\s*\\))' THEN '합명회사'
                        WHEN t ~ '(사단법인|\\(\\s*사\\s*\\)|사\\s*\\))' THEN '사단법인'
                        WHEN t ~ '(재단법인|\\(\\s*재\\s*\\)|재\\s*\\))' THEN '재단법인'
                        WHEN t ~ '(사복|\\(\\s*사복\\s*\\)|\\(\\s*복\\s*\\))' THEN '사회복지법인'
                        WHEN t ~ '의료법인' THEN '의료법인'
                        WHEN t ~ '법무법인' THEN '법무법인'
                        WHEN t ~ '사회복지법인' THEN '사회복지법인'
                        WHEN t ~ '세무법인' THEN '세무법인'
                        WHEN t ~ '학교법인' THEN '학교법인'
                        WHEN t ~ '(^|\\s)농협(\\s|$)' THEN '농업협동조합'
                        WHEN t ~ '신용협동조합' THEN '신용협동조합'
                        WHEN t ~ '(^|\\s)신협(\\s|$)' THEN '신용협동조합'
                        WHEN t ~ '농업협동조합' THEN '농업협동조합'
                        WHEN t ~ '수산업협동조합' THEN '수산업협동조합'
                        WHEN t ~ '새마을금고' THEN '새마을금고'
                        WHEN t ~ '사회적협동조합' THEN '사회적협동조합'
                        WHEN t ~ '협동조합' THEN '협동조합'
                        WHEN t ~ '영농조합법인' THEN '영농조합법인'
                        WHEN t ~ '영어조합법인' THEN '영어조합법인'
                        WHEN t ~ '농업회사법인' THEN '농업회사법인'
                        ELSE NULL
                      END
                      FROM s;
                    $$;
                    """
                )
                actions.append(execute_sql_command(cursor, company_type_fn_sql, "회사 형태 추출 함수 생성/갱신", context))
                context.log.info("✅ 회사 형태 추출 함수 생성/갱신 완료")

                # 표시용 정식명(사람이 읽는 용도) 정규화 함수
                canonical_fn_sql = (
                    """
                    CREATE OR REPLACE FUNCTION nps_canonicalize_company_name(txt text)
                    RETURNS text LANGUAGE sql IMMUTABLE
                    SET search_path = pg_catalog, public
                    AS $$
                      WITH s0 AS (
                        SELECT coalesce($1,'') AS t
                      ), s AS (
                        -- 전각 괄호/점/공백 변환 후 악센트 제거
                        SELECT public.unaccent(translate(t, '（）．，　', '()., ')) AS t
                        FROM s0
                      ), protect AS (
                        SELECT 
                          regexp_replace(
                            regexp_replace(
                              regexp_replace(
                                regexp_replace(t, '새마을금고', '___KEEP_SMG___', 'g'),
                                '신용협동조합', '___KEEP_SHC___', 'g'
                              ),
                              '농업협동조합', '___KEEP_NH_AGR___', 'g'
                            ),
                            '(^|\\s)농협(\\s|$)', '___KEEP_NH___', 'g'
                          ) AS t
                        FROM s
                      ), rm_legal AS (
                        SELECT regexp_replace(t,
                          '('
                            || '주식\\s*회사'   || '|' || '\\(주\\)' || '|' || '㈜' || '|'
                            || '유한\\s*회사'   || '|' || '유한법인' || '|' || '\\(유\\)' || '|'
                            || '유한책임회사'   || '|'
                            || '합자\\s*회사'   || '|' || '합명\\s*회사' || '|' || '\\(합\\)' || '|'
                            || '사단법인'       || '|' || '재단법인' || '|' || '학교법인' || '|' || '사회복지법인' || '|' || '세무법인' || '|' || '의료법인' || '|' || '법무법인' || '|'
                            || '신용협동조합'   || '|' || '농업협동조합' || '|' || '수산업협동조합' || '|' || '새마을금고' || '|'
                            || '사\\s*\\)'     || '|' || '재\\s*\\)' || '|' || '주\\s*\\)' || '|' || '유\\s*\\)' || '|' || '합\\s*\\)' || '|'
                            || '영농조합법인'   || '|' || '영어조합법인' || '|'
                            || '사회적?협동조합' || '|' || '협동조합' || '|'
                            || '농업회사법인'
                          || ')',
                          '', 'g') AS t
                        FROM protect
                      ), rm_branch AS (
                        SELECT regexp_replace(t,
                          '(\\s*(본점|본사|지점|영업소|센터|서비스센터|대리점|가맹점|사업소|공장|연구소|본부|점))+$',
                          '', 'g') AS t
                        FROM rm_legal
                      ), rm_paren AS (
                        -- 괄호로 둘러싼 부가코드/영문표기 제거
                        SELECT regexp_replace(t, '\\([^)]+\\)', '', 'g') AS t FROM rm_branch
                      ), unprotect AS (
                        SELECT 
                          replace(
                            replace(
                              replace(
                                replace(t, '___KEEP_SMG___', '새마을금고'),
                                '___KEEP_SHC___', '신용협동조합'
                              ),
                              '___KEEP_NH_AGR___', '농업협동조합'
                            ),
                            '___KEEP_NH___', '농협'
                          ) AS t
                        FROM rm_paren
                      ), cleanup AS (
                        SELECT regexp_replace(regexp_replace(t, '\\s+', ' ', 'g'), '(^\\s+|\\s+$)', '', 'g') AS t
                        FROM unprotect
                      )
                      SELECT NULLIF(t, '') FROM cleanup;
                    $$;
                    """
                )
                actions.append(execute_sql_command(cursor, canonical_fn_sql, "표시용 canonicalizer 함수 생성/갱신", context))
                context.log.info("✅ 표시용 canonicalizer 함수 생성/갱신 완료")

                # 다중 회사 형태 추출 함수(배열)
                company_types_arr_fn_sql = (
                    """
                    CREATE OR REPLACE FUNCTION nps_extract_company_types(txt text)
                    RETURNS text[] LANGUAGE sql IMMUTABLE
                    SET search_path = pg_catalog, public
                    AS $$
                      WITH s0 AS (
                        SELECT coalesce($1,'') AS t
                      ), s AS (
                        -- 전각 → 반각 변환 후 소문자/악센트 제거
                        SELECT lower(public.unaccent(translate(t, '（）．，　', '()., '))) AS t FROM s0
                      ), flags AS (
                        SELECT ARRAY_REMOVE(ARRAY[
                          CASE WHEN t ~ '(주식\\s*회사|\\(\\s*주\\s*\\)|주\\s*\\)|㈜)' THEN '주식회사' END,
                          CASE WHEN t ~ '(유한\\s*회사|유한법인|\\(\\s*유\\s*\\)|유\\s*\\))' THEN '유한법인' END,
                          CASE WHEN t ~ '유한책임회사' THEN '유한책임회사' END,
                          CASE WHEN t ~ '(합자\\s*회사|\\(\\s*합\\s*\\)|합\\s*\\))' THEN '합자회사' END,
                          CASE WHEN t ~ '합명\\s*회사' THEN '합명회사' END,
                          CASE WHEN t ~ '(사단법인|\\(\\s*사\\s*\\)|사\\s*\\))' THEN '사단법인' END,
                          CASE WHEN t ~ '(재단법인|\\(\\s*재\\s*\\)|재\\s*\\))' THEN '재단법인' END,
                          CASE WHEN t ~ '의료법인' THEN '의료법인' END,
                          CASE WHEN t ~ '법무법인' THEN '법무법인' END,
                          CASE WHEN t ~ '사회복지법인' THEN '사회복지법인' END,
                          CASE WHEN t ~ '세무법인' THEN '세무법인' END,
                          CASE WHEN t ~ '학교법인' THEN '학교법인' END,
                          CASE WHEN t ~ '(^|\\s)신협(\\s|$)' THEN '신용협동조합' END,
                          CASE WHEN t ~ '(^|\\s)농협(\\s|$)' THEN '농업협동조합' END,
                          CASE WHEN t ~ '신용협동조합' THEN '신용협동조합' END,
                          CASE WHEN t ~ '농업협동조합' THEN '농업협동조합' END,
                          CASE WHEN t ~ '수산업협동조합' THEN '수산업협동조합' END,
                          CASE WHEN t ~ '새마을금고' THEN '새마을금고' END,
                          CASE WHEN t ~ '사회적협동조합' THEN '사회적협동조합' END,
                          CASE WHEN t ~ '협동조합' THEN '협동조합' END,
                          CASE WHEN t ~ '영농조합법인' THEN '영농조합법인' END,
                          CASE WHEN t ~ '영어조합법인' THEN '영어조합법인' END,
                          CASE WHEN t ~ '농업회사법인' THEN '농업회사법인' END
                        ], NULL) AS arr
                        FROM s
                      )
                      SELECT (
                        SELECT array_agg(DISTINCT x) FROM unnest(arr) AS x
                      )
                      FROM flags;
                    $$;
                    """
                )
                actions.append(execute_sql_command(cursor, company_types_arr_fn_sql, "다중 회사 형태 추출 함수 생성/갱신", context))
                context.log.info("✅ 다중 회사 형태 추출 함수 생성/갱신 완료")

                # 3) 인덱스(모두 CONCURRENTLY 없이 생성)
                ddl_list = [
                    (
                        f"CREATE INDEX IF NOT EXISTS {IDX_NAME_NORM_TRGM_GIN} "
                        f"ON {POSTGRES_NPS_TABLE} USING gin (nps_normalize_company_name(company_name) gin_trgm_ops);",
                        "정규화 이름 GIN(trgm) 인덱스"
                    ),
                ]
                if CREATE_GIST_TRGM:
                    ddl_list.append(
                        (
                            f"CREATE INDEX IF NOT EXISTS {IDX_NAME_NORM_TRGM_GIST} "
                            f"ON {POSTGRES_NPS_TABLE} USING gist (nps_normalize_company_name(company_name) gist_trgm_ops);",
                            "정규화 이름 GiST(trgm) 인덱스(kNN)"
                        )
                    )

                prefix_expr = "nps_normalize_company_name(company_name)"
                if USE_C_COLLATE_PREFIX:
                    prefix_expr += " COLLATE \"C\""

                ddl_list.extend([
                    (
                        f"CREATE INDEX IF NOT EXISTS {IDX_NAME_NORM_PREFIX} "
                        f"ON {POSTGRES_NPS_TABLE} ({prefix_expr} text_pattern_ops);",
                        "정규화 이름 접두어 인덱스(text_pattern_ops)"
                    ),
                    (
                        f"CREATE INDEX IF NOT EXISTS {IDX_NAME_BRN} "
                        f"ON {POSTGRES_NPS_TABLE} (business_reg_num) WHERE business_reg_num IS NOT NULL;",
                        "사업자등록번호(6자리) 필터용 인덱스(비고유)"
                    ),
                    (
                        f"CREATE INDEX IF NOT EXISTS {IDX_NAME_BLOCK_ADDRIND} "
                        f"ON {POSTGRES_NPS_TABLE} (addr_sigungu_code, industry_code);",
                        "주소/업종 블로킹용 복합 인덱스"
                    ),
                ])

                brin_with = ""
                if BRIN_PAGES_PER_RANGE:
                    brin_with = f" WITH (pages_per_range = {BRIN_PAGES_PER_RANGE})"

                ddl_list.append(
                    (
                        f"CREATE INDEX IF NOT EXISTS {IDX_NAME_YM_BRIN} "
                        f"ON {POSTGRES_NPS_TABLE} USING brin (data_created_ym){brin_with};",
                        "data_created_ym BRIN 인덱스"
                    )
                )
                for sql, desc in ddl_list:
                    actions.append(execute_sql_command(cursor, sql, desc, context))
                    context.log.info(f"✅ {desc}")

                # 4) 통계 타깃 상향
                alter_stats_sql = (
                    f"ALTER TABLE {POSTGRES_NPS_TABLE} "
                    "ALTER COLUMN company_name SET STATISTICS 1000, "
                    "ALTER COLUMN business_reg_num SET STATISTICS 1000, "
                    "ALTER COLUMN data_created_ym SET STATISTICS 1000;"
                )
                actions.append(execute_sql_command(cursor, alter_stats_sql, "통계 타깃 상향(1000)", context))
                context.log.info("✅ 통계 타깃 상향 완료")

            # 트랜잭션 커밋
            conn.commit()
            context.log.info("💾 DDL 커밋 완료")

        # 5) VACUUM ANALYZE (autocommit)
        with nps_postgres.get_connection() as vacuum_conn:
            actions.append(
                execute_sql_command_autocommit(
                    vacuum_conn,
                    f"VACUUM ANALYZE {POSTGRES_NPS_TABLE};",
                    "pension 테이블 VACUUM ANALYZE",
                    context,
                )
            )
            context.log.info("🧹 VACUUM ANALYZE 완료")

    except Exception as e:
        context.log.error(f"💥 전환 보조 인덱스 생성 중 오류: {e}")
        raise dg.Failure(description=str(e)) from e

    finally:
        total = round(time.time() - start_time, 2)
        context.log.info(f"🏁 전환 보조 인덱스 생성 종료. 총 소요: {total}s")

    metadata = {
        "total_duration_seconds": total,
        "actions_completed_count": len(actions),
        "actions": actions,
    }
    return dg.MaterializeResult(metadata=metadata)


# ------------------------------
# 자산체크: 필수 오브젝트 존재 여부 Sanity
# ------------------------------
@dg.asset_check(
    asset=nps_pension_staging_indexes,
    name="check_pension_staging_indexes",
    description="pg_trgm/unaccent 확장, 정규화 함수, 전환 보조 인덱스 존재 여부를 점검합니다.",
)
def check_pension_staging_indexes(
    context: dg.AssetCheckExecutionContext,
    nps_postgres: PostgresResource,
) -> dg.AssetCheckResult:
    start_time = time.time()
    missing: List[str] = []
    info: Dict[str, Any] = {}

    try:
        with nps_postgres.get_connection() as conn:
            with conn.cursor() as cursor:
                # 확장 확인
                cursor.execute("SELECT extname FROM pg_extension WHERE extname IN ('pg_trgm','unaccent');")
                ext = {row[0] for row in cursor.fetchall()}
                info["extensions"] = list(ext)
                for need in ("pg_trgm", "unaccent"):
                    if need not in ext:
                        missing.append(f"extension:{need}")

                # 함수 확인
                cursor.execute("SELECT 1 FROM pg_proc WHERE proname = 'nps_normalize_company_name' LIMIT 1;")
                has_fn = cursor.fetchone() is not None
                info["normalize_function_present"] = has_fn
                if not has_fn:
                    missing.append("function:nps_normalize_company_name")

                # 인덱스 확인
                index_names = (
                    IDX_NAME_NORM_TRGM_GIN,
                    IDX_NAME_NORM_TRGM_GIST,
                    IDX_NAME_NORM_PREFIX,
                    IDX_NAME_BRN,
                    IDX_NAME_BLOCK_ADDRIND,
                    IDX_NAME_YM_BRIN,
                )
                cursor.execute(
                    "SELECT indexname FROM pg_indexes WHERE tablename = 'pension';"
                )
                have = {row[0] for row in cursor.fetchall()}
                info["indexes_found"] = list(sorted(have))
                for name in index_names:
                    if name not in have:
                        missing.append(f"index:{name}")

    except Exception as e:
        context.log.error(f"💥 체크 중 오류: {e}")
        return dg.AssetCheckResult(passed=False, description=str(e), metadata={"error": str(e)})

    finally:
        info["total_duration_seconds"] = round(time.time() - start_time, 2)

    passed = len(missing) == 0
    desc = "모든 전환 보조 오브젝트가 존재합니다." if passed else ("누락: " + ", ".join(missing))
    return dg.AssetCheckResult(passed=passed, description=desc, metadata={"missing": missing, **info})
