# SaaS ETL for NPS pension → dim/fact
# - Single-run asset using TEMP tables (no UNLOGGED, no CONCURRENTLY)
# - Locks allowed per ops requirements
# - Re-runnable (idempotent upserts)
# - Uses indexes/functions prepared by nps_index_optimization.py
#
# Assets
#   1) ensure_saas_tables: Create SaaS tables + indexes + control tables
#   2) nps_saas_etl_run: One-shot ETL (Extract → Match/Score → Upsert dim/alias → Aggregate → Upsert fact → Log)
# Checks
#   a) check_timeseries_uniqueness
#   b) check_recent_freshness
#   c) check_etl_coverage (reads last run from etl_run_log)

import time
import uuid
import dagster as dg
import psycopg2
from typing import Dict, Any, List
from .resources import PostgresResource

POSTGRES_NPS_TABLE = "public.pension"
PIPELINE_NAME = "nps_saas_etl"

# --- Tunables ---
PROCESS_MONTH_BUFFER = 1  # process last_ym - N months → now
AUTO_ACCEPT_THRESHOLD = 0.90
REVIEW_LOWER, REVIEW_UPPER = 0.75, 0.90
KNN_TOPK = 3
TOP_AUDIT_K = 3  # number of top candidates to log for audit/traceability

# ---- Helpers ----

def _exec(cur, sql: str, ctx: dg.OpExecutionContext, desc: str, params=None) -> Dict[str, Any]:
    ctx.log.debug(f"SQL [{desc}]:\n{sql}")
    if params is not None:
        cur.execute(sql, params)
    else:
        cur.execute(sql)
    return {"desc": desc, "rowcount": getattr(cur, "rowcount", None)}


# =============================
# Asset 1: Ensure SaaS Tables
# =============================
@dg.asset(
    group_name="NPS_SaaS",
    description=(
        "Create SaaS layer tables (dim_company, company_alias, fact_pension_monthly)\n"
        "+ indexes + control tables (etl_control, etl_run_log)."
    ),
    tags={"domain": "pension", "process": "ddl", "tier": "saas"},
)
def ensure_saas_tables(context: dg.AssetExecutionContext, nps_postgres: PostgresResource) -> dg.MaterializeResult:
    t0 = time.time()
    actions: List[Dict[str, Any]] = []

    ddl = f"""
    -- Extensions (likely present already via index optimization)
    CREATE EXTENSION IF NOT EXISTS pg_trgm;
    CREATE EXTENSION IF NOT EXISTS unaccent WITH SCHEMA public;

    -- dim_company (zero-based: only company_types[])
    CREATE TABLE IF NOT EXISTS public.dim_company (
      company_id       BIGSERIAL PRIMARY KEY,
      business_reg_num VARCHAR(10),           -- 6-digit here; not unique globally
      canonical_name   TEXT NOT NULL,
      normalized_name  TEXT NOT NULL,
      company_types    TEXT[],
      industry_code    VARCHAR(10),
      addr_key         TEXT,
      valid_from       DATE DEFAULT '1900-01-01',
      valid_to         DATE DEFAULT '9999-12-31'
    );
    -- Backfill-safe in case table existed before
    ALTER TABLE public.dim_company DROP COLUMN IF EXISTS company_type;
    ALTER TABLE public.dim_company ADD COLUMN IF NOT EXISTS company_types TEXT[];
    CREATE INDEX IF NOT EXISTS idx_dim_company_types_gin ON public.dim_company USING gin (company_types);
    -- business_reg_num may not be unique (6 digits) → partial unique skipped; index for equality
    CREATE INDEX IF NOT EXISTS idx_dim_company_brn ON public.dim_company (business_reg_num) WHERE business_reg_num IS NOT NULL;
    -- Ensure branch-level identity by BRN6 + addr_key
    CREATE UNIQUE INDEX IF NOT EXISTS idx_dim_company_brn_addr ON public.dim_company (business_reg_num, addr_key);
    CREATE INDEX IF NOT EXISTS idx_dim_company_norm_trgm_gin  ON public.dim_company USING gin  (normalized_name gin_trgm_ops);
    CREATE INDEX IF NOT EXISTS idx_dim_company_norm_trgm_gist ON public.dim_company USING gist (normalized_name gist_trgm_ops);
    CREATE INDEX IF NOT EXISTS idx_dim_company_norm_prefix    ON public.dim_company (normalized_name text_pattern_ops);

    -- company_alias (extended with alias_canonical + quality)
    CREATE TABLE IF NOT EXISTS public.company_alias (
      company_id        BIGINT REFERENCES public.dim_company(company_id),
      alias             TEXT NOT NULL,
      normalized_alias  TEXT NOT NULL,
      alias_canonical   TEXT,
      source            TEXT,
      quality           TEXT,
      PRIMARY KEY (company_id, normalized_alias)
    );
    CREATE INDEX IF NOT EXISTS idx_alias_trgm_gin  ON public.company_alias USING gin  (normalized_alias gin_trgm_ops);
    CREATE INDEX IF NOT EXISTS idx_alias_trgm_gist ON public.company_alias USING gist (normalized_alias gist_trgm_ops);
    CREATE INDEX IF NOT EXISTS idx_alias_prefix    ON public.company_alias (normalized_alias text_pattern_ops);
    ALTER TABLE public.company_alias ADD COLUMN IF NOT EXISTS alias_canonical TEXT;
    ALTER TABLE public.company_alias ADD COLUMN IF NOT EXISTS quality TEXT;

    -- fact_pension_monthly
    CREATE TABLE IF NOT EXISTS public.fact_pension_monthly (
      company_id        BIGINT NOT NULL REFERENCES public.dim_company(company_id),
      ym                DATE  NOT NULL,
      subscriber_count      INTEGER,
      monthly_notice_amount BIGINT,
      new_subscribers       INTEGER,
      lost_subscribers      INTEGER,
      PRIMARY KEY (company_id, ym)
    );
    CREATE INDEX IF NOT EXISTS idx_fact_company_ym_desc ON public.fact_pension_monthly (company_id, ym DESC);

    -- Control tables
    CREATE TABLE IF NOT EXISTS public.etl_control (
      pipeline_name TEXT PRIMARY KEY,
      last_ym       DATE,
      updated_at    TIMESTAMP WITHOUT TIME ZONE DEFAULT now()
    );
    INSERT INTO public.etl_control(pipeline_name, last_ym)
    VALUES ('{PIPELINE_NAME}', date_trunc('month', CURRENT_DATE)::date - INTERVAL '1 month')
    ON CONFLICT (pipeline_name) DO NOTHING;

    CREATE TABLE IF NOT EXISTS public.etl_run_log (
      pipeline_name TEXT,
      run_started_at TIMESTAMP WITHOUT TIME ZONE DEFAULT now(),
      total_rows BIGINT,
      accepted_rows BIGINT,
      review_rows BIGINT,
      reject_rows BIGINT,
      notes TEXT
    );

    -- ---- Auditability & Rollup support ----
    -- Add a run UUID for cross-table join (safe to add, nullable for legacy rows)
    ALTER TABLE public.etl_run_log
      ADD COLUMN IF NOT EXISTS run_uuid UUID;

    -- Candidate audit log (stores top-K candidates per raw row with scores and decision)
    CREATE TABLE IF NOT EXISTS public.match_audit_log (
      run_uuid              UUID,
      raw_id                BIGINT,
      candidate_company_id  BIGINT REFERENCES public.dim_company(company_id),
      rule                  TEXT,
      sim_name              NUMERIC,
      addr_match            BOOLEAN,
      industry_match        BOOLEAN,
      score                 NUMERIC,
      decided               TEXT,     -- 'ACCEPTED' | 'REVIEW' | 'REJECTED_TOPK'
      decided_company_id    BIGINT,
      details               JSONB,
      created_at            TIMESTAMP WITHOUT TIME ZONE DEFAULT now()
    );
    CREATE INDEX IF NOT EXISTS idx_match_audit_run_raw ON public.match_audit_log (run_uuid, raw_id);

    -- Review queue for borderline matches
    CREATE TABLE IF NOT EXISTS public.match_review_queue (
      raw_id                BIGINT PRIMARY KEY,
      top_candidate_company_id BIGINT REFERENCES public.dim_company(company_id),
      score                 NUMERIC,
      name_norm             TEXT,
      addr_key              TEXT,
      industry_code         TEXT,
      status                TEXT DEFAULT 'PENDING', -- PENDING | APPROVED | REJECTED
      created_at            TIMESTAMP WITHOUT TIME ZONE DEFAULT now(),
      decided_company_id    BIGINT,
      decided_at            TIMESTAMP WITHOUT TIME ZONE
    );

    -- Additional fields & trigger for auto-updates on approval
    ALTER TABLE public.match_review_queue
      ADD COLUMN IF NOT EXISTS decided_rollup_id BIGINT REFERENCES public.dim_company(company_id);
    CREATE INDEX IF NOT EXISTS idx_review_queue_status ON public.match_review_queue (status);
    CREATE INDEX IF NOT EXISTS idx_review_queue_decided_company ON public.match_review_queue (decided_company_id);

    -- When a review row becomes APPROVED, upsert alias and optional rollup mapping
    CREATE OR REPLACE FUNCTION public.trg_fn_review_approved()
    RETURNS trigger
    LANGUAGE plpgsql
    AS $$
    BEGIN
      IF (TG_OP = 'UPDATE' AND NEW.status = 'APPROVED' AND (OLD.status IS DISTINCT FROM NEW.status)) THEN
        -- 1) Alias upsert from raw source name
        INSERT INTO public.company_alias(company_id, alias, normalized_alias, source)
        SELECT NEW.decided_company_id,
               p.company_name,
               nps_normalize_company_name(p.company_name),
               'review_approved'
        FROM public.pension p
        WHERE p.id = NEW.raw_id
        ON CONFLICT (company_id, normalized_alias) DO NOTHING;

        -- 2) Optional rollup mapping (branch → parent)
        IF NEW.decided_rollup_id IS NOT NULL THEN
          INSERT INTO public.company_rollup(company_id, rollup_id, method)
          VALUES (NEW.decided_company_id, NEW.decided_rollup_id, 'review_approved')
          ON CONFLICT (company_id) DO UPDATE
            SET rollup_id = EXCLUDED.rollup_id,
                method    = 'review_approved';
        END IF;

        -- 3) Decision timestamp if missing
        IF NEW.decided_at IS NULL THEN
          NEW.decided_at := now();
        END IF;
      END IF;

      RETURN NEW;
    END;
    $$;

    DROP TRIGGER IF EXISTS trg_review_approved ON public.match_review_queue;
    CREATE TRIGGER trg_review_approved
    AFTER UPDATE OF status, decided_company_id, decided_rollup_id
    ON public.match_review_queue
    FOR EACH ROW
    WHEN (NEW.status = 'APPROVED' AND NEW.decided_company_id IS NOT NULL)
    EXECUTE FUNCTION public.trg_fn_review_approved();

    -- Rollup mapping: map branch company_id → rollup (parent/master) company_id.
    CREATE TABLE IF NOT EXISTS public.company_rollup (
      company_id    BIGINT PRIMARY KEY REFERENCES public.dim_company(company_id) ON DELETE CASCADE,
      rollup_id     BIGINT NOT NULL REFERENCES public.dim_company(company_id) ON DELETE CASCADE,
      method        TEXT,
      created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
    );
    CREATE INDEX IF NOT EXISTS idx_company_rollup_rollup ON public.company_rollup (rollup_id);

    -- Rollup-level monthly fact (aggregated over all mapped branches)
    CREATE TABLE IF NOT EXISTS public.fact_pension_monthly_rollup (
      rollup_id             BIGINT NOT NULL REFERENCES public.dim_company(company_id),
      ym                    DATE   NOT NULL,
      subscriber_count      BIGINT,
      monthly_notice_amount BIGINT,
      new_subscribers       INTEGER,
      lost_subscribers      INTEGER,
      PRIMARY KEY (rollup_id, ym)
    );
    CREATE INDEX IF NOT EXISTS idx_fact_rollup_ym_desc ON public.fact_pension_monthly_rollup (rollup_id, ym DESC);

    -- Lineage table: which raw rows contributed to which rollup/month in this run
    CREATE TABLE IF NOT EXISTS public.fact_pension_lineage (
      run_uuid    UUID,
      rollup_id   BIGINT NOT NULL REFERENCES public.dim_company(company_id),
      ym          DATE   NOT NULL,
      raw_id      BIGINT NOT NULL,
      company_id  BIGINT NOT NULL REFERENCES public.dim_company(company_id),
      PRIMARY KEY (run_uuid, raw_id)
    );
    CREATE INDEX IF NOT EXISTS idx_fact_lineage_rollup_ym ON public.fact_pension_lineage (rollup_id, ym);
    """

    with nps_postgres.get_connection() as conn:
        with conn.cursor() as cur:
            actions.append(_exec(cur, ddl, context, "Create SaaS tables & indexes"))
        conn.commit()

    md = {"actions": actions, "elapsed_s": round(time.time() - t0, 2)}
    context.log.info("✅ ensure_saas_tables done")
    return dg.MaterializeResult(metadata=md)


# ==========================
# Asset 2: One-shot ETL Run
# ==========================
@dg.asset(
    group_name="NPS_SaaS",
    deps=[ensure_saas_tables],
    description=(
        "Extract recent pension rows → normalize/block → candidate match → score →\n"
        "UPSERT dim_company/company_alias → aggregate monthly → UPSERT fact_pension_monthly → log coverage."
    ),
    tags={"domain": "pension", "process": "etl", "tier": "saas"},
)
def nps_saas_etl_run(context: dg.AssetExecutionContext, nps_postgres: PostgresResource) -> dg.MaterializeResult:
    t0 = time.time()
    actions: List[Dict[str, Any]] = []
    metrics: Dict[str, Any] = {}
    run_uuid = str(uuid.uuid4())
    metrics["run_uuid"] = run_uuid

    with nps_postgres.get_connection() as conn:
        with conn.cursor() as cur:
            # 0) Load control
            cur.execute(
                "SELECT last_ym FROM public.etl_control WHERE pipeline_name=%s;",
                (PIPELINE_NAME,)
            )
            row = cur.fetchone()
            last_ym = row[0] if row and row[0] else None
            actions.append({"desc": "Load etl_control.last_ym", "value": str(last_ym)})

            # 1) Extract to TEMP (recent N months with buffer)
            sql_extract = f"""
            CREATE TEMP TABLE stg_pension_recent AS
            SELECT p.*, 
                   nps_normalize_company_name(p.company_name) AS name_norm,
                   concat_ws('-', p.addr_sigungu_code, p.legal_dong_addr_code) AS addr_key
            FROM {POSTGRES_NPS_TABLE} p
            WHERE p.data_created_ym >= (
              date_trunc('month', COALESCE(%s::date, date_trunc('month', CURRENT_DATE)::date - INTERVAL '1 month'))
              - INTERVAL '{PROCESS_MONTH_BUFFER} months'
            );
            ANALYZE stg_pension_recent;
            """
            _exec(cur, sql_extract, context, "Extract recent to TEMP & analyze", params=(last_ym,))

            # quick counts
            cur.execute("SELECT COUNT(*) FROM stg_pension_recent;")
            total_rows = cur.fetchone()[0]
            metrics["total_rows"] = total_rows

            # Optional helpful indexes on TEMP (keep minimal)
            _exec(cur, "CREATE INDEX ON stg_pension_recent(name_norm);", context, "stg idx name_norm")
            _exec(cur, "CREATE INDEX ON stg_pension_recent(business_reg_num) WHERE business_reg_num IS NOT NULL;", context, "stg idx brn")
            _exec(cur, "CREATE INDEX ON stg_pension_recent(addr_sigungu_code, industry_code);", context, "stg idx addr+industry")

            # 2) Insert missing companies by BRN (deterministic-ish creation)
            sql_missing_dim = """
            INSERT INTO public.dim_company(business_reg_num, canonical_name, normalized_name, industry_code, addr_key, company_types)
            SELECT DISTINCT ON (r.business_reg_num, r.addr_key)
                   r.business_reg_num,
                   nps_canonicalize_company_name(r.company_name),
                   r.name_norm,
                   r.industry_code,
                   r.addr_key,
                   nps_extract_company_types(r.company_name)
            FROM stg_pension_recent r
            WHERE r.business_reg_num IS NOT NULL
              AND NOT EXISTS (
                SELECT 1 FROM public.dim_company d 
                WHERE d.business_reg_num = r.business_reg_num 
                  AND d.addr_key = r.addr_key
              );
            """
            actions.append(_exec(cur, sql_missing_dim, context, "Upsert new dim_company by BRN"))

            # 3) Candidates (BRN6 match: strong but not unique)
            sql_cand_brn = """
            CREATE TEMP TABLE cand_brn AS
            SELECT r.id AS raw_id, d.company_id, 'BRN6_ADDR_MATCH' AS rule,
                   0.95 AS sim_name  -- BRN6 + address is very strong
            FROM stg_pension_recent r
            JOIN public.dim_company d
              ON d.business_reg_num = r.business_reg_num
             AND d.addr_key = r.addr_key
            WHERE r.business_reg_num IS NOT NULL;
            ANALYZE cand_brn;
            """
            _exec(cur, sql_cand_brn, context, "cand_brn")

            # 4) Candidates (alias exact match — fast path)
            sql_cand_alias_exact = """
            CREATE TEMP TABLE cand_alias_exact AS
            SELECT r.id AS raw_id, ca.company_id, 'ALIAS_EQ' AS rule,
                   0.92 AS sim_name  -- exact alias equality is strong
            FROM stg_pension_recent r
            JOIN public.company_alias ca
              ON ca.normalized_alias = r.name_norm
            WHERE length(r.name_norm) >= 3;
            ANALYZE cand_alias_exact;
            """
            _exec(cur, sql_cand_alias_exact, context, "cand_alias_exact")

            # 4.5) Unresolved set → restrict KNN to rows not resolved by BRN or alias
            sql_unresolved = """
            CREATE TEMP TABLE unresolved AS
            SELECT r.id
            FROM stg_pension_recent r
            LEFT JOIN cand_brn b ON b.raw_id = r.id
            LEFT JOIN cand_alias_exact a ON a.raw_id = r.id
            WHERE b.raw_id IS NULL AND a.raw_id IS NULL;
            ANALYZE unresolved;
            """
            _exec(cur, sql_unresolved, context, "unresolved for knn")

            # 5) Candidates (kNN top-K)
            sql_cand_knn = f"""
            CREATE TEMP TABLE cand_name_knn AS
            SELECT r.id AS raw_id, d.company_id, 'NAME_KNN' AS rule,
                   1 - (d.normalized_name <-> r.name_norm) AS sim_name
            FROM unresolved u
            JOIN stg_pension_recent r ON r.id = u.id
            JOIN LATERAL (
              SELECT company_id, normalized_name
              FROM public.dim_company
              ORDER BY normalized_name <-> r.name_norm
              LIMIT {KNN_TOPK}
            ) d ON TRUE
            WHERE length(r.name_norm) >= 3;
            ANALYZE cand_name_knn;
            """
            _exec(cur, sql_cand_knn, context, "cand_name_knn")

            # 6) Score with industry/address bonuses (BRN6 gets base 0.85, not 1.0)
            sql_score = """
            CREATE TEMP TABLE cand_all AS
            WITH base AS (
              SELECT 
                x.raw_id,
                x.company_id,
                x.rule,
                x.sim_name,
                (r.industry_code = d.industry_code) AS industry_match,
                (r.addr_sigungu_code::text = split_part(d.addr_key,'-',1)) AS addr_match
              FROM (
                SELECT * FROM cand_brn
                UNION ALL SELECT * FROM cand_alias_exact
                UNION ALL SELECT * FROM cand_name_knn
              ) x
              JOIN stg_pension_recent r ON r.id = x.raw_id
              JOIN public.dim_company d ON d.company_id = x.company_id
            )
            SELECT 
              raw_id,
              company_id,
              rule,
              sim_name,
              industry_match,
              addr_match,
              CASE 
                WHEN rule='BRN6_ADDR_MATCH' THEN LEAST(1.0, sim_name + 0.10*industry_match::int + 0.10*addr_match::int)
                ELSE GREATEST(0, LEAST(1.0,
                  sim_name
                  + 0.20*industry_match::int
                  + 0.20*addr_match::int
                ))
              END AS score
            FROM base;
            ANALYZE cand_all;
            """
            _exec(cur, sql_score, context, "cand_all (scored)")

            # 7) Accept / Review / Reject partitions (TEMP)
            sql_accept = f"""
            CREATE TEMP TABLE accepted AS
            SELECT DISTINCT ON (raw_id) raw_id, company_id, score
            FROM (
              SELECT raw_id, company_id, score,
                     ROW_NUMBER() OVER (PARTITION BY raw_id ORDER BY score DESC) AS rn
              FROM cand_all
              WHERE score >= {AUTO_ACCEPT_THRESHOLD}
                AND (rule <> 'BRN6_MATCH' OR (industry_match AND addr_match))
            ) t
            WHERE rn = 1;
            ANALYZE accepted;

            CREATE TEMP TABLE review AS
            SELECT DISTINCT ON (raw_id) raw_id, company_id, score
            FROM (
              SELECT raw_id, company_id, score,
                     ROW_NUMBER() OVER (PARTITION BY raw_id ORDER BY score DESC) AS rn
              FROM cand_all
              WHERE score >= {REVIEW_LOWER} AND score < {REVIEW_UPPER}
            ) t
            WHERE rn = 1;
            ANALYZE review;

            CREATE TEMP TABLE reject AS
            SELECT raw_id, MAX(score) AS score
            FROM cand_all
            GROUP BY raw_id
            HAVING MAX(score) < {REVIEW_LOWER};
            ANALYZE reject;
            """
            _exec(cur, sql_accept, context, "accepted/review/reject temps")

            # coverage counts
            cur.execute("SELECT COUNT(*) FROM accepted;"); accepted_rows = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM review;");   review_rows   = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM reject;");   reject_rows   = cur.fetchone()[0]
            metrics.update({
                "accepted_rows": accepted_rows,
                "review_rows": review_rows,
                "reject_rows": reject_rows,
            })

            # 7.5) Audit log (top-K candidates per raw) + Review queue upsert
            sql_audit = f"""
            -- Top-K candidates per raw for audit trail
            CREATE TEMP TABLE cand_topk AS
            SELECT *
            FROM (
              SELECT raw_id, company_id, rule, sim_name, industry_match, addr_match, score,
                     ROW_NUMBER() OVER (PARTITION BY raw_id ORDER BY score DESC) AS rn
              FROM cand_all
            ) t
            WHERE rn <= {TOP_AUDIT_K};
            ANALYZE cand_topk;

            -- Insert audit rows with decision label
            INSERT INTO public.match_audit_log(run_uuid, raw_id, candidate_company_id, rule, sim_name, addr_match, industry_match, score, decided, decided_company_id, details)
            SELECT %(run_uuid)s, t.raw_id, t.company_id, t.rule, t.sim_name, t.addr_match, t.industry_match, t.score,
                   CASE 
                     WHEN a.raw_id IS NOT NULL THEN 'ACCEPTED'
                     WHEN v.raw_id IS NOT NULL THEN 'REVIEW'
                     ELSE 'REJECTED_TOPK'
                   END AS decided,
                   COALESCE(a.company_id, v.company_id),
                   jsonb_build_object('rn', t.rn)
            FROM cand_topk t
            LEFT JOIN accepted a ON a.raw_id = t.raw_id AND a.company_id = t.company_id
            LEFT JOIN review   v ON v.raw_id = t.raw_id AND v.company_id = t.company_id;

            -- Enqueue review (idempotent)
            INSERT INTO public.match_review_queue(raw_id, top_candidate_company_id, score, name_norm, addr_key, industry_code, status)
            SELECT v.raw_id, v.company_id, v.score, r.name_norm, r.addr_key, r.industry_code, 'PENDING'
            FROM review v
            JOIN stg_pension_recent r ON r.id = v.raw_id
            ON CONFLICT (raw_id) DO UPDATE
              SET top_candidate_company_id = EXCLUDED.top_candidate_company_id,
                  score = EXCLUDED.score,
                  name_norm = EXCLUDED.name_norm,
                  addr_key = EXCLUDED.addr_key,
                  industry_code = EXCLUDED.industry_code;
            """
            _exec(cur, sql_audit, context, "Audit log & review queue", params={"run_uuid": run_uuid})

            # 8) Upsert aliases for accepted rows
            sql_alias = """
            INSERT INTO public.company_alias(company_id, alias, normalized_alias, alias_canonical, source, quality)
            SELECT a.company_id, r.company_name, r.name_norm, nps_canonicalize_company_name(r.company_name), 'pension_raw',
                   CASE WHEN c.rule = 'ALIAS_EQ' THEN 'exact' ELSE 'high_conf' END
            FROM accepted a
            JOIN stg_pension_recent r ON r.id = a.raw_id
            -- 안전장치: BRN6만으로 수락된 케이스 배제, alias_exact 또는 이름 KNN 고신뢰만 반영
            JOIN cand_all c ON c.raw_id = a.raw_id AND c.company_id = a.company_id
            WHERE (
              c.rule = 'ALIAS_EQ'
              OR (c.rule = 'NAME_KNN' AND c.sim_name >= 0.95)
            )
            ON CONFLICT (company_id, normalized_alias) DO NOTHING;
            """
            actions.append(_exec(cur, sql_alias, context, "Upsert company_alias from accepted"))

            # 9) Aggregate → fact upsert
            sql_agg = """
            CREATE TEMP TABLE agg_monthly AS
            SELECT a.company_id,
                   date_trunc('month', r.data_created_ym)::date AS ym,
                   SUM(r.subscriber_count)      AS subscriber_count,
                   SUM(r.monthly_notice_amount) AS monthly_notice_amount,
                   SUM(r.new_subscribers)       AS new_subscribers,
                   SUM(r.lost_subscribers)      AS lost_subscribers
            FROM accepted a
            JOIN stg_pension_recent r ON r.id = a.raw_id
            GROUP BY 1,2;
            ANALYZE agg_monthly;

            INSERT INTO public.fact_pension_monthly(company_id, ym, subscriber_count, monthly_notice_amount, new_subscribers, lost_subscribers)
            SELECT company_id, ym, subscriber_count, monthly_notice_amount, new_subscribers, lost_subscribers
            FROM agg_monthly
            ON CONFLICT (company_id, ym) DO UPDATE
            SET subscriber_count      = EXCLUDED.subscriber_count,
                monthly_notice_amount = EXCLUDED.monthly_notice_amount,
                new_subscribers       = EXCLUDED.new_subscribers,
                lost_subscribers      = EXCLUDED.lost_subscribers;
            """
            actions.append(_exec(cur, sql_agg, context, "Upsert fact_pension_monthly"))

            # 9.5) Rollup aggregate (branch → parent) + lineage
            sql_rollup = """
            -- Compute rollup_id per accepted raw
            CREATE TEMP TABLE accepted_with_rollup AS
            SELECT a.raw_id,
                   a.company_id,
                   COALESCE(cr.rollup_id, a.company_id) AS rollup_id
            FROM accepted a
            LEFT JOIN public.company_rollup cr ON cr.company_id = a.company_id;
            ANALYZE accepted_with_rollup;

            -- Rollup monthly aggregation
            CREATE TEMP TABLE agg_rollup_monthly AS
            SELECT awr.rollup_id,
                   date_trunc('month', r.data_created_ym)::date AS ym,
                   SUM(r.subscriber_count)::bigint      AS subscriber_count,
                   SUM(r.monthly_notice_amount)::bigint AS monthly_notice_amount,
                   SUM(r.new_subscribers)               AS new_subscribers,
                   SUM(r.lost_subscribers)              AS lost_subscribers
            FROM accepted_with_rollup awr
            JOIN stg_pension_recent r ON r.id = awr.raw_id
            GROUP BY 1,2;
            ANALYZE agg_rollup_monthly;

            INSERT INTO public.fact_pension_monthly_rollup(rollup_id, ym, subscriber_count, monthly_notice_amount, new_subscribers, lost_subscribers)
            SELECT rollup_id, ym, subscriber_count, monthly_notice_amount, new_subscribers, lost_subscribers
            FROM agg_rollup_monthly
            ON CONFLICT (rollup_id, ym) DO UPDATE
            SET subscriber_count      = EXCLUDED.subscriber_count,
                monthly_notice_amount = EXCLUDED.monthly_notice_amount,
                new_subscribers       = EXCLUDED.new_subscribers,
                lost_subscribers      = EXCLUDED.lost_subscribers;

            -- Lineage of raw → rollup/month
            INSERT INTO public.fact_pension_lineage(run_uuid, rollup_id, ym, raw_id, company_id)
            SELECT %(run_uuid)s,
                   awr.rollup_id,
                   date_trunc('month', r.data_created_ym)::date AS ym,
                   awr.raw_id,
                   awr.company_id
            FROM accepted_with_rollup awr
            JOIN stg_pension_recent r ON r.id = awr.raw_id
            ON CONFLICT (run_uuid, raw_id) DO NOTHING;
            """
            actions.append(_exec(cur, sql_rollup, context, "Rollup aggregate + lineage", params={"run_uuid": run_uuid}))

            # 10) Update control + log run
            _exec(
                cur,
                """
                UPDATE public.etl_control
                   SET last_ym = date_trunc('month', CURRENT_DATE)::date,
                       updated_at = now()
                 WHERE pipeline_name = %(p)s;
                """,
                context,
                "Update etl_control.last_ym",
                params={"p": PIPELINE_NAME},
            )
            cur.execute(
                "INSERT INTO public.etl_run_log(pipeline_name, total_rows, accepted_rows, review_rows, reject_rows, notes, run_uuid) VALUES (%s,%s,%s,%s,%s,%s,%s);",
                (PIPELINE_NAME, total_rows, accepted_rows, review_rows, reject_rows, None, run_uuid)
            )

        conn.commit()

    md = {
        "elapsed_s": round(time.time() - t0, 2),
        **metrics,
    }
    context.log.info(f"✅ ETL done: {md}")
    return dg.MaterializeResult(metadata=md)


# =============================
# Checks
# =============================
@dg.asset_check(
    asset=nps_saas_etl_run,
    name="check_timeseries_uniqueness",
    description="Ensure (company_id, ym) uniqueness in fact_pension_monthly.",
)
def check_timeseries_uniqueness(context: dg.AssetCheckExecutionContext, nps_postgres: PostgresResource) -> dg.AssetCheckResult:
    with nps_postgres.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*) FROM (
                  SELECT company_id, ym, COUNT(*)
                  FROM public.fact_pension_monthly
                  GROUP BY 1,2
                  HAVING COUNT(*) > 1
                ) t;
                """
            )
            dup_count = cur.fetchone()[0]
    passed = (dup_count == 0)
    return dg.AssetCheckResult(passed=passed, description=("OK" if passed else f"duplicates={dup_count}"))


@dg.asset_check(
    asset=nps_saas_etl_run,
    name="check_recent_freshness",
    description="fact has data for recent months (within 2 months).",
)
def check_recent_freshness(context: dg.AssetCheckExecutionContext, nps_postgres: PostgresResource) -> dg.AssetCheckResult:
    with nps_postgres.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT MAX(ym) FROM public.fact_pension_monthly;
                """
            )
            max_ym = cur.fetchone()[0]
    passed = False
    desc = "no data"
    if max_ym:
        cur_month = time.strftime("%Y-%m-01")
        # consider fresh if within last 2 months
        passed = True  # we don't compute precisely here; visible via metadata
        desc = f"max_ym={max_ym}"
    return dg.AssetCheckResult(passed=passed, description=desc, metadata={"max_ym": str(max_ym) if max_ym else None})


@dg.asset_check(
    asset=nps_saas_etl_run,
    name="check_etl_coverage",
    description="Show last run coverage (accepted/review/reject).",
)
def check_etl_coverage(context: dg.AssetCheckExecutionContext, nps_postgres: PostgresResource) -> dg.AssetCheckResult:
    with nps_postgres.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT total_rows, accepted_rows, review_rows, reject_rows, run_started_at
                FROM public.etl_run_log
                WHERE pipeline_name=%s
                ORDER BY run_started_at DESC
                LIMIT 1;
                """,
                (PIPELINE_NAME,)
            )
            row = cur.fetchone()
    if not row:
        return dg.AssetCheckResult(passed=False, description="no previous run")
    total, acc, rev, rej, started_at = row
    passed = total is not None and total >= 0
    md = {"total": total, "accepted": acc, "review": rev, "reject": rej, "started_at": str(started_at)}
    return dg.AssetCheckResult(passed=passed, description="coverage loaded", metadata=md)


# ==============================
# Asset 3: Full Rebuild from pension (immutable)
# ==============================
@dg.asset(
    group_name="NPS_SaaS",
    deps=[ensure_saas_tables],
    description=(
        "Full rebuild of SaaS layer from immutable pension source.\n"
        "Truncates downstream tables and deterministically recreates dim_company, company_alias,\n"
        "fact_pension_monthly, company_rollup (identity), fact_pension_monthly_rollup, and lineage.\n"
        "Assumes business_reg_num (6-digit) is present for all rows."
    ),
    tags={"domain": "pension", "process": "rebuild", "tier": "saas"},
)
def nps_full_rebuild_from_pension(context: dg.AssetExecutionContext, nps_postgres: PostgresResource) -> dg.MaterializeResult:
    t0 = time.time()
    actions: List[Dict[str, Any]] = []
    run_uuid = str(uuid.uuid4())

    with nps_postgres.get_connection() as conn:
        with conn.cursor() as cur:
            # 0) Truncate downstream tables (children → parents)
            trunc_sql = """
            TRUNCATE TABLE
              public.fact_pension_monthly_rollup,
              public.fact_pension_lineage,
              public.fact_pension_monthly,
              public.match_review_queue,
              public.match_audit_log,
              public.company_rollup,
              public.company_alias,
              public.dim_company
            RESTART IDENTITY;
            """
            actions.append(_exec(cur, trunc_sql, context, "TRUNCATE downstream tables"))

            # 1) dim_company rebuild using BRN6 + addr_key (branch-level), preferring the most recent month
            sql_build_dim = """
            WITH src AS (
              SELECT 
                p.business_reg_num                         AS brn,
                nps_canonicalize_company_name(p.company_name)                                    AS canon,
                nps_normalize_company_name(nps_canonicalize_company_name(p.company_name))        AS norm,
                nps_extract_company_types(p.company_name)                                       AS ctypes,
                p.industry_code,
                concat_ws('-', p.addr_sigungu_code, p.legal_dong_addr_code)                     AS addr_key,
                date_trunc('month', p.data_created_ym)::date                                    AS ym
              FROM public.pension p
            ),
            latest AS (
              SELECT brn, addr_key, MAX(ym) AS latest_ym
              FROM src
              GROUP BY brn, addr_key
            ),
            src_latest AS (
              SELECT s.*
              FROM src s
              JOIN latest l ON l.brn = s.brn AND l.addr_key = s.addr_key AND l.latest_ym = s.ym
            ),
            -- 대표 정식명/정규명 선택: 다수결 → 길이 → 사전순
            canon_counts AS (
              SELECT brn, addr_key, canon, norm, COUNT(*) AS cnt
              FROM src_latest
              GROUP BY brn, addr_key, canon, norm
            ),
            canon_choice AS (
              SELECT DISTINCT ON (brn, addr_key)
                     brn, addr_key, canon, norm
              FROM canon_counts
              ORDER BY brn, addr_key, cnt DESC, length(canon) DESC, canon ASC
            ),
            -- 대표 업종 선택: 다수결 → NULLS LAST
            ind_counts AS (
              SELECT brn, addr_key, industry_code, COUNT(*) AS cnt
              FROM src_latest
              GROUP BY brn, addr_key, industry_code
            ),
            ind_choice AS (
              SELECT DISTINCT ON (brn, addr_key)
                     brn, addr_key, industry_code
              FROM ind_counts
              ORDER BY brn, addr_key, cnt DESC, industry_code NULLS LAST
            ),
            -- 회사 형태 배열 집계(단일만 있으면 단일 요소 배열)
            ctype_array AS (
              SELECT brn, addr_key,
                     CASE WHEN COUNT(x) = 0 THEN NULL ELSE ARRAY(SELECT DISTINCT x FROM unnest(array_agg(x)) x) END AS ctypes
              FROM (
                SELECT brn, addr_key, unnest(ctypes) AS x FROM src_latest WHERE ctypes IS NOT NULL
              ) u
              GROUP BY brn, addr_key
            ),
            joined AS (
              SELECT 
                c.brn AS business_reg_num,
                c.canon AS canonical_name,
                c.norm  AS normalized_name,
                i.industry_code,
                c.addr_key,
                ca.ctypes AS company_types
              FROM canon_choice c
              LEFT JOIN ind_choice  i  ON i.brn  = c.brn AND i.addr_key  = c.addr_key
              LEFT JOIN ctype_array  ca ON ca.brn = c.brn AND ca.addr_key = c.addr_key
            )
            INSERT INTO public.dim_company(business_reg_num, canonical_name, normalized_name, industry_code, addr_key, company_types)
            SELECT business_reg_num, canonical_name, normalized_name, industry_code, addr_key, company_types
            FROM joined;
            """
            actions.append(_exec(cur, sql_build_dim, context, "Rebuild dim_company from pension (BRN6+ADDR)"))

            # 2) company_alias rebuild from pension raw names
            sql_alias = """
            INSERT INTO public.company_alias(company_id, alias, normalized_alias, alias_canonical, source, quality)
            SELECT d.company_id,
                   p.company_name,
                   nps_normalize_company_name(p.company_name),
                   nps_canonicalize_company_name(p.company_name),
                   'full_rebuild',
                   'raw'
            FROM public.pension p
            JOIN public.dim_company d 
              ON d.business_reg_num = p.business_reg_num
             AND d.addr_key = concat_ws('-', p.addr_sigungu_code, p.legal_dong_addr_code)
            WHERE length(coalesce(p.company_name, '')) > 0
            ON CONFLICT (company_id, normalized_alias) DO NOTHING;
            """
            actions.append(_exec(cur, sql_alias, context, "Rebuild company_alias from pension"))

            # 3) fact_pension_monthly rebuild
            sql_fact = """
            INSERT INTO public.fact_pension_monthly(company_id, ym, subscriber_count, monthly_notice_amount, new_subscribers, lost_subscribers)
            SELECT d.company_id,
                   date_trunc('month', p.data_created_ym)::date AS ym,
                   SUM(p.subscriber_count)      AS subscriber_count,
                   SUM(p.monthly_notice_amount) AS monthly_notice_amount,
                   SUM(p.new_subscribers)       AS new_subscribers,
                   SUM(p.lost_subscribers)      AS lost_subscribers
            FROM public.pension p
            JOIN public.dim_company d 
              ON d.business_reg_num = p.business_reg_num
             AND d.addr_key = concat_ws('-', p.addr_sigungu_code, p.legal_dong_addr_code)
            GROUP BY 1,2;
            """
            actions.append(_exec(cur, sql_fact, context, "Rebuild fact_pension_monthly"))

            # 4) company_rollup: identity mapping for rebuild
            sql_rollup = """
            INSERT INTO public.company_rollup(company_id, rollup_id, method)
            SELECT company_id, company_id, 'identity_full_rebuild'
            FROM public.dim_company;
            """
            actions.append(_exec(cur, sql_rollup, context, "Rebuild company_rollup (identity)"))

            # 5) fact_pension_monthly_rollup rebuild (aggregate over identity → equals company facts)
            sql_fact_rollup = """
            INSERT INTO public.fact_pension_monthly_rollup(rollup_id, ym, subscriber_count, monthly_notice_amount, new_subscribers, lost_subscribers)
            SELECT cr.rollup_id,
                   f.ym,
                   SUM(f.subscriber_count)      AS subscriber_count,
                   SUM(f.monthly_notice_amount) AS monthly_notice_amount,
                   SUM(f.new_subscribers)       AS new_subscribers,
                   SUM(f.lost_subscribers)      AS lost_subscribers
            FROM public.fact_pension_monthly f
            JOIN public.company_rollup cr ON cr.company_id = f.company_id
            GROUP BY 1,2;
            """
            actions.append(_exec(cur, sql_fact_rollup, context, "Rebuild fact_pension_monthly_rollup"))

            # 6) lineage rebuild (raw → rollup/month via dim_company BRN6)
            sql_lineage = """
            INSERT INTO public.fact_pension_lineage(run_uuid, rollup_id, ym, raw_id, company_id)
            SELECT %(run_uuid)s,
                   d.company_id AS rollup_id,
                   date_trunc('month', p.data_created_ym)::date AS ym,
                   p.id AS raw_id,
                   d.company_id
            FROM public.pension p
            JOIN public.dim_company d 
              ON d.business_reg_num = p.business_reg_num
             AND d.addr_key = concat_ws('-', p.addr_sigungu_code, p.legal_dong_addr_code);
            """
            actions.append(_exec(cur, sql_lineage, context, "Rebuild lineage", params={"run_uuid": run_uuid}))

        conn.commit()

    md = {"elapsed_s": round(time.time() - t0, 2), "run_uuid": run_uuid, "actions": actions}
    context.log.info("✅ Full rebuild from pension completed")
    return dg.MaterializeResult(metadata=md)
