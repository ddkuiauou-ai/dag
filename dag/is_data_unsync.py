# dag/dag/is_data_unsync.py
"""IS 비동기(Unsync) 데이터 자산 모음
- 가독성 향상을 위한 정리(섹션/헬퍼 통일, SQL dedent, 일관 커서 헬퍼)
- **기능 변화 없음**
"""

# === Standard Library ===
import os
from contextlib import contextmanager
from textwrap import dedent

# === Third-Party ===
import dagster as dg

# === Local ===
from .resources import PostgresResource
from .schedules import ON10, ON2

# -----------------------------------------------------------------------------
# Config & Tunables
# -----------------------------------------------------------------------------
# 1) Trends window (>=3 snapshots for delta stability)
TRENDS_WINDOW_MINUTES = 30

# 2) Image rollup summarization tops (readability & perf)
OCR_TOP_K = 5
OBJECTS_TOP_K = 20
COLORS_TOP_K = 10
LABELS_TOP_K = 20  # max distinct labels to keep when rolling up per-post

# 3) Recent frontpage lookback window (minutes) for P0 promotion
FRONT_LOOKBACK_MINUTES = 20

# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
# Helper: safe interval string for Postgres binds

def _interval_str(minutes: int) -> str:
    """Return a safe Postgres interval string like '30 minutes'."""
    return f"{int(minutes)} minutes"


# Env var helpers (typed + defaults)

def _env_str(key: str, default: str) -> str:
    return os.getenv(key, default)


def _env_int(key: str, default: int) -> int:
    try:
        return int(os.getenv(key, str(default)))
    except Exception:
        return default

KEYWORD_TRENDS_SQL = (
    """
WITH bounds AS (
    SELECT to_timestamp(floor(extract(epoch from NOW())/600)*600) AS window_end
),
ranges AS (
    SELECT '3h'::text AS range_label, INTERVAL '3 hours' AS iv
    UNION ALL SELECT '6h',  INTERVAL '6 hours'
    UNION ALL SELECT '24h', INTERVAL '24 hours'
    UNION ALL SELECT '1w',  INTERVAL '7 days'
),
posts_in_range AS (
    SELECT
      r.range_label,
      (SELECT window_end - r.iv FROM bounds) AS window_start,
      (SELECT window_end FROM bounds)        AS window_end,
      p.id AS post_id,
      COALESCE(pe.fused_keywords, '[]'::jsonb) AS keywords
    FROM ranges r
    JOIN posts p
      ON p.is_deleted = FALSE
     AND p.timestamp >= (SELECT window_end - r.iv FROM bounds)
     AND p.timestamp <= (SELECT window_end FROM bounds)
    LEFT JOIN post_enrichment pe ON pe.post_id = p.id
),
exploded AS (
    SELECT
      pir.range_label,
      pir.window_start,
      pir.window_end,
      LOWER(TRIM(k.value)) AS keyword
    FROM posts_in_range pir
    JOIN LATERAL jsonb_array_elements_text(pir.keywords) AS k(value) ON TRUE
),
agg AS (
    SELECT range_label, window_start, window_end, keyword, COUNT(*) AS cnt
    FROM exploded
    WHERE keyword <> '' AND keyword IS NOT NULL
    GROUP BY range_label, window_start, window_end, keyword
)
INSERT INTO keyword_trends (keyword, range_label, window_start, window_end, count, computed_at)
SELECT keyword, range_label, window_start, window_end, cnt, NOW()
FROM agg
ON CONFLICT (keyword, range_label, window_start, window_end) DO UPDATE
  SET count = EXCLUDED.count,
      computed_at = NOW();
    """
)

# Common Postgres transaction helper
@contextmanager
def pg_cursor(is_postgres: PostgresResource, context: dg.OpExecutionContext, label: str):
    """Yield a cursor with commit/rollback and consistent error logging."""
    with is_postgres.get_connection() as conn:
        try:
            with conn.cursor() as cur:
                yield cur
            conn.commit()
        except Exception as e:
            try:
                conn.rollback()
            except Exception:
                pass
            context.log.exception(f"{label} failed; rolled back: {e}")
            raise


# -----------------------------------------------------------------------------
# Section A: Real-time Trends Assets
# -----------------------------------------------------------------------------
@dg.asset(
    name="post_trends_asset",
    group_name="IS",
    automation_condition=ON10,
    tags={"data_tier": "gold", "table": "post_trends"},
    description=(
        "post_snapshots 집계하여 트렌드/랭킹 산출 (슬라이딩 윈도우). "
        "hot_score는 가중합된 원시 델타(뷰 + 3*댓글 + 2*좋아요)로 저장되며, 소비 측에서 정규화/감쇠를 적용하는 것이 원칙"
    ),
)

def post_trends_asset(
    context: dg.OpExecutionContext,
    is_postgres: PostgresResource,
    post_snapshots_asset,
):
    """`post_snapshots`를 집계해 포스트별 트렌드(Δ, hot_score)를 계산·저장한다.
    윈도우는 30분(3스냅샷 이상 확보, 델타 안정화). hot_score는 윈도우 내 가중합 원시 델타(views + 3*comments + 2*likes)이며, 사이트/시간 정규화는 조회 시점에 수행한다.
    """
    window_minutes: int = TRENDS_WINDOW_MINUTES

    inserted = 0
    sql = dedent(
        """
        WITH bounds AS (
            SELECT
              to_timestamp(floor(extract(epoch from NOW())/600)*600) AS bucket_end,
              (%s)::interval AS iv
        ),
        snap AS (
            SELECT *
            FROM post_snapshots
            WHERE timestamp > (SELECT bucket_end - iv FROM bounds)
        ),
        latest AS (
            SELECT DISTINCT ON (post_id)
                   post_id, view_count, comment_count, like_count, dislike_count
            FROM   snap
            ORDER  BY post_id, timestamp DESC
        ),
        earliest AS (
            SELECT DISTINCT ON (post_id)
                   post_id, view_count, comment_count, like_count, dislike_count
            FROM   snap
            ORDER  BY post_id, timestamp ASC
        )
        INSERT INTO post_trends (
            post_id, window_start, window_end,
            view_delta, comment_delta, like_delta, dislike_delta, hot_score
        )
        SELECT
            l.post_id,
            (SELECT bucket_end - iv FROM bounds) AS window_start,
            (SELECT bucket_end FROM bounds)      AS window_end,
            (COALESCE(l.view_count,0)    - COALESCE(e.view_count,0))    AS view_delta,
            (COALESCE(l.comment_count,0) - COALESCE(e.comment_count,0)) AS comment_delta,
            (COALESCE(l.like_count,0)    - COALESCE(e.like_count,0))    AS like_delta,
            (COALESCE(l.dislike_count,0) - COALESCE(e.dislike_count,0)) AS dislike_delta,
            (COALESCE(l.view_count,0)    - COALESCE(e.view_count,0))
          + (COALESCE(l.comment_count,0) - COALESCE(e.comment_count,0)) * 3
          + (COALESCE(l.like_count,0)    - COALESCE(e.like_count,0))    * 2  AS hot_score
        FROM   latest l
        JOIN   earliest e USING (post_id)
        ON CONFLICT (post_id, window_start, window_end) DO UPDATE SET
            view_delta     = EXCLUDED.view_delta,
            comment_delta  = EXCLUDED.comment_delta,
            like_delta     = EXCLUDED.like_delta,
            dislike_delta  = EXCLUDED.dislike_delta,
            hot_score      = EXCLUDED.hot_score;
        """
    )

    with pg_cursor(is_postgres, context, "post_trends_asset") as cur:
        cur.execute(sql, (_interval_str(window_minutes),))
        inserted = cur.rowcount  # number of rows inserted/updated

    context.add_output_metadata({"window_minutes": window_minutes, "rows_upserted": inserted})
    return inserted

# Web(FRESH) query notes:
# - Base: mv_post_trends_30m (10분 버킷 정렬, 30분 핫 윈도우)
# - Robust normalization: 사이트별 분위/윈저라이즈 기반 정규화는 웹 쿼리 시점에 적용
# - Heat boost: 생성/최종갱신 ≤30m 글에 소폭(+~15%) 가중
# - Dynamic site cap: 리프레시마다 사이트 편중을 줄이도록 할당/인터리빙
# - Exposure damping: 메인 노출(선정=노출) 직후 단기 감쇄
@dg.asset(
    name="refresh_mv_post_trends_30m",
    group_name="IS",
    automation_condition=ON10,
    tags={"data_tier": "gold", "table": "mv_post_trends_30m"},
    description="주기적으로 mv_post_trends_30m Materialized View를 CONCURRENTLY로 갱신합니다.",
)

def refresh_mv_post_trends_30m(
    context: dg.OpExecutionContext, is_postgres: PostgresResource, post_snapshots_asset
):
    """Refresh the mv_post_trends_30m materialized view."""
    context.log.info("Refreshing materialized view mv_post_trends_30m")
    with pg_cursor(is_postgres, context, "refresh_mv_post_trends_30m") as cur:
        cur.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY mv_post_trends_30m;")
    return {"refreshed": True}

# Web(RANKED) query notes:
# - Base: mv_post_trends_agg (3h/6h/24h/1w 집계)
# - Normalization: 사이트/기간 단위 백분위/로버스트 정규화 → [0..1]
# - Interleaving: 동적 site cap + 중복 제거(섹션 간/내)
# - Cooldown: 클러스터 회전(cooldown) 또는 노출 감쇄 시그널과 조합
@dg.asset(
    name="refresh_mv_post_trends_agg",
    group_name="IS",
    automation_condition=ON10,
    tags={"data_tier": "gold", "table": "mv_post_trends_agg"},
    description="주기적으로 mv_post_trends_agg Materialized View를 CONCURRENTLY로 갱신합니다.",
)

def refresh_mv_post_trends_agg(
    context: dg.OpExecutionContext, is_postgres: PostgresResource, post_trends_asset
):
    context.log.info("Refreshing materialized view mv_post_trends_agg")
    with pg_cursor(is_postgres, context, "refresh_mv_post_trends_agg") as cur:
        cur.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY mv_post_trends_agg;")
    return {"refreshed": True}


# -----------------------------------------------------------------------------
# Section B: Enrichment Jobs – Promotion & Queueing
# -----------------------------------------------------------------------------
@dg.asset(
    name="promote_p0_from_frontpage_asset",
    group_name="IS",
    automation_condition=ON2,
    tags={"data_tier": "silver", "table": "fusion_jobs", "table2": "media_enrichment_jobs"},
    description="메인 노출 post_rotation.last_shown_at 최근된 포스트의 텍스트 이미지 잡을 P0로 승격",
)
def promote_p0_from_frontpage_asset(
    context: dg.OpExecutionContext, is_postgres: PostgresResource
):
    """
    - 최근 노출된(frontpage) 포스트 기준으로 LLM(텍스트)와 VLM(이미지) 잡을 모두 P0로 승격한다.
    - 기준: post_rotation.last_shown_at >= NOW() - FRONT_LOOKBACK_MINUTES minutes
    - 동일한 동작을 단일 SQL 문으로 수행해 DB 왕복을 최소화
    """
    LOOKBACK_MINUTES = FRONT_LOOKBACK_MINUTES

    sql = dedent(
        """
        WITH recent_posts AS (
            SELECT DISTINCT pr.post_id::text AS post_id
            FROM post_rotation pr
            JOIN posts p ON p.id = pr.post_id
            WHERE pr.last_shown_at >= NOW() - (%s)::interval
              AND p.is_deleted = FALSE
        ),
        upd_text AS (
            UPDATE fusion_jobs
               SET priority = 'P0',
                   status = 'queued',
                   next_attempt_at = NOW(),
                   updated_at = NOW()
             WHERE post_id IN (SELECT post_id FROM recent_posts)
               AND status IN ('queued','error')
               AND priority <> 'P0'
             RETURNING 1
        ),
        upd_media AS (
            UPDATE media_enrichment_jobs
               SET priority = 'P0',
                   status = 'queued',
                   next_attempt_at = NOW(),
                   updated_at = NOW()
             WHERE post_id IN (SELECT post_id FROM recent_posts)
               AND status IN ('queued','error')
               AND priority <> 'P0'
             RETURNING 1
        ),
        ins_media AS (
            INSERT INTO media_enrichment_jobs (post_id, url_hash, image_url, priority, status, next_attempt_at)
            SELECT s.post_id, s.url_hash, s.url, 'P0', 'queued', NOW()
            FROM (
                SELECT
                    pi.post_id::text AS post_id,
                    pi.url_hash,
                    pi.url,
                    ROW_NUMBER() OVER (PARTITION BY pi.post_id ORDER BY pi.url_hash ASC)  AS rn_asc,
                    ROW_NUMBER() OVER (PARTITION BY pi.post_id ORDER BY pi.url_hash DESC) AS rn_desc
                FROM post_images AS pi
                JOIN recent_posts rp ON rp.post_id = pi.post_id::text
            ) AS s
            WHERE s.rn_asc = 1 OR s.rn_desc = 1
            ON CONFLICT (post_id, url_hash) DO NOTHING
            RETURNING 1
        )
        SELECT
          COALESCE((SELECT COUNT(*) FROM upd_text), 0)  AS p0_text_upgraded,
          0                                           AS p0_text_inserted,
          COALESCE((SELECT COUNT(*) FROM upd_media), 0) AS p0_media_upgraded,
          COALESCE((SELECT COUNT(*) FROM ins_media), 0) AS p0_media_inserted;
        """
    )

    with pg_cursor(is_postgres, context, "promote_p0_from_frontpage_asset") as cur:
        cur.execute(sql, (_interval_str(LOOKBACK_MINUTES),))
        r = cur.fetchone() or (0, 0, 0, 0)
        p0_upgraded_text, p0_inserted_text, p0_upgraded_media, p0_inserted_media = map(int, r)

    context.add_output_metadata(
        {
            "p0_text_upgraded": p0_upgraded_text,
            "p0_text_inserted": p0_inserted_text,
            "p0_media_upgraded": p0_upgraded_media,
            "p0_media_inserted": p0_inserted_media,
        }
    )
    return {
        "p0_text_upgraded": p0_upgraded_text,
        "p0_text_inserted": p0_inserted_text,
        "p0_media_upgraded": p0_upgraded_media,
        "p0_media_inserted": p0_inserted_media,
    }


#  === VLM 백필(프롬프트/모델 변경 시 대량 재처리) asset ===
@dg.asset(
    name="text_only_enqueue_asset",
    group_name="IS",
    automation_condition=ON2,
    tags={"data_tier": "ops", "table": "fusion_jobs"},
    description=(
        "텍스트-only LLM 인큐어: has_images 여부와 전역 VLM ETA 기반으로 즉시 또는 "
        "짧은 홀드(next_attempt_at 지연)로 fusion_jobs에 upsert"
    ),
)

def text_only_enqueue_asset(
    context: dg.OpExecutionContext, is_postgres: PostgresResource, posts_asset
) -> dict:
    """
    제로나설계:
      - has_images = EXISTS(post_images) 로 확정
      - 전역 ETA_vlm = Q / R (Q: media_enrichment_jobs queued, R: 최근 ETA_WINDOW 분 처리량/분)
      - 정책:
          * has_images = false  → 즉시 인큐
          * has_images = true & ETA_vlm <= T_hold_max → 인큐하되 next_attempt_at = now + T_hold_max (홀드)
          * has_images = true & ETA_vlm >  T_hold_max → 즉시 인큐
      - 텍스트-only rev_key = md5(text_rev || ':' || 'noimg' || '::pv=' || FUSION_PROMPT_VER)
      - image_rev 가 이미 존재하는 글은 대상에서 제외 (이미지 버전이 준비된 상태)
    """
    LIMIT = _env_int("TEXT_ONLY_ENQUEUE_LIMIT", 1000)
    PRIORITY = _env_str("TEXT_ONLY_ENQUEUE_PRIORITY", "P1")
    ETA_WINDOW_MIN = _env_int("ETA_WINDOW_MINUTES", 15)
    T_HOLD_MAX_MIN = _env_int("T_HOLD_MAX_MINUTES", 3)
    PROMPT_VER = _env_str("FUSION_PROMPT_VER", "pv_default")

    # --- Metrics for ETA ---
    with pg_cursor(is_postgres, context, "text_only_enqueue_asset") as cur:
        cur.execute(
            "SELECT COUNT(*) FROM media_enrichment_jobs WHERE status='queued' AND next_attempt_at <= NOW()"
        )
        Q = int(cur.fetchone()[0])

        cur.execute(
            "SELECT COUNT(*) FROM post_image_enrichment WHERE enriched_at >= NOW() - (%s)::interval",
            (_interval_str(ETA_WINDOW_MIN),),
        )
        recent = int(cur.fetchone()[0])
        R = (recent / max(ETA_WINDOW_MIN, 1.0))  # images per minute
        ETA_vlm = (Q / max(R, 0.0001)) if Q > 0 else 0.0

        context.log.info(
            f"VLM ETA ~ {ETA_vlm:.2f} min (Q={Q}, R={R:.2f}/min, window={ETA_WINDOW_MIN}m)"
        )

        # --- 1) 즉시 인큐 집합 (이미지 없음 OR ETA 큰 이미지 글) ---
        cur.execute(
            dedent(
                """
                WITH
                  base AS (
                      SELECT p.id AS post_id, pe.text_rev
                      FROM posts p
                      LEFT JOIN post_enrichment pe ON pe.post_id = p.id
                      WHERE p.is_deleted = FALSE
                        AND pe.text_rev IS NOT NULL
                        AND COALESCE(pe.image_rev, 'noimg') = 'noimg'
                  ),
                  candidates AS (
                      SELECT b.post_id,
                             md5(b.text_rev || ':' || 'noimg' || '::pv=' || %s) AS rev_key
                      FROM base b
                      WHERE NOT EXISTS (SELECT 1 FROM post_images x WHERE x.post_id = b.post_id)
                            OR (%s)::boolean = TRUE
                      ORDER BY b.post_id
                      LIMIT %s
                  ),
                  ins AS (
                      INSERT INTO fusion_jobs (post_id, rev_key, priority, status, next_attempt_at, created_at, updated_at)
                      SELECT c.post_id, c.rev_key, %s, 'queued', NOW(), NOW(), NOW()
                        FROM candidates c
                      ON CONFLICT (post_id, rev_key) DO UPDATE
                        SET status = CASE WHEN fusion_jobs.status = 'error' THEN 'queued' ELSE fusion_jobs.status END,
                            next_attempt_at = CASE WHEN fusion_jobs.status <> 'done' THEN NOW() ELSE fusion_jobs.next_attempt_at END,
                            updated_at = NOW(),
                            priority = LEAST(fusion_jobs.priority, EXCLUDED.priority)
                        WHERE fusion_jobs.status <> 'done'
                      RETURNING 1
                  )
                SELECT COALESCE((SELECT COUNT(*) FROM ins), 0) AS upserted;
                """
            ),
            (PROMPT_VER, ETA_vlm > T_HOLD_MAX_MIN, LIMIT, PRIORITY),
        )
        row = cur.fetchone()
        upserted_immediate = int(row[0] if row else 0)

        # --- 2) 홀드 인큐 집합 (이미지 있고 ETA 작음) ---
        cur.execute(
            dedent(
                """
                WITH
                  base AS (
                      SELECT p.id AS post_id, pe.text_rev
                      FROM posts p
                      LEFT JOIN post_enrichment pe ON pe.post_id = p.id
                      WHERE p.is_deleted = FALSE
                        AND pe.text_rev IS NOT NULL
                        AND COALESCE(pe.image_rev, 'noimg') = 'noimg'
                        AND EXISTS (SELECT 1 FROM post_images x WHERE x.post_id = p.id)
                  ),
                  candidates AS (
                      SELECT b.post_id,
                             md5(b.text_rev || ':' || 'noimg' || '::pv=' || %s) AS rev_key
                      FROM base b
                      WHERE (%s)::boolean = TRUE
                      ORDER BY b.post_id
                      LIMIT %s
                  ),
                  ins AS (
                      INSERT INTO fusion_jobs (post_id, rev_key, priority, status, next_attempt_at, created_at, updated_at)
                      SELECT c.post_id, c.rev_key, %s, 'queued', NOW() + (%s)::interval, NOW(), NOW()
                        FROM candidates c
                      ON CONFLICT (post_id, rev_key) DO UPDATE
                        SET status = CASE WHEN fusion_jobs.status = 'error' THEN 'queued' ELSE fusion_jobs.status END,
                            next_attempt_at = CASE WHEN fusion_jobs.status <> 'done' THEN NOW() + (%s)::interval ELSE fusion_jobs.next_attempt_at END,
                            updated_at = NOW(),
                            priority = LEAST(fusion_jobs.priority, EXCLUDED.priority)
                        WHERE fusion_jobs.status <> 'done'
                      RETURNING 1
                  )
                SELECT COALESCE((SELECT COUNT(*) FROM ins), 0) AS upserted;
                """
            ),
            (
                PROMPT_VER,
                ETA_vlm <= T_HOLD_MAX_MIN,
                LIMIT,
                PRIORITY,
                _interval_str(T_HOLD_MAX_MIN),
                _interval_str(T_HOLD_MAX_MIN),
            ),
        )
        row2 = cur.fetchone()
        upserted_hold = int(row2[0] if row2 else 0)

    result = {
        "eta_vlm_min": round(ETA_vlm, 3),
        "queued_immediate": upserted_immediate,
        "queued_hold": upserted_hold,
        "hold_minutes": T_HOLD_MAX_MIN,
        "limit": LIMIT,
        "priority": PRIORITY,
        "prompt_ver": PROMPT_VER,
    }
    context.add_output_metadata(result)
    return result
@dg.asset(
    name="image_rollup_asset",
    group_name="IS",
    automation_condition=ON2,
    tags={"data_tier": "gold", "table": "post_enrichment"},
    description=(
        "변동분만 롤업: 이번 틱에 VLM으로 영향받은 post_id만 per-post 요약을 재계산하고, "
        "image_rev가 바뀐 경우에만 post_enrichment 업서트 및 fusion_jobs 인큐"
    ),
)

def image_rollup_asset(
    context: dg.OpExecutionContext, is_postgres: PostgresResource, vlm_worker_asset
):
    """
    변경점:
      - 이전(전체 업서트) → 이번: **변동분만 롤업**
      - `vlm_worker_asset`가 반환한 변경된 post_id 집합만 대상으로 함
      - 새로 계산한 요약/해시와 기존 값을 비교하여 **image_rev가 바뀐 경우에만** upsert
      - text_rev가 준비된 건에 한해 **rev_key(md5(text_rev||':'||image_rev))**를 fusion_jobs에 인큐 (변경된 케이스만)
    출력 메타:
      - changed_in: 이번 틱에 전달된 변경 post_id 수
      - rollup_candidates: 실제로 비교/업서트 후보가 된 수
      - rollup_upserted: post_enrichment에 실제 업서트된 수(=실변경)
      - fusion_enqueued: fusion_jobs에 새로 인큐된 수
    """
    # 0) 이번 틱에 변경된 post_id 목록 수집 (키 가변성 대비)
    changed_ids = []
    try:
        if isinstance(vlm_worker_asset, dict):
            for key in ("changed_post_ids", "affected_post_ids", "post_ids", "changed"):
                v = vlm_worker_asset.get(key)
                if v:
                    changed_ids = list({str(x) for x in v if x})
                    break
    except Exception:
        changed_ids = []

    if not changed_ids:
        context.add_output_metadata({"rolled_posts": 0, "skipped": "no VLM changes this tick"})
        return 0

    labels_top_k = _env_int("LABELS_TOP_K", LABELS_TOP_K)
    objects_top_k = _env_int("OBJECTS_TOP_K", OBJECTS_TOP_K)
    colors_top_k = _env_int("COLORS_TOP_K", COLORS_TOP_K)

    with pg_cursor(is_postgres, context, "image_rollup_asset") as cur:
        cur.execute(
            dedent(
                """
            WITH
              -- changed: 이번 틱에 변경된 post_id 집합
              changed AS (
                  SELECT UNNEST(%s::text[]) AS post_id
              ),
              -- caps: 최근순 캡션 결합
              caps AS (
                  SELECT pie.post_id,
                         NULLIF(STRING_AGG(NULLIF(TRIM(pie.caption), ''), ' | ' ORDER BY pie.enriched_at DESC, pie.url_hash), '') AS caption_join
                  FROM post_image_enrichment AS pie
                  JOIN changed USING (post_id)
                  GROUP BY pie.post_id
              ),
              -- labels_exploded: 라벨 펼치기
              labels_exploded AS (
                  SELECT pie.post_id,
                         TRIM(LOWER(jsonb_array_elements_text(COALESCE(pie.labels, '[]'::jsonb)))) AS label
                  FROM post_image_enrichment AS pie
                  JOIN changed USING (post_id)
              ),
              -- labels_ranked: 라벨 카운트/랭크 산출
              labels_ranked AS (
                  SELECT post_id, label, COUNT(*) AS cnt,
                         ROW_NUMBER() OVER (PARTITION BY post_id ORDER BY COUNT(*) DESC, label) AS rn
                  FROM labels_exploded
                  WHERE label <> '' AND label IS NOT NULL
                  GROUP BY post_id, label
              ),
              -- kw: 상위 K개의 라벨만 보존
              kw AS (
                  SELECT post_id,
                         COALESCE(jsonb_agg(label ORDER BY rn) FILTER (WHERE rn <= %s), '[]'::jsonb) AS image_keywords
                  FROM labels_ranked
                  GROUP BY post_id
              ),
              -- ocr: OCR 텍스트 결합
              ocr AS (
                  SELECT pie.post_id,
                         NULLIF(STRING_AGG(NULLIF(TRIM(pie.ocr_text), ''), ' | ' ORDER BY pie.enriched_at DESC, pie.url_hash), '') AS ocr_join
                  FROM post_image_enrichment AS pie
                  JOIN changed USING (post_id)
                  GROUP BY pie.post_id
              ),
              -- safety: 안전성 지표 요약 통계
              safety AS (
                  SELECT pie.post_id,
                         jsonb_build_object(
                           'nsfw_avg',     AVG(NULLIF((pie.safety->>'nsfw')::numeric, NULL)),
                           'nsfw_max',     MAX(NULLIF((pie.safety->>'nsfw')::numeric, NULL)),
                           'violence_avg', AVG(NULLIF((pie.safety->>'violence')::numeric, NULL)),
                           'violence_max', MAX(NULLIF((pie.safety->>'violence')::numeric, NULL))
                         ) AS safety_json
                  FROM post_image_enrichment AS pie
                  JOIN changed USING (post_id)
                  GROUP BY pie.post_id
              ),
              -- obj_exploded: 객체 리스트 펼치기
              obj_exploded AS (
                  SELECT pie.post_id,
                         TRIM(LOWER(COALESCE(obj->>'name', (obj)::text))) AS name
                  FROM post_image_enrichment AS pie
                  JOIN changed USING (post_id),
                       LATERAL jsonb_array_elements(COALESCE(pie.objects, '[]'::jsonb)) AS obj
              ),
              -- obj_ranked: 객체 카운트/랭크
              obj_ranked AS (
                  SELECT post_id, name, COUNT(*) AS cnt,
                         ROW_NUMBER() OVER (PARTITION BY post_id ORDER BY COUNT(*) DESC, name) AS rn
                  FROM obj_exploded
                  WHERE name <> '' AND name IS NOT NULL
                  GROUP BY post_id, name
              ),
              -- objects_top: 상위 K개의 객체만 보존
              objects_top AS (
                  SELECT post_id,
                         COALESCE(jsonb_agg(name ORDER BY rn) FILTER (WHERE rn <= %s), '[]'::jsonb) AS objects
                  FROM obj_ranked
                  GROUP BY post_id
              ),
              -- col_exploded: 색상 리스트 펼치기
              col_exploded AS (
                  SELECT pie.post_id,
                         TRIM(LOWER(CASE
                           WHEN jsonb_typeof(col) = 'string' THEN col::text
                           ELSE col->>'name'
                         END)) AS name
                  FROM post_image_enrichment AS pie
                  JOIN changed USING (post_id),
                       LATERAL jsonb_array_elements(COALESCE(pie.colors, '[]'::jsonb)) AS col
              ),
              -- col_ranked: 색상 카운트/랭크
              col_ranked AS (
                  SELECT post_id, name, COUNT(*) AS cnt,
                         ROW_NUMBER() OVER (PARTITION BY post_id ORDER BY COUNT(*) DESC, name) AS rn
                  FROM col_exploded
                  WHERE name <> '' AND name IS NOT NULL
                  GROUP BY post_id, name
              ),
              -- colors_top: 상위 K개 색상만 보존
              colors_top AS (
                  SELECT post_id,
                         COALESCE(jsonb_agg(name ORDER BY rn) FILTER (WHERE rn <= %s), '[]'::jsonb) AS colors
                  FROM col_ranked
                  GROUP BY post_id
              ),
              -- hashes: url_hash의 안정적 결합 문자열
              hashes AS (
                  SELECT pie.post_id,
                         STRING_AGG(pie.url_hash, ',' ORDER BY pie.url_hash) AS urlhash_join
                  FROM post_image_enrichment AS pie
                  JOIN changed USING (post_id)
                  GROUP BY pie.post_id
              ),
              -- rolled: per-post 요약 행 구성
              rolled AS (
                  SELECT c.post_id,
                         caps.caption_join                                    AS image_summary,
                         kw.image_keywords                                    AS image_keywords,
                         ocr.ocr_join                                         AS image_ocr,
                         safety.safety_json                                   AS image_safety,
                         objects_top.objects                                  AS image_objects,
                         colors_top.colors                                    AS image_colors
                  FROM (SELECT DISTINCT post_id FROM changed) c
                  LEFT JOIN caps        ON caps.post_id        = c.post_id
                  LEFT JOIN kw          ON kw.post_id          = c.post_id
                  LEFT JOIN ocr         ON ocr.post_id         = c.post_id
                  LEFT JOIN safety      ON safety.post_id      = c.post_id
                  LEFT JOIN objects_top ON objects_top.post_id = c.post_id
                  LEFT JOIN colors_top  ON colors_top.post_id  = c.post_id
              ),
              -- rolled_h: 요약 기반 image_rev 해시 생성
              rolled_h AS (
                  SELECT r.*,
                         md5(
                             COALESCE(h.urlhash_join,'') || '|' ||
                             COALESCE(r.image_summary,'') || '|' ||
                             COALESCE(r.image_ocr,'') || '|' ||
                             COALESCE(r.image_keywords::text,'[]') || '|' ||
                             COALESCE(r.image_objects::text,'[]') || '|' ||
                             COALESCE(r.image_colors::text,'[]')
                         ) AS image_rev
                  FROM rolled r
                  LEFT JOIN hashes h ON h.post_id = r.post_id
              ),
              -- to_upsert: 변경 감지 (IS DISTINCT FROM)
              to_upsert AS (
                  SELECT r.post_id, r.image_summary, r.image_keywords, r.image_ocr, r.image_safety, r.image_objects, r.image_colors, r.image_rev,
                         pe.text_rev,
                         pe.image_rev      AS old_image_rev,
                         pe.image_summary  AS old_image_summary,
                         pe.image_ocr      AS old_image_ocr,
                         pe.image_objects  AS old_image_objects,
                         pe.image_colors   AS old_image_colors
                  FROM rolled_h r
                  LEFT JOIN post_enrichment pe ON pe.post_id = r.post_id
                  WHERE pe.image_rev IS DISTINCT FROM r.image_rev
                     OR pe.image_rev IS NULL
                     OR pe.image_summary IS DISTINCT FROM r.image_summary
                     OR pe.image_ocr     IS DISTINCT FROM r.image_ocr
                     OR pe.image_objects IS DISTINCT FROM r.image_objects
                     OR pe.image_colors  IS DISTINCT FROM r.image_colors
              ),
              -- upsert: post_enrichment 업서트
              upsert AS (
                  INSERT INTO post_enrichment
                      (post_id, image_summary, image_keywords, image_ocr, image_safety, image_objects, image_colors, image_rev, enriched_at)
                  SELECT post_id, image_summary, image_keywords, image_ocr, image_safety, image_objects, image_colors, image_rev, NOW()
                  FROM to_upsert
                  ON CONFLICT (post_id) DO UPDATE SET
                      image_summary = EXCLUDED.image_summary,
                      image_keywords = EXCLUDED.image_keywords,
                      image_ocr      = EXCLUDED.image_ocr,
                      image_safety   = EXCLUDED.image_safety,
                      image_objects  = EXCLUDED.image_objects,
                      image_colors   = EXCLUDED.image_colors,
                      image_rev      = EXCLUDED.image_rev,
                      enriched_at    = NOW()
                  RETURNING post_id, image_rev, text_rev
              ),
              -- enq: 변경된 항목에 한해 fusion_jobs 인큐
              enq AS (
                  INSERT INTO fusion_jobs (post_id, rev_key, priority, status, next_attempt_at, created_at, updated_at)
                  SELECT u.post_id, md5(u.text_rev || ':' || u.image_rev), 'P1', 'queued', NOW(), NOW(), NOW()
                  FROM upsert u
                  WHERE u.text_rev IS NOT NULL
                  ON CONFLICT DO NOTHING
                  RETURNING 1
              )
            SELECT
              (SELECT COUNT(*) FROM changed)   AS changed_in,
              (SELECT COUNT(*) FROM to_upsert) AS candidates,
              (SELECT COUNT(*) FROM upsert)    AS upserted,
              (SELECT COUNT(*) FROM enq)       AS enqueued;
                """
            ),
            (changed_ids, labels_top_k, objects_top_k, colors_top_k),
        )
        row = cur.fetchone() or (0, 0, 0, 0)
        changed_in, candidates, upserted, enqueued = row

    context.add_output_metadata(
        {
            "changed_in": changed_in,
            "rollup_candidates": candidates,
            "rollup_upserted": upserted,
            "fusion_enqueued": enqueued,
        }
    )
    return upserted


@dg.asset(
    name="upgrade_to_image_llm_asset",
    group_name="IS",
    automation_condition=ON2,
    tags={"data_tier": "ops", "table": "fusion_jobs"},
    description=(
        "이미지 rev가 준비된 포스트에 대해 queued 상태의 텍스트-only LLM 잡을 취소하고, "
        "이미지 rev로 LLM 잡을 보장(upsert)"
    ),
)

def upgrade_to_image_llm_asset(
    context: dg.OpExecutionContext, is_postgres: PostgresResource, image_rollup_asset
) -> dict:
    """
    - 전제: post_enrichment.image_rev 가 'noimg'가 아닌 글은 이미지 버전이 준비됨
    - 동작:
        1) 동일 post의 텍스트-only rev(md5(text_rev || ':noimg::pv=...'))가 queued면 status='canceled'
        2) 이미지 rev 키로 fusion_jobs upsert (기본 P0; 환경변수로 조절)
    """
    LIMIT = _env_int("UPGRADE_IMAGE_LLM_LIMIT", 2000)
    PRIORITY = _env_str("UPGRADE_IMAGE_LLM_PRIORITY", "P0")
    PROMPT_VER = _env_str("FUSION_PROMPT_VER", "pv_default")

    with pg_cursor(is_postgres, context, "upgrade_to_image_llm_asset") as cur:
        cur.execute(
            dedent(
                """
                WITH
                  -- base: 이미지 rev 준비 & text_rev 존재
                  base AS (
                      SELECT p.id AS post_id, pe.text_rev, pe.image_rev
                      FROM posts p
                      JOIN post_enrichment pe ON pe.post_id = p.id
                      WHERE p.is_deleted = FALSE
                        AND pe.text_rev IS NOT NULL
                        AND COALESCE(pe.image_rev, 'noimg') <> 'noimg'
                      ORDER BY pe.fused_at NULLS FIRST, p.id
                      LIMIT %s
                  ),
                  -- to_cancel: 텍스트-only rev 키
                  to_cancel AS (
                      SELECT post_id,
                             md5(text_rev || ':' || 'noimg' || '::pv=' || %s) AS rev_key
                      FROM base
                  ),
                  -- upd_cancel: 텍스트-only 큐 취소
                  upd_cancel AS (
                      UPDATE fusion_jobs j
                         SET status='canceled',
                             updated_at=NOW(),
                             last_error = COALESCE(j.last_error,'') || ' | superseded_by_image'
                        FROM to_cancel c
                       WHERE j.post_id = c.post_id
                         AND j.rev_key  = c.rev_key
                         AND j.status   = 'queued'
                      RETURNING 1
                  ),
                  -- to_image: 이미지 rev 키
                  to_image AS (
                      SELECT post_id,
                             md5(text_rev || ':' || image_rev || '::pv=' || %s) AS rev_key
                      FROM base
                  ),
                  -- ins_image: 이미지 rev로 보장 upsert
                  ins_image AS (
                      INSERT INTO fusion_jobs (post_id, rev_key, priority, status, next_attempt_at, created_at, updated_at)
                      SELECT post_id, rev_key, %s, 'queued', NOW(), NOW(), NOW()
                        FROM to_image
                      ON CONFLICT (post_id, rev_key) DO UPDATE
                          SET status = CASE WHEN fusion_jobs.status = 'error' THEN 'queued' ELSE fusion_jobs.status END,
                              next_attempt_at = CASE WHEN fusion_jobs.status <> 'done' THEN NOW() ELSE fusion_jobs.next_attempt_at END,
                              updated_at = NOW(),
                              priority = LEAST(fusion_jobs.priority, EXCLUDED.priority)
                          WHERE fusion_jobs.status <> 'done'
                      RETURNING 1
                  )
                SELECT
                  COALESCE((SELECT COUNT(*) FROM upd_cancel), 0) AS canceled,
                  COALESCE((SELECT COUNT(*) FROM ins_image), 0) AS upserted;
                """
            ),
            (LIMIT, PROMPT_VER, PROMPT_VER, PRIORITY),
        )
        r = cur.fetchone() or (0, 0)
        canceled, inserted_or_updated = int(r[0]), int(r[1])

    result = {
        "text_only_canceled": canceled,
        "image_rev_llm_upserted": inserted_or_updated,
        "priority": PRIORITY,
        "prompt_ver": PROMPT_VER,
        "limit": LIMIT,
    }
    context.add_output_metadata(result)
    return result


# -----------------------------------------------------------------------------
# Section C: Keyword Trends Aggregation
# -----------------------------------------------------------------------------
@dg.asset(
    name="keyword_trends_asset",
    group_name="IS",
    automation_condition=ON10,
    tags={"data_tier": "gold", "table": "keyword_trends"},
    description="post_enrichment 기반 키워드 트렌드 집계 (3h/6h/24h/1w, 10분 격자 윈도우)",
)

def keyword_trends_asset(
    context: dg.OpExecutionContext, is_postgres: PostgresResource, posts_asset
) -> dict:
    """
    최신 10분 버킷 기준의 고정된 window_end에 대해, post_enrichment.keywords를 집계하여
    keyword_trends 테이블에 upsert한다. posts.is_deleted = false 인 글만 포함.
    """
    with pg_cursor(is_postgres, context, "keyword_trends_asset") as cur:
        # KEYWORD_TRENDS_SQL 은 외부에서 제공되는 상수 SQL
        cur.execute(KEYWORD_TRENDS_SQL)
    return {"ok": True}