
-- =============================================================================
-- One-off scripts for VLM/LLM reprocessing (single post)
-- Usage (psql):
--   VLM만 다시:   psql "$DATABASE_URL" -v post_id=abc123 -f dag/is_data_sql.sql -v run=vlm_only
--   LLM만 다시:   psql "$DATABASE_URL" -v post_id=abc123 -v prompt_ver=pv_hotfix_20250904 -f dag/is_data_sql.sql -v run=llm_only
--   VLM→LLM:     psql "$DATABASE_URL" -v post_id=abc123 -v prompt_ver=pv_hotfix_20250904 -f dag/is_data_sql.sql -v run=vlm_then_llm
--
-- NOTE: This file contains three independent sections. Copy only the section you need,
--       or execute the file with your editor's "selected text" feature. The SQL below
--       uses psql variables `:'post_id'` and `:'prompt_ver'` so you only need to set those.
-- =============================================================================


-- ============================================================================
-- SECTION 1) VLM(이미지)만 다시 돌리기 — 포스트의 첫/마지막 2장 재큐잉
-- ============================================================================
-- 목적: media_enrichment_jobs에 (post_id, 첫/마지막 url_hash) 두 건을 P0/queued로 업서트
-- 주의: 모든 이미지를 돌리고 싶으면 마지막 WHERE 절의 rn_asc/rn_desc 조건을 제거하세요.
BEGIN;
WITH imgs AS (
  SELECT
    pi.post_id::text AS post_id,
    pi.url_hash,
    pi.url,
    ROW_NUMBER() OVER (PARTITION BY pi.post_id ORDER BY pi.url_hash ASC)  AS rn_asc,
    ROW_NUMBER() OVER (PARTITION BY pi.post_id ORDER BY pi.url_hash DESC) AS rn_desc
  FROM post_images pi
  WHERE pi.post_id::text = :'post_id'
),
up AS (
  INSERT INTO media_enrichment_jobs (post_id, url_hash, image_url, priority, status, next_attempt_at, created_at, updated_at)
  SELECT post_id, url_hash, url, 'P0', 'queued', NOW(), NOW(), NOW()
  FROM imgs
  WHERE rn_asc = 1 OR rn_desc = 1
  ON CONFLICT (post_id, url_hash) DO UPDATE
    SET status          = 'queued',
        priority        = 'P0',
        next_attempt_at = NOW(),
        updated_at      = NOW(),
        finished_at     = NULL,
        locked_at       = NULL,
        locked_by       = NULL,
        attempts        = 0
  RETURNING 1
)
SELECT COUNT(*) AS vlm_jobs_upserted FROM up;
COMMIT;


-- ============================================================================
-- SECTION 2) LLM(퓨전)만 다시 돌리기 — 현재 text_rev + image_rev 기준 새 잡 인큐
-- ============================================================================
-- 목적: post_enrichment.text_rev(+image_rev)로 rev_key를 만들고 fusion_jobs에 P0/queued 업서트
-- 팁: 동일 rev_key가 이미 done이면 그대로 보존됩니다. 반드시 다시 돌리고 싶다면 :prompt_ver를 새 값으로 주세요.
BEGIN;
WITH base AS (
  SELECT
    pe.post_id::text AS post_id,
    pe.text_rev,
    COALESCE(pe.image_rev, 'noimg') AS image_rev
  FROM post_enrichment pe
  WHERE pe.post_id::text = :'post_id'
    AND pe.text_rev IS NOT NULL
),
cand AS (
  SELECT
    post_id,
    md5(text_rev || ':' || image_rev || '::pv=' || :'prompt_ver') AS rev_key
  FROM base
)
INSERT INTO fusion_jobs (post_id, rev_key, priority, status, next_attempt_at, created_at, updated_at)
SELECT post_id, rev_key, 'P0', 'queued', NOW(), NOW(), NOW()
FROM cand
ON CONFLICT (post_id, rev_key) DO UPDATE
  SET status          = CASE WHEN fusion_jobs.status = 'done' THEN fusion_jobs.status ELSE 'queued' END,
      next_attempt_at = CASE WHEN fusion_jobs.status <> 'done' THEN NOW() ELSE fusion_jobs.next_attempt_at END,
      updated_at      = NOW(),
      priority        = LEAST(fusion_jobs.priority, EXCLUDED.priority)
WHERE fusion_jobs.status <> 'done'
RETURNING post_id, rev_key, status;
COMMIT;


-- ============================================================================
-- SECTION 3) VLM 다시 → (간이 롤업) → LLM 다시
-- ============================================================================
-- 목적: (1) VLM 재큐 → (2) per-image를 간이 롤업하여 image_rev 갱신 → (3) 새 rev_key로 LLM 잡 인큐
-- 주의: 운영 자산의 롤업 해시 레시피와 100% 동일하지는 않지만, “방금 처리된 이미지 반영 + LLM 재실행 트리거” 용도로 충분합니다.
BEGIN;
-- (3-1) VLM 재큐: SECTION 1과 동일
WITH imgs AS (
  SELECT
    pi.post_id::text AS post_id,
    pi.url_hash,
    pi.url,
    ROW_NUMBER() OVER (PARTITION BY pi.post_id ORDER BY pi.url_hash ASC)  AS rn_asc,
    ROW_NUMBER() OVER (PARTITION BY pi.post_id ORDER BY pi.url_hash DESC) AS rn_desc
  FROM post_images pi
  WHERE pi.post_id::text = :'post_id'
),
up AS (
  INSERT INTO media_enrichment_jobs (post_id, url_hash, image_url, priority, status, next_attempt_at, created_at, updated_at)
  SELECT post_id, url_hash, url, 'P0', 'queued', NOW(), NOW(), NOW()
  FROM imgs
  WHERE rn_asc = 1 OR rn_desc = 1
  ON CONFLICT (post_id, url_hash) DO UPDATE
    SET status          = 'queued',
        priority        = 'P0',
        next_attempt_at = NOW(),
        updated_at      = NOW(),
        finished_at     = NULL,
        locked_at       = NULL,
        locked_by       = NULL,
        attempts        = 0
  RETURNING 1
)
SELECT COUNT(*) AS vlm_jobs_upserted FROM up;

-- (3-2) 간이 롤업 + image_rev 계산 (현재까지 완료된 per-image 기준)
WITH changed AS (
  SELECT :'post_id'::text AS post_id
),
caps AS (
  SELECT pie.post_id::text AS post_id,
         NULLIF(STRING_AGG(NULLIF(TRIM(pie.caption), ''), ' | ' ORDER BY pie.enriched_at DESC, pie.url_hash), '') AS image_summary
  FROM post_image_enrichment pie
  JOIN changed c ON c.post_id = pie.post_id::text
  GROUP BY pie.post_id
),
kw AS (
  SELECT pie.post_id::text AS post_id,
         COALESCE(jsonb_agg(DISTINCT TRIM(LOWER(k))) FILTER (WHERE TRIM(LOWER(k)) <> ''), '[]'::jsonb) AS image_keywords
  FROM post_image_enrichment pie
  JOIN changed c ON c.post_id = pie.post_id::text,
       LATERAL jsonb_array_elements_text(COALESCE(pie.labels, '[]'::jsonb)) AS k
  GROUP BY pie.post_id
),
ocr AS (
  SELECT pie.post_id::text AS post_id,
         NULLIF(STRING_AGG(NULLIF(TRIM(pie.ocr_text), ''), ' | ' ORDER BY pie.enriched_at DESC, pie.url_hash), '') AS image_ocr
  FROM post_image_enrichment pie
  JOIN changed c ON c.post_id = pie.post_id::text
  GROUP BY pie.post_id
),
objs AS (
  SELECT pie.post_id::text AS post_id,
         COALESCE(jsonb_agg(DISTINCT jsonb_strip_nulls(obj)) FILTER (WHERE obj IS NOT NULL), '[]'::jsonb) AS image_objects
  FROM post_image_enrichment pie
  JOIN changed c ON c.post_id = pie.post_id::text,
       LATERAL jsonb_array_elements(COALESCE(pie.objects, '[]'::jsonb)) AS obj
  GROUP BY pie.post_id
),
cols AS (
  SELECT pie.post_id::text AS post_id,
         COALESCE(jsonb_agg(DISTINCT jsonb_strip_nulls(col)) FILTER (WHERE col IS NOT NULL), '[]'::jsonb) AS image_colors
  FROM post_image_enrichment pie
  JOIN changed c ON c.post_id = pie.post_id::text,
       LATERAL jsonb_array_elements(COALESCE(pie.colors, '[]'::jsonb)) AS col
  GROUP BY pie.post_id
),
-- url_hash 조합 + 주요 필드로 간이 image_rev 계산
hashes AS (
  SELECT pie.post_id::text AS post_id,
         STRING_AGG(pie.url_hash, ',' ORDER BY pie.url_hash) AS urlhash_join
  FROM post_image_enrichment pie
  JOIN changed c ON c.post_id = pie.post_id::text
  GROUP BY pie.post_id
),
rolled AS (
  SELECT
    c.post_id,
    COALESCE(cp.image_summary, '') AS image_summary,
    COALESCE(kw.image_keywords, '[]'::jsonb) AS image_keywords,
    COALESCE(oc.image_ocr, '') AS image_ocr,
    COALESCE(ob.image_objects, '[]'::jsonb) AS image_objects,
    COALESCE(co.image_colors, '[]'::jsonb)  AS image_colors,
    md5(
      COALESCE(h.urlhash_join,'') || '|' || COALESCE(cp.image_summary,'') || '|' ||
      COALESCE(oc.image_ocr,'')   || '|' || COALESCE(ob.image_objects::text,'[]') || '|' ||
      COALESCE(co.image_colors::text,'[]')
    ) AS image_rev
  FROM changed c
  LEFT JOIN caps   cp ON cp.post_id = c.post_id
  LEFT JOIN kw     kw ON kw.post_id = c.post_id
  LEFT JOIN ocr    oc ON oc.post_id = c.post_id
  LEFT JOIN objs   ob ON ob.post_id = c.post_id
  LEFT JOIN cols   co ON co.post_id = c.post_id
  LEFT JOIN hashes h  ON h.post_id  = c.post_id
),
up_pe AS (
  INSERT INTO post_enrichment (post_id, image_summary, image_keywords, image_ocr, image_objects, image_colors, image_rev, enriched_at)
  SELECT r.post_id, r.image_summary, r.image_keywords, r.image_ocr, r.image_objects, r.image_colors, r.image_rev, NOW()
  FROM rolled r
  ON CONFLICT (post_id) DO UPDATE
    SET image_summary = EXCLUDED.image_summary,
        image_keywords = EXCLUDED.image_keywords,
        image_ocr      = EXCLUDED.image_ocr,
        image_objects  = EXCLUDED.image_objects,
        image_colors   = EXCLUDED.image_colors,
        image_rev      = EXCLUDED.image_rev,
        enriched_at    = NOW()
  RETURNING post_id, image_rev
)
SELECT * FROM up_pe;  -- 간이 롤업 결과 확인용

-- (3-3) LLM 재인큐: 최신 text_rev + (위에서 갱신된) image_rev 기준
WITH base AS (
  SELECT pe.post_id::text AS post_id,
         pe.text_rev,
         pe.image_rev
  FROM post_enrichment pe
  WHERE pe.post_id::text = :'post_id'
    AND pe.text_rev IS NOT NULL
),
cand AS (
  SELECT post_id,
         md5(text_rev || ':' || image_rev || '::pv=' || :'prompt_ver') AS rev_key
  FROM base
)
INSERT INTO fusion_jobs (post_id, rev_key, priority, status, next_attempt_at, created_at, updated_at)
SELECT post_id, rev_key, 'P0', 'queued', NOW(), NOW(), NOW()
FROM cand
ON CONFLICT (post_id, rev_key) DO UPDATE
  SET status          = CASE WHEN fusion_jobs.status = 'done' THEN fusion_jobs.status ELSE 'queued' END,
      next_attempt_at = CASE WHEN fusion_jobs.status <> 'done' THEN NOW() ELSE fusion_jobs.next_attempt_at END,
      updated_at      = NOW(),
      priority        = LEAST(fusion_jobs.priority, EXCLUDED.priority)
WHERE fusion_jobs.status <> 'done'
RETURNING post_id, rev_key, status;
COMMIT;


-- ============================================================================
-- 빠른 체크 쿼리 (원하면 복사해 사용)
-- ============================================================================
-- VLM 큐 상태
-- SELECT status, COUNT(*) FROM media_enrichment_jobs WHERE post_id::text = :'post_id' GROUP BY 1;
-- LLM 큐 상태
-- SELECT status, COUNT(*) FROM fusion_jobs WHERE post_id::text = :'post_id' GROUP BY 1;
-- 최신 fused/rollup 상태
-- SELECT post_id, fused_version, fuse_rev, image_rev, enriched_at, fused_at FROM post_enrichment WHERE post_id::text = :'post_id';


-- Snapshot — VLM/LLM/Enrichment 상태 한 번에 보기 (단일 post)
-- 사용법: psql "$DATABASE_URL" -v post_id=abc123 -c "<이 쿼리 복붙>"
WITH pid AS (
  SELECT :'post_id'::text AS post_id
),
vlm AS (
  SELECT
    :'post_id'::text AS post_id,
    COUNT(*) FILTER (WHERE status = 'queued')     AS vlm_queued,
    COUNT(*) FILTER (WHERE status = 'processing') AS vlm_processing,
    COUNT(*) FILTER (WHERE status = 'done')       AS vlm_done,
    COUNT(*) FILTER (WHERE status = 'error')      AS vlm_error,
    MAX(updated_at)                               AS vlm_last_updated
  FROM media_enrichment_jobs
  WHERE post_id::text = :'post_id'
),
llm AS (
  SELECT
    :'post_id'::text AS post_id,
    COUNT(*) FILTER (WHERE status = 'queued')     AS llm_queued,
    COUNT(*) FILTER (WHERE status = 'processing') AS llm_processing,
    COUNT(*) FILTER (WHERE status = 'done')       AS llm_done,
    COUNT(*) FILTER (WHERE status = 'error')      AS llm_error,
    MAX(updated_at)                               AS llm_last_updated
  FROM fusion_jobs
  WHERE post_id::text = :'post_id'
),
pe AS (
  SELECT
    pe.post_id::text AS post_id,
    pe.text_rev,
    pe.image_rev,
    pe.fuse_rev,
    pe.fused_version,
    pe.enriched_at,
    pe.fused_at
  FROM post_enrichment pe
  WHERE pe.post_id::text = :'post_id'
)
SELECT
  pid.post_id,
  COALESCE(vlm.vlm_queued, 0)      AS vlm_queued,
  COALESCE(vlm.vlm_processing, 0)  AS vlm_processing,
  COALESCE(vlm.vlm_done, 0)        AS vlm_done,
  COALESCE(vlm.vlm_error, 0)       AS vlm_error,
  (COALESCE(vlm.vlm_queued,0) + COALESCE(vlm.vlm_processing,0) + COALESCE(vlm.vlm_done,0) + COALESCE(vlm.vlm_error,0)) AS vlm_total,
  vlm.vlm_last_updated,
  COALESCE(llm.llm_queued, 0)      AS llm_queued,
  COALESCE(llm.llm_processing, 0)  AS llm_processing,
  COALESCE(llm.llm_done, 0)        AS llm_done,
  COALESCE(llm.llm_error, 0)       AS llm_error,
  (COALESCE(llm.llm_queued,0) + COALESCE(llm.llm_processing,0) + COALESCE(llm.llm_done,0) + COALESCE(llm.llm_error,0)) AS llm_total,
  llm.llm_last_updated,
  pe.text_rev,
  pe.image_rev,
  pe.fuse_rev,
  pe.fused_version,
  pe.enriched_at,
  pe.fused_at,
  CASE WHEN pe.image_rev IS NULL OR pe.image_rev = 'noimg' THEN FALSE ELSE TRUE END AS has_image
FROM pid
LEFT JOIN vlm USING (post_id)
LEFT JOIN llm USING (post_id)
LEFT JOIN pe  USING (post_id);