"""
IS Data LLM operations: backfill and fusion worker.

This module defines Dagster assets for (1) backfilling/updating fusion jobs
and (2) running the final LLM fusion worker. It groups configuration, helpers,
prompts, SQL templates, and assets for improved readability without changing
behavior.
"""

"""
IS Data LLM operations: backfill and fusion worker.

This module defines Dagster assets for (1) backfilling/updating fusion jobs
and (2) running the final LLM fusion worker. It groups configuration, helpers,
prompts, SQL templates, and assets for improved readability without changing
behavior.
"""

# =============================================================================
# Imports
# =============================================================================

# Standard library
from contextlib import suppress
from datetime import datetime
import json as _json
import os
import re as _re
from typing import Any, List

# Third-party
import dagster as dg
from dagster_openai import OpenAIResource
from psycopg2.extras import Json as PgJson

# Local
from .resources import PostgresResource
from .schedules import ON3, ON10


# =============================================================================
# Configuration & Constants
# =============================================================================

ALLOWED_CATEGORIES = frozenset({
    "유머", "정보", "질문", "후기", "뉴스", "토론", "후방", "짤", "정치", "쇼핑", "IT", "스포츠", "게임", "기타"
})


# Model / batch config
DEFAULT_MODEL_NAME = os.getenv("LLM_MODEL", "qwen/qwen3-4b-2507")  # lm studio
# DEFAULT_MODEL_NAME = os.getenv("LLM_MODEL", "qwen/qwen3:4b-2507")
FUSION_PROMPT_VER = os.getenv("FUSION_PROMPT_VER", f"pv_{datetime.now().strftime('%Y%m%d%H%M')}")

# Comments ingestion (optional)
FUSION_COMMENTS_TOP_N = int(os.getenv("FUSION_COMMENTS_TOP_N", "0"))  # 0 = disabled
FUSION_COMMENTS_TABLE = os.getenv("FUSION_COMMENTS_TABLE", "post_comments")

# Log preview lengths
LLM_LOG_PREVIEW = int(os.getenv("LLM_LOG_PREVIEW", "1500"))
PREVIEW_TITLE = int(os.getenv("LLM_LOG_PREVIEW_TITLE", "200"))
PREVIEW_CONTENT = int(os.getenv("LLM_LOG_PREVIEW_CONTENT", "1200"))
PREVIEW_PROMPT = int(os.getenv("LLM_LOG_PREVIEW_PROMPT", "1500"))
PREVIEW_RESP = int(os.getenv("LLM_LOG_PREVIEW_RESP", "1500"))
PREVIEW_PARSED = int(os.getenv("LLM_LOG_PREVIEW_PARSED", "800"))

# Worker pick size
FUSION_BATCH_SIZE = int(os.getenv("FUSION_BATCH", "110"))


# =============================================================================
# SQL Templates (used by assets)
# =============================================================================

PICK_FUSION_SQL = (
    """
    WITH cte AS (
        SELECT
            post_id,
            rev_key
        FROM fusion_jobs
        WHERE status = 'queued'
          AND next_attempt_at <= NOW()
        ORDER BY
            CASE WHEN priority = 'P0' THEN 0 ELSE 1 END,
            created_at
        FOR UPDATE SKIP LOCKED
        LIMIT %s
    )
    UPDATE fusion_jobs AS j
    SET
        status       = 'processing',
        locked_at    = NOW(),
        locked_by    = %s,
        finished_at  = NULL,
        updated_at   = NOW()
    FROM cte
    WHERE j.post_id = cte.post_id
      AND j.rev_key = cte.rev_key
    RETURNING j.post_id, j.rev_key
    """
)

LOAD_FUSION_INPUT_SQL = (
    """
    SELECT
        p.id,
        p.site,
        p.board,
        p.title,
        p.content,
        p.content_html,
        COALESCE(pe.image_summary, ''),
        COALESCE(pe.image_keywords, '[]'::jsonb),
        COALESCE(pe.image_ocr, ''),
        COALESCE(pe.image_objects, '[]'::jsonb),
        COALESCE(pe.image_colors, '[]'::jsonb),
        COALESCE(pe.image_safety, '{}'::jsonb),
        pe.text_rev,
        pe.image_rev
    FROM posts AS p
    LEFT JOIN post_enrichment AS pe
        ON pe.post_id = p.id
    WHERE p.id = ANY(%s)
    """
)

LOAD_PER_IMAGE_ENRICH_SQL = (
    """
    SELECT
        pie.post_id,
        COALESCE(pi.url, '')                             AS url,
        pie.url_hash,
        COALESCE(pie.caption, '')                        AS caption,
        COALESCE(pie.labels, '[]'::jsonb)                AS labels,
        COALESCE(pie.ocr_lines, '[]'::jsonb)             AS ocr_lines,
        COALESCE(pie.objects, '[]'::jsonb)               AS objects,
        COALESCE(pie.colors, '[]'::jsonb)                AS colors,
        COALESCE(pie.safety, '{}'::jsonb)                AS safety,
        pie.enriched_at
    FROM post_image_enrichment AS pie
    LEFT JOIN post_images AS pi
        ON pi.post_id = pie.post_id
       AND pi.url_hash = pie.url_hash
    WHERE pie.post_id = %s
    ORDER BY pie.enriched_at DESC, pie.url_hash
    """
)

UPSERT_FUSED_SQL = (
    """
    INSERT INTO post_enrichment (
        post_id,
        fused_categories,
        fused_keywords,
        fused_model,
        fused_version,
        fused_at,
        fuse_rev
    ) VALUES (
        %s, %s, %s, %s, %s, NOW(), %s
    )
    ON CONFLICT (post_id) DO UPDATE
    SET
        fused_categories = EXCLUDED.fused_categories,
        fused_keywords   = EXCLUDED.fused_keywords,
        fused_model      = EXCLUDED.fused_model,
        fused_version    = EXCLUDED.fused_version,
        fused_at         = NOW(),
        fuse_rev         = EXCLUDED.fuse_rev
    """
)

MARK_FUSION_DONE_SQL = (
    """
    UPDATE fusion_jobs
    SET
        status       = 'done',
        attempts     = COALESCE(attempts, 0) + 1,
        updated_at   = NOW(),
        finished_at  = NOW(),
        locked_at    = NULL,
        locked_by    = NULL
    WHERE post_id = %s
      AND rev_key = %s
    """
)

MARK_FUSION_ERROR_SQL = (
    """
    UPDATE fusion_jobs
    SET
        status          = 'error',
        attempts        = COALESCE(attempts, 0) + 1,
        next_attempt_at = NULL,
        last_error      = %s,
        updated_at      = NOW(),
        finished_at     = NOW(),
        locked_at       = NULL,
        locked_by       = NULL
    WHERE post_id = %s
      AND rev_key = %s
    """
)


# =============================================================================
# Prompt Templates & Builders
# =============================================================================


FUSION_SYSTEM_MSG = {
    "role": "system",
    "content": (
        """
당신은 커뮤니티 게시글을 분류하고 핵심 키워드를 추출하는 **전문 분류기**입니다.
입력은 하나의 JSON 객체이며, 출력도 **오직 하나의 JSON 객체**입니다.

[입력]
- `title`: 게시글 제목
- `content_blocks`: 텍스트·이미지 블록 시퀀스(이미지는 `{type:"image", meta:{...}}`).
  댓글 요약이 있을 수 있으나 **보조 신호**로만 사용하세요.

[출력]
- 형식: {"categories": ["…"], "keywords": ["…"]}
- `categories`: 보통 **1개**. 내용이 **동등하게** 두 성격을 띠면 **최대 2개**까지(중요도 순).
- `keywords`: 3~8개. 핵심을 대표하는 **명사/명사구** 위주, **중복 제거**, **중요도 순**.

[허용 카테고리]
유머, 정보, 질문, 후기, 뉴스, 토론, 후방, 짤, 정치, 쇼핑, IT, 스포츠, 게임, 기타

[판단 가이드 — 필요 만큼만 활용]
- 목적/행위 중심으로 판단: 정보 공유 / 도움 요청 / 의견 교환 / 소식 전달 / 유머·밈 등.
- 질문 ↔ 토론: 해결·추천을 **묻는** 목적이면 질문, 찬반·논쟁 유도가 핵심이면 토론.
- 정보/후기/뉴스: 활용 안내·팁이면 정보, 기사 전달·속보면 뉴스.
- 후기 엄격 기준: 제목이나 본문에 아래 표현이 **명시적**으로 포함되거나, 실제 **사용/구매/방문 후**의 개인 경험 서술이 글의 **핵심**일 때만 '후기'로 분류. 허용 표현 예: `후기`, `리뷰`, `사용기/사용후기`, `구매기/구매후기`, `경험기`, `체험기`, `여행기/여행후기` (띄어쓰기·괄호·이모지 등 변형 허용). 애매하면 **'정보'** 또는 주제별 라벨(IT/스포츠/게임 등)을 선택.
- 쇼핑: 가격·쿠폰·기간·재고 등 **구매 판단 요소**가 구체적이면 고려.
- 구매 전 판단을 묻는 글(예: "추천 부탁", "사야 하나요?")은 후기 아님 → 질문/쇼핑.
- 짤 vs 유머: 밈/반응 이미지 중심이면 짤, 글 기반 농담·풍자면 유머.
- IT/스포츠/게임: 해당 분야 주제가 중심이면 해당 라벨.
- 애매하면 **가장 근접한 단일 라벨**을, 정말 동등하면 2개를 선택.

[키워드 가이드]
- 제목과 텍스트 블록에서 핵심 명사/고유명사를 우선 추출.
  필요 시 이미지 meta(ocr/labels/objects)는 약하게 보조 사용.
- 불용어·감탄사·URL·해시태그 기호는 제외. 카테고리명 자체는 키워드 금지.

[예]
{"categories":["IT"], "keywords":["Python 3.13","릴리스 노트","성능","패턴 매칭"]}
{"categories":["스포츠"], "keywords":["KBO","플레이오프","일정","예매"]}

[중요]
- 내부적으로 충분히 **사고/추론**하되, **출력은 JSON 한 객체만** 내세요
  (마크다운/설명/코드펜스 금지).
        """
    ),
}


FUSION_PROMPT_TMPL = (
    """
{
  "site": "$site",
  "board": "$board",
  "title": "$title",
  "content_blocks": $content_blocks
}
    """
)


FUSION_MESSAGES_PREFIX = [FUSION_SYSTEM_MSG]

# Structured output schema for response_format (FusionWorker only)
FUSION_RESPONSE_FORMAT = {
    "type": "json_schema",
    "json_schema": {
        "name": "fusion_result",
        "schema": {
            "type": "object",
            "properties": {
                "categories": {
                    "type": "array",
                    "items": {
                        "type": "string",
                        "enum": sorted(list(ALLOWED_CATEGORIES)),
                    },
                    "minItems": 1,
                    "maxItems": 2,
                    "uniqueItems": True,
                },
                "keywords": {
                    "type": "array",
                    "items": {"type": "string"},
                    "minItems": 3,
                    "maxItems": 8,
                },
            },
            "required": ["categories", "keywords"],
            "additionalProperties": False,
        },
    },
}


# =============================================================================
# Helper Utilities (logging, parsing, prompts)
# =============================================================================


# --- Additional ENV/interval helpers for backfill ---

def _env_bool(key: str, default: bool = False) -> bool:
    v = str(os.getenv(key, "1" if default else "0")).strip().lower()
    return v in ("1", "true", "yes", "y", "on")


def _env_int_llm(key: str, default: int) -> int:
    try:
        return int(os.getenv(key, str(default)))
    except Exception:
        return default


def _interval_str_days(days: int) -> str:
    """Return a safe Postgres interval string like '7 days'."""
    return f"{int(days)} days"

def _trim(s: object, n: int = LLM_LOG_PREVIEW) -> str:
    if s is None:
        return ""
    t = str(s)
    return t if len(t) <= n else t[:n] + "…(truncated)"


def _jdumps(obj) -> str:
    """JSON dumps with UTF-8 and safe default str conversion."""
    return _json.dumps(obj, ensure_ascii=False, default=str)


def _log(
    context: dg.OpExecutionContext,
    level: str,
    tag: str,
    payload: object,
    preview_len: int = LLM_LOG_PREVIEW,
) -> None:
    """Unified logger with JSON-safe preview + exception suppression (VLM-style)."""
    with suppress(Exception):
        logger = getattr(context.log, level, None)
        if not callable(logger):
            return
        if isinstance(payload, str):
            msg = _trim(payload, preview_len)
        else:
            try:
                msg = _trim(_jdumps(payload), preview_len)
            except Exception:
                msg = _trim(str(payload), preview_len)
        logger(f"{tag} " + msg)


def _resp_preview(resp) -> str:
    """Best-effort extraction of response text for compact preview."""
    try:
        ch0 = resp.choices[0]
        msg0 = getattr(ch0, "message", None)
        raw = msg0.get("content") if isinstance(msg0, dict) else getattr(msg0, "content", None)
        if isinstance(raw, str):
            return _trim(raw, PREVIEW_RESP)
    except Exception:
        pass
    try:
        return _trim(_jdumps(getattr(resp, "model_dump", lambda: resp)()), PREVIEW_RESP)
    except Exception:
        try:
            return _trim(_jdumps(resp), PREVIEW_RESP)
        except Exception:
            return _trim(str(resp), PREVIEW_RESP)


def _extract_json(resp) -> dict[str, Any]:
    """Extract a single JSON object from an OpenAI chat.completions response.
    
    Returns
    -------
    dict[str, Any]
        Parsed JSON object.
    
    Raises
    ------
    dg.Failure
        If the response does not contain a valid JSON object.
    """
    try:
        choice0 = resp.choices[0]
        msg = getattr(choice0, "message", None)
        content = msg.get("content") if isinstance(msg, dict) else getattr(msg, "content", None)
    except Exception as e:
        raise dg.Failure(description=f"LLM 응답 형식 오류: {e}")

    if not isinstance(content, str) or not content.strip():
        raise dg.Failure(description="LLM JSON 파싱 실패: empty content")

    text = content.strip()
    # Strip code fences if present
    if text.startswith("```"):
        text = text.strip().lstrip("`")
        if text.lower().startswith("json"):
            text = text[4:]
        text = text.strip().strip("`")

    try:
        return _json.loads(text)
    except Exception:
        # Try to salvage a JSON object substring
        start = text.find('{'); end = text.rfind('}')
        if start != -1 and end != -1 and end > start:
            candidate = text[start:end+1]
            return _json.loads(candidate)
        raise dg.Failure(description="LLM JSON 파싱 실패: no JSON object found")


def _as_list(v: Any) -> List[str]:
    if not v:
        return []
    if isinstance(v, str):
        t = v.strip()
        return [t] if t else []
    if isinstance(v, list):
        out: List[str] = []
        for x in v:
            if isinstance(x, str):
                t = x.strip()
                if t:
                    out.append(t)
        return out
    return []



def _clean_keywords(kw_in: Any, limit: int = 8) -> List[str]:
    """Normalize, deduplicate, and cap keywords.

    Parameters
    ----------
    kw_in : Any
        Iterable of candidate keyword strings.
    limit : int
        Maximum number of keywords to keep.

    Returns
    -------
    List[str]
        Cleaned keywords (<= limit).
    """
    kw_clean: List[str] = []
    _seen: set[str] = set()
    for w in kw_in or []:
        if isinstance(w, str):
            t = w.strip()
            if t and t not in _seen:
                _seen.add(t)
                kw_clean.append(t)
    return kw_clean[:limit]


# --- Optional comments loader (schema-tolerant) ---

def _load_top_comments(cur, post_id: str, top_n: int, table: str) -> list[dict]:
    """Best-effort loader for top-N comments from `table` for a given post.

    Tries common column names; returns a list of dicts with at least `text`.
    Any failure returns [].
    """
    if not top_n or top_n <= 0:
        return []
    candidates = [
        # Prefer like_count ordering if present
        (f"""
        SELECT
            COALESCE(content, comment, body, text, '') AS text,
            COALESCE(like_count, likes, upvotes, 0)    AS upvotes,
            COALESCE(timestamp, created_at, updated_at, NOW()) AS created_at
        FROM {table}
        WHERE post_id = %s AND COALESCE(is_deleted, FALSE) = FALSE
        ORDER BY COALESCE(like_count, likes, upvotes, 0) DESC, created_at ASC
        LIMIT %s
        """, (post_id, top_n)),
        # Fallback: chronological
        (f"""
        SELECT
            COALESCE(content, comment, body, text, '') AS text,
            0 AS upvotes,
            COALESCE(timestamp, created_at, NOW()) AS created_at
        FROM {table}
        WHERE post_id = %s AND COALESCE(is_deleted, FALSE) = FALSE
        ORDER BY created_at ASC
        LIMIT %s
        """, (post_id, top_n)),
    ]
    for sql, params in candidates:
        try:
            cur.execute(sql, params)
            rows = cur.fetchall()
            out: list[dict] = []
            for r in rows:
                try:
                    txt = (r[0] or '').strip()
                except Exception:
                    txt = ''
                if not txt:
                    continue
                out.append({"text": txt})
            if out:
                return out
        except Exception:
            continue
    return []


# --- HTML/text/image block helpers for block-sequenced content ---
def _html_to_text(html: str) -> str:
    """Lightweight HTML→plain text converter (keeps semantic newlines for <br> and blocks)."""
    if not html:
        return ""
    t = str(html)
    # remove script/style
    t = _re.sub(r"(?is)<(script|style)[^>]*>.*?</\\1>", " ", t)
    # br/paragraphs to newline
    t = _re.sub(r"(?i)<br\s*/?>", "\n", t)
    t = _re.sub(r"(?i)</p>", "\n", t)
    # strip tags
    t = _re.sub(r"<[^>]+>", " ", t)
    # collapse whitespace
    t = _re.sub(r"\s+", " ", t).strip()
    return t


def _norm_url(u: str) -> str:
    """Normalize URL for fuzzy matching (strip query/fragment, decode, lower)."""
    if not u:
        return ""
    try:
        from urllib.parse import urlparse, unquote
        pu = urlparse(u)
        netloc = (pu.netloc or "").lower()
        path = unquote(pu.path or "")
        return (netloc + path).lower()
    except Exception:
        return str(u).strip().lower()


def _build_content_blocks(content_html: str, per_image: list[dict], comments: list[dict] | None = None) -> list[dict]:
    """Split HTML into an ordered list of text/image blocks; attach VLM meta to matched images; optionally append a comments summary.

    per_image: list of {url, caption, labels, ocr_text, objects, colors, safety}
    comments: optional list of {text}
    """
    blocks: list[dict] = []
    html = content_html or ""
    if not html:
        return blocks

    # Build enrichment index by normalized URL and by basename
    enrich_list = []
    for e in per_image or []:
        url = (e.get("url") or "").strip()
        enrich_list.append({
            "norm": _norm_url(url),
            "basename": _re.sub(r"^.*?/", "", _norm_url(url)),
            "raw": e,
        })

    # Regex over <img src="...">
    last = 0
    for m in _re.finditer(r"<img[^>]+src=\"([^\"]+)\"[^>]*>", html, flags=_re.I|_re.S):
        start, end = m.span()
        src = m.group(1)
        # preceding text segment
        seg = html[last:start]
        txt = _html_to_text(seg)
        if txt:
            blocks.append({"type": "text", "text": txt})

        # find best enrichment match
        nsrc = _norm_url(src)
        match = None
        # 1) exact normalized match
        for e in enrich_list:
            if e["norm"] and e["norm"] == nsrc:
                match = e["raw"]
                break
        if match is None and nsrc:
            # 2) basename match
            base = _re.sub(r"^.*?/", "", nsrc)
            for e in enrich_list:
                if e["basename"] and e["basename"] == base:
                    match = e["raw"]
                    break
        if match is None and nsrc:
            # 3) fuzzy similarity
            try:
                from difflib import SequenceMatcher
                best, best_e = 0.0, None
                for e in enrich_list:
                    r = SequenceMatcher(None, nsrc, e["norm"]).ratio()
                    if r > best:
                        best, best_e = r, e["raw"]
                if best >= 0.85:
                    match = best_e
            except Exception:
                pass

        if match:
            img_block = {
                "type": "image",
                "url": src,
                "meta": {
                    "caption": match.get("caption") or "",
                    "labels": match.get("labels") or [],
                    "ocr": (
                        " | ".join(
                            [x.strip() for x in (match.get("ocr_lines") or []) if isinstance(x, str) and x.strip()]
                        )
                        if isinstance(match.get("ocr_lines"), list)
                        else (match.get("ocr_lines") if isinstance(match.get("ocr_lines"), str) else "")
                    ),
                    "objects": match.get("objects") or [],
                    "colors": match.get("colors") or [],
                    "safety": match.get("safety") or {},
                    "url": match.get("url") or src,
                },
            }
        else:
            img_block = {"type": "image", "url": src}
        blocks.append(img_block)
        last = end

    # tail text
    tail = html[last:]
    tail_txt = _html_to_text(tail)
    if tail_txt:
        blocks.append({"type": "text", "text": tail_txt})

    # Optional: append comments summary as the last text block
    if comments:
        lines: list[str] = []
        for i, c in enumerate((comments or [])[:10], start=1):
            t = (c.get("text") or "").strip()
            if not t:
                continue
            t = _re.sub(r"\s+", " ", t)
            lines.append(f"{i}) {t}")
        if lines:
            summary = "[댓글 요약]\n" + "\n".join(lines)
            blocks.append({"type": "text", "text": summary})
    return blocks


def _template_placeholders(template: str) -> set[str]:
    """Extract $placeholders from a string.Template-compatible template."""
    try:
        return set(_re.findall(r"\$([a-zA-Z_][a-zA-Z0-9_]*)", template))
    except Exception:
        return set()


def _check_prompt_template(context: dg.OpExecutionContext, template: str, provided: dict[str, object]) -> None:
    """Warn if the prompt template has missing/unused keys relative to provided mapping."""
    with suppress(Exception):
        names = _template_placeholders(template)
        missing = sorted([n for n in names if n not in provided])
        unused = sorted([k for k in provided.keys() if k not in names])
        if missing:
            context.log.info(f"Prompt template expects missing keys: {missing}")
        if unused:
            context.log.info(f"Prompt provided keys not used in template: {unused}")


def _fusion_build_messages(
    context: dg.OpExecutionContext,
    site: str,
    board: str,
    title: str,
    content: str,
    ctx: dict[str, object],
) -> tuple[list[dict[str, str]], str]:
    """Build final prompt/messages for the fusion model.

    Also performs a light sanity check to warn about template placeholders
    not provided (or extra provided keys not used by the template).
    """
    from string import Template as _T

    provided = {
        "site": site or "",
        "board": board or "",
        "title": title or "",
        # placeholders used by template end here
    }

    # Build ordered blocks from HTML + per-image enrichment
    content_html = ctx.get("content_html", "") or ""
    per_image = ctx.get("per_image", []) or []
    comments = ctx.get("comments", []) or []
    blocks = _build_content_blocks(content_html, per_image, comments)

    # warn if placeholders/provided keys mismatch
    _check_prompt_template(context, FUSION_PROMPT_TMPL, provided)

    prompt = _T(FUSION_PROMPT_TMPL).substitute(
        site=provided["site"],
        board=provided["board"],
        title=provided["title"],
        content_blocks=_jdumps(blocks),
    )
    return FUSION_MESSAGES_PREFIX + [{"role": "user", "content": prompt}], prompt


@dg.asset(
    name="fusion_worker_asset",
    group_name="IS",
    automation_condition=ON10,
    tags={"data_tier": "ops", "table": "post_enrichment", "llm": "fusion_final"},
    description="최종 LLM 퓨전 워커: fusion_jobs 큐를 소비하여 fused_categories/keywords 생성 및 fuse_rev 확정"
)
def fusion_worker_asset(
    context: dg.OpExecutionContext,
    is_postgres: PostgresResource,
    openai: OpenAIResource,
) -> dict:
    """Consume fusion_jobs, call the LLM, and upsert fused outputs.
    
    Returns
    -------
    dict
        Counts of picked/done/failed and a small preview of responses.
    Raises
    ------
    dg.Failure
        If any jobs failed in the batch (metadata includes counts and previews).
    """
    start_ts = datetime.utcnow()
    worker_id = f"fusion-{context.run_id[:8]}"

    with openai.get_client(context) as client, is_postgres.get_connection() as conn:
        with conn.cursor() as cur:
            # 1) Pick batch
            cur.execute(PICK_FUSION_SQL, (FUSION_BATCH_SIZE, worker_id))
            rows = cur.fetchall()
            context.log.info(f"picked {len(rows)} fusion job(s)")
            if not rows:
                conn.commit()
                result = {"picked": 0, "done": 0, "failed": 0}
                context.add_output_metadata(result)
                return result

            ids = [r[0] for r in rows]
            rev_map = {r[0]: r[1] for r in rows}

            # 2) Load inputs
            cur.execute(LOAD_FUSION_INPUT_SQL, (ids,))
            posts_map = {
                r[0]: {
                    "site": r[1],
                    "board": r[2],
                    "title": r[3],
                    "content": r[4],
                    "content_html": r[5],
                    "image_summary": r[6],
                    "image_keywords": r[7],
                    "image_ocr": r[8],
                    "image_objects": r[9],
                    "image_colors": r[10],
                    "image_safety": r[11],
                    "text_rev": r[12],
                    "image_rev": r[13],
                }
                for r in cur.fetchall()
            }

            done = 0
            failed = 0
            resp_previews = []

            for pid in ids:
                rev_key = rev_map.get(pid)
                try:
                    src = posts_map.get(pid) or {}
                    # Per-image enrichment
                    cur.execute(LOAD_PER_IMAGE_ENRICH_SQL, (pid,))
                    per_img_rows = cur.fetchall()
                    per_image = [
                        {
                            "url": r[1],
                            "url_hash": r[2],
                            "caption": r[3],
                            "labels": r[4],
                            "ocr_lines": r[5],
                            "objects": r[6],
                            "colors": r[7],
                            "safety": r[8],
                        }
                        for r in per_img_rows
                    ]
                    site = src.get("site") or ""; board = src.get("board") or ""
                    title = src.get("title") or ""; content = src.get("content") or ""
                    # Optional: load top comments (best-effort)
                    comments = []
                    try:
                        if FUSION_COMMENTS_TOP_N > 0:
                            comments = _load_top_comments(cur, pid, FUSION_COMMENTS_TOP_N, FUSION_COMMENTS_TABLE)
                    except Exception:
                        comments = []
                    ctx = {
                        "image_summary": src.get("image_summary"),
                        "image_keywords": src.get("image_keywords"),
                        "image_ocr": src.get("image_ocr"),
                        "image_objects": src.get("image_objects"),
                        "image_colors": src.get("image_colors"),
                        "image_safety": src.get("image_safety"),
                        "content_html": src.get("content_html"),
                        "per_image": per_image,
                        "comments": comments,
                    }

                    messages, prompt = _fusion_build_messages(context, site, board, title, content, ctx)

                    # Request log (preview)
                    req_log = {
                        "model": DEFAULT_MODEL_NAME,
                        "post_id": pid,
                        "rev_key": rev_key,
                        "title_preview": _trim(title, PREVIEW_TITLE),
                        "content_preview": _trim(content, PREVIEW_CONTENT),
                        "prompt_ver": FUSION_PROMPT_VER,
                        "prompt_preview": _trim(prompt, PREVIEW_PROMPT),
                    }
                    _log(context, "info", "[FUSION][REQ]", req_log, PREVIEW_RESP)

                    try:
                        resp = client.chat.completions.create(
                            model=DEFAULT_MODEL_NAME,
                            messages=messages,
                            temperature=0,
                            response_format=FUSION_RESPONSE_FORMAT,
                            extra_body={"format": "json"},
                        )
                    except Exception as _rf_err:
                        _log(context, "warning", "[FUSION][WARN]", f"response_format failed or unsupported: {_rf_err}", PREVIEW_RESP)
                        resp = client.chat.completions.create(
                            model=DEFAULT_MODEL_NAME,
                            messages=messages,
                            temperature=0,
                            extra_body={"format": "json"},
                        )

                    # Response preview (best-effort)
                    raw_prev = _resp_preview(resp)
                    if raw_prev:
                        _log(context, "info", "[FUSION][RESP]", raw_prev, PREVIEW_RESP)
                        resp_previews.append({"post_id": pid, "rev_key": rev_key, "resp": raw_prev})

                    data = _extract_json(resp)
                    cats = [c for c in _as_list(data.get("categories")) if c in ALLOWED_CATEGORIES]
                    if not cats:
                        cats = ["기타"]
                    kws = _clean_keywords(_as_list(data.get("keywords")), limit=8)

                    # Parsed preview (compact)
                    parsed_preview = _trim(_jdumps({"categories": cats, "keywords": kws}), PREVIEW_PARSED)
                    _log(context, "info", "[FUSION][PARSED]", parsed_preview, PREVIEW_PARSED)

                    # Upsert fused result + confirm fuse_rev
                    cur.execute(
                        UPSERT_FUSED_SQL,
                    (pid, PgJson(cats), PgJson(kws), DEFAULT_MODEL_NAME, f"fusion:{FUSION_PROMPT_VER}", rev_key)
)
                    # Mark job done
                    cur.execute(MARK_FUSION_DONE_SQL, (pid, rev_key))
                    done += 1

                except Exception as e:
                    context.log.exception(f"fusion_worker: failed for post_id={pid}: {e}")
                    try:
                        cur.execute(MARK_FUSION_ERROR_SQL, (str(e), pid, rev_key))
                    except Exception:
                        pass
                    failed += 1

            conn.commit()
            result = {"picked": len(ids), "done": done, "failed": failed, "fusion_resp_previews": resp_previews[:5]}
            context.add_output_metadata(result)
            if failed > 0:
                raise dg.Failure(
                    description=f"[fusion_worker_asset] 작업 일부 실패: {failed}/{len(ids)}",
                    metadata=result,
                )
            return result


@dg.asset(
    name="fusion_backfill_enqueue_asset",
    group_name="IS",
    tags={"data_tier": "ops", "table": "fusion_jobs"},
    description=(
        "LLM 백필 인큐어 (콘텐츠 블록 기반 프롬프트): provider/프롬프트 버전 변경 시 대량 재처리를 위해 대상 post에 대해 \n"
        "새로운 rev_key(md5(text_rev||':'||COALESCE(image_rev,'noimg')||'::pv='||FUSION_PROMPT_VER))로 fusion_jobs에 upsert합니다.\n"
        "\n"
        "※ 로직은 기존과 동일하지만, 최종 워커는 `content_blocks` 전용 프롬프트를 사용합니다. 이 자산은 버전 차이(`fusion:{FUSION_PROMPT_VER}`)를 기준으로 재큐잉만 수행합니다.\n"
    ),
)

def fusion_backfill_enqueue_asset(
    context: dg.OpExecutionContext,
    is_postgres: PostgresResource,
) -> dict:
    """
    Ad-hoc LLM backfill enqueuer.

    정책
    -----
    - 기본 대상: text_rev가 준비된 모든 포스트.
    - 새 target rev_key = md5(text_rev || ':' || COALESCE(image_rev, 'noimg') || '::pv=' || FUSION_PROMPT_VER)
    - 조건:
      * FORCE 모드가 아니면, 기존 post_enrichment.fused_version != f"fusion:{FUSION_PROMPT_VER}" 
        또는 post_enrichment.fuse_rev != target_rev_key 인 경우만 인큐.
      * FORCE 모드면 조건 무시하고 무조건 인큐(중복 job은 ON CONFLICT로 제어).
    - 우선순위/한도/스코프는 ENV로 제어:
      * FUSION_BACKFILL_LIMIT (default: 1000)
      * FUSION_BACKFILL_PRIORITY (default: 'P1')
      * FUSION_BACKFILL_SINCE_DAYS (default: 0 → 무시)
      * FUSION_BACKFILL_SITE_LIKE, FUSION_BACKFILL_BOARD_LIKE (부분 일치, ILIKE)
      * FUSION_BACKFILL_ONLY_NOIMG (true면 image_rev='noimg'만)
      * FUSION_BACKFILL_FORCE (true면 조건 무시)
    결과
    -----
    - upserted: 인큐/업서트된 잡 수
    - scope 미리보기 메타 반환

    참고
    -----
    - 워커(`fusion_worker_asset`)는 `content_blocks` 전용 프롬프트를 사용하며, HTML과 중복되는 필드는 사용하지 않습니다.
    - 본 자산은 **로직 변경 없이** 버전 갱신에 따른 재큐잉만 담당합니다.
    """
    LIMIT = _env_int_llm("FUSION_BACKFILL_LIMIT", 1000)
    PRIORITY = os.getenv("FUSION_BACKFILL_PRIORITY", "P1")
    SINCE_DAYS = _env_int_llm("FUSION_BACKFILL_SINCE_DAYS", 0)
    SITE_LIKE = (os.getenv("FUSION_BACKFILL_SITE_LIKE", "") or "").strip()
    BOARD_LIKE = (os.getenv("FUSION_BACKFILL_BOARD_LIKE", "") or "").strip()
    ONLY_NOIMG = _env_bool("FUSION_BACKFILL_ONLY_NOIMG", False)
    FORCE = _env_bool("FUSION_BACKFILL_FORCE", False)

    since_flag = True if SINCE_DAYS and int(SINCE_DAYS) > 0 else False
    interval_str = _interval_str_days(SINCE_DAYS if since_flag else 0)
    site_pat = f"%{SITE_LIKE}%"
    board_pat = f"%{BOARD_LIKE}%"
    target_version = f"fusion:{FUSION_PROMPT_VER}"

    sql = (
        """
        WITH base AS (
            SELECT p.id AS post_id,
                   p.site, p.board, p.created_at,
                   pe.text_rev,
                   COALESCE(pe.image_rev, 'noimg') AS image_rev,
                   COALESCE(pe.fuse_rev, '')       AS fuse_rev,
                   COALESCE(pe.fused_version, '')  AS fused_version
            FROM posts p
            LEFT JOIN post_enrichment pe ON pe.post_id = p.id
            WHERE p.is_deleted = FALSE
              AND pe.text_rev IS NOT NULL
        ),
        filtered AS (
            SELECT *
            FROM base
            WHERE ((%s)::boolean = FALSE OR created_at >= NOW() - (%s)::interval)
              AND (%s = '' OR site  ILIKE %s)
              AND (%s = '' OR board ILIKE %s)
              AND ((%s)::boolean = FALSE OR image_rev = 'noimg')
        ),
        candidates AS (
            SELECT
                post_id,
                md5(text_rev || ':' || image_rev || '::pv=' || %s) AS target_rev_key,
                fuse_rev,
                fused_version
            FROM filtered
            WHERE (%s)::boolean = TRUE
               OR fused_version <> %s
               OR fuse_rev <> md5(text_rev || ':' || image_rev || '::pv=' || %s)
            ORDER BY post_id
            LIMIT %s
        ),
        ins AS (
            INSERT INTO fusion_jobs (post_id, rev_key, priority, status, next_attempt_at, created_at, updated_at)
            SELECT post_id, target_rev_key, %s, 'queued', NOW(), NOW(), NOW()
            FROM candidates
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
    )

    with is_postgres.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql,
                (
                    since_flag,
                    interval_str,
                    SITE_LIKE,
                    site_pat,
                    BOARD_LIKE,
                    board_pat,
                    ONLY_NOIMG,
                    FUSION_PROMPT_VER,
                    FORCE,
                    target_version,
                    FUSION_PROMPT_VER,
                    LIMIT,
                    PRIORITY,
                ),
            )
            row = cur.fetchone()
            upserted = int(row[0] if row and len(row) > 0 else 0)
            conn.commit()

    result = {
        "upserted": upserted,
        "limit": LIMIT,
        "priority": PRIORITY,
        "since_days": SINCE_DAYS,
        "site_like": SITE_LIKE,
        "board_like": BOARD_LIKE,
        "only_noimg": ONLY_NOIMG,
        "force": FORCE,
        "prompt_ver": FUSION_PROMPT_VER,
        "target_version": target_version,
    }
    context.add_output_metadata(result)
    return result