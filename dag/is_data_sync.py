"""IS 데이터 처리 DAG

크롤러가 생성한 JSON을 수집하여 Drizzle/Postgres 관계형 테이블로 정규화하고,
버전·댓글·이미지·임베드·스냅샷·트렌드 등 상위 계층 자산을 파생합니다.

데이터 티어 구분
🟤 Bronze : 크롤러 원본 JSON
🟣 Silver : 정규화된 핵심 테이블(posts, comments …)
🟡 Gold   : 집계·트렌드(post_trends) 및 머티리얼라이즈드 뷰
※ Gold 계층(post_trends/MV/클러스터 회전 등) 자산 정의는 비동기 파일 `is_data_unsync.py`로 이동하여 관리합니다.
"""
import json
import uuid
import re
import hashlib
from typing import List, Set
from collections import Counter

import psycopg2
from psycopg2.extras import execute_values, Json

import dagster as dg
from .resources import PostgresResource
from .schedules import EAGER


# on_cron() already waits until all upstream deps have updated since the cron tick.
# If you ever need the sub‑condition explicitly, use:
# DEPS_AFTER_TICK_10 = dg.AutomationCondition.all_deps_updated_since_cron("4,14,24,34,44,54 * * * *")
BLOCKS_OK = dg.AutomationCondition.all_deps_blocking_checks_passed()

# =============================================================
# Helpers — grouped for readability (pure functions, no DB side effects)
# =============================================================

# Regexes and constants
_URL_RE = re.compile(r"https?://\S+")
_WS_RE = re.compile(r"\s+")
_IMG_EXT_RE = re.compile(r"\.(png|jpe?g|gif|webp|bmp|svg)(\?.*)?$", re.IGNORECASE)

# Small utilities
# Simple MD5 helper for stable URL hashing used by unique keys
def _md5_hex(s: str) -> str:
    return hashlib.md5((s or "").encode("utf-8")).hexdigest()


def _looks_like_image_url(u: str) -> bool:
    return bool(u and _IMG_EXT_RE.search(u.strip()))


#오류 확인후 삭제, 댓글의 중복 id 여부 판단을 위해, 몇몇 사이트는 게시글별 동일 회원 id 사용 허용, 그래서 오류 발생 가능성
def _safe_preview(s: str | None, n: int = 120) -> str:
    s = (s or "").replace("\n", " ").replace("\r", " ")
    return (s[:n] + "…") if len(s) > n else s


def _hist_by_depth(rows):
    hist = {}
    for r in rows:
        d = r.get("depth", 0) or 0
        hist[d] = hist.get(d, 0) + 1
    return hist


# Comments → flat rows (DFS order)
def _flatten_comments(comments, parent_id=None, root_id=None, depth=0, seq_prefix=None):
    """DFS 방식으로 댓글 트리를 평탄화하여 DB 업서트용 dict를 생성한다."""
    if seq_prefix is None:
        seq_prefix = []
    for idx, c in enumerate(comments, 1):
        seq = seq_prefix + [idx]
        path = ".".join(f"{n:03d}" for n in seq)  # 001.002 … ensures lexicographic sort == DFS order
        cid = c.get("id")
        if not cid:
            continue  # skip malformed
        root_id = root_id or cid
        yield {
            "id": cid,
            "parent_id": parent_id,
            "root_id": root_id,
            "path": path,
            "depth": depth,
            "author": c.get("author"),
            "avatar": c.get("avatar"),
            "content": c.get("content"),
            "content_html": c.get("html"),
            "raw": c.get("raw"),
            "timestamp": c.get("timestamp"),
            "like_count": c.get("likeCount", 0),
            "dislike_count": c.get("dislikeCount", 0),
            "reaction_count": c.get("reactionCount", 0),
            "is_deleted": c.get("isDeleted", False),
        }
        # recurse into replies
        for child in _flatten_comments(c.get("replies", []), cid, root_id, depth + 1, seq):
            yield child


# Stable JSON list string and post_enrichment text_rev helpers
def _sorted_json_list_str(seq) -> str:
    """Return a stable JSON string of a list, sorted; '[]' on None/invalid."""
    if not seq:
        return "[]"
    try:
        return json.dumps(sorted(seq))
    except Exception:
        return "[]"


def _upsert_post_enrichment_text_rev(cur, post_pk: str, post: dict, context: dg.OpExecutionContext | None = None) -> None:
    """Best-effort text_rev upsert for post_enrichment; idempotent."""
    try:
        text_src = f"{post.get('title','')}|{post.get('content','')}|" + json.dumps(sorted(post.get('tags', [])))
        text_rev = hashlib.md5(text_src.encode('utf-8')).hexdigest()
        cur.execute(
            """
            INSERT INTO post_enrichment (post_id, text_rev, enriched_at)
            VALUES (%s, %s, NOW())
            ON CONFLICT (post_id) DO UPDATE SET text_rev = EXCLUDED.text_rev, enriched_at = NOW()
            """,
            (post_pk, text_rev)
        )
        if context:
            context.log.info(f"upserted text_rev for post_id={post_pk}")
    except Exception:
        # best-effort; ignore errors
        pass


# Text normalization, shingles & hashing
def _normalize_text(s: str) -> List[str]:
    """Rough Korean-friendly tokenizer.
    - Lowercase
    - Strip URLs
    - Keep Hangul/Latin/Numbers, drop the rest
    - Split on whitespace and short-filter (len>=2)
    """
    if not s:
        return []
    s = s.lower()
    s = _URL_RE.sub(" ", s)
    # Keep Hangul, basic Latin, numbers, space
    s = re.sub(r"[^\uAC00-\uD7A3a-z0-9\s]", " ", s)
    s = _WS_RE.sub(" ", s).strip()
    toks = [t for t in s.split(" ") if len(t) >= 2]
    return toks


def _k_shingles(tokens: List[str], k: int = 5) -> List[str]:
    if len(tokens) < k:
        return tokens[:]  # fallback: short docs
    return [" ".join(tokens[i:i+k]) for i in range(len(tokens)-k+1)]


def _hash64(x: str, seed: int = 0) -> int:
    h = hashlib.blake2b((str(seed)+"|"+x).encode("utf-8"), digest_size=8).digest()
    return int.from_bytes(h, byteorder="big", signed=False)


def _simhash64(features: List[str]) -> int:
    """Classic SimHash for 64-bit."""
    if not features:
        return 0
    vec = [0]*64
    for f in features:
        h = _hash64(f)
        for i in range(64):
            bit = (h >> i) & 1
            vec[i] += 1 if bit == 1 else -1
    out = 0
    for i in range(64):
        if vec[i] > 0:
            out |= (1 << i)
    return out


def _minhash(tokens: Set[str], num: int = 128) -> List[int]:
    """Simple MinHash using Blake2b-based hash families.
    Deterministic and light-weight for our batch sizes.
    """
    if not tokens:
        return [2**64-1]*num
    mins = [2**64-1]*num
    for i in range(num):
        for t in tokens:
            hv = _hash64(t, seed=i)
            if hv < mins[i]:
                mins[i] = hv
    return mins


def _minhash_to_bytes(arr: List[int]) -> bytes:
    return b"".join([v.to_bytes(8, byteorder="big", signed=False) for v in arr])


# DB utility helpers (best-effort enqueues)
def _enqueue_enrichment_job(cur, post_id: str, context: dg.OpExecutionContext | None = None, priority: str = 'P1') -> None:
    """Best-effort enqueue for textual enrichment; ignores conflicts/errors."""
    try:
        cur.execute(
            """
            INSERT INTO enrichment_jobs (post_id, priority, status, next_attempt_at)
            VALUES (%s, %s, 'queued', NOW())
            ON CONFLICT (post_id) DO NOTHING
            """,
            (post_id, priority)
        )
        if context:
            context.log.info(f"queued enrichment job for post_id={post_id} priority={priority} source=is_data_sync")
    except Exception:
        # enqueue is best-effort; swallow any transient errors
        pass


def _bulk_enqueue_media_jobs(cur, rows: list[tuple[str, str, str]], context: dg.OpExecutionContext | None = None) -> None:
    """Best-effort bulk enqueue for media enrichment.
    rows = [(post_id, url_hash, image_url), ...]
    """
    if not rows:
        return
    try:
        from psycopg2.extras import execute_values as _ev
        _ev(
            cur,
            """
            INSERT INTO media_enrichment_jobs (post_id, url_hash, image_url, priority, status, next_attempt_at)
            VALUES %s
            ON CONFLICT (post_id, url_hash) DO NOTHING
            """,
            rows,
            template="(%s, %s, %s, 'P1', 'queued', NOW())",
        )
        if context:
            context.log.info(f"queued {len(rows)} media_enrichment_jobs (bulk)")
    except Exception:
        # best-effort; ignore errors
        pass

# 5) keywords (post_enrichment 기반으로 간소화)
# Deprecated/removed assets:
# - post_llm_enrich_asset : 동기 LLM 처리 자산은 제거됨. 비동기 큐+워커(enrichment_worker_asset)로 대체.
#    트렌드는 10분 격자 window에 맞춰 3h/6h/24h/1w 범위를 집계한다.
#    상위 개수 제한은 소비(웹) 측에서 LIMIT로 처리.



@dg.asset(
    name="posts_asset",
    config_schema={"file_path": str},
    group_name="IS",
    automation_condition=EAGER,
    kinds={"source"},
    tags={
        "domain": "community_content",
        "data_tier": "silver",
        "source": "crawler_json",
        "technology": "python",
        "table": "posts"
    },
    description="JSON → posts 테이블 upsert 및 downstream 자산으로 fan‑out. 중복 제어, 변경 감지 포함."
)
def posts_asset(context: dg.AssetExecutionContext, is_postgres: PostgresResource):
    """Crawler JSON을 읽어 `posts` 테이블에 멱등적으로 upsert한다.
    변경 여부를 판단해 중복을 방지하고, 하위 자산(댓글·이미지 등)이 사용할
    메타 정보를 반환한다."""
    file_path = context.op_config["file_path"]
    run_id = context.run_id
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        context.log.error(f"파일 열기/파싱 실패: {e}")
        raise dg.Failure(description=f"파일 열기/파싱 실패: {e}")

    meta = data[0].get("meta", {}) if isinstance(data, list) else data.get("meta", {})
    posts = data[0].get("posts", []) if isinstance(data, list) else data.get("posts", [])
    site = meta.get("site")
    board = meta.get("board")
    
    inserted, updated, skipped = 0, 0, 0
    post_results = []
    with is_postgres.get_connection() as conn:
        with conn.cursor() as cur:
            for post in posts:
                # meta fallback 보장
                site_for_lookup = post.get("site") or site
                board_for_write = post.get("board") or board

                # (site, post_id)로 기존 row 조회 (updated_at 포함)
                cur.execute(
                    "SELECT id, content_hash, comment_count, image_count, embed_count, tags, updated_at FROM posts WHERE site = %s AND post_id = %s",
                    (site_for_lookup, post.get("post_id"))
                )
                row = cur.fetchone()
                changed = False
                db_updated_at = None
                if row:
                    db_id, db_content_hash, db_comment_count, db_image_count, db_embed_count, db_tags, existing_updated_at = row
                    post_pk = db_id  # 기존 id 사용
                    db_tags_sorted = _sorted_json_list_str(db_tags)
                    incoming_tags_sorted = _sorted_json_list_str(post.get("tags"))
                    content_changed = (db_content_hash != post.get("content_hash")) or (db_tags_sorted != incoming_tags_sorted)
                    if not content_changed and (db_comment_count or 0) == post.get("comment_count", 0) and (db_image_count or 0) == post.get("image_count", 0) and (db_embed_count or 0) == post.get("embed_count", 0):
                        skipped += 1
                        changed = False
                        db_updated_at = existing_updated_at
                    else:
                        cur.execute(
                            """
                            UPDATE posts SET
                                post_id=%s, site=%s, board=%s, url=%s, title=%s, author=%s, avatar=%s, timestamp=%s, content=%s, content_html=%s, content_hash=%s, category=%s, tags=%s,
                                view_count=%s, like_count=%s, dislike_count=%s, comment_count=%s, image_count=%s, embed_count=%s, crawled_at=%s, updated_at=NOW(), is_deleted=%s
                            WHERE id=%s
                            RETURNING updated_at
                            """,
                            (
                                post.get("post_id"), site_for_lookup, board_for_write, post.get("url"), post.get("title"), post.get("author"), post.get("avatar"),
                                post.get("timestamp"), post.get("content"), post.get("contentHtml"), post.get("content_hash"), post.get("category"),
                                Json(post.get("tags", [])),
                                post.get("view_count", 0), post.get("like_count", 0), post.get("dislike_count", 0), post.get("comment_count", 0), post.get("image_count", 0), post.get("embed_count", 0),
                                post.get("crawledAt"), post.get("isDeleted", False), post_pk
                            )
                        )
                        updated += 1
                        changed = content_changed
                        ret = cur.fetchone()
                        db_updated_at = ret[0] if ret else None
                        # 텍스트 변동 시 text_rev만 업서트(VLM-first; LLM 인큐 없음)
                        if content_changed:
                            _upsert_post_enrichment_text_rev(cur, post_pk, post, context)
                        
                else:
                    post_pk = post.get("id") or str(uuid.uuid4())  # 크롤러 id or 새로 생성
                    cur.execute(
                        """
                        INSERT INTO posts (
                            id, post_id, site, board, url, title, author, avatar, timestamp, content, content_html, content_hash, category, tags,
                            view_count, like_count, dislike_count, comment_count, image_count, embed_count, crawled_at, is_deleted
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                        )
                        RETURNING updated_at
                        """,
                        (
                            post_pk, post.get("post_id"), site_for_lookup, board_for_write, post.get("url"), post.get("title"), post.get("author"), post.get("avatar"),
                            post.get("timestamp"), post.get("content"), post.get("contentHtml"), post.get("content_hash"), post.get("category"),
                            Json(post.get("tags", [])),
                            post.get("view_count", 0), post.get("like_count", 0), post.get("dislike_count", 0), post.get("comment_count", 0), post.get("image_count", 0), post.get("embed_count", 0),
                            post.get("crawledAt"), post.get("isDeleted", False)
                        )
                    )
                    inserted += 1
                    changed = True
                    ret = cur.fetchone()
                    db_updated_at = ret[0] if ret else None
                    # 신규 글: 최초 text_rev 세팅(VLM-first; LLM 인큐 없음)
                    _upsert_post_enrichment_text_rev(cur, post_pk, post, context)

                # downstream asset으로 전달
                post_results.append({
                    "id": post_pk, "site": site_for_lookup, "board": board_for_write, "file_path": file_path, "run_id": run_id,
                    "post_id": post.get("post_id"), "content_hash": post.get("content_hash"), "title": post.get("title"),
                    "content": post.get("content"), "content_html": post.get("contentHtml"), "updated_at": db_updated_at,
                    "author": post.get("author"),
                    "avatar": post.get("avatar"),
                    "comment_count": post.get("comment_count", 0), "comments": post.get("comments", []),
                    "images": post.get("images", []), "embeddedContent": post.get("embeddedContent", []),
                    "view_count": post.get("view_count", 0), "like_count": post.get("like_count", 0), "dislike_count": post.get("dislike_count", 0),
                    "crawledAt": post.get("crawledAt"), "crawled_at": post.get("crawledAt"),
                    "changed": changed
                })
            conn.commit()
    context.add_output_metadata({
        "site": site, "board": board, "file_path": file_path, "run_id": run_id,
        "inserted": inserted, "updated": updated, "skipped": skipped, "total": len(posts),
    })
    return post_results

@dg.asset(
    name="post_versions_asset",
    group_name="IS",
    automation_condition=EAGER,
    tags={
        "data_tier": "silver",
        "table": "post_versions"
    },
    description="본문/제목/태그 변동 시 버전 스냅 저장"
)
def post_versions_asset(context: dg.AssetExecutionContext, is_postgres: PostgresResource, posts_asset):
    """본문·제목·태그가 변경된 포스트에 대해 버전 스냅샷을 저장한다."""
    inserted = 0
    with is_postgres.get_connection() as conn:
        with conn.cursor() as cur:
            for post in posts_asset:
                if not post.get("changed"):
                    continue  # 변경된 post만 버전 기록

                # 1) 다음 버전 계산: 최신 버전 하나만 빠르게 조회
                cur.execute(
                    """
                    SELECT version
                    FROM post_versions
                    WHERE post_id = %s
                    ORDER BY version DESC
                    LIMIT 1
                    """,
                    (post["id"],)
                )
                row = cur.fetchone()
                next_version = ((row[0] if row and row[0] is not None else 0) + 1)

                # 2) 중복 해시는 DB가 처리: ON CONFLICT DO NOTHING
                cur.execute(
                    """
                    INSERT INTO post_versions (
                        post_id, version, title, content, content_html, content_hash, changed_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (post_id, content_hash) DO NOTHING
                    """,
                    (post["id"], next_version, post.get("title"), post.get("content"), post.get("content_html"), post.get("content_hash"), post.get("updated_at"))
                )

                # 실제 삽입된 경우에만 카운트 증가
                if cur.rowcount > 0:
                    inserted += 1
            conn.commit()
    context.add_output_metadata({"inserted": inserted, "total": len([p for p in posts_asset if p.get("changed")])})
    return inserted

@dg.asset(
    name="post_comments_asset",
    group_name="IS",
    automation_condition=EAGER,
    tags={
        "data_tier": "silver",
        "table": "post_comments"
    },
    description="중첩 댓글 flatten → upsert"
)
def post_comments_asset(context: dg.AssetExecutionContext, is_postgres: PostgresResource, posts_asset):
    """중첩 댓글을 평탄화하여 `post_comments` 테이블에 일괄 upsert한다."""
    inserted = 0
    with is_postgres.get_connection() as conn:
        with conn.cursor() as cur:
            for post in posts_asset:
                post_id = post["id"]
                new_comments = post.get("comments", [])
                flat_rows = list(_flatten_comments(new_comments))
                if not flat_rows:
                    continue

                # Diagnostics: log size, unique ids, depth histogram, and detect duplicates *before* bulk upsert
                ids = [r.get("id") for r in flat_rows if r.get("id")]
                depth_hist = _hist_by_depth(flat_rows)
                n_flat = len(flat_rows)
                n_unique = len(set(ids))
                context.log.info(
                    f"[post_comments] post_id={post_id} flattened={n_flat} unique_ids={n_unique} depth_hist={depth_hist}"
                )

                dupe_ids = [cid for cid, cnt in Counter(ids).items() if cnt > 1]
                if dupe_ids:
                    sample = dupe_ids[:5]
                    context.log.warning(
                        f"[post_comments] duplicate_ids post_id={post_id} count={len(dupe_ids)} sample={sample}"
                    )
                    # For the first few duplicates, print per-row small diffs
                    by_id_sample = {}
                    for r in flat_rows:
                        cid = r.get("id")
                        if cid in sample:
                            by_id_sample.setdefault(cid, []).append(r)
                    for cid in sample:
                        rows = by_id_sample.get(cid, [])
                        for idx, rr in enumerate(rows, 1):
                            context.log.warning(
                                f"[post_comments] dup_detail post_id={post_id} cid={cid} idx={idx} "
                                f"parent={rr.get('parent_id')} depth={rr.get('depth')} ts={rr.get('timestamp')} "
                                f"like={rr.get('like_count')} del={rr.get('is_deleted')} preview=\"{_safe_preview(rr.get('content'))}\""
                            )

                values = [
                    (
                        r["id"], post_id, r["parent_id"], r["root_id"], r["path"], r["depth"],
                        r["author"], r["avatar"], r["content"], r["content_html"], r["raw"], r["timestamp"],
                        r["like_count"], r["dislike_count"], r["reaction_count"], r["is_deleted"]
                    )
                    for r in flat_rows
                ]

                query = """
                INSERT INTO post_comments (
                    id, post_id, parent_id, root_id, path, depth, author, avatar, content, content_html, raw,
                    timestamp, like_count, dislike_count, reaction_count, is_deleted
                ) VALUES %s
                ON CONFLICT (id) DO UPDATE SET
                    parent_id = EXCLUDED.parent_id,
                    root_id = EXCLUDED.root_id,
                    path = EXCLUDED.path,
                    depth = EXCLUDED.depth,
                    author = EXCLUDED.author,
                    avatar = EXCLUDED.avatar,
                    content = EXCLUDED.content,
                    content_html = EXCLUDED.content_html,
                    raw = EXCLUDED.raw,
                    timestamp = EXCLUDED.timestamp,
                    like_count = EXCLUDED.like_count,
                    dislike_count = EXCLUDED.dislike_count,
                    reaction_count = EXCLUDED.reaction_count,
                    is_deleted = EXCLUDED.is_deleted
                """
                try:
                    execute_values(cur, query, values, page_size=500)
                except Exception as e:
                    # Focused error log with quick stats and duplicate hints
                    context.log.error(
                        f"[post_comments] execute_values_failed post_id={post_id} values={len(values)} "
                        f"duplicate_ids={len(dupe_ids)} err_type={type(e).__name__} err={e}"
                    )
                    # If this is a CardinalityViolation, dump detailed rows for a few duplicated ids
                    try:
                        from psycopg2.errors import CardinalityViolation  # type: ignore
                        is_card = isinstance(e, CardinalityViolation)
                    except Exception:
                        is_card = False
                    if is_card:
                        by_id_full = {}
                        for r in flat_rows:
                            by_id_full.setdefault(r.get("id"), []).append(r)
                        dumped = 0
                        for cid, rows in by_id_full.items():
                            if dumped >= 5:
                                break
                            if len(rows) > 1:
                                dumped += 1
                                for idx, rr in enumerate(rows, 1):
                                    context.log.error(
                                        f"[post_comments] cv_detail post_id={post_id} cid={cid} idx={idx} "
                                        f"parent={rr.get('parent_id')} depth={rr.get('depth')} ts={rr.get('timestamp')} "
                                        f"path={rr.get('path')} preview=\"{_safe_preview(rr.get('content'))}\""
                                    )
                    raise
                else:
                    inserted += len(values)
            conn.commit()
    context.add_output_metadata({"inserted": inserted})
    return inserted

@dg.asset(
    name="post_images_asset",
    group_name="IS",
    automation_condition=EAGER,
    tags={
        "data_tier": "silver",
        "table": "post_images"
    },
    description="포스트 이미지 URL 및 메타데이터 upsert (해시 기반 유니크 키 사용)"
)
def post_images_asset(context: dg.AssetExecutionContext, is_postgres: PostgresResource, posts_asset):
    """포스트별 이미지 URL 및 메타데이터를 동기화(upsert)한다.
    유니크 키는 (post_id, url_hash=md5(url))를 사용한다.
    """
    inserted = 0
    with is_postgres.get_connection() as conn:
        with conn.cursor() as cur:
            for post in posts_asset:
                images = post.get("images", [])
                post_pk = post["id"]
                if not images:
                    continue

                # 현재 DB에 있는 url_hash 집합
                cur.execute("SELECT url_hash FROM post_images WHERE post_id = %s", (post_pk,))
                existing = {r[0] for r in cur.fetchall()}

                incoming_hashes = {_md5_hex(u) for u in images if u}

                # 삭제: incoming에 없는 기존 것만 제거 (해시 기준)
                if existing and incoming_hashes:
                    cur.execute(
                        "DELETE FROM post_images WHERE post_id = %s AND NOT (url_hash = ANY(%s))",
                        (post_pk, list(incoming_hashes))
                    )
                elif existing and not incoming_hashes:
                    cur.execute("DELETE FROM post_images WHERE post_id = %s", (post_pk,))

                # upsert: 신규/변경 반영 (해시 기준) — batched
                to_upsert = []
                to_queue = []
                for img_url in images:
                    if not img_url:
                        continue
                    h = _md5_hex(img_url)
                    to_upsert.append((post_pk, img_url, h, None, None, None))
                    if h not in existing:
                        to_queue.append((post_pk, h, img_url))

                if to_upsert:
                    execute_values(
                        cur,
                        """
                        INSERT INTO post_images (post_id, url, url_hash, alt, width, height)
                        VALUES %s
                        ON CONFLICT (post_id, url_hash) DO UPDATE
                            SET alt = EXCLUDED.alt, width = EXCLUDED.width, height = EXCLUDED.height
                        """,
                        to_upsert,
                        page_size=500,
                    )
                    inserted += len(to_upsert)

                # Bulk enqueue media enrichment jobs only for new images
                _bulk_enqueue_media_jobs(cur, to_queue, context)

            conn.commit()
    context.add_output_metadata({"inserted": inserted})
    return inserted

@dg.asset(
    name="post_embeds_asset",
    group_name="IS",
    automation_condition=EAGER,
    tags={
        "data_tier": "silver",
        "table": "post_embeds"
    },
    description="임베드/링크/동영상 등 외부 컨텐츠 upsert (해시 기반 유니크 키 사용)"
)
def post_embeds_asset(context: dg.AssetExecutionContext, is_postgres: PostgresResource, posts_asset):
    """포스트에 포함된 임베드(링크·동영상 등)를 upsert하며, 불필요한 항목을 제거한다. mime_type도 저장한다.
    유니크 키는 (post_id, url_hash=md5(url))를 사용한다.
    """
    inserted = 0
    with is_postgres.get_connection() as conn:
        with conn.cursor() as cur:
            for post in posts_asset:
                embeds = post.get("embeddedContent", [])
                post_pk = post["id"]
                if not embeds:
                    continue

                new_urls = [e.get("url") for e in embeds if e.get("url")]
                if not new_urls:
                    continue

                new_hashes = [_md5_hex(u) for u in new_urls]

                # 삭제: 없는 것만 제거 (해시 기준)
                cur.execute(
                    "DELETE FROM post_embeds WHERE post_id = %s AND NOT (url_hash = ANY(%s))",
                    (post_pk, new_hashes)
                )

                # upsert: type/title/desc/mime_type 갱신 (해시 기준) — batched
                to_upsert = []
                to_queue = []
                for e in embeds:
                    url = e.get("url")
                    if not url:
                        continue
                    h = _md5_hex(url)
                    to_upsert.append((
                        post_pk,
                        e.get("type"),
                        (e.get("mimeType") or None),
                        url,
                        h,
                        e.get("videoId"),
                        e.get("thumbnail"),
                        e.get("title"),
                        e.get("description"),
                    ))

                    # 선택적 VLM 큐잉: 이미지형 임베드 또는 썸네일이 있는 경우에만
                    mime = (e.get("mimeType") or "").lower()
                    thumb = e.get("thumbnail")
                    image_url = None
                    if mime.startswith("image/"):
                        image_url = url
                    elif thumb and _looks_like_image_url(thumb):
                        image_url = thumb
                    elif _looks_like_image_url(url):
                        image_url = url
                    if image_url:
                        h_img = _md5_hex(image_url)
                        to_queue.append((post_pk, h_img, image_url))

                if to_upsert:
                    execute_values(
                        cur,
                        """
                        INSERT INTO post_embeds (post_id, type, mime_type, url, url_hash, video_id, thumbnail, title, description)
                        VALUES %s
                        ON CONFLICT (post_id, url_hash) DO UPDATE SET
                            type = EXCLUDED.type,
                            mime_type = EXCLUDED.mime_type,
                            video_id = EXCLUDED.video_id,
                            thumbnail = EXCLUDED.thumbnail,
                            title = EXCLUDED.title,
                            description = EXCLUDED.description
                        """,
                        to_upsert,
                        page_size=500,
                    )
                    inserted += len(to_upsert)

                _bulk_enqueue_media_jobs(cur, to_queue, context)

            conn.commit()
    context.add_output_metadata({"inserted": inserted})
    return inserted


@dg.asset(
    name="post_snapshots_asset",
    group_name="IS",
    automation_condition=EAGER,
    tags={
        "data_tier": "gold",
        "table": "post_snapshots"
    },
    description="포스트별 카운트 스냅샷 저장 (뷰/좋아요/댓글)"
)
def post_snapshots_asset(context: dg.AssetExecutionContext, is_postgres: PostgresResource, posts_asset):
    """포스트의 조회·좋아요·댓글 수를 주기적으로 스냅샷으로 저장한다."""
    inserted = 0
    with is_postgres.get_connection() as conn:
        with conn.cursor() as cur:
            for post in posts_asset:
                cur.execute(
                    "INSERT INTO post_snapshots (post_id, timestamp, view_count, like_count, dislike_count, comment_count) VALUES (%s, NOW(), %s, %s, %s, %s)",
                    (post["id"], post.get("view_count") or 0, post.get("like_count") or 0, post.get("dislike_count") or 0, post.get("comment_count") or 0)
                )
                inserted += 1
            conn.commit()
    context.add_output_metadata({"inserted": inserted})
    return inserted



@dg.asset(
    name="sites_asset",
    group_name="IS",
    automation_condition=EAGER,
    tags={
        "data_tier": "silver",
        "table": "sites"
    },
    description="사이트/게시판별 last_crawled_at 관리"
)
def sites_asset(context: dg.AssetExecutionContext, is_postgres: PostgresResource, posts_asset):
    """포스트에서 추출한 site·board의 마지막 크롤 시각을 `sites` 테이블에 기록한다."""
    updated = 0
    sites_info = set()
    for post in posts_asset:
        site_id = post.get("site")
        board = post.get("board")
        last_crawled_at = post.get("updated_at") or post.get("crawledAt")
        if site_id:
            sites_info.add((site_id, board, last_crawled_at))
    with is_postgres.get_connection() as conn:
        with conn.cursor() as cur:
            for site_id, board, last_crawled_at in sites_info:
                cur.execute(
                    """
                    INSERT INTO sites (id, board, last_crawled_at)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (id, board) DO UPDATE SET last_crawled_at = EXCLUDED.last_crawled_at
                    """,
                    (site_id, board, last_crawled_at)
                )
                updated += 1
            conn.commit()
    context.add_output_metadata({"updated": updated, "unique_sites": len(sites_info)})
    return updated 





# =============================================================
# Asset: post_signatures_asset
#   - Compute text SimHash/MinHash, gallery MinHash, embed MinHash
#   - Upsert into post_signatures
# =============================================================
@dg.asset(
    name="post_signatures_asset",
    group_name="IS",
    automation_condition=EAGER,
    tags={"data_tier": "silver", "table": "post_signatures"},
    description="본문/이미지/임베드 기반 시그니처 계산 및 저장 (증분)"
)
def post_signatures_asset(context: dg.AssetExecutionContext, is_postgres: PostgresResource, posts_asset):
    """Compute signatures only for posts in this run (from posts_asset).
    Assumes DB has table post_signatures with columns:
      - post_id TEXT PK
      - text_simhash64 VARCHAR(32)
      - text_minhash128 BYTEA
      - gallery_minhash128 BYTEA
      - embed_minhash128 BYTEA
      - image_count INT
      - embed_count INT
      - computed_at TIMESTAMPTZ
    Adjust column names if your schema differs.
    """
    upserted = 0
    with is_postgres.get_connection() as conn:
        with conn.cursor() as cur:
            for p in posts_asset:
                pid = p.get("id")
                if not pid:
                    continue
                # 1) Text tokens from title + content
                text_src = (p.get("title") or "") + "\n" + (p.get("content") or "")
                toks = _normalize_text(text_src)
                shingles = _k_shingles(toks, k=5)
                sim64 = _simhash64(shingles)
                sim64_hex = f"{sim64:016x}"
                text_tokens = set(shingles)
                text_mh = _minhash(text_tokens, num=128)

                # 2) Gallery tokens: image URLs as order-insensitive set (host+path basename)
                images = p.get("images", []) or []
                def _img_token(u: str) -> str:
                    # normalize: drop query, keep host + last path segment
                    m = re.match(r"https?://([^/]+)/(.+)", u or "")
                    if not m:
                        return u or ""
                    host = m.group(1)
                    last = m.group(2).split("?")[0].split("/")[-1]
                    return f"{host}/{last}"
                img_tokens = { _img_token(u) for u in images if u }
                gallery_mh = _minhash(img_tokens, num=128) if img_tokens else []

                # 3) Embed tokens: youtube/X id primary
                embeds = p.get("embeddedContent", []) or []
                em_tokens: Set[str] = set()
                for e in embeds:
                    et = (e.get("type") or "").lower()
                    if et == "youtube" and e.get("videoId"):
                        em_tokens.add(f"yt:{e.get('videoId')}")
                    elif et in ("x", "twitter"):
                        # Normalize legacy 'twitter' to 'X'. Try explicit id, else derive from URL
                        tid = e.get("videoId") or e.get("id")
                        if not tid:
                            url = e.get("url") or ""
                            m = re.search(r"/status/(\d+)", url)
                            if m:
                                tid = m.group(1)
                        if tid:
                            em_tokens.add(f"x:{tid}")
                    else:
                        url = e.get("url") or ""
                        if url:
                            host = re.sub(r"^https?://", "", url).split("/")[0]
                            em_tokens.add(f"em:{host}")
                embed_mh = _minhash(em_tokens, num=128) if em_tokens else []

                # 4) Upsert
                cur.execute(
                    """
                    INSERT INTO post_signatures (
                        post_id, text_simhash64, text_minhash128, gallery_minhash128, embed_minhash128, image_count, embed_count, computed_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (post_id) DO UPDATE SET
                        text_simhash64 = EXCLUDED.text_simhash64,
                        text_minhash128 = EXCLUDED.text_minhash128,
                        gallery_minhash128 = EXCLUDED.gallery_minhash128,
                        embed_minhash128 = EXCLUDED.embed_minhash128,
                        image_count = EXCLUDED.image_count,
                        embed_count = EXCLUDED.embed_count,
                        computed_at = NOW()
                    """,
                    (
                        pid,
                        sim64_hex,
                        psycopg2.Binary(_minhash_to_bytes(text_mh)) if text_mh else None,
                        psycopg2.Binary(_minhash_to_bytes(gallery_mh)) if gallery_mh else None,
                        psycopg2.Binary(_minhash_to_bytes(embed_mh)) if embed_mh else None,
                        len(images),
                        len(em_tokens),
                    ),
                )
                upserted += 1
            conn.commit()
    context.add_output_metadata({"upserted": upserted})
    return upserted
