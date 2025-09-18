import dagster as dg
from typing import List, Dict, Tuple, Set, Optional, Iterable
import hashlib
import math
from datetime import datetime, timedelta
from collections import defaultdict
from itertools import combinations

from .resources import PostgresResource
from .schedules import ON10, ON30, ON1H, BLOCKS_OK

# =============================================================
# Tuning constants (agreed defaults)
# - Reasoning in comments next to each constant
# =============================================================

# 2) clusters_build_asset
#    Re-uploads/dup posts often recur within 24–72h. Keep recall with 72h lookback,
#    but run less frequently to save cost.
CLUSTERS_BUILD_LOOKBACK_HOURS = 72  # run every 30–60m externally

# 3) cluster_rotation_asset
#    Weekly window (1w) to stabilize ranking; exponential decay τ=48h to drop stale clusters.
#    If a cluster is selected top_k in 3 consecutive runs (≈30m in 10m cadence), cool it down for 24h.
ROTATION_WINDOW_MINUTES = 10080  # 1 week
ROTATION_WINDOW_LABEL = "1w"
ROTATION_TOP_K = 20
ROTATION_DECAY_TAU_HOURS = 48
ROTATION_MAX_CONSECUTIVE = 3
ROTATION_COOLDOWN_HOURS = 24
# Consider selections non-consecutive if last show was > 2× cadence (10m) ago.
ROTATION_CONSEC_RESET_MINUTES = 20

# 4) clusters_merge_asset
#    Merge window: 14d. Sampling top-8 members per cluster is a good cost/quality tradeoff.
MERGE_LOOKBACK_DAYS = 14
MERGE_TOPK_PER_CLUSTER = 8
MERGE_MIN_PAIR_COUNT = 3
MERGE_AVG_SIM_THRESHOLD = 0.80


# =============================================================
# Helpers
# =============================================================

def _bytes_to_minhash(b: Optional[bytes]) -> List[int]:
    """Convert 128-byte (or multiple-of-8) blob to list of 64-bit ints.
    Accepts None / empty and returns []. Uses memoryview to avoid copying.
    """
    if not b:
        return []
    mv = memoryview(b)
    # Truncate to 8-byte boundary just in case
    usable = len(mv) - (len(mv) % 8)
    return [int.from_bytes(mv[i : i + 8], byteorder="big", signed=False) for i in range(0, usable, 8)]


def _jaccard_from_minhash(a: List[int], b: List[int]) -> float:
    if not a or not b or len(a) != len(b):
        return 0.0
    eq = sum(1 for i in range(len(a)) if a[i] == b[i])
    return eq / float(len(a))


def _band_buckets(sig: List[int], bands: int = 32, rows: int = 4) -> List[int]:
    """Create LSH band signatures by hashing contiguous chunks of the minhash.
    32x4 = 128 by default. Returns integer band-hashes for bucketing.
    """
    assert bands * rows == len(sig)
    buckets: List[int] = []
    for b in range(bands):
        start = b * rows
        chunk = sig[start : start + rows]
        h = hashlib.blake2b(b"|".join([x.to_bytes(8, "big", signed=False) for x in chunk]), digest_size=8).digest()
        buckets.append(int.from_bytes(h, "big", signed=False))
    return buckets


def _pairwise_indices(arr: Iterable[int]) -> Iterable[Tuple[int, int]]:
    """Yield unique index pairs using combinations (readability + avoids nested loops)."""
    return combinations(sorted(set(arr)), 2)


def _percentile_map(values: List[float]) -> Dict[float, float]:
    """Return mapping value -> percentile (0..1, higher is better) in O(n log n).
    Handles ties by assigning the percentile based on the first occurrence in the
    descending order (same behavior as the original but without O(n^2) lookups).
    """
    if not values:
        return {}
    sorted_vals = sorted(values, reverse=True)
    n = len(sorted_vals)
    pmap: Dict[float, float] = {}
    for idx, v in enumerate(sorted_vals):
        # Only set on first occurrence to emulate "first index" behavior
        if v not in pmap:
            pmap[v] = (n - idx) / float(n)
    return pmap


# =============================================================
# Asset: clusters_build_asset
#   - Generate candidates via SimHash buckets + MinHash LSH + embed overlap
#   - Confirm with (text OR (gallery & text) OR (embed & (text or gallery)))
#   - Union-Find to form clusters
#   - Upsert clusters & cluster_posts, pick representative by hot_score
# =============================================================
@dg.asset(
    name="clusters_build_asset",
    group_name="IS",
    automation_condition=ON30,
    tags={"data_tier": "gold", "table": "clusters"},
    description="유사 게시글 클러스터링(텍스트/갤러리/임베드) 및 대표글 선정",
)

def clusters_build_asset(
    context: dg.OpExecutionContext,
    is_postgres: PostgresResource,
    post_signatures_asset,
    post_trends_asset,
):
    """
    최근 72시간(3일) 내 게시글만 대상으로 클러스터 빌드. 72h는 리업로드/중복이 자주 발생하는 현실적 간격이며, 더 긴 주기는 비용 증가 대비 효과 미미.
    """
    lookback_hours = CLUSTERS_BUILD_LOOKBACK_HOURS
    text_thr = 0.75
    gal_thr = 0.80
    text_lo = 0.55
    emb_thr = 0.85

    # 1) Load recent signatures + basic post attrs
    with is_postgres.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT p.id, p.site, p.title, p.timestamp, p.like_count, p.comment_count,
                       s.text_simhash64, s.text_minhash128, s.gallery_minhash128, s.embed_minhash128
                FROM posts p
                JOIN post_signatures s ON s.post_id = p.id
                WHERE p.timestamp >= NOW() - INTERVAL '{lookback_hours} hours'
                AND p.is_deleted = FALSE
                """
            )
            rows = cur.fetchall()

            if not rows:
                context.log.info("No rows to cluster.")
                return 0

            # Build in-memory structures
            idx: Dict[str, int] = {}
            posts: List[Dict] = []
            for i, r in enumerate(rows):
                (pid, site, title, ts, like_c, cmt_c, sim_hex, tmh_b, gmh_b, emh_b) = r
                idx[pid] = i
                posts.append(
                    {
                        "id": pid,
                        "site": site,
                        "title": title,
                        "ts": ts,
                        "like": like_c or 0,
                        "cmt": cmt_c or 0,
                        "sim": int(sim_hex or "0", 16),
                        "tmh": _bytes_to_minhash(tmh_b),
                        "gmh": _bytes_to_minhash(gmh_b),
                        "emh": _bytes_to_minhash(emh_b),
                    }
                )

            n = len(posts)

            # 2) Candidate generation (SimHash buckets + MinHash LSH bands)
            band_buckets: Dict[int, List[int]] = defaultdict(list)
            for i, p in enumerate(posts):
                # SimHash prefix buckets (coarse)
                sim = p["sim"]
                prefixes = [(sim >> k) & ((1 << 16) - 1) for k in (0, 16, 32, 48)]  # 4 buckets of 16 bits
                for pb in prefixes:
                    band_buckets[(0 << 20) | pb].append(i)
                # MinHash LSH on text if exists
                if p["tmh"] and len(p["tmh"]) == 128:
                    for j, hb in enumerate(_band_buckets(p["tmh"], bands=32, rows=4)):
                        band_buckets[((1 + j) << 20) | hb].append(i)

            cand_pairs: Set[Tuple[int, int]] = set()
            for arr in band_buckets.values():
                if len(arr) >= 2:
                    cand_pairs.update(_pairwise_indices(arr))

            # ---- Metrics: candidate generation ----
            cand_cnt = len(cand_pairs)
            occ = [len(v) for v in band_buckets.values()]
            if occ:
                occ_sorted = sorted(occ)
                p50 = occ_sorted[len(occ_sorted)//2]
                p90 = occ_sorted[int(0.9*(len(occ_sorted)-1))]
                context.log.debug(f"[build] bucket_occupancy min={min(occ)} p50={p50} p90={p90} max={max(occ)} buckets={len(occ)}")
            context.log.info(f"[build] candidates={cand_cnt}")

            # 3) Confirm by approximate similarities
            def _confirm(a: Dict, b: Dict) -> Tuple[bool, float, str]:
                tj = _jaccard_from_minhash(a.get("tmh", []), b.get("tmh", []))
                gj = _jaccard_from_minhash(a.get("gmh", []), b.get("gmh", []))
                ej = _jaccard_from_minhash(a.get("emh", []), b.get("emh", []))
                if tj >= text_thr:
                    return True, tj, "text"
                if gj >= gal_thr and tj >= text_lo:
                    return True, (gj + tj) / 2.0, "both"
                if ej >= emb_thr and (tj >= 0.50 or gj >= 0.60):
                    return True, max(ej, (tj + gj) / 2.0), "embed"
                return False, 0.0, ""

            # Union-Find
            uf_parent = list(range(n))

            def _find(x: int) -> int:
                while uf_parent[x] != x:
                    uf_parent[x] = uf_parent[uf_parent[x]]
                    x = uf_parent[x]
                return x

            def _union(a: int, b: int):
                ra, rb = _find(a), _find(b)
                if ra != rb:
                    uf_parent[rb] = ra

            pair_sims: Dict[Tuple[int, int], Tuple[float, str]] = {}
            for a, b in cand_pairs:
                ok, simv, mtype = _confirm(posts[a], posts[b])
                if ok:
                    _union(a, b)
                    pair_sims[(min(a, b), max(a, b))] = (simv, mtype)

            # 4) Build clusters from union-find
            comp: Dict[int, List[int]] = defaultdict(list)
            for i in range(n):
                comp[_find(i)].append(i)

            comps = [v for v in comp.values() if len(v) >= 2]
            if not comps:
                confirmed_cnt = len(pair_sims)
                confirm_rate = (confirmed_cnt / cand_cnt) if cand_cnt else 0.0
                context.log.info(f"[build] confirmed={confirmed_cnt} rate={confirm_rate:.3f} clusters=0")
                context.add_output_metadata({
                    "build_metrics": {
                        "posts": n,
                        "buckets": len(band_buckets),
                        "candidate_pairs": cand_cnt,
                        "confirmed_pairs": confirmed_cnt,
                        "confirm_rate": round(confirm_rate, 4),
                        "clusters": 0
                    }
                })
                return 0

            # ---- Metrics: confirmation & clustering ----
            confirmed_cnt = len(pair_sims)
            confirm_rate = (confirmed_cnt / cand_cnt) if cand_cnt else 0.0
            context.log.info(f"[build] confirmed={confirmed_cnt} rate={confirm_rate:.3f} clusters={len(comps)}")

            # 5) Representative selection (prefer hot_score if available)
            all_ids = [posts[i]["id"] for g in comps for i in g]
            cur.execute(
                """
                SELECT DISTINCT ON (post_id) post_id, hot_score
                FROM post_trends
                WHERE window_end >= NOW() - INTERVAL '3 hours'
                  AND post_id = ANY(%s)
                ORDER BY post_id, window_end DESC
                """,
                (all_ids,),
            )
            hs = {r[0]: (r[1] or 0.0) for r in cur.fetchall()}

            def _repr_idx(group: List[int]) -> int:
                # pick by hot_score else like+3*comment as tiebreaker
                best = None
                best_val = -1e9
                for i in group:
                    p = posts[i]
                    val = hs.get(p["id"], 0.0)
                    if val == 0:
                        val = (p["like"] or 0) + 3 * (p["cmt"] or 0)
                    if val > best_val:
                        best_val = val
                        best = i
                return best if best is not None else group[0]

            # 6) Upsert clusters & cluster_posts
            up_cluster = 0
            up_members = 0
            for group in comps:
                rep_i = _repr_idx(group)
                rep_post = posts[rep_i]
                member_ids = [posts[i]["id"] for i in group]

                # Try to reuse existing cluster if any member already belongs to one
                cur.execute(
                    "SELECT cluster_id FROM cluster_posts WHERE post_id = ANY(%s) LIMIT 1",
                    (member_ids,),
                )
                row = cur.fetchone()
                if row:
                    cluster_id = row[0]
                    # update representative/size
                    cur.execute(
                        """
                        UPDATE clusters
                        SET representative_post_id = %s, title = %s, size = %s, updated_at = NOW()
                        WHERE id = %s
                        """,
                        (rep_post["id"], rep_post["title"], len(member_ids), cluster_id),
                    )
                else:
                    # create new cluster
                    cur.execute(
                        """
                        INSERT INTO clusters (representative_post_id, title, size)
                        VALUES (%s, %s, %s)
                        RETURNING id
                        """,
                        (rep_post["id"], rep_post["title"], len(member_ids)),
                    )
                    cluster_id = cur.fetchone()[0]
                up_cluster += 1

                # Upsert cluster_posts for members
                for i in group:
                    pid = posts[i]["id"]
                    key = (min(rep_i, i), max(rep_i, i))
                    simv, mtype = pair_sims.get(key, (0.0, ""))
                    cur.execute(
                        """
                        INSERT INTO cluster_posts (cluster_id, post_id, similarity, match_type, is_representative)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (cluster_id, post_id) DO UPDATE SET
                            similarity = EXCLUDED.similarity,
                            match_type = EXCLUDED.match_type,
                            is_representative = EXCLUDED.is_representative
                        """,
                        (cluster_id, pid, float(simv), mtype or None, pid == rep_post["id"]),
                    )
                    up_members += 1

            conn.commit()
            context.add_output_metadata({
                "clusters_upserted": up_cluster,
                "cluster_posts_upserted": up_members,
                "build_metrics": {
                    "posts": n,
                    "buckets": len(band_buckets),
                    "candidate_pairs": cand_cnt,
                    "confirmed_pairs": confirmed_cnt,
                    "confirm_rate": round(confirm_rate, 4),
                    "clusters": len(comps)
                }
            })
            return up_cluster


# =============================================================
# Asset: clusters_merge_asset
#   - Merge highly similar clusters formed in different time windows.
#   - Strategy: use representative post signatures for coarse candidate gen,
#     then confirm with top‑K cross matches between members.
#   - Keeps the older cluster id; migrates members; updates representative/size.
# =============================================================
@dg.asset(
    name="clusters_merge_asset",
    group_name="IS",
    automation_condition=ON1H,
    tags={"data_tier": "gold", "table": "clusters"},
    description="윈도우 간 중복 생성된 유사 클러스터 병합",
)

def clusters_merge_asset(
    context: dg.OpExecutionContext, is_postgres: PostgresResource, clusters_build_asset
):
    """
    최근 14일 내 생성/업데이트된 클러스터만 병합. 각 클러스터에서 상위 8개 멤버만 샘플링, 평균 유사도 0.8 이상 또는 3쌍 이상 유사할 경우 병합.
    """
    lookback_days = MERGE_LOOKBACK_DAYS
    topk = MERGE_TOPK_PER_CLUSTER
    min_pairs = MERGE_MIN_PAIR_COUNT
    avg_thr = MERGE_AVG_SIM_THRESHOLD

    with is_postgres.get_connection() as conn:
        with conn.cursor() as cur:
            # 1) 최근 생성/업데이트된 클러스터와 대표글 시그니처 로딩
            cur.execute(
                f"""
                WITH recent_clusters AS (
                    SELECT c.id, c.representative_post_id
                    FROM clusters c
                    WHERE c.updated_at >= NOW() - INTERVAL '{lookback_days} days'
                )
                SELECT rc.id AS cluster_id, p.id AS post_id, s.text_minhash128, s.gallery_minhash128, s.embed_minhash128
                FROM recent_clusters rc
                JOIN posts p ON p.id = rc.representative_post_id
                JOIN post_signatures s ON s.post_id = p.id
                """
            )
            rep_rows = cur.fetchall()
            if not rep_rows:
                context.log.info("No recent clusters to merge.")
                return 0

            # Build rep sig map: cluster_id -> (tmh, gmh, emh)
            reps: Dict[str, Dict[str, List[int]]] = {}
            for (cluster_id, _post_id, tmh_b, gmh_b, emh_b) in rep_rows:
                reps[cluster_id] = {
                    "tmh": _bytes_to_minhash(tmh_b),
                    "gmh": _bytes_to_minhash(gmh_b),
                    "emh": _bytes_to_minhash(emh_b),
                }

            # Load top‑K members per cluster (by like+3*comment)
            with conn.cursor() as cur2:
                cur2.execute(
                    """
                    WITH scored AS (
                        SELECT cp.cluster_id, p.id AS post_id,
                               COALESCE(p.like_count,0) + 3*COALESCE(p.comment_count,0) AS s,
                               s.text_minhash128, s.gallery_minhash128, s.embed_minhash128
                        FROM cluster_posts cp
                        JOIN posts p ON p.id = cp.post_id
                        JOIN post_signatures s ON s.post_id = p.id
                        WHERE cp.cluster_id = ANY(%s::uuid[])
                    ), ranked AS (
                        SELECT *, ROW_NUMBER() OVER (PARTITION BY cluster_id ORDER BY s DESC) AS rn
                        FROM scored
                    )
                    SELECT cluster_id, post_id, text_minhash128, gallery_minhash128, embed_minhash128
                    FROM ranked
                    WHERE rn <= %s
                    """,
                    (list(reps.keys()), topk),
                )
                member_by_cluster: Dict[str, List[Tuple[List[int], List[int], List[int]]]] = defaultdict(list)
                for (cid, _pid, tmh_b, gmh_b, emh_b) in cur2.fetchall():
                    member_by_cluster[cid].append(
                        (
                            _bytes_to_minhash(tmh_b),
                            _bytes_to_minhash(gmh_b),
                            _bytes_to_minhash(emh_b),
                        )
                    )

            # 2) 후보 생성: 대표글의 텍스트 MinHash LSH
            bands = 32
            rows = 4
            buckets: Dict[int, List[str]] = defaultdict(list)
            for cid, sig in reps.items():
                tmh = sig.get("tmh") or []
                if len(tmh) == 128:
                    for j, hb in enumerate(_band_buckets(tmh, bands=bands, rows=rows)):
                        buckets[((1 + j) << 20) | hb].append(cid)

            cand_pairs: Set[Tuple[str, str]] = set()
            for arr in buckets.values():
                if len(arr) >= 2:
                    cand_pairs.update(combinations(sorted(set(arr)), 2))

            # ---- Metrics: candidate generation (merge) ----
            cand_cnt = len(cand_pairs)
            occ = [len(v) for v in buckets.values()]
            if occ:
                occ_sorted = sorted(occ)
                p50 = occ_sorted[len(occ_sorted)//2]
                p90 = occ_sorted[int(0.9*(len(occ_sorted)-1))]
                context.log.debug(f"[merge] bucket_occupancy min={min(occ)} p50={p50} p90={p90} max={max(occ)} buckets={len(occ)}")
            context.log.info(f"[merge] candidates={cand_cnt}")

            merged = 0
            # 3) 후보 확인: 양방향 상위 K 멤버들 간 교차 매칭으로 유사쌍 수/평균 유사도 측정
            for (a, b) in cand_pairs:
                A = member_by_cluster.get(a, [])
                B = member_by_cluster.get(b, [])
                if not A or not B:
                    continue
                sims: List[float] = []
                for ta, ga, ea in A:
                    best = 0.0
                    for tb, gb, eb in B:
                        tj = _jaccard_from_minhash(ta, tb)
                        gj = _jaccard_from_minhash(ga, gb)
                        ej = _jaccard_from_minhash(ea, eb)
                        cand = max(
                            tj,
                            (gj + tj) / 2.0 if (gj and tj) else 0.0,
                            ej if ej and (tj >= 0.5 or gj >= 0.6) else 0.0,
                        )
                        if cand > best:
                            best = cand
                    if best > 0:
                        sims.append(best)
                sims.sort(reverse=True)
                pair_cnt = sum(1 for s in sims if s >= avg_thr)
                avg_s = (sum(sims[: min_pairs]) / max(1, min_pairs)) if sims else 0.0

                if pair_cnt >= min_pairs or avg_s >= avg_thr:
                    # 4) 병합 실행: 더 오래된(또는 size 큰) 클러스터 id를 유지
                    cur.execute("SELECT created_at, size FROM clusters WHERE id = %s", (a,))
                    ca = cur.fetchone() or (None, 0)
                    cur.execute("SELECT created_at, size FROM clusters WHERE id = %s", (b,))
                    cb = cur.fetchone() or (None, 0)
                    keep, drop = (a, b)
                    if ca[0] is None or (cb[0] is not None and cb[0] < ca[0]) or (cb[1] > ca[1]):
                        keep, drop = (b, a)

                    # 4-1) 멤버 이전 (drop → keep)
                    cur.execute(
                        """
                        INSERT INTO cluster_posts (cluster_id, post_id, similarity, match_type, is_representative)
                        SELECT %s, post_id, similarity, match_type,
                               CASE WHEN is_representative THEN FALSE ELSE is_representative END
                        FROM cluster_posts
                        WHERE cluster_id = %s
                        ON CONFLICT (cluster_id, post_id) DO NOTHING
                        """,
                        (keep, drop),
                    )
                    # 4-2) drop 클러스터 삭제 (cascade로 멤버 정리)
                    cur.execute("DELETE FROM clusters WHERE id = %s", (drop,))

                    # 4-3) 크기/대표 업데이트
                    cur.execute(
                        """
                        WITH sz AS (
                            SELECT COUNT(*) AS cnt FROM cluster_posts WHERE cluster_id = %s
                        ), rep AS (
                            SELECT cp.post_id
                            FROM cluster_posts cp
                            LEFT JOIN post_trends pt ON pt.post_id = cp.post_id
                            WHERE cp.cluster_id = %s
                            ORDER BY COALESCE(pt.hot_score,0) DESC NULLS LAST
                            LIMIT 1
                        )
                        UPDATE clusters c
                        SET size = (SELECT cnt FROM sz),
                            representative_post_id = (SELECT post_id FROM rep),
                            updated_at = NOW()
                        WHERE c.id = %s
                        """,
                        (keep, keep, keep),
                    )
                    merged += 1
            conn.commit()
    merge_rate = (merged / cand_cnt) if cand_cnt else 0.0
    context.log.info(f"[merge] merged={merged} rate={merge_rate:.3f}")
    context.add_output_metadata({"merged_clusters": merged, "candidate_pairs": cand_cnt, "merge_rate": round(merge_rate, 4)})
    return merged


# =============================================================
# Asset: cluster_rotation_asset
#   - Apply decay + rotation suppression and produce ranks for a given window.
#   - Updates `cluster_rotation` (stateful) and writes ranks into latest cluster_trends rows.
# =============================================================
@dg.asset(
    name="cluster_rotation_asset",
    group_name="IS",
    automation_condition=(ON10 & BLOCKS_OK),
    tags={"data_tier": "gold", "table": "cluster_trends"},
    description="백분위 정규화 + 감쇠 + 회전 규칙 적용하여 섹션용 랭킹 산출/상태 갱신",
)

def cluster_rotation_asset(
    context: dg.OpExecutionContext, is_postgres: PostgresResource, post_trends_asset
):
    """
    1주(10080분) 윈도우, top-20, τ=48h 감쇠, 3회 연속 노출 시 24h suppress. 20분 이상 미노출시 연속성 리셋.
    """
    win_min = ROTATION_WINDOW_MINUTES
    label = ROTATION_WINDOW_LABEL
    top_k = ROTATION_TOP_K
    tau_h = ROTATION_DECAY_TAU_HOURS
    max_consec = ROTATION_MAX_CONSECUTIVE
    cooldown_h = ROTATION_COOLDOWN_HOURS

    now = datetime.utcnow()

    with is_postgres.get_connection() as conn:
        with conn.cursor() as cur:
            # 1) 최신 cluster_trends (각 클러스터별 최신 1행) + 최신 멤버시각 로딩
            cur.execute(
                f"""
                WITH latest_ct AS (
                    SELECT DISTINCT ON (cluster_id)
                           cluster_id, window_start, window_end, hot_score
                    FROM cluster_trends
                    WHERE window_end >= NOW() - INTERVAL '{win_min} minutes'
                    ORDER BY cluster_id, window_end DESC
                ),
                last_post_ts AS (
                    SELECT cp.cluster_id, MAX(p.timestamp) AS last_ts
                    FROM cluster_posts cp
                    JOIN posts p ON p.id = cp.post_id
                    GROUP BY cp.cluster_id
                )
                SELECT l.cluster_id, l.hot_score, COALESCE(lp.last_ts, NOW()) AS last_ts
                FROM latest_ct l
                LEFT JOIN last_post_ts lp ON lp.cluster_id = l.cluster_id
                """
            )
            rows = cur.fetchall()
            if not rows:
                context.log.info("No cluster_trends rows for rotation.")
                return 0

            # 2) 윈도우 내 hot_score를 백분위 정규화(CUME_DIST 근사) 후 시간 감쇠 적용
            raw_hots = [float(r[1] or 0.0) for r in rows]
            pmap = _percentile_map(raw_hots)

            scored: List[Tuple[str, float]] = []
            for (cid, hot, last_ts) in rows:
                p = pmap.get(float(hot or 0.0), 0.0)  # 0.0 ~ 1.0
                last_dt = last_ts if isinstance(last_ts, datetime) else datetime.utcnow()
                age_h = max(0.0, (now - last_dt).total_seconds() / 3600.0)
                decayed = p * math.exp(-age_h / float(max(tau_h, 1e-6)))
                scored.append((cid, decayed))
            scored.sort(key=lambda x: x[1], reverse=True)

            # 3) 회전 상태 로딩
            cur.execute(
                "SELECT cluster_id, consecutive_hits, last_shown_at, suppressed_until FROM cluster_rotation WHERE window_label = %s",
                (label,),
            )
            state = {r[0]: {"consec": r[1] or 0, "last": r[2], "suppress": r[3]} for r in cur.fetchall()}

            # 4) 억제/suppress 고려하면서 top_k 선별
            selected: List[Tuple[str, float]] = []
            for (cid, sc) in scored:
                st = state.get(cid)
                if st and st.get("suppress") and st["suppress"] >= now:
                    continue  # 아직 쿨다운 중
                selected.append((cid, sc))
                if len(selected) >= top_k:
                    break

            # 5) 상태 업데이트 및 rank 기록
            rank = 1
            for (cid, sc) in selected:
                st = state.get(cid, {"consec": 0, "last": None, "suppress": None})
                last = st.get("last")
                consec_prev = st.get("consec") or 0
                # Reset if last show was too long ago (> 2× cadence ≈ 20m)
                if last and isinstance(last, datetime) and (now - last) > timedelta(minutes=ROTATION_CONSEC_RESET_MINUTES):
                    consec = 1
                else:
                    consec = consec_prev + 1 if last else 1
                suppress_until = None
                if consec >= max_consec:
                    consec = 0
                    suppress_until = now + timedelta(hours=cooldown_h)

                # upsert state
                cur.execute(
                    """
                    INSERT INTO cluster_rotation (cluster_id, window_label, consecutive_hits, last_shown_at, suppressed_until, last_score)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (cluster_id, window_label) DO UPDATE SET
                        consecutive_hits = EXCLUDED.consecutive_hits,
                        last_shown_at = EXCLUDED.last_shown_at,
                        suppressed_until = EXCLUDED.suppressed_until,
                        last_score = EXCLUDED.last_score
                    """,
                    (cid, label, consec, now, suppress_until, sc),
                )

                # write rank into the latest cluster_trends row for this cluster
                cur.execute(
                    """
                    UPDATE cluster_trends ct
                    SET rank = %s
                    WHERE (ct.cluster_id, ct.window_end) IN (
                      SELECT cluster_id, MAX(window_end) FROM cluster_trends WHERE cluster_id = %s GROUP BY cluster_id
                    )
                    """,
                    (rank, cid),
                )
                rank += 1

            conn.commit()
    context.add_output_metadata({"selected": len(selected), "window_label": label})
    return len(selected)