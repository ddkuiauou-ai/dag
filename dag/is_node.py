"""
IS (Isshoo) 프로젝트 - 커뮤니티 크롤링 시스템

이 모듈은 한국의 주요 커뮤니티 사이트들을 크롤링하여 게시물 데이터를 수집하는 시스템입니다.
Node.js 기반의 Crawlee 프레임워크와 Playwright를 사용하여 고성능 크롤링을 구현합니다.

주요 특징:
- 🚀 빌드된 JavaScript 파일 사용으로 높은 성능
- 📊 디렉토리 스캔 기반 크롤러 순차 실행
- 🔄 증분 크롤링 지원 (중복 데이터 방지)
- 🛡️ 견고한 에러 처리 및 재시도 로직
- 📈 상세한 실행 통계 및 메타데이터

지원 사이트:
- 다모앙 (Damoang): 정치/사회 커뮤니티
- 클리앙 (Clien): IT/기술 커뮤니티 (향후 지원 예정)
- 기타 크롤러 디렉토리는 디렉토리 이름을 기반으로 자동 탐색됩니다.

아키텍처:
1. is_crawler_build: TypeScript → JavaScript 빌드 관리
2. is_crawler_executor: 디렉토리 내 Crawlee JS 순차 실행
3. 출력: JSON 파일로 구조화된 게시물 데이터 저장

작성자: DAG 프로젝트 팀
최종 수정: 2025-06-19
병렬 실행: Dynamic Mapping 기반 is_crawler_parallel_job 추가 (2025-08-13)
"""

import json
import os
import re
import hashlib
import subprocess
from datetime import datetime
from pathlib import Path

import dagster as dg

from dagster import schedule, RunRequest, op, job, DynamicOut, DynamicOutput

# ===== Performance: shared constants, precompiled regex, and helpers =====
from typing import Tuple

FALLBACK_CRAWLER_PATH = Path("./dag/is_crawlee")

# Centralized timeouts (seconds)
BUILD_TIMEOUT_SEC = 300
SEQUENTIAL_TIMEOUT_SEC = 1800

# Common env used for all subprocess runs
COMMON_ENV = {**os.environ, "NODE_ENV": "production", "NO_COLOR": "1"}

# Precompiled regex to avoid recompilation overhead per call
ANSI_ESCAPE_RE = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')
FINISHED_RE = re.compile(r'Finished!\s+Total\s+(\d+)\s+requests:\s+(\d+)\s+succeeded,\s+(\d+)\s+failed\.\s*(\{.*?\})?')
FINAL_STATS_RE = re.compile(r'Final request statistics: (\{.*?\})')
DETAIL_RE = re.compile(r'상세 페이지 처리')
MIME_RE = re.compile(r"(?:\bmimeType\b|\bmime_type\b)\s*[:=]\s*['\"]?([\w.-]+\/[\w.+-]+)", re.IGNORECASE)


def _resolve_base_path() -> Path:
    """Return the actual base crawlee path with a robust fallback."""
    return BASE_CRAWLER_PATH if BASE_CRAWLER_PATH.exists() else FALLBACK_CRAWLER_PATH


def _iter_crawler_dirs_sorted() -> list[Path]:
    """Single-level scan of crawler directories, sorted by name."""
    base = _resolve_base_path()
    return sorted([d for d in base.iterdir() if d.is_dir()], key=lambda d: d.name)


def _summarize_logs(name: str, stdout: str | bytes, stderr: str | bytes, duration: float, timeout_seconds: int | None = None) -> Tuple[dict, bool]:
    """Common log cleaner + stat extractor used by all runners.
    Returns (out_dict, finished_ok).
    """
    clean_stdout = clean_ansi_codes(stdout)
    clean_stderr = clean_ansi_codes(stderr)
    stats = extract_crawl_stats(clean_stdout)
    finished_ok = (
        stats.get("failed") == 0
        and (stats.get("total_requests") or 0) > 0
        and (stats.get("succeeded") or 0) >= (stats.get("total_requests") or 0)
    )

    out = {
        "name": name,
        "status": "success",  # caller may override
        "terminal": bool(stats.get("terminal")) if "terminal" in stats else None,
        "requests": stats.get("total_requests"),
        "succeeded": stats.get("succeeded"),
        "failed": stats.get("failed"),
        "posts_collected": stats.get("posts_collected"),
        "embedded_total": stats.get("embedded_total"),
        "embedded_by_mime": stats.get("embedded_by_mime"),
        "duration_seconds": duration,
        "timeout_seconds": timeout_seconds,
        "log_stdout_snippet": (clean_stdout[-500:] if clean_stdout else ""),
        "log_stderr_snippet": (clean_stderr[-500:] if clean_stderr else ""),
    }
    return out, finished_ok


# 크롤러 빌드 또는 실행 중에 발생하는 예외입니다.
class CrawlerExecutionError(Exception):
    """크롤러 빌드 또는 실행 중에 발생하는 예외입니다."""
    pass



# (Duplicate regex definitions removed here)


def clean_ansi_codes(text: str | bytes) -> str:
    """
    ANSI 컬러 코드를 제거하여 깔끔한 로그 텍스트로 변환

    Crawlee와 Playwright에서 출력되는 컬러 코드를 제거하여
    Dagster 로그에서 읽기 쉽게 만듭니다.

    Args:
        text: ANSI 코드가 포함된 원본 텍스트 (str 또는 bytes)

    Returns:
        ANSI 코드가 제거된 깔끔한 텍스트
    """
    if isinstance(text, (bytes, bytearray)):
        try:
            text = text.decode("utf-8", errors="ignore")
        except Exception:
            # 혹시 모를 디코딩 이슈에 대비
            text = str(text)
    ansi_escape = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')
    return ansi_escape.sub('', text)


def extract_crawl_stats(log_text: str) -> dict[str, int]:
    """
    크롤링 통계 정보를 로그에서 추출
    Crawlee 로그에서 요청 통계와 상세 페이지 처리 횟수를 기반으로 수집 건수를 추출합니다.
    또한 로그 내 임베디드(embedded) 항목의 mimeType 빈도를 집계합니다.
    """
    stats = {
        "total_requests": 0,
        "succeeded": 0,
        "failed": 0,
        "posts_collected": 0,
        "embedded_total": 0,
        "embedded_by_mime": {},
        "terminal": None,
    }
    # Also parse the finishing summary line to reduce false negatives
    finished_match = re.search(r'Finished!\s+Total\s+(\d+)\s+requests:\s+(\d+)\s+succeeded,\s+(\d+)\s+failed\.\s*(\{.*?\})?', log_text)
    if finished_match:
        try:
            total = int(finished_match.group(1))
            succ = int(finished_match.group(2))
            fail = int(finished_match.group(3))
            # Prefer explicit finished numbers when present
            stats["total_requests"] = max(stats.get("total_requests", 0), total)
            stats["succeeded"] = succ
            stats["failed"] = fail
            # Parse optional trailing JSON for terminal flag
            tail = finished_match.group(4)
            if tail:
                try:
                    tail_json = json.loads(tail)
                    stats["terminal"] = bool(tail_json.get("terminal"))
                except Exception:
                    stats["terminal"] = None
            else:
                stats["terminal"] = None
        except Exception:
            pass

    # Final statistics JSON 파싱
    final_pattern = r'Final request statistics: (\{.*?\})'
    final_match = re.search(final_pattern, log_text)
    if final_match:
        try:
            stats_json = json.loads(final_match.group(1))
            stats["total_requests"] = stats_json.get("requestsFinished", 0)
            stats["succeeded"] = stats_json.get("requestsFinished", 0) - stats_json.get("requestsFailed", 0)
            stats["failed"] = stats_json.get("requestsFailed", 0)
        except Exception:
            # 통계 JSON 파싱 실패 시 무시하고 진행
            pass

    # 상세 페이지 처리 횟수 카운트
    detail_pattern = r'상세 페이지 처리'
    stats["posts_collected"] = len(re.findall(detail_pattern, log_text))

    # embedded mimeType 카운트: JSON/로그 형태 모두 허용 (키/값 순서 불문, 작은따옴표/큰따옴표 혼용 허용)
    # 예시 매치 대상:
    #  - { "embedded": [{ "mimeType": "image/png", ... }] }
    #  - mimeType: 'application/pdf'
    #  - mime_type = "text/html"
    mime_regex = re.compile(r"(?:\bmimeType\b|\bmime_type\b)\s*[:=]\s*['\"]?([\w.-]+\/[\w.+-]+)", re.IGNORECASE)
    mime_counts: dict[str, int] = {}
    for match in mime_regex.finditer(log_text):
        mime = match.group(1).lower()
        mime_counts[mime] = mime_counts.get(mime, 0) + 1

    stats["embedded_by_mime"] = mime_counts
    stats["embedded_total"] = sum(mime_counts.values())

    return stats


# =========================
# Dagster-native parallelization (Dynamic Mapping + Collect)
# =========================

BASE_CRAWLER_PATH = Path("/Users/craigchoi/silla/dag/dag/is_crawlee")

@op
def build_crawlers(context: dg.OpExecutionContext) -> int:
    """Build all Crawlee projects (op version of is_crawler_build).
    Returns the number of crawler directories built. This op exists so that
    the parallel run job can depend on a build step within the same run.
    """
    context.log.info("🔨 [build_crawlers] 시작")
    base_path = BASE_CRAWLER_PATH if BASE_CRAWLER_PATH.exists() else Path("./dag/is_crawlee")
    crawler_dirs = [d for d in base_path.iterdir() if d.is_dir()]
    if not crawler_dirs:
        context.log.warning("⚠️ 빌드 대상 크롤러 디렉토리가 없습니다: %s", base_path)
        return 0

    build_command = ["npm", "run", "build"]
    timeout_seconds = 300

    for crawler_dir in crawler_dirs:
        context.log.info(f"🔨 빌드: {crawler_dir.name}")
        result = subprocess.run(
            build_command,
            cwd=str(crawler_dir),
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
            env={**os.environ, "NODE_ENV": "production"}
        )
        if result.returncode != 0:
            context.log.error(f"❌ 빌드 실패: {crawler_dir.name} (코드: {result.returncode})")
            context.log.error(result.stdout)
            context.log.error(result.stderr)
            raise CrawlerExecutionError(f"{crawler_dir.name} 빌드 실패: {result.stderr or result.stdout}")
    context.log.info(f"✅ 빌드 완료: {len(crawler_dirs)}개")
    return len(crawler_dirs)



# Helper: produce valid Dagster mapping keys from arbitrary directory names
def _to_valid_mapping_key(name: str) -> str:
    """Convert arbitrary directory names to a Dagster-valid mapping_key.
    Dagster requires names to match ^[A-Za-z0-9_]+$ for mapping keys.
    We replace invalid characters with `_`, collapse repeats, and trim.
    """
    # Replace any non [A-Za-z0-9_] with '_'
    key = re.sub(r"[^A-Za-z0-9_]", "_", name)
    # Collapse multiple underscores and strip edges
    key = re.sub(r"_+", "_", key).strip("_")
    # Ensure non-empty
    if not key:
        key = "item"
    # Optional: cap length to avoid unwieldy op labels in UI
    return key[:128]


@op(out=DynamicOut(str))
def list_crawler_dirs(context: dg.OpExecutionContext, _built_count: int) -> DynamicOutput[str]:
    """상위 디렉토리 스캔 → 각 크롤러 디렉토리 경로를 DynamicOutput으로 방출."""
    base_path = BASE_CRAWLER_PATH if BASE_CRAWLER_PATH.exists() else Path("./dag/is_crawlee")
    crawler_dirs = sorted([d for d in base_path.iterdir() if d.is_dir()], key=lambda d: d.name)
    if not crawler_dirs:
        context.log.warning("⚠️ 실행 대상 크롤러 디렉토리가 없습니다: %s", base_path)
    # Ensure mapping_key uniqueness after sanitization
    seen: set[str] = set()
    for d in crawler_dirs:
        raw = d.name
        key = _to_valid_mapping_key(raw)
        if key in seen:
            # Append short hash of original name to disambiguate collisions
            suffix = hashlib.sha1(raw.encode("utf-8")).hexdigest()[:6]
            key = f"{key}_{suffix}"
        seen.add(key)
        context.log.info(f"📄 대상: {raw} (mapping_key={key})")
        yield DynamicOutput(str(d), mapping_key=key)


@op(config_schema={"args": [str]}, pool="node_crawlers")
def run_crawler(context: dg.OpExecutionContext, crawler_dir: str) -> dict:
    """한 개의 Crawlee 빌드 산출물(dist/main.js)을 실행.
    - pool="node_crawlers": 동시 실행 개수를 배포 설정에서 제어합니다.
    - config_schema.args: 스케줄에서 전달되는 간격 파라미터 (예: ["--hours", "3"]).
    반환값은 집계를 위해 필요한 최소 통계만 포함합니다.
    """
    # 🔧 Parallel timeout: single source of truth
    PARALLEL_TIMEOUT_SEC = 3000  # 50 minutes for parallel run_crawler
    TIMEOUT_ERROR_MSG = f"{PARALLEL_TIMEOUT_SEC // 60}분 타임아웃 발생"

    args = context.op_config.get("args") or []
    cmd = ["node", "dist/main.js", "--mode", "full"] + [str(a) for a in args]
    env = {**os.environ, "NODE_ENV": "production", "NO_COLOR": "1"}

    context.log.info(f"▶ 실행({Path(crawler_dir).name}): {cmd}")
    start_time = datetime.now()

    try:
        result = subprocess.run(
            cmd,
            cwd=str(crawler_dir),
            capture_output=True,
            text=True,
            timeout=PARALLEL_TIMEOUT_SEC,
            env=env,
        )
        duration = (datetime.now() - start_time).total_seconds()

        clean_stdout = clean_ansi_codes(result.stdout)
        clean_stderr = clean_ansi_codes(result.stderr)
        stats = extract_crawl_stats(clean_stdout)
        finished_ok = (
            stats.get("failed") == 0
            and (stats.get("total_requests") or 0) > 0
            and (stats.get("succeeded") or 0) >= (stats.get("total_requests") or 0)
        )
        terminal = bool(stats.get("terminal")) if "terminal" in stats else None

        out = {
            "name": Path(crawler_dir).name,
            "status": "success" if result.returncode == 0 or finished_ok else "failure",
            "terminal": terminal,
            "requests": stats.get("total_requests"),
            "succeeded": stats.get("succeeded"),
            "failed": stats.get("failed"),
            "posts_collected": stats.get("posts_collected"),
            "embedded_total": stats.get("embedded_total"),
            "embedded_by_mime": stats.get("embedded_by_mime"),
            "duration_seconds": duration,
            "timeout_seconds": PARALLEL_TIMEOUT_SEC,
            "log_stdout_snippet": (clean_stdout[-500:] if clean_stdout else ""),
            "log_stderr_snippet": (clean_stderr[-500:] if clean_stderr else ""),
        }

        # 실패 조건: 프로세스 비정상 종료이면서 명확한 완료 신호가 없는 경우 → 실패로 간주하여 Run 실패 처리
        if result.returncode != 0 and not finished_ok:
            context.log.error(
                f"💥 실패: {out['name']} — {out['log_stderr_snippet'] or out['log_stdout_snippet']}"
            )
            # 요약 로그 남기고 Dagster Failure 발생시켜 step 실패로 마킹
            context.log.info(
                "📊 [%s] status=%s posts=%s req=%s ok=%s fail=%s embed=%s dur=%.1fs",
                out.get("name"),
                out.get("status"),
                out.get("posts_collected") or 0,
                out.get("requests") or 0,
                out.get("succeeded") or 0,
                out.get("failed") or 0,
                out.get("embedded_total") or 0,
                (out.get("duration_seconds") or 0.0),
            )
            raise dg.Failure(
                description=f"크롤러 실패: {out['name']}",
                metadata={
                    **out,
                    "error_type": "process_exit",
                    "error_message": out.get("log_stderr_snippet") or out.get("log_stdout_snippet") or "non-zero exit",
                },
            )

        # Per-crawler one-line summary for easy scanning in logs
        context.log.info(
            "📊 [%s] status=%s posts=%s req=%s ok=%s fail=%s embed=%s dur=%.1fs",
            out.get("name"),
            out.get("status"),
            out.get("posts_collected") or 0,
            out.get("requests") or 0,
            out.get("succeeded") or 0,
            out.get("failed") or 0,
            out.get("embedded_total") or 0,
            (out.get("duration_seconds") or 0.0),
        )
        return out

    except subprocess.TimeoutExpired as e:
        # Ensure timeouts don't crash the whole parallel map; record and continue aggregation
        duration = (datetime.now() - start_time).total_seconds()
        raw_out = getattr(e, "output", None) or getattr(e, "stdout", None) or b""
        raw_err = getattr(e, "stderr", None) or b""
        clean_stdout = clean_ansi_codes(raw_out)
        clean_stderr = clean_ansi_codes(raw_err)
        stats = extract_crawl_stats(clean_stdout)
        timeout_msg = TIMEOUT_ERROR_MSG

        out = {
            "name": Path(crawler_dir).name,
            "status": "timeout",
            "terminal": None,
            "requests": stats.get("total_requests"),
            "succeeded": stats.get("succeeded"),
            "failed": stats.get("failed"),
            "posts_collected": stats.get("posts_collected"),
            "embedded_total": stats.get("embedded_total"),
            "embedded_by_mime": stats.get("embedded_by_mime"),
            "duration_seconds": duration,
            "timeout_seconds": PARALLEL_TIMEOUT_SEC,
            "error_message": timeout_msg,
            "log_stdout_snippet": (clean_stdout[-500:] if clean_stdout else ""),
            "log_stderr_snippet": (clean_stderr[-500:] if clean_stderr else ""),
        }

        context.log.error(f"⏰ 타임아웃: {out['name']} — {timeout_msg}")
        # 요약 로그는 남기되, Dagster Failure를 발생시켜 step을 실패로 표시
        context.log.info(
            "📊 [%s] status=%s posts=%s req=%s ok=%s fail=%s embed=%s dur=%.1fs",
            out.get("name"),
            out.get("status"),
            out.get("posts_collected") or 0,
            out.get("requests") or 0,
            out.get("succeeded") or 0,
            out.get("failed") or 0,
            out.get("embedded_total") or 0,
            (out.get("duration_seconds") or 0.0),
        )
        raise dg.Failure(
            description=f"타임아웃: {out['name']} — {timeout_msg}",
            metadata={
                **out,
                "error_type": "timeout",
                "error_message": timeout_msg,
            },
        )


@op
def aggregate_crawler_results(context: dg.OpExecutionContext, results: list[dict]) -> dict:
    """Dynamic map 결과 집계 (기존 is_crawler_executor 집계 로직과 동일한 필드 유지)."""
    total_requests = sum((r.get("requests") or 0) for r in results)
    total_succeeded = sum((r.get("succeeded") or 0) for r in results)
    total_failed = sum((r.get("failed") or 0) for r in results)
    total_posts = sum((r.get("posts_collected") or 0) for r in results)

    from collections import Counter
    embedded_counter = Counter()
    for r in results:
        for k, v in (r.get("embedded_by_mime") or {}).items():
            embedded_counter[k] += v
    total_embedded = sum(embedded_counter.values())

    total_duration = sum((r.get("duration_seconds") or 0) for r in results)
    avg_requests_per_second = total_requests / total_duration if total_duration > 0 else 0
    avg_posts_per_second = total_posts / total_duration if total_duration > 0 else 0

    summary = {
        "total_crawlers": len(results),
        "total_requests": total_requests,
        "total_succeeded": total_succeeded,
        "total_failed": total_failed,
        "posts_collected": total_posts,
        "average_requests_per_second": round(avg_requests_per_second, 2),
        "average_posts_per_second": round(avg_posts_per_second, 2),
        "embedded_total": total_embedded,
        "embedded_by_mime": dict(embedded_counter),
        "per_crawler_stats": results,
    }
    # Per-crawler summaries
    context.log.info("──────── per-crawler summary ────────")
    for r in results:
        try:
            context.log.info(
                "• %s → status=%s posts=%s req=%s ok=%s fail=%s embed=%s dur=%.1fs",
                r.get("name"),
                r.get("status"),
                r.get("posts_collected") or 0,
                r.get("requests") or 0,
                r.get("succeeded") or 0,
                r.get("failed") or 0,
                r.get("embedded_total") or 0,
                (r.get("duration_seconds") or 0.0),
            )
        except Exception:
            # 방어적 로깅: 한 항목 문제로 전체 로그가 깨지지 않도록
            context.log.debug(f"per-crawler log skipped for: {r}")

    context.log.info(f"✅ 병렬 실행 완료: {len(results)}개, 총 게시물 {total_posts}개")
    return summary


@job
def is_crawler_parallel_job():
    """Dagster-native 병렬 실행 잡.
    1) build_crawlers → 2) list_crawler_dirs (DynamicOut) → 3) run_crawler(map) → 4) aggregate
    """
    built = build_crawlers()
    dirs = list_crawler_dirs(built)
    results = dirs.map(run_crawler)
    aggregate_crawler_results(results.collect())




@dg.asset(
    group_name="IS",
    kinds={"build"},
    tags={
        "domain": "community_content",
        "process": "build",
        "technology": "typescript",
        "framework": "crawlee",
        "tier": "infrastructure"
    },
    description="IS 크롤러 디렉토리 내 모든 Crawlee TypeScript 코드를 JavaScript로 빌드합니다."
)
def is_crawler_build(
    context: dg.AssetExecutionContext
) -> dg.MaterializeResult:
    """
    IS 크롤러 상위 디렉토리의 모든 Crawlee 프로젝트를 빌드합니다.

    이 Asset은 지정된 상위 폴더를 단일 레벨로 스캔하여, 각 크롤러 디렉토리에서
    `npm run build` 명령어를 실행하여 JavaScript 파일을 생성합니다.

    빌드 과정:
    1. 최상위 한 레벨의 크롤러 디렉토리 목록 수집
    2. 각 디렉토리에서 `npm run build` 실행 (5분 타임아웃)
    3. 모든 크롤러 빌드 완료 여부 확인
    4. 빌드된 크롤러 수를 `dagster/row_count` 메타데이터로 반환

    Returns:
        MaterializeResult: 빌드 메타데이터
            - build_success: 전체 빌드 성공 여부 (bool)
            - dagster/row_count: 빌드된 크롤러 디렉토리 수 (int)

    Raises:
        CrawlerExecutionError: 빌드 실패 또는 타임아웃 발생 시
    """

    context.log.info("🔨 IS 크롤러 빌드 시작 - 상위 단일 레벨 스캔")
    build_start_time = datetime.now()

    # 빌드할 크롤러 디렉토리 상위 경로 (단일 레벨 스캔)
    base_path = Path("/Users/craigchoi/silla/dag/dag/is_crawlee")
    if not base_path.exists():
        base_path = Path("./dag/is_crawlee")
    context.log.info(f"🔍 크롤러 디렉토리 탐색 경로: {base_path}")

    crawler_dirs = [d for d in base_path.iterdir() if d.is_dir()]
    context.log.info(f"🔍 발견된 크롤러 디렉토리: {[d.name for d in crawler_dirs]}")

    build_command = ["npm", "run", "build"]
    timeout_seconds = 300  # 5분 타임아웃

    for crawler_dir in crawler_dirs:
        context.log.info(f"🔨 빌드 시작: {crawler_dir.name}")
        result = subprocess.run(
            build_command,
            cwd=str(crawler_dir),
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
            env={**os.environ, "NODE_ENV": "production"}
        )
        if result.returncode != 0:
            context.log.error(f"❌ 빌드 실패: {crawler_dir.name} (코드: {result.returncode})")
            context.log.error(result.stdout)
            context.log.error(result.stderr)
            raise CrawlerExecutionError(f"{crawler_dir.name} 빌드 실패: {result.stderr or result.stdout}")

    build_duration = (datetime.now() - build_start_time).total_seconds()
    built_count = len(crawler_dirs)
    context.log.info(f"✅ 모든 크롤러 빌드 성공! 빌드 개수: {built_count}, 소요 시간: {build_duration:.2f}초")

    return dg.MaterializeResult(
        metadata={
            "build_success": True,
            "dagster/row_count": built_count
        }
    )


@dg.asset(
    deps=[is_crawler_build],  # 빌드 Asset에 의존
    group_name="IS",
    kinds={"source"},
    tags={
        "domain": "community_content",
        "data_tier": "bronze",
        "source": "node_script",
        "technology": "crawlee",
        "framework": "playwright",
        "extraction_type": "web_scraping"
    },
    description="IS 크롤러 디렉토리 내 모든 Crawlee 빌드된 JS를 순차 실행하고, 로그 기반 통계 정보를 수집합니다."
)
def is_crawler_executor(
    context: dg.AssetExecutionContext
) -> dg.MaterializeResult:
    """
    디렉토리 기반 Crawlee JS 실행 및 로그 기반 통계 수집 Asset.

    이 Asset은 빌드된 Crawlee JavaScript 파일이 있는 각 크롤러 디렉토리를 단일 레벨로 스캔하여,
    순차적으로 `node dist/main.js`를 실행합니다. 실행 후 로그의 최종 요청 통계와
    상세 페이지 처리 횟수를 추출하여 메타데이터로 반환합니다.

    실행 과정:
    1. 상위 크롤러 디렉토리 단일 레벨 스캔
    2. 각 디렉토리별 `node dist/main.js` 실행 (30분 타임아웃)
    3. Crawlee 로그의 "Final request statistics" JSON 파싱:
       - requestsFinished → total_requests
       - requestsFailed → failed, succeeded = total_requests - failed
    4. 로그에서 "상세 페이지 처리" 발생 횟수를 카운트하여 posts_collected로 설정
    5. 크롤러별 및 전체 통계 집계 후 반환

    Returns:
        MaterializeResult: 실행 통계 및 크롤러별 상세 정보 포함 메타데이터
        - total_crawlers: 실행된 크롤러 디렉토리 수 (int)
        - total_requests: 총 요청 수 (int)
        - succeeded: 성공 요청 수 (int)
        - failed: 실패 요청 수 (int)
        - posts_collected: 상세 페이지 처리 건수 합계 (int)
        - per_crawler_stats: 개별 크롤러별 통계 리스트 (list)
        - dagster/row_count: total_posts와 동일 (int)
    Raises:
        Exception: 개별 크롤러 실행 실패 또는 타임아웃 발생 시
    """

    # 디렉토리 기반 Crawlee JS 실행 및 로그 기반 통계 수집
    context.log.info("🔄 IS 크롤러 디렉토리 스캔 및 순차 실행 시작")

    base_path = Path("/Users/craigchoi/silla/dag/dag/is_crawlee")
    if not base_path.exists():
        base_path = Path("./dag/is_crawlee")
    crawler_dirs = sorted([d for d in base_path.iterdir() if d.is_dir()], key=lambda d: d.name)
    if not crawler_dirs:
        context.log.warning("⚠️ 크롤러 디렉토리가 없습니다. 스캔 경로를 확인하세요.")
        return dg.MaterializeResult(metadata={"total_crawlers": 0, "dagster/row_count": 0})
    context.log.info(f"🔍 실행 대상 크롤러 디렉토리: {[d.name for d in crawler_dirs]}")

    per_crawler_stats = []

    try:
        for crawler_dir in crawler_dirs:
            context.log.info(f"▶ '{crawler_dir.name}' 크롤러 실행 시작")
            start_time = datetime.now()
            command = ["node", "dist/main.js"]
            result = subprocess.run(
                command,
                cwd=str(crawler_dir),
                capture_output=True,
                text=True,
                timeout=1800,
                env={**os.environ, "NODE_ENV": "production", "NO_COLOR": "1"}
            )
            duration = (datetime.now() - start_time).total_seconds()
            clean_stdout = clean_ansi_codes(result.stdout)
            clean_stderr = clean_ansi_codes(result.stderr)
            stats = extract_crawl_stats(clean_stdout)
            collected = stats.get("posts_collected", 0)
            context.log.info(f"📥 '{crawler_dir.name}' 크롤러 수집된 게시물 수: {collected}개")
            snippet_stdout = clean_stdout[-500:] if clean_stdout else ""
            snippet_stderr = clean_stderr[-500:] if clean_stderr else ""
            finished_ok = (stats.get("failed") == 0 and (stats.get("total_requests") or 0) > 0 and (stats.get("succeeded") or 0) >= (stats.get("total_requests") or 0))
            terminal = bool(stats.get("terminal")) if "terminal" in stats else None
            per_crawler_stats.append({
                "name": crawler_dir.name,
                "status": "success" if result.returncode == 0 or finished_ok else "failure",
                "terminal": terminal,
                "requests": stats.get("total_requests"),
                "succeeded": stats.get("succeeded"),
                "failed": stats.get("failed"),
                "posts_collected": stats.get("posts_collected"),
                "embedded_total": stats.get("embedded_total"),
                "embedded_by_mime": stats.get("embedded_by_mime"),
                "duration_seconds": duration,
                "log_stdout_snippet": snippet_stdout,
                "log_stderr_snippet": snippet_stderr
            })
            if result.returncode != 0 and not finished_ok:
                raise CrawlerExecutionError(f"{crawler_dir.name} 크롤러 실행 실패: {snippet_stderr or snippet_stdout}")

        total_requests = sum(item["requests"] or 0 for item in per_crawler_stats)
        total_succeeded = sum(item["succeeded"] or 0 for item in per_crawler_stats)
        total_failed = sum(item["failed"] or 0 for item in per_crawler_stats)
        total_posts = sum(item["posts_collected"] or 0 for item in per_crawler_stats)
        # 임베디드 mimeType 집계
        from collections import Counter
        embedded_counter = Counter()
        for item in per_crawler_stats:
            for k, v in (item.get("embedded_by_mime") or {}).items():
                embedded_counter[k] += v
        total_embedded = sum(embedded_counter.values())
        total_duration = sum(item["duration_seconds"] or 0 for item in per_crawler_stats)
        avg_requests_per_second = total_requests / total_duration if total_duration > 0 else 0
        avg_posts_per_second = total_posts / total_duration if total_duration > 0 else 0
        posts_collected = total_posts

        metadata = {
            "total_crawlers": len(per_crawler_stats),
            "total_requests": total_requests,
            "total_succeeded": total_succeeded,
            "total_failed": total_failed,
            "posts_collected": posts_collected,
            "average_requests_per_second": round(avg_requests_per_second, 2),
            "average_posts_per_second": round(avg_posts_per_second, 2),
            "embedded_total": total_embedded,
            "embedded_by_mime": dict(embedded_counter),
            "per_crawler_stats": per_crawler_stats,
            "dagster/row_count": posts_collected
        }

        context.log.info(f"✅ 모든 크롤러 실행 완료: {len(per_crawler_stats)}개 크롤러, 총 수집 게시물 {posts_collected}개")
        return dg.MaterializeResult(metadata=metadata)

    except subprocess.TimeoutExpired as e:
        context.log.error("⏰ 크롤러 실행 타임아웃 발생")
        raw_out = getattr(e, "output", None) or getattr(e, "stdout", None) or b""
        raw_err = getattr(e, "stderr", None) or b""
        snippet_stdout = clean_ansi_codes(raw_out)
        snippet_stderr = clean_ansi_codes(raw_err)
        per_crawler_stats.append({
            "name": crawler_dir.name if 'crawler_dir' in locals() else None,
            "status": "timeout",
            "log_stdout_snippet": snippet_stdout,
            "log_stderr_snippet": snippet_stderr
        })
        # Fast-fail: surface timeout as a Dagster failure
        raise dg.Failure(
            description="30분 타임아웃 발생",
            metadata={
                "total_crawlers": len(per_crawler_stats),
                "per_crawler_stats": per_crawler_stats,
                "error_type": "timeout",
                "error_message": "30분 타임아웃 발생",
                "dagster/row_count": sum(item.get("posts_collected") or 0 for item in per_crawler_stats)
            }
        )

    except Exception as e:
        context.log.error(f"💥 크롤러 실행 중 예외 발생: {str(e)}")
        # Fast-fail: bubble up as a Dagster failure so the run is marked failed
        raise dg.Failure(
            description="크롤러 실행 중 예외 발생",
            metadata={
                "total_crawlers": len(per_crawler_stats),
                "per_crawler_stats": per_crawler_stats,
                "error_type": "exception",
                "error_message": str(e)[:500],
                "dagster/row_count": sum(item.get("posts_collected") or 0 for item in per_crawler_stats)
            }
        )

@schedule(
    # (30m=8/38, 1h=:18, 3h=:28, 6h=:48, 12h=0분 12시간 단위, 24h=2:58/14:58, 48h=4:50 */2)
    cron_schedule=[
        "8,38 * * * *",    # every 30 minutes
        "18 * * * *",      # every hour
        "28 */3 * * *",    # every 3 hours
        "48 */6 * * *",    # every 6 hours
        "0 */12 * * *",    # every 12 hours
        "58 2,14 * * *",   # every 24 hours
        "50 4 */2 * *",     # every 48 hours
    ],
    job=is_crawler_parallel_job,
    execution_timezone="Asia/Seoul",
)
def is_crawler_executor_interval_schedule(context):
    ts = context.scheduled_execution_time
    minute, hour, day = ts.minute, ts.hour, ts.day

    if minute in (8, 38):
        flag, value = "--minutes", 30
    elif minute == 50 and hour == 4:
        # 48h: 04:50 every 2 days
        flag, value = "--hours", 48
    elif minute == 58 and hour in (2, 14):
        # 24h: 02:58 and 14:58 daily
        flag, value = "--hours", 24
    elif minute == 0 and (hour % 12 == 0):
        # 12h: at 00:00 and 12:00
        flag, value = "--hours", 12
    elif minute == 48 and (hour % 6 == 0):
        # 6h windows
        flag, value = "--hours", 6
    elif minute == 28 and (hour % 3 == 0):
        # 3h windows
        flag, value = "--hours", 3
    elif minute == 18:
        # 1h windows
        flag, value = "--hours", 1
    else:
        # Fallback (should not normally trigger if cron list is authoritative)
        flag, value = "--hours", 1

    return RunRequest(
        run_key=f"{flag}_{value}_{ts.strftime('%Y%m%dT%H%M')}",
        run_config={
            "ops": {
                "run_crawler": {
                    "config": {
                        "args": [flag, str(value)]
                    }
                }
            },
            "execution": {
                "config": {
                    "multiprocess": {
                        "max_concurrent": 4
                    }
                }
            }
        },
        tags={"interval": f"{value}{flag}"}
    )


@dg.asset(
    config_schema={"args": list},
    deps=[is_crawler_build],
    group_name="IS",
    kinds={"source"},
    tags={
        "domain": "community_content",
        "data_tier": "bronze",
        "source": "node_script",
        "technology": "crawlee",
        "framework": "playwright",
        "extraction_type": "web_scraping"
    },
    description="스케줄 전달 인자에 따라 Crawlee JS를 실행하고, 로그 통계를 수집합니다."
)
def is_crawler_executor_interval(
    context: dg.AssetExecutionContext
) -> dg.MaterializeResult:
    """
    Interval-based Crawlee JS 실행 Asset.

    스케줄러로 전달된 `args` 파라미터(예: `--minute 30`, `--hour 3`)를 사용하여,
    각 크롤러 디렉토리 내 `node dist/main.js`를 실행하고, 로그에서 통계를 추출하여 메타데이터로 반환합니다.

    Args:
        context: Dagster AssetExecutionContext; context.op_config["args"]에 실행 인자 리스트가 포함됩니다.

    Returns:
        dg.MaterializeResult: 실행 통계 및 메타데이터 포함 결과.
    """

    # 디렉토리 기반 Crawlee JS 실행 및 로그 기반 통계 수집
    context.log.info("🔄 IS 크롤러 디렉토리 스캔 및 순차 실행 시작")

    base_path = Path("/Users/craigchoi/silla/dag/dag/is_crawlee")
    if not base_path.exists():
        base_path = Path("./dag/is_crawlee")
    crawler_dirs = sorted([d for d in base_path.iterdir() if d.is_dir()], key=lambda d: d.name)
    if not crawler_dirs:
        context.log.warning("⚠️ 크롤러 디렉토리가 없습니다. 스캔 경로를 확인하세요.")
        return dg.MaterializeResult(metadata={"total_crawlers": 0, "dagster/row_count": 0})
    context.log.info(f"🔍 실행 대상 크롤러 디렉토리: {[d.name for d in crawler_dirs]}")

    per_crawler_stats = []

    try:
        for crawler_dir in crawler_dirs:
            context.log.info(f"▶ '{crawler_dir.name}' 크롤러 실행 시작")
            start_time = datetime.now()
            # 안전하게 스케줄 인자를 가져와 node 실행 커맨드에 추가
            args = context.op_config.get("args") or []
            # 모든 인자를 문자열로 변환하여 명령어에 병합
            cmd_args = ["node", "dist/main.js", "--mode", "full"] + [str(arg) for arg in args]
            context.log.info(f"▶ 실행 커맨드: {cmd_args}")
            result = subprocess.run(
                cmd_args,
                cwd=str(crawler_dir),
                capture_output=True,
                text=True,
                timeout=1800,
                env={**os.environ, "NODE_ENV": "production", "NO_COLOR": "1"}
            )
            duration = (datetime.now() - start_time).total_seconds()
            clean_stdout = clean_ansi_codes(result.stdout)
            clean_stderr = clean_ansi_codes(result.stderr)
            stats = extract_crawl_stats(clean_stdout)
            collected = stats.get("posts_collected", 0)
            context.log.info(f"📥 '{crawler_dir.name}' 크롤러 수집된 게시물 수: {collected}개")
            snippet_stdout = clean_stdout[-500:] if clean_stdout else ""
            snippet_stderr = clean_stderr[-500:] if clean_stderr else ""
            finished_ok = (stats.get("failed") == 0 and (stats.get("total_requests") or 0) > 0 and (stats.get("succeeded") or 0) >= (stats.get("total_requests") or 0))
            terminal = bool(stats.get("terminal")) if "terminal" in stats else None
            per_crawler_stats.append({
                "name": crawler_dir.name,
                "status": "success" if result.returncode == 0 or finished_ok else "failure",
                "terminal": terminal,
                "requests": stats.get("total_requests"),
                "succeeded": stats.get("succeeded"),
                "failed": stats.get("failed"),
                "posts_collected": stats.get("posts_collected"),
                "embedded_total": stats.get("embedded_total"),
                "embedded_by_mime": stats.get("embedded_by_mime"),
                "duration_seconds": duration,
                "log_stdout_snippet": snippet_stdout,
                "log_stderr_snippet": snippet_stderr
            })
            if result.returncode != 0 and not finished_ok:
                raise CrawlerExecutionError(f"{crawler_dir.name} 크롤러 실행 실패: {snippet_stderr or snippet_stdout}")

        total_requests = sum(item["requests"] or 0 for item in per_crawler_stats)
        total_succeeded = sum(item["succeeded"] or 0 for item in per_crawler_stats)
        total_failed = sum(item["failed"] or 0 for item in per_crawler_stats)
        total_posts = sum(item["posts_collected"] or 0 for item in per_crawler_stats)
        # 임베디드 mimeType 집계
        from collections import Counter
        embedded_counter = Counter()
        for item in per_crawler_stats:
            for k, v in (item.get("embedded_by_mime") or {}).items():
                embedded_counter[k] += v
        total_embedded = sum(embedded_counter.values())
        total_duration = sum(item["duration_seconds"] or 0 for item in per_crawler_stats)
        avg_requests_per_second = total_requests / total_duration if total_duration > 0 else 0
        avg_posts_per_second = total_posts / total_duration if total_duration > 0 else 0
        posts_collected = total_posts

        metadata = {
            "total_crawlers": len(per_crawler_stats),
            "total_requests": total_requests,
            "total_succeeded": total_succeeded,
            "total_failed": total_failed,
            "posts_collected": posts_collected,
            "average_requests_per_second": round(avg_requests_per_second, 2),
            "average_posts_per_second": round(avg_posts_per_second, 2),
            "embedded_total": total_embedded,
            "embedded_by_mime": dict(embedded_counter),
            "per_crawler_stats": per_crawler_stats,
            "dagster/row_count": posts_collected
        }

        context.log.info(f"✅ 모든 크롤러 실행 완료: {len(per_crawler_stats)}개 크롤러, 총 수집 게시물 {posts_collected}개")
        return dg.MaterializeResult(metadata=metadata)

    except subprocess.TimeoutExpired as e:
        context.log.error("⏰ 크롤러 실행 타임아웃 발생")
        raw_out = getattr(e, "output", None) or getattr(e, "stdout", None) or b""
        raw_err = getattr(e, "stderr", None) or b""
        snippet_stdout = clean_ansi_codes(raw_out)
        snippet_stderr = clean_ansi_codes(raw_err)
        per_crawler_stats.append({
            "name": crawler_dir.name if 'crawler_dir' in locals() else None,
            "status": "timeout",
            "log_stdout_snippet": snippet_stdout,
            "log_stderr_snippet": snippet_stderr
        })
        # Fast-fail: surface timeout as a Dagster failure
        raise dg.Failure(
            description="30분 타임아웃 발생",
            metadata={
                "total_crawlers": len(per_crawler_stats),
                "per_crawler_stats": per_crawler_stats,
                "error_type": "timeout",
                "error_message": "30분 타임아웃 발생",
                "dagster/row_count": sum(item.get("posts_collected") or 0 for item in per_crawler_stats)
            }
        )

    except Exception as e:
        context.log.error(f"💥 크롤러 실행 중 예외 발생: {str(e)}")
        # Fast-fail: bubble up as a Dagster failure so the run is marked failed
        raise dg.Failure(
            description="크롤러 실행 중 예외 발생",
            metadata={
                "total_crawlers": len(per_crawler_stats),
                "per_crawler_stats": per_crawler_stats,
                "error_type": "exception",
                "error_message": str(e)[:500],
                "dagster/row_count": sum(item.get("posts_collected") or 0 for item in per_crawler_stats)
            }
        )
