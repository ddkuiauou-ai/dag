import json
import os
import re
import subprocess
from datetime import datetime
from pathlib import Path

import dagster as dg

from dagster import op

# 크롤러 빌드 또는 실행 중에 발생하는 예외입니다.
class CrawlerExecutionError(Exception):
    """크롤러 빌드 또는 실행 중에 발생하는 예외입니다."""
    pass


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
    final_match = re.search(final_pattern, log_text, re.DOTALL)
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

BASE_CRAWLER_PATH = Path("/Users/craigchoi/silla/dag/dag/ss_crawlee")

def list_crawler_dirs(base_path: Path) -> list[Path]:
    """Return sorted immediate child directories under base_path."""
    return sorted([d for d in base_path.iterdir() if d.is_dir()], key=lambda d: d.name)

@op
def build_crawlers(context: dg.OpExecutionContext) -> int:
    """Build all Crawlee projects (op version of ss_crawler_build).
    Returns the number of crawler directories built. This op exists so that
    the parallel run job can depend on a build step within the same run.
    """
    context.log.info("🔨 [build_crawlers] 시작")
    base_path = BASE_CRAWLER_PATH if BASE_CRAWLER_PATH.exists() else Path("./dag/ss_crawlee")
    crawler_dirs = list_crawler_dirs(base_path)
    if not crawler_dirs:
        context.log.warning(f"⚠️ 빌드 대상 크롤러 디렉토리가 없습니다: {base_path}")
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
            env={**os.environ, "NODE_ENV": "production", "NO_COLOR": "1"}
        )
        if result.returncode != 0:
            context.log.error(f"❌ 빌드 실패: {crawler_dir.name} (코드: {result.returncode})")
            context.log.error(result.stdout)
            context.log.error(result.stderr)
            raise CrawlerExecutionError(f"{crawler_dir.name} 빌드 실패: {result.stderr or result.stdout}")
    context.log.info(f"✅ 빌드 완료: {len(crawler_dirs)}개")
    return len(crawler_dirs)

@dg.asset(
    group_name="SS",
    kinds={"build"},
    tags={
        "domain": "community_content",
        "process": "build",
        "technology": "typescript",
        "framework": "crawlee",
        "tier": "infrastructure"
    },
    description="SS 크롤러 디렉토리 내 모든 Crawlee TypeScript 코드를 JavaScript로 빌드합니다."
)
def ss_crawler_build(
    context: dg.AssetExecutionContext
) -> dg.MaterializeResult:
    """
    SS 크롤러 상위 디렉토리의 모든 Crawlee 프로젝트를 빌드합니다.

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

    context.log.info("🔨 SS 크롤러 빌드 시작 - 상위 단일 레벨 스캔")
    build_start_time = datetime.now()

    # 빌드할 크롤러 디렉토리 상위 경로 (단일 레벨 스캔)
    base_path = BASE_CRAWLER_PATH if BASE_CRAWLER_PATH.exists() else Path("./dag/ss_crawlee")
    context.log.info(f"🔍 크롤러 디렉토리 탐색 경로: {base_path}")

    crawler_dirs = list_crawler_dirs(base_path)
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
            env={**os.environ, "NODE_ENV": "production", "NO_COLOR": "1"}
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
    deps=[ss_crawler_build],  # 빌드 Asset에 의존
    group_name="SS",
    kinds={"source"},
    tags={
        "domain": "community_content",
        "data_tier": "bronze",
        "source": "node_script",
        "technology": "crawlee",
        "framework": "playwright",
        "extraction_type": "web_scraping"
    },
    description="SS 크롤러 디렉토리 내 모든 Crawlee 빌드된 JS를 순차 실행하고, 로그 기반 통계 정보를 수집합니다."
)
def ss_crawler_executor(
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
    context.log.info("🔄 SS 크롤러 디렉토리 스캔 및 순차 실행 시작")

    base_path = BASE_CRAWLER_PATH if BASE_CRAWLER_PATH.exists() else Path("./dag/ss_crawlee")
    crawler_dirs = list_crawler_dirs(base_path)
    if not crawler_dirs:
        context.log.warning("⚠️ 크롤러 디렉토리가 없습니다. 스캔 경로를 확인하세요.")
        return dg.MaterializeResult(metadata={"total_crawlers": 0, "dagster/row_count": 0})
    context.log.info(f"🔍 실행 대상 크롤러 디렉토리: {[d.name for d in crawler_dirs]}")

    per_crawler_stats = []

    try:
        for crawler_dir in crawler_dirs:
            context.log.info(f"▶ '{crawler_dir.name}' 크롤러 실행 시작")
            start_time = datetime.now()
            command = ["node", "dist/main.js", "--mode", "full", "--hours", "48"]
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
