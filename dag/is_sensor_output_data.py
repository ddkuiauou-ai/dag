import os
import json
import dagster as dg
from dagster import SensorEvaluationContext, SensorResult, RunRequest
from .jobs import is_ingest_job

# output_data 폴더를 프로젝트 루트 기준으로 지정
OUTPUT_DATA_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "../output_data"))

@dg.sensor(
    name="is_output_data_sensor",
    job=is_ingest_job,
    minimum_interval_seconds=30,  # 최소 폴링 간격
)
def is_output_data_sensor(context: SensorEvaluationContext) -> SensorResult:
    """
    Detect new or modified JSON files and trigger ingest runs.
    - run_key = posts_asset:<filename>:<mtime>  (수정 시 재실행, 중복 방지)
    - Run tags: site/board/mode/range(or cutoff_date) + dagster/priority=3
    """
    previous_state = json.loads(context.cursor) if context.cursor else {}
    current_state = {}
    run_requests = []

    if not os.path.exists(OUTPUT_DATA_PATH):
        context.log.info(f"output_data directory does not exist: {OUTPUT_DATA_PATH}")
        return SensorResult(run_requests=[], cursor=context.cursor)

    for filename in sorted(os.listdir(OUTPUT_DATA_PATH)):  # 결정적 순서 처리
        file_path = os.path.join(OUTPUT_DATA_PATH, filename)
        if not (filename.endswith(".json") and os.path.isfile(file_path)):
            continue

        last_modified = os.path.getmtime(file_path)
        current_state[filename] = last_modified

        # 새 파일이거나 mtime 변경 시에만 실행
        if filename not in previous_state or previous_state[filename] != last_modified:
            context.log.info(f"Detected new or modified file: {filename}")
            run_key = f"posts_asset:{filename}:{last_modified}"

            # 기본 태그 + JSON meta에서 확장
            tags = {"dagster/priority": "3"}
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    jd = json.load(f)
                meta = jd[0].get("meta", {}) if isinstance(jd, list) else jd.get("meta", {})
                if isinstance(meta, dict):
                    tags.update({
                        "site":  (meta.get("site") or ""),
                        "board": (meta.get("board") or ""),
                        "mode":  ((meta.get("crawl_config") or {}).get("mode") or ""),
                    })
                    cc = meta.get("crawl_config") or {}
                    hours = cc.get("hours")
                    cutoff = cc.get("cutoff_date")
                    if hours is not None:
                        tags["range"] = f"{hours}h"
                    elif cutoff:
                        tags["cutoff_date"] = str(cutoff)
            except Exception as e:
                context.log.warning(f"Failed to parse meta for tags from {filename}: {e}")

            run_requests.append(
                RunRequest(
                    run_key=run_key,
                    run_config={
                        "ops": {
                            "posts_asset": {
                                "config": {
                                    "file_path": file_path
                                }
                            }
                        }
                    },
                    tags=tags,
                )
            )

    return SensorResult(run_requests=run_requests, cursor=json.dumps(current_state))
