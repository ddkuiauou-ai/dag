import dagster as dg
import re
import json
import subprocess # Ensured
import os # Ensured
from datetime import datetime # Ensured


def clean_ansi_codes(text: str) -> str:
    """ANSI 컬러 코드를 제거하여 깔끔한 로그 텍스트로 변환"""
    ansi_escape = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')
    return ansi_escape.sub('', text)


def extract_crawl_stats(log_text: str) -> dict:
    """크롤링 통계 정보를 로그에서 추출"""
    stats = {"total_requests": 0, "succeeded": 0, "failed": 0}
    
    # "Total X requests: Y succeeded, Z failed" 패턴 매칭
    pattern = r'Total (\d+) requests: (\d+) succeeded, (\d+) failed'
    match = re.search(pattern, log_text)
    
    if match:
        stats["total_requests"] = int(match.group(1))
        stats["succeeded"] = int(match.group(2))
        stats["failed"] = int(match.group(3))
    
    return stats


@dg.asset(
    group_name="DS", 
    kinds={"source"},
    tags={
        "domain": "image_processing",
        "data_tier": "bronze",
        "source": "node_script"
    },
    description="DS 프로젝트 - Node.js 스크립트를 직접 실행하여 이미지 처리 (스트리밍)" # Updated description
)
def ds_img_r2_node(
    context: dg.AssetExecutionContext
) -> dg.MaterializeResult:
    """DS 이미지 처리용 Node.js 스크립트를 직접 실행하고 로그를 스트리밍합니다."""
    
    context.log.info("🚀 DS 이미지 처리 Node.js 스크립트 시작 (스트리밍)")

    # --- CONFIGURATION: User needs to verify these ---
    command = ["npx", "tsx", "src/main.ts"]
    cwd = "dag/ds_crawlee/ds-naver-crawler" # Assumed path for DS project
    # --- END CONFIGURATION ---

    script_env = os.environ.copy()
    script_env.update({
        "LOG_LEVEL": "DEBUG",
        "DS_PROJECT": "true",
        "IMAGE_PROCESSING": "true",
        "NO_COLOR": "1",
        "FORCE_COLOR": "0",
        "CRAWLEE_MEMORY_MBYTES": "4096",
    })

    log_count_stdout = 0
    log_count_stderr = 0
    processed_images = []
    crawl_stats = {"total_requests": 0, "succeeded": 0, "failed": 0}
    full_stdout_log = []
    full_stderr_log = []
    process = None

    try:
        context.log.info(f"Executing Node.js script: {' '.join(command)} in {cwd}")
        
        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=script_env,
            cwd=cwd,
            bufsize=1 # Line-buffered
        )

        context.log.info("--- DS Node.js 스크립트 STDOUT 스트리밍 시작 ---")
        if process.stdout:
            for line in iter(process.stdout.readline, ''):
                cleaned_line = line.strip()
                context.log.info(cleaned_line)
                log_count_stdout += 1
                full_stdout_log.append(cleaned_line)
                
                if "Company logo saved:" in cleaned_line:
                    filename_match = re.search(r'clogo-[a-f0-9-]+\.png', cleaned_line)
                    if filename_match:
                        filename = filename_match.group()
                        processed_images.append(filename)
                        context.log.info(f"✅ DS 로고 저장 완료 (from stream): {filename}")
            process.stdout.close()
        context.log.info("--- DS Node.js 스크립트 STDOUT 스트리밍 종료 ---")

        context.log.info("--- DS Node.js 스크립트 STDERR 스트리밍 시작 ---")
        if process.stderr:
            for line in iter(process.stderr.readline, ''):
                cleaned_line = line.strip()
                context.log.error(cleaned_line)
                log_count_stderr += 1
                full_stderr_log.append(cleaned_line)
            process.stderr.close()
        context.log.info("--- DS Node.js 스크립트 STDERR 스트리밍 종료 ---")

        return_code = process.wait()

        stdout_full_text = "\n".join(full_stdout_log)
        stats_from_stdout = extract_crawl_stats(clean_ansi_codes(stdout_full_text))
        if stats_from_stdout["total_requests"] > 0:
            crawl_stats = stats_from_stdout
            context.log.info(f"📈 DS 크롤링 통계 (STDOUT 전체 분석): 총 {crawl_stats['total_requests']}개, 성공 {crawl_stats['succeeded']}개, 실패 {crawl_stats['failed']}개")
        
        success = return_code == 0
        context.log.info(f"=== ✅ DS 이미지 처리 Node.js 스크립트 완료 (exit_code: {return_code}) ===")

        if not success:
            error_message = f"DS Node.js script '{' '.join(command)}' failed with exit code {return_code} in {cwd}."
            context.log.error(error_message)
            if full_stderr_log:
                context.log.error("--- DS 전체 STDERR 내용 ---")
                for err_line in full_stderr_log:
                    context.log.error(err_line)
            raise Exception(error_message)

        if processed_images:
            context.log.info(f"📊 총 {len(processed_images)}개 DS 회사 로고 처리 완료:")
            for img in processed_images:
                context.log.info(f"  📸 {img}")
        
        count_value = max(crawl_stats.get("succeeded", 0), len(processed_images))
        
        return dg.MaterializeResult(
            metadata={
                "dagster/row_count": dg.MetadataValue.int(count_value),
                "script_command": dg.MetadataValue.text(" ".join(command)),
                "script_cwd": dg.MetadataValue.text(cwd),
                "exit_code": dg.MetadataValue.int(return_code),
                "log_lines_stdout": dg.MetadataValue.int(log_count_stdout),
                "log_lines_stderr": dg.MetadataValue.int(log_count_stderr),
                "processed_images_count": dg.MetadataValue.int(len(processed_images)),
                "processed_images": dg.MetadataValue.json(processed_images),
                "crawl_total_requests": dg.MetadataValue.int(crawl_stats["total_requests"]),
                "crawl_succeeded": dg.MetadataValue.int(crawl_stats["succeeded"]),
                "crawl_failed": dg.MetadataValue.int(crawl_stats["failed"]),
                "success": dg.MetadataValue.bool(success),
                "execution_method": dg.MetadataValue.text("subprocess.Popen_node.js_streaming"),
                "processing_timestamp": dg.MetadataValue.text(datetime.now().isoformat()),
                "project": dg.MetadataValue.text("DS")
            }
        )
        
    except subprocess.TimeoutExpired as te: # Popen.wait()에 timeout 설정 시 필요
        context.log.error(f"❌ DS 이미지 처리 Node.js 스크립트 시간 초과: {te}")
        if process: process.kill()
        if hasattr(te, 'stdout') and te.stdout:
            context.log.error(f"STDOUT on timeout (DS): {te.stdout.decode(errors='ignore')}")
        if hasattr(te, 'stderr') and te.stderr:
            context.log.error(f"STDERR on timeout (DS): {te.stderr.decode(errors='ignore')}")
        raise
    except FileNotFoundError:
        context.log.error(f"❌ DS Node.js 스크립트 또는 'npx'/'tsx' 실행 파일을 찾을 수 없습니다. Command: {' '.join(command)}, CWD: {cwd}. PATH와 스크립트 경로를 확인하세요.")
        raise
    except Exception as e:
        context.log.error(f"❌ DS 이미지 처리 Node.js 스크립트 실행 중 예상치 못한 오류: {e}")
        if full_stdout_log:
            stdout_excerpt = "\n".join(full_stdout_log)[-1000:]
            context.log.error(f"Captured STDOUT so far (DS): {stdout_excerpt}")
        if full_stderr_log:
            stderr_excerpt = "\n".join(full_stderr_log)[-1000:]
            context.log.error(f"Captured STDERR so far (DS): {stderr_excerpt}")
        raise
    finally:
        if process:
            if process.stdout and not process.stdout.closed:
                process.stdout.close()
            if process.stderr and not process.stderr.closed:
                process.stderr.close()
            if process.poll() is None:
                try:
                    process.terminate()
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
                except Exception as e_finally:
                    context.log.error(f"Error during DS process cleanup: {e_finally}")

# ... (rest of the file, if any)
