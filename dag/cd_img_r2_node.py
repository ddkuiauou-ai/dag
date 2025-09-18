import dagster as dg
import re
import json
import subprocess
import os # Ensure os is imported if not already for script_env
from datetime import datetime


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
    group_name="CD", 
    kinds={"source"},
    tags={
        "domain": "image_processing",
        "data_tier": "bronze",
        "source": "node_script"  # Changed from "docker"
    },
    description="CD 프로젝트 - Node.js 스크립트를 직접 실행하여 이미지 처리" # Updated description
)
def cd_img_r2_node(
    context: dg.AssetExecutionContext
) -> dg.MaterializeResult:
    """CD 이미지 처리용 Node.js 스크립트를 직접 실행하고 로그를 스트리밍합니다."""
    
    context.log.info("🚀 CD 이미지 처리 Node.js 스크립트 시작 (스트리밍)")

    command = ["npx", "tsx", "src/main.ts"]
    cwd = "dag/cd_crawlee/cd-naver-crawler"

    script_env = os.environ.copy()
    script_env.update({
        "LOG_LEVEL": "DEBUG",
        "CD_PROJECT": "true",
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
    full_stderr_log = [] # To capture full stderr for error reporting
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

        context.log.info("--- Node.js 스크립트 STDOUT 스트리밍 시작 ---")
        if process.stdout:
            for line in iter(process.stdout.readline, ''):
                cleaned_line = line.strip()
                context.log.info(cleaned_line)
                log_count_stdout += 1
                # ANSI 코드는 Dagster UI/CLI에서 직접 처리될 수 있으므로, 
                # clean_ansi_codes는 통계 추출 시점에만 적용하거나 필요시 여기서도 적용
                full_stdout_log.append(cleaned_line) 
                
                # 회사 로고 저장 이벤트 특별 처리
                # clean_ansi_codes를 적용한 후 비교하는 것이 더 안전할 수 있습니다.
                # 여기서는 원본 로그 라인에서 직접 확인합니다.
                if "Company logo saved:" in cleaned_line: # Use cleaned_line for pattern matching
                    filename_match = re.search(r'clogo-[a-f0-9-]+\.png', cleaned_line)
                    if filename_match:
                        filename = filename_match.group()
                        processed_images.append(filename)
                        context.log.info(f"✅ CD 로고 저장 완료 (from stream): {filename}")
            process.stdout.close()
        context.log.info("--- Node.js 스크립트 STDOUT 스트리밍 종료 ---")

        # stderr 스트리밍 (비동기적으로 발생 가능, stdout 후 또는 동시에)
        # Popen.communicate() 또는 별도 스레드가 더 견고할 수 있으나, 
        # 여기서는 stdout 처리 후 stderr를 읽습니다.
        # 또는, select 모듈을 사용하여 stdout/stderr를 동시에 폴링할 수 있습니다.
        # 간단하게 하기 위해, stdout을 모두 읽은 후 stderr를 읽습니다.
        # 하지만 이 방식은 stderr가 stdout보다 먼저 많이 쌓이면 버퍼 문제 생길 수 있음.
        # 더 견고한 방식은 select 또는 threading.
        
        # 임시: stderr는 communicate로 한 번에 읽기 (스트리밍 효과는 떨어지나 안정적)
        # stdout_data, stderr_data = process.communicate() # stdout은 이미 위에서 읽었으므로 이렇게 하면 안됨
        # return_code = process.returncode

        # stderr도 스트리밍 (stdout 처럼)
        context.log.info("--- Node.js 스크립트 STDERR 스트리밍 시작 ---")
        if process.stderr:
            for line in iter(process.stderr.readline, ''):
                cleaned_line = line.strip()
                context.log.error(cleaned_line) # Log as error
                log_count_stderr += 1
                full_stderr_log.append(cleaned_line)
            process.stderr.close()
        context.log.info("--- Node.js 스크립트 STDERR 스트리밍 종료 ---")

        return_code = process.wait() # 프로세스 완료 대기 및 종료 코드 가져오기

        # 전체 stdout 로그에서 통계 추출
        stdout_full_text = "\n".join(full_stdout_log)
        # clean_ansi_codes를 전체 텍스트에 적용 후 통계 추출
        stats_from_stdout = extract_crawl_stats(clean_ansi_codes(stdout_full_text))
        if stats_from_stdout["total_requests"] > 0:
            crawl_stats = stats_from_stdout
            context.log.info(f"📈 크롤링 통계 (STDOUT 전체 분석): 총 {crawl_stats['total_requests']}개, 성공 {crawl_stats['succeeded']}개, 실패 {crawl_stats['failed']}개")
        
        success = return_code == 0
        context.log.info(f"=== ✅ CD 이미지 처리 Node.js 스크립트 완료 (exit_code: {return_code}) ==-")

        if not success:
            error_message = f"Node.js script '{' '.join(command)}' failed with exit code {return_code}."
            context.log.error(error_message)
            # full_stderr_log 사용
            if full_stderr_log:
                context.log.error("--- 전체 STDERR 내용 ---")
                for err_line in full_stderr_log:
                    context.log.error(err_line)
            # 아니면 마지막 N 라인만
            # context.log.error(f"STDERR (last 500 chars from captured): {('\n'.join(full_stderr_log))[-500:]}")
            raise Exception(error_message)

        # 처리된 이미지 요약 (from original logic)
        if processed_images:
            context.log.info(f"📊 총 {len(processed_images)}개 회사 로고 처리 완료:")
            for img in processed_images:
                context.log.info(f"  📸 {img}")
        
        # dagster/row_count에 성공한 요청 수 또는 처리된 이미지 수 사용 (from original logic)
        count_value = max(crawl_stats.get("succeeded", 0), len(processed_images))
        
        return dg.MaterializeResult(
            metadata={
                "dagster/row_count": dg.MetadataValue.int(count_value),
                "script_path": dg.MetadataValue.text(command[-1]),
                "script_command": dg.MetadataValue.text(" ".join(command)),
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
                "project": dg.MetadataValue.text("CD")
            }
        )
        
    except subprocess.TimeoutExpired as te: # Popen.wait()에 timeout 설정 시 필요
        context.log.error(f"❌ CD 이미지 처리 Node.js 스크립트 시간 초과: {te}")
        if process: process.kill()
        # stdout/stderr 데이터가 te 객체에 있을 수 있음 (Python 3.x+)
        if hasattr(te, 'stdout') and te.stdout:
            context.log.error(f"STDOUT on timeout: {te.stdout.decode(errors='ignore')}")
        if hasattr(te, 'stderr') and te.stderr:
            context.log.error(f"STDERR on timeout: {te.stderr.decode(errors='ignore')}")
        raise
    except FileNotFoundError:
        context.log.error(f"❌ Node.js 스크립트 또는 'npx'/'tsx' 실행 파일을 찾을 수 없습니다. Command: {' '.join(command)}. PATH와 스크립트 경로를 확인하세요.")
        raise
    except Exception as e:
        context.log.error(f"❌ CD 이미지 처리 Node.js 스크립트 실행 중 예상치 못한 오류: {e}")
        # process 객체가 있고, stdout/stderr가 캡처되었다면 로그 남기기
        if process and process.stdout and not process.stdout.closed:
            stdout_remains = process.stdout.read()
            if stdout_remains:
                context.log.error(f"Remaining STDOUT: {stdout_remains}")
        if process and process.stderr and not process.stderr.closed:
            stderr_remains = process.stderr.read()
            if stderr_remains:
                context.log.error(f"Remaining STDERR: {stderr_remains}")
        # 또는 full_stdout_log / full_stderr_log 사용
        if full_stdout_log:
            context.log.error(f"Captured STDOUT so far: {('\n'.join(full_stdout_log))[-1000:]}")
        if full_stderr_log:
            context.log.error(f"Captured STDERR so far: {('\n'.join(full_stderr_log))[-1000:]}")
        raise
    finally:
        if process: # Ensure pipes are closed and process is reaped
            if process.stdout and not process.stdout.closed:
                process.stdout.close()
            if process.stderr and not process.stderr.closed:
                process.stderr.close()
            if process.poll() is None: # If still running, terminate
                try:
                    process.terminate()
                    process.wait(timeout=5) # Give it a chance to terminate gracefully
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
                except Exception as e_finally:
                    context.log.error(f"Error during process cleanup: {e_finally}")
