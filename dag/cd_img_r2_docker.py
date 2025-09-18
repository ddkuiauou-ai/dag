from dagster_docker import PipesDockerClient
import dagster as dg
import docker
import re
import json


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
        "source": "docker"
    },
    description="CD 프로젝트 - 도커 컨테이너 실시간 로그 스트리밍으로 이미지 처리"
)
def cd_img_r2_docker(
    context: dg.AssetExecutionContext
) -> dg.MaterializeResult:
    """CD 이미지 처리용 도커 컨테이너를 실시간 로그 스트리밍으로 실행합니다."""
    
    docker_client = docker.from_env()
    log_count = 0
    container = None
    processed_images = []  # 처리된 이미지 추적
    crawl_stats = {"total_requests": 0, "succeeded": 0, "failed": 0}  # 크롤링 통계
    
    try:
        context.log.info("🚀 CD 이미지 처리 컨테이너 시작")
        
        # CD 이미지 처리용 도커 컨테이너 실행
        container = docker_client.containers.run(
            image="cd-naver-crawler:dev",
            detach=True,
            stdout=True,
            stderr=True,
            remove=False,
            mem_limit="4g",
            environment={
                "LOG_LEVEL": "DEBUG",
                "PYTHONUNBUFFERED": "1",
                "CD_PROJECT": "true",
                "IMAGE_PROCESSING": "true",
                "NO_COLOR": "1",  # 컬러 출력 비활성화
                "FORCE_COLOR": "0",  # 강제 컬러 비활성화
                "CRAWLEE_MEMORY_MBYTES": "4096"                
            }
        )
        
        context.log.info(f"📦 CD 컨테이너 시작됨: {container.short_id}")
        context.log.info("=== 🔄 실시간 이미지 처리 로그 스트리밍 시작 ===")
        
        # 실시간 로그 스트리밍
        for log_line in container.logs(stream=True, follow=True, timestamps=True):
            try:
                log_text = log_line.decode('utf-8').strip()
                if log_text:
                    # ANSI 컬러 코드 제거
                    clean_log = clean_ansi_codes(log_text)
                    
                    # 회사 로고 저장 이벤트 특별 처리
                    if "Company logo saved:" in clean_log:
                        # 파일명 추출
                        filename_match = re.search(r'clogo-[a-f0-9-]+\.png', clean_log)
                        if filename_match:
                            filename = filename_match.group()
                            processed_images.append(filename)
                            context.log.info(f"✅ CD 로고 저장 완료: {filename}")
                        else:
                            context.log.info(f"📸 CD 로고 처리: {clean_log}")
                    
                    # 크롤링 완료 통계 추출
                    elif "PlaywrightCrawler: Finished!" in clean_log and "Total" in clean_log:
                        stats = extract_crawl_stats(clean_log)
                        if stats["total_requests"] > 0:
                            crawl_stats = stats
                            context.log.info(f"📈 크롤링 완료 통계: 총 {stats['total_requests']}개, 성공 {stats['succeeded']}개, 실패 {stats['failed']}개")
                        context.log.info(f"🏁 CD 크롤링 완료: {clean_log}")
                    
                    else:
                        # 일반 로그 출력 (깔끔하게 정리됨)
                        context.log.info(f"📋 CD_PROCESSOR: {clean_log}")
                    
                    log_count += 1
                    
                    # CD 이미지 처리는 상대적으로 로그가 적을 것으로 예상
                    # if log_count > 1500:
                    #     context.log.info("⚠️ 로그 라인 1500개 초과 - 스트리밍 중단")
                    #     break
                        
            except UnicodeDecodeError:
                # 바이너리 데이터는 무시
                continue
            except Exception as e:
                context.log.warning(f"로그 처리 중 오류: {e}")
                continue
        
        # 컨테이너 완료 대기
        exit_code = container.wait()
        success = exit_code['StatusCode'] == 0
        
        context.log.info(f"=== ✅ CD 이미지 처리 완료 (exit_code: {exit_code['StatusCode']}) ===")
        
        # 처리된 이미지 요약
        if processed_images:
            context.log.info(f"📊 총 {len(processed_images)}개 회사 로고 처리 완료:")
            for img in processed_images:
                context.log.info(f"  📸 {img}")
        
        # 최종 로그 확인 및 통계 추출
        final_logs = container.logs(tail=10).decode('utf-8', errors='ignore')
        if final_logs.strip():
            context.log.info("📝 CD 처리 최종 로그:")
            for line in final_logs.split('\n')[-5:]:
                if line.strip():
                    clean_final = clean_ansi_codes(line)
                    context.log.info(f"🔚 CD_FINAL: {clean_final}")
                    
                    # 최종 로그에서도 통계 추출 시도 (놓친 경우를 위해)
                    if "PlaywrightCrawler: Finished!" in clean_final and crawl_stats["total_requests"] == 0:
                        stats = extract_crawl_stats(clean_final)
                        if stats["total_requests"] > 0:
                            crawl_stats = stats
                            context.log.info(f"📈 최종 통계 추출: 총 {stats['total_requests']}개 처리")
        
        # dagster/row_count에 성공한 요청 수 또는 처리된 이미지 수 사용
        count_value = max(crawl_stats["succeeded"], len(processed_images))
        
        return dg.MaterializeResult(
            metadata={
                "dagster/row_count": dg.MetadataValue.int(count_value),
                "container_id": dg.MetadataValue.text(container.short_id),
                "exit_code": dg.MetadataValue.int(exit_code['StatusCode']),
                "log_lines_captured": dg.MetadataValue.int(log_count),
                "processed_images_count": dg.MetadataValue.int(len(processed_images)),
                "processed_images": dg.MetadataValue.json(processed_images),
                "crawl_total_requests": dg.MetadataValue.int(crawl_stats["total_requests"]),
                "crawl_succeeded": dg.MetadataValue.int(crawl_stats["succeeded"]),
                "crawl_failed": dg.MetadataValue.int(crawl_stats["failed"]),
                "success": dg.MetadataValue.bool(success),
                "container_image": dg.MetadataValue.text("crawlee-naver-crawler"),
                "processing_timestamp": dg.MetadataValue.text(context.run.run_id),
                "project": dg.MetadataValue.text("CD")
            }
        )
        
    except Exception as e:
        context.log.error(f"❌ CD 이미지 처리 실행 실패: {e}")
        raise
    finally:
        # 컨테이너 정리
        if container:
            try:
                container.stop()
                container.remove()
                context.log.info("🧹 CD 컨테이너 정리 완료")
            except Exception as e:
                context.log.warning(f"CD 컨테이너 정리 중 오류: {e}")
        
        try:
            docker_client.close()
        except:
            pass
