"""
NPS Data Processing - Silver Tier
국민연금 데이터 처리 및 변환 (멀티프로세싱 최적화)
"""

from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime
import re
import time
from typing import Any
import pandas as pd
import dagster as dg
from dagster import AssetExecutionContext, asset
from dagster_duckdb import DuckDBResource
from tqdm import tqdm
import multiprocessing as mp
import io
import os
import logging

# NPS 설정 임포트
from .nps_raw_ingestion import CONFIG, PATHS, DATABASE

# =============================================================================
# 날짜 추출을 위한 정규식 패턴
# =============================================================================

YYYYMMDD_PATTERN = re.compile(r"(\d{4})(\d{2})(\d{2})")
MMDDYYYY_PATTERN = re.compile(r"(\d{1,2})[/_-](\d{1,2})[/_-](\d{4})")  
YYYYMM_PATTERN = re.compile(r"(\d{4})\D*(\d{1,2})\D")  
KOREAN_DATE_PATTERN = re.compile(r"(\d{4})년\s*(\d{1,2})월")
UNDERLINE_DATE_PATTERN = re.compile(r"(\d{1,2})_(\d{1,2})_(\d{4})")

# =============================================================================
# 멀티프로세싱을 위한 독립적인 함수들 (모듈 레벨에서 정의)
# =============================================================================

def extract_date_from_filename(file_name: str) -> str | None:
    """파일명에서 날짜 추출 (멀티프로세싱용 독립 함수)"""
    
    # 1. YYYYMMDD 형식
    match = YYYYMMDD_PATTERN.search(file_name)
    if match:
        year_str, month_str, day_str = match.groups() 
        try:
            year, month, day = int(year_str), int(month_str), int(day_str)
            # 날짜 유효성 검사
            datetime(year, month, day) 
            if CONFIG.min_year <= year <= CONFIG.max_year:
                return f"{year_str}{month_str.zfill(2)}{day_str.zfill(2)}"
        except ValueError:
            pass
    
    # 2. 한국어 연월 형식
    match = KOREAN_DATE_PATTERN.search(file_name)
    if match:
        year_str, month_str = match.groups()
        try:
            year, month = int(year_str), int(month_str)
            if CONFIG.min_year <= year <= CONFIG.max_year and 1 <= month <= 12:
                day = 1 
                return f"{year}{month:02d}{day:02d}"
        except ValueError:
            pass

    # 3. MMDDYYYY 형식 (예: 06_19_2020 또는 06-19-2020)
    match = MMDDYYYY_PATTERN.search(file_name)
    if match:
        month_str, day_str, year_str = match.groups() 
        try:
            year, month, day = int(year_str), int(month_str), int(day_str)
            # 날짜 유효성 검사
            datetime(year, month, day) 
            if CONFIG.min_year <= year <= CONFIG.max_year:
                return f"{year_str}{month_str.zfill(2)}{day_str.zfill(2)}"
        except ValueError:
            pass
            
    return None

def load_csv_with_smart_encoding(file_path_str: str) -> pd.DataFrame:
    """스마트 인코딩 감지로 CSV 로드 (멀티프로세싱용)"""
    file_path = Path(file_path_str)
    encoding_candidates = ['utf-8', 'utf-8-sig', 'cp949', 'euc-kr', 'iso-8859-1']
    
    for encoding in encoding_candidates:
        try:
            return pd.read_csv(
                file_path,
                encoding=encoding,
                dtype=str,  # 원본 데이터 타입 유지
                na_values=[''],
                keep_default_na=True,
                low_memory=False,
                on_bad_lines='skip'
            )
        except UnicodeDecodeError:
            continue
        except Exception as e:
            if "codec can't decode" in str(e).lower():
                continue
            raise
    
    # 모든 인코딩 실패 시 errors='replace' 옵션으로 시도
    for encoding in encoding_candidates:
        try:
            with open(file_path, 'r', encoding=encoding, errors='replace') as f:
                content = f.read()
            
            return pd.read_csv(
                io.StringIO(content),
                dtype=str,
                na_values=[''],
                keep_default_na=True,
                on_bad_lines='skip'
            )
        except Exception:
            continue
    
    raise RuntimeError(f"모든 인코딩 시도 실패: {file_path.name}")

def process_single_csv_file_mp(file_info: tuple[str, str]) -> dict[str, Any]:
    """
    단일 CSV 파일 처리 (멀티프로세싱용)
    
    Args:
        file_info: (input_file_path_str, output_dir_str) 튜플
    
    Returns:
        처리 결과 딕셔너리
    """
    input_file_path_str, output_dir_str = file_info
    
    try:
        start_time = time.time()
        
        input_file_path = Path(input_file_path_str)
        output_dir = Path(output_dir_str)
        
        # 날짜 추출
        extracted_date = extract_date_from_filename(input_file_path.name)
        if not extracted_date:
            extracted_date = "unknown_date"
        
        # 인코딩 감지 및 DataFrame 로드
        df = load_csv_with_smart_encoding(input_file_path_str)
        
        if df.empty:
            return {
                "status": "skipped",
                "reason": "empty_file",
                "original_file": input_file_path.name,
                "process_id": os.getpid()
            }
        
        row_count = len(df)
        
        # 출력 파일 이름 생성
        base_name = input_file_path.stem
        output_file_name = f"{extracted_date}_{base_name}.csv"
        output_file_path = output_dir / output_file_name
        
        # 출력 디렉토리 생성
        output_file_path.parent.mkdir(parents=True, exist_ok=True)
        
        # UTF-8으로 저장 (원본 데이터 그대로)
        df.to_csv(
            output_file_path,
            index=False,
            encoding='utf-8-sig',
            na_rep='',
            lineterminator='\n'
        )
        
        processing_time = time.time() - start_time
        
        return {
            "status": "success",
            "original_file": input_file_path.name,
            "output_file": output_file_name,
            "rows": row_count,
            "extracted_date": extracted_date,
            "columns": len(df.columns),
            "encoding": "utf-8-sig",
            "processing_time_seconds": round(processing_time, 2),
            "process_id": os.getpid(),
            "file_size_mb": round(input_file_path.stat().st_size / (1024 * 1024), 2)
        }
        
    except Exception as e:
        return {
            "status": "error",
            "original_file": Path(input_file_path_str).name,
            "error": str(e),
            "processing_time_seconds": time.time() - start_time if 'start_time' in locals() else 0,
            "process_id": os.getpid()
        }

def process_batch_csv_files_mp(batch_info: tuple[list[str], str]) -> list[dict[str, Any]]:
    """
    배치로 여러 CSV 파일 처리 (멀티프로세싱용)
    
    Args:
        batch_info: (file_paths_list, output_dir_str) 튜플
    
    Returns:
        처리 결과 리스트
    """
    file_paths, output_dir_str = batch_info
    results = []
    
    for file_path_str in file_paths:
        result = process_single_csv_file_mp((file_path_str, output_dir_str))
        results.append(result)
    
    return results

# =============================================================================
# NPS 히스토리 데이터 처리 Asset (멀티프로세싱 최적화)
# =============================================================================

@dg.asset(
    description="다운로드된 NPS CSV 파일들을 멀티프로세싱으로 UTF-8 인코딩 변환 - Silver Tier",
    group_name="NPS",
    kinds={"python", "csv"},
    tags={
        "domain": "finance", 
        "data_tier": "silver",
        "source": "national_pension",
        "optimization": "multiprocessing"
    },
    deps=["nps_raw_ingestion"]
) 
def nps_data_processing(
    context: AssetExecutionContext,
    nps_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    """NPS 히스토리 데이터를 멀티프로세싱으로 UTF-8 인코딩 변환하여 out 폴더로 복사"""
    start_time = time.time()
    context.log.info("NPS 히스토리 데이터 처리 시작 - 멀티프로세싱 최적화")

    # 1. 출력 디렉토리 생성
    PATHS.processed_history_dir.mkdir(parents=True, exist_ok=True)
    
    # 2. 파일 목록 수집
    csv_files = list(PATHS.raw_history_dir.glob("*.csv"))
    
    if not csv_files:
        context.log.warning("처리할 CSV 파일이 없습니다.")
        return dg.MaterializeResult(
            metadata={
                "status": "No CSV files to process",
                "processed_files_count": 0,
            }
        )
    
    total_files = len(csv_files)
    context.log.info(f"총 {total_files}개 CSV 파일 발견")
    
    # 3. 멀티프로세싱 설정 (CPU 집약적 작업에 최적화)
    max_workers = min(mp.cpu_count(), max(4, mp.cpu_count() - 1))  # 최소 4개, 최대 CPU-1개
    batch_size = max(1, total_files // max_workers)  # 배치 크기 자동 계산
    
    context.log.info(f"멀티프로세싱 설정: {max_workers}개 프로세스, 배치 크기: {batch_size}")
    context.log.info(f"사용 가능한 CPU 코어: {mp.cpu_count()}개")
    
    # 4. 파일들을 배치로 분할
    file_batches = []
    file_paths_str = [str(f) for f in csv_files]
    output_dir_str = str(PATHS.processed_history_dir)
    
    for i in range(0, len(file_paths_str), batch_size):
        batch = file_paths_str[i:i + batch_size]
        file_batches.append((batch, output_dir_str))
    
    context.log.info(f"파일을 {len(file_batches)}개 배치로 분할")
    
    # 5. 멀티프로세싱 실행
    results = []
    
    try:
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            # 배치 단위로 작업 제출
            future_to_batch = {}
            for i, batch_info in enumerate(file_batches):
                future = executor.submit(process_batch_csv_files_mp, batch_info)
                future_to_batch[future] = i
            
            # 진행상황 모니터링
            for future in tqdm(as_completed(future_to_batch), total=len(future_to_batch), 
                            desc="NPS 배치 처리", unit="batch"):
                batch_idx = future_to_batch[future]
                
                try:
                    batch_results = future.result()
                    results.extend(batch_results)
                    
                    # 배치 처리 결과 로깅
                    successful_in_batch = sum(1 for r in batch_results if r["status"] == "success")
                    context.log.info(
                        f"배치 {batch_idx + 1}/{len(file_batches)} 완료: {successful_in_batch}/{len(batch_results)}개 성공"
                    )
                        
                except Exception as e:
                    context.log.error(f"배치 {batch_idx + 1} 처리 중 오류: {e}")
                    # 개별 파일 처리 실패 시에도 계속 진행
                    batch_files = file_batches[batch_idx][0]
                    for file_path_str in batch_files:
                        results.append({
                            "status": "error",
                            "original_file": Path(file_path_str).name,
                            "error": f"배치 처리 실패: {str(e)}"
                        })
        
    except KeyboardInterrupt:
        context.log.warning("키보드 중단 감지")
        raise KeyboardInterrupt("사용자가 처리를 중단했습니다")
    
    # 6. 결과 집계 및 분석
    successful_results = [r for r in results if r["status"] == "success"]
    failed_results = [r for r in results if r["status"] == "error"]
    skipped_results = [r for r in results if r["status"] == "skipped"]
    
    total_rows = sum(r.get("rows", 0) for r in successful_results)
    total_time = time.time() - start_time
    
    # 성능 분석
    process_ids = set(r.get("process_id") for r in successful_results if r.get("process_id"))
    avg_file_size = sum(r.get("file_size_mb", 0) for r in successful_results) / max(1, len(successful_results))
    throughput_files_per_sec = len(successful_results) / max(0.001, total_time)
    throughput_rows_per_sec = total_rows / max(0.001, total_time)
    
    context.log.info(
        f"처리 완료: {len(successful_results)}개 성공, {len(failed_results)}개 실패, "
        f"{len(skipped_results)}개 건너뜀"
    )
    context.log.info(
        f"성능: {total_rows:,}행 처리, {total_time:.2f}초 소요 "
        f"({throughput_files_per_sec:.1f} 파일/초, {throughput_rows_per_sec:,.0f} 행/초)"
    )
    context.log.info(f"활용된 프로세스 수: {len(process_ids)}개, 평균 파일 크기: {avg_file_size:.2f}MB")
    
    # 7. 메타데이터 저장
    try:
        with nps_duckdb.get_connection() as conn:
            meta_table = f"{DATABASE.metadata_table}_processed"
            create_meta_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {meta_table} (
                id INTEGER PRIMARY KEY,
                original_file VARCHAR,
                output_file VARCHAR,
                rows INTEGER,
                extracted_date VARCHAR,
                columns INTEGER,
                encoding VARCHAR,
                processing_time_seconds REAL,
                process_id INTEGER,
                file_size_mb REAL,
                process_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
            
            conn.execute(create_meta_table_sql)
            
            if successful_results:
                max_id_result = conn.execute(f"SELECT COALESCE(MAX(id), 0) FROM {meta_table}").fetchone()
                next_id = max_id_result[0] + 1 if max_id_result else 1
                
                insert_data = [
                    (
                        next_id + i,
                        result["original_file"],
                        result["output_file"],
                        result["rows"],
                        result["extracted_date"],
                        result["columns"],
                        result.get("encoding", "utf-8-sig"),
                        result.get("processing_time_seconds", 0),
                        result.get("process_id", 0),
                        result.get("file_size_mb", 0.0)
                    )
                    for i, result in enumerate(successful_results)
                ]
                
                conn.executemany(
                    f"""
                    INSERT INTO {meta_table} (
                        id, original_file, output_file, rows, extracted_date, 
                        columns, encoding, processing_time_seconds, process_id, file_size_mb
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    insert_data
                )
                
                context.log.info(f"메타데이터 저장 완료: {len(insert_data)}개 레코드")
                
    except Exception as db_error:
        context.log.warning(f"메타데이터 저장 실패 (처리는 완료됨): {db_error}")
    
    # 8. 성능 지표 계산
    processing_stats = {
        "total_processing_time": round(total_time, 2),
        "avg_file_processing_time": round(total_time/max(1, len(successful_results)), 2),
        "rows_per_second": int(throughput_rows_per_sec),
        "files_per_second": round(throughput_files_per_sec, 2),
        "parallel_workers": max_workers,
        "processes_used": len(process_ids),
        "batch_size": batch_size,
        "batches_total": len(file_batches),
        "encoding_conversion": "auto-detect → utf-8-sig",
        "success_rate": round((len(successful_results) / total_files) * 100, 1) if total_files > 0 else 0,
        "avg_file_size_mb": round(avg_file_size, 2),
        "parallelization_efficiency": round((len(process_ids) / max_workers) * 100, 1)
    }
    
    # 9. 결과 반환
    return dg.MaterializeResult(
        metadata={
            "dagster/row_count": dg.MetadataValue.int(total_rows),
            "total_csv_files": dg.MetadataValue.int(total_files),
            "processed_files": dg.MetadataValue.int(len(successful_results)),
            "failed_files": dg.MetadataValue.int(len(failed_results)),
            "skipped_files": dg.MetadataValue.int(len(skipped_results)),
            "output_directory": dg.MetadataValue.text(str(PATHS.processed_history_dir)),
            "processing_time_seconds": dg.MetadataValue.float(total_time),
            "throughput_rows_per_second": dg.MetadataValue.int(int(throughput_rows_per_sec)),
            "throughput_files_per_second": dg.MetadataValue.float(throughput_files_per_sec),
            "multiprocessing_performance": dg.MetadataValue.json(processing_stats),
            "processed_examples": dg.MetadataValue.json(successful_results[:5] if successful_results else []),
            "failed_examples": dg.MetadataValue.json(failed_results[:3] if failed_results else [])
        }
    )
