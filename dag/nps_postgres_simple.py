"""
NPS PostgreSQL 간소화 버전
핵심 원칙:
1. PostgreSQL COPY 최적화: StringIO 스트리밍
2. 시그널 핸들링: 강제 종료 시 안전한 리소스 정리
"""

import dagster as dg
import pandas as pd
import signal
import threading
import time
import io
import re
from pathlib import Path
from typing import Dict, List, Optional
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor, as_completed
from .resources import PostgresResource

# 🔥 핵심 설정만 유지
POSTGRES_NPS_TABLE = "pension"
DEFAULT_MAX_WORKERS = 2

# 🚨 인덱스 및 제약조건 관리 (대량 삽입 최적화)
# 삽입 전 제거할 인덱스들
DROP_INDEXES_SQL = [
    "DROP INDEX IF EXISTS pension_opt_date_idx",
    "DROP INDEX IF EXISTS pension_opt_company_name_idx", 
    "DROP INDEX IF EXISTS pension_opt_industry_code_idx",
    "DROP INDEX IF EXISTS pension_opt_zip_code_idx",
    "DROP INDEX IF EXISTS pension_opt_region_industry_idx",
    "DROP INDEX IF EXISTS pension_opt_unique_business_month"
]

# 컬럼 목록을 전역 변수로 정의하여 재사용
# 이 순서는 PostgreSQL 테이블의 컬럼 순서 및 COPY 명령어의 컬럼 순서와 일치해야 합니다.
CSV_COLUMNS_SCHEMA = [
    'data_created_ym',         # 자료생성년월
    'company_name',            # 사업장명
    'business_reg_num',        # 사업자등록번호
    'join_status',             # 사업장가입상태코드
    'zip_code',                # 우편번호
    'lot_number_address',      # 사업장지번상세주소
    'road_name_address',       # 사업장도로명상세주소
    'legal_dong_addr_code',    # 고객법정동주소코드
    'admin_dong_addr_code',    # 고객행정동주소코드
    'addr_sido_code',          # 법정동주소광역시도코드
    'addr_sigungu_code',       # 시군구코드
    'addr_emdong_code',        # 읍면동코드
    'workplace_type',          # 법인/개인
    'industry_code',           # 업종코드
    'industry_name',           # 업종명
    'applied_at',              # 적용일자
    're_registered_at',        # 재등록일자
    'withdrawn_at',            # 탈퇴일자
    'subscriber_count',        # 가입자수
    'monthly_notice_amount',   # 당월고지금액
    'new_subscribers',         # 신규취득자수
    'lost_subscribers'         # 상실가입자수
]


def drop_indexes_and_constraints(postgres: PostgresResource, context=None) -> bool:
    """대량 삽입 전 인덱스와 제약조건 제거"""
    try:
        with postgres.get_connection() as conn:
            with conn.cursor() as cursor:
                if context:
                    context.log.info("🗑️ 인덱스 제거 시작 (삽입 성능 최적화)")
                else:
                    print("🗑️ 인덱스 제거 시작 (삽입 성능 최적화)")
                
                for drop_sql in DROP_INDEXES_SQL:
                    try:
                        cursor.execute(drop_sql)
                        if context:
                            context.log.info(f"✅ {drop_sql}")
                        else:
                            print(f"✅ {drop_sql}")
                    except Exception as e:
                        # 인덱스가 존재하지 않는 경우는 무시
                        if context:
                            context.log.warning(f"⚠️ {drop_sql} - {str(e)}")
                        else:
                            print(f"⚠️ {drop_sql} - {str(e)}")
                
                conn.commit()
                
                if context:
                    context.log.info("🏁 인덱스 제거 완료")
                else:
                    print("🏁 인덱스 제거 완료")
                return True
                
    except Exception as e:
        if context:
            context.log.error(f"💥 인덱스 제거 실패: {str(e)}")
        else:
            print(f"💥 인덱스 제거 실패: {str(e)}")
        return False


# 🚨 시그널 핸들링 및 Semaphore 기반 워커 슬롯 제어
_shutdown_requested = threading.Event()
_active_executors = set()
_executor_lock = threading.Lock()
_worker_semaphore = None  # 동적으로 설정됨
_worker_stats = {}  # 워커별 통계 정보
_stats_lock = threading.Lock()

def signal_handler(signum, frame):
    """시그널 핸들러 - 안전한 리소스 정리"""
    print(f"\n🚨 시그널 {signum} 감지 - 안전한 종료 시작")
    _shutdown_requested.set()
    
    # 활성 executor 정리
    with _executor_lock:
        for executor in list(_active_executors):
            try:
                executor.shutdown(wait=False)
            except Exception:
                pass

def install_signal_handlers():
    """시그널 핸들러 설치"""
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    print("🛡️ 시그널 핸들러 설치 완료")

def init_worker_semaphore(max_workers: int):
    """워커 Semaphore 초기화"""
    global _worker_semaphore, _worker_stats
    _worker_semaphore = threading.Semaphore(max_workers)
    _worker_stats.clear()
    print(f"🎯 워커 Semaphore 초기화: {max_workers}개 슬롯")

def get_worker_stats() -> Dict:
    """워커 통계 정보 반환"""
    with _stats_lock:
        return _worker_stats.copy()

def update_worker_stats(worker_id: str, file_path: str, records: int, duration: float, success: bool):
    """워커 통계 업데이트"""
    with _stats_lock:
        if worker_id not in _worker_stats:
            _worker_stats[worker_id] = {
                'files_processed': 0,
                'total_records': 0,
                'total_duration': 0.0,
                'successful_files': 0,
                'failed_files': 0,
                'avg_records_per_second': 0.0
            }
        
        stats = _worker_stats[worker_id]
        stats['files_processed'] += 1
        stats['total_records'] += records
        stats['total_duration'] += duration
        
        if success:
            stats['successful_files'] += 1
        else:
            stats['failed_files'] += 1
        
        # 평균 처리 속도 계산
        if stats['total_duration'] > 0:
            stats['avg_records_per_second'] = stats['total_records'] / stats['total_duration']

def register_executor(executor):
    """활성 executor 등록"""
    with _executor_lock:
        _active_executors.add(executor)

def unregister_executor(executor):
    """executor 등록 해제"""
    with _executor_lock:
        _active_executors.discard(executor)

# NPSFilePaths 인스턴스 생성
@dataclass
class NPSFilePaths:
    """NPS 파일 경로 관리"""
    base_dir: Path = Path("data") / "nps" 
    
    @property
    def out_history_dir(self) -> Path:
        return self.base_dir / "history" / "out"
        
    @property
    def processed_history_dir(self) -> Path:
        return self.base_dir / "history" / "processed_history"

PATHS = NPSFilePaths()

class SimpleFileResult:
    """간단한 파일 처리 결과"""
    def __init__(self, file_path: str, success: bool, records: int = 0, error: str = None):
        self.file_path = file_path
        self.success = success
        self.records = records
        self.error = error

def get_file_date(filename: str) -> Optional[str]:
    """파일명에서 날짜 추출 (간소화)"""
    import re
    
    # YYYY년 MM월 패턴
    match = re.search(r'(\d{4})년\s*(\d{1,2})월', filename)
    if match:
        year, month = match.groups()
        return f"{year}-{month.zfill(2)}"
    
    # YYYYMM 패턴
    match = re.search(r'(\d{4})(\d{2})', filename)
    if match:
        year, month = match.groups()
        return f"{year}-{month}"
    
    return None

def convert_to_copy_format(df: pd.DataFrame, ordered_columns: List[str]) -> str:
    """
    DataFrame을 PostgreSQL COPY 형식으로 변환 (최적화됨)
    StringIO 스트리밍 및 df.to_csv() 사용.
    process_single_file에서 모든 데이터 클리닝 및 NA 처리가 완료되었다고 가정합니다.
    """
    output = io.StringIO()
    # DataFrame의 컬럼 순서를 ordered_columns에 맞춰서 CSV로 변환
    # process_single_file에서 NA 처리가 완료되었으므로, 여기서는 추가적인 replace 불필요
    df.to_csv(output, sep='\t', header=False, index=False, na_rep='\\N', columns=ordered_columns)
    return output.getvalue()

def process_single_file(csv_file: Path, data_created_ym: str, postgres: PostgresResource, context: dg.AssetExecutionContext) -> SimpleFileResult:
    """단일 파일 처리 (중복 제거 포함, Pandas 연산 최적화)"""
    try:
        if _shutdown_requested.is_set():
            return SimpleFileResult(str(csv_file), False, 0, "Shutdown requested")

        # CSV 읽기 (스키마에 정의된 컬럼명 사용)
        df = pd.read_csv(csv_file, header=None, skiprows=1, dtype=str, encoding='utf-8')
        df.columns = CSV_COLUMNS_SCHEMA # 전역 컬럼 스키마 사용

        # 모든 컬럼에 대해 클리닝 및 NA 처리 (data_created_ym는 별도 처리)
        for col_name in df.columns:
            if col_name == 'data_created_ym':
                continue

            df[col_name] = df[col_name].fillna('').astype(str)
            df[col_name] = df[col_name].str.replace('\t', ' ', regex=False)
            df[col_name] = df[col_name].str.replace('\n', ' ', regex=False)
            df[col_name] = df[col_name].str.replace('\r', ' ', regex=False)
            df[col_name] = df[col_name].str.strip()
            df[col_name] = df[col_name].replace('', pd.NA)
            
        df['data_created_ym'] = f"{data_created_ym}-15 00:00:00"
        
        unique_key = [
            'data_created_ym', 'company_name', 'business_reg_num', 'zip_code', 
            'subscriber_count', 'monthly_notice_amount'
        ]
        
        before_dedup = len(df)
        df.drop_duplicates(subset=unique_key, inplace=True, keep='first')
        after_dedup = len(df)

        deduplicated_rows = before_dedup - after_dedup
        if deduplicated_rows > 0:
            context.log.info(f"{csv_file.name} - {deduplicated_rows} duplicate rows removed based on unique key constraint.")

        total_records = len(df)
        if total_records == 0:
            context.log.info(f"{csv_file.name} - No records to process after deduplication.")
            return SimpleFileResult(str(csv_file), True, 0)
        
        copy_data = convert_to_copy_format(df, CSV_COLUMNS_SCHEMA)
        
        with postgres.get_connection() as conn:
            with conn.cursor() as cursor:
                copy_stream = io.StringIO(copy_data)
                try:
                    cursor.copy_from(
                        copy_stream,
                        POSTGRES_NPS_TABLE,
                        columns=CSV_COLUMNS_SCHEMA,
                        sep='\t',
                        null='\\N'
                    )
                    conn.commit()
                except Exception as e:
                    conn.rollback()
                    context_data_for_log = copy_data[:500] 
                    context.log.error(f"COPY command failed for {csv_file.name}. Data preview: {context_data_for_log}")
                    raise e
        
        return SimpleFileResult(str(csv_file), True, total_records)
        
    except Exception as e:
        return SimpleFileResult(str(csv_file), False, 0, str(e))

def process_single_file_with_semaphore(csv_file: Path, data_created_ym: str, postgres: PostgresResource, context: dg.AssetExecutionContext) -> SimpleFileResult:
    """Semaphore 기반 단일 파일 처리 (정확한 워커별 시간 측정)"""
    worker_id = threading.current_thread().name
    start_time = time.time()
    
    _worker_semaphore.acquire()
    try:
        context.log.info(f"🔷 [{worker_id}] 슬롯 획득 - {csv_file.name} 처리 시작")
        
        result = process_single_file(csv_file, data_created_ym, postgres, context)
        
        duration = time.time() - start_time
        
        update_worker_stats(worker_id, str(csv_file), result.records, duration, result.success)
        
        if result.success:
            context.log.info(f"✅ [{worker_id}] {csv_file.name}: {result.records:,}건 완료 ({duration:.2f}초, {result.records/duration:.0f}건/초)")
        else:
            context.log.error(f"❌ [{worker_id}] {csv_file.name}: 실패 - {result.error} ({duration:.2f}초)")
        
        return result
        
    finally:
        _worker_semaphore.release()
        context.log.info(f"🔶 [{worker_id}] 슬롯 해제 - {csv_file.name} 처리 완료")

def process_files_parallel(file_info: Dict[Path, str], postgres: PostgresResource, max_workers: int, context: dg.AssetExecutionContext) -> Dict:
    """병렬 파일 처리 (Semaphore 기반 워커 슬롯 제어 + Fast-Fail 적용)"""
    install_signal_handlers() # 시그널 핸들러는 print를 유지하거나 별도 로거 사용 고려
    
    file_info = dict(sorted(file_info.items(), key=lambda x: x[0].stat().st_size, reverse=True))

    init_worker_semaphore(max_workers) # Semaphore 초기화 메시지는 print 유지 또는 별도 로거
    
    results = []
    successful_files = 0
    failed_files = 0
    total_records = 0
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        register_executor(executor)
        
        try:
            future_to_file = {
                executor.submit(process_single_file_with_semaphore, csv_file, date_str, postgres, context): csv_file
                for csv_file, date_str in file_info.items()
            }

            for future in as_completed(future_to_file):
                if _shutdown_requested.is_set():
                    context.log.warning("🚨 종료 신호 감지 - 처리 중단")
                    break

                try:
                    csv_file = future_to_file[future]
                    result = future.result()
                    results.append(result)
                    
                    if result.success:
                        successful_files += 1
                        total_records += result.records
                    else:
                        failed_files += 1
                        context.log.error(f"❌ {csv_file.name}: 실패 - {result.error}")
                        
                        context.log.error(f"💥 Fast-Fail 발동: 첫 번째 파일 실패로 전체 처리 중단")
                        _shutdown_requested.set()
                        break
                        
                except Exception as e:
                    failed_files += 1
                    csv_file = future_to_file[future]
                    context.log.error(f"❌ {csv_file.name}: 예외 발생 - {str(e)}")
                    
                    context.log.error(f"💥 Fast-Fail 발동: 예외 발생으로 전체 처리 중단")
                    _shutdown_requested.set()
                    break
                    
        finally:
            unregister_executor(executor)
            
            # 워커별 통계는 nps_to_postgres_simple에서 context.log로 출력하므로 여기서는 중복 출력 안 함
            # 필요시 context.log.info로 추가 로깅 가능
    
    return {
        'successful_files': successful_files,
        'failed_files': failed_files,
        'total_records': total_records,
        'results': results,
        'fast_fail_triggered': failed_files > 0,
        'fast_fail_reason': f"파일 처리 실패: {failed_files}개 파일" if failed_files > 0 else None,
        'worker_stats': get_worker_stats()
    }

def scan_processed_files() -> Dict[Path, str]:
    """처리된 파일 스캔"""
    processed_files = {}
    
    if not PATHS.out_history_dir.exists():
        return processed_files
    
    for csv_file in PATHS.out_history_dir.glob("*.csv"):
        if csv_file.is_file() and csv_file.stat().st_size > 0:
            date_str = get_file_date(csv_file.name)
            if date_str:
                processed_files[csv_file] = date_str
    
    return processed_files

# 🚨 메인 Asset (간소화)
@dg.asset(
    group_name="NPS",
    description="NPS 데이터를 PostgreSQL에 간소화된 방식으로 적재",
    # deps=["nps_data_processing"], # 이전 단계가 있다면 주석 해제
    metadata={
        "table_name": dg.MetadataValue.text(POSTGRES_NPS_TABLE),
        "processing_method": dg.MetadataValue.text("Optimized Parallel Processing with df.to_csv"),
        "indexes_recreated": dg.MetadataValue.bool(False), # TODO: 데이터 적재 후 인덱스 재생성 로직 추가 필요
    },
    tags={"layer": "silver", "database": "postgres", "source": "nps"},
    kinds={"postgres"},
    deps=["nps_data_processing"]
)
def nps_to_postgres_simple(
    context: dg.AssetExecutionContext,
    nps_postgres: PostgresResource,
) -> dg.MaterializeResult:
    """간소화된 NPS PostgreSQL 적재"""
    
    max_workers = context.run_config.get("ops", {}).get("nps_to_postgres_simple", {}).get("config", {}).get("max_workers", DEFAULT_MAX_WORKERS)
    
    context.log.info(f"🚀 NPS PostgreSQL 간소화 적재 시작 (워커: {max_workers}개)")
    
    file_info = scan_processed_files()
    
    if not file_info:
        context.log.warning("⚠️ 처리할 파일이 없습니다")
        return dg.MaterializeResult(
            metadata={
                "processed_files": dg.MetadataValue.int(0),
                "total_records": dg.MetadataValue.int(0),
            }
        )
    
    context.log.info(f"📁 {len(file_info)}개 파일 발견")
    
    context.log.info("🚀 1단계: 인덱스 제거 (삽입 성능 최적화)")
    if not drop_indexes_and_constraints(nps_postgres, context): # drop_indexes_and_constraints는 이미 context를 사용
        context.log.warning("⚠️ 인덱스 제거 실패 - 계속 진행")
    
    try:
        context.log.info("🚀 2단계: 데이터 삽입 시작")
        result = process_files_parallel(file_info, nps_postgres, max_workers, context) # context 전달

        if result.get('fast_fail_triggered', False):
            fast_fail_reason = result.get('fast_fail_reason', '알 수 없는 이유')
            context.log.error(f"💥 Fast-Fail 발동: {fast_fail_reason}")
            context.log.error(f"🚨 성공: {result['successful_files']}개, 실패: {result['failed_files']}개")
            
            for file_result in result['results']:
                if not file_result.success:
                    context.log.error(f"❌ {Path(file_result.file_path).name}: {file_result.error}")
            
            raise Exception(f"Fast-Fail 발동: {fast_fail_reason}")
        
        if result['failed_files'] > 0:
            context.log.error(f"❌ {result['failed_files']}개 파일 처리 실패")
            context.log.error(f"성공: {result['successful_files']}개, 실패: {result['failed_files']}개")
            
            for file_result in result['results']:
                if not file_result.success:
                    context.log.error(f"❌ {Path(file_result.file_path).name}: {file_result.error}")
            
            raise Exception(f"파일 처리 실패: {result['failed_files']}개 파일에서 오류 발생")
        
        context.log.info(f"🏁 처리 완료: 성공 {result['successful_files']}개, 실패 {result['failed_files']}개")
        context.log.info(f"📊 총 {result['total_records']:,}건 적재")
        
        worker_stats = result.get('worker_stats', {})
        if worker_stats:
            context.log.info("📊 워커별 성능 통계:")
            for worker_id, stats in worker_stats.items():
                context.log.info(f"  🔸 {worker_id}: {stats['files_processed']}개 파일, "
                               f"{stats['total_records']:,}건, "
                               f"{stats['total_duration']:.2f}초, "
                               f"{stats['avg_records_per_second']:.0f}건/초")
        
        return dg.MaterializeResult(
            metadata={
                "processed_files": dg.MetadataValue.int(result['successful_files']),
                "failed_files": dg.MetadataValue.int(result['failed_files']),
                "total_records": dg.MetadataValue.int(result['total_records']),
                "max_workers": dg.MetadataValue.int(max_workers),
                "fast_fail_triggered": dg.MetadataValue.bool(result.get('fast_fail_triggered', False)),
                "worker_count": dg.MetadataValue.int(len(worker_stats)),
                "indexes_recreated": dg.MetadataValue.bool(False), # TODO: 데이터 적재 후 인덱스 재생성 로직 추가 필요
            }
        )
        
    except Exception as e:
        context.log.error(f"💥 PostgreSQL 적재 실패: {str(e)}")
        context.log.error(f"🚨 전체 파이프라인이 실패로 처리됩니다")
          
        # 명시적으로 예외를 재발생시켜 Dagster가 실패로 인식하도록 함
        raise

# Asset Check (간소화)
@dg.asset_check(
    asset="nps_to_postgres_simple",
    name="simple_postgres_check", 
    description="PostgreSQL 연결 및 데이터 확인",
    blocking=True
)
def check_postgres_simple(context: dg.AssetCheckExecutionContext, nps_postgres: PostgresResource):
    """간소화된 PostgreSQL 체크 (Fast-Fail 적용)"""
    try:
        context.log.info("🔍 PostgreSQL 연결 및 테이블 확인 중...")

        with nps_postgres.get_connection() as conn:
            with conn.cursor() as cursor:
                # 연결 테스트
                cursor.execute("SELECT 1")
                context.log.info("✅ PostgreSQL 연결 성공")
                
                # 테이블 존재 확인 및 레코드 수 카운트
                # 테이블 이름에 따옴표를 사용하여 예약어와 충돌 방지 및 대소문자 구분 (필요시)
                cursor.execute(f"""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_name = '{POSTGRES_NPS_TABLE}'
                    );
                """)
                table_exists = cursor.fetchone()[0]

                if not table_exists:
                    context.log.warning(f"⚠️ 테이블 '{POSTGRES_NPS_TABLE}'이(가) 존재하지 않습니다.")
                    return dg.AssetCheckResult(
                        passed=False,
                        description=f"테이블 '{POSTGRES_NPS_TABLE}'이(가) 존재하지 않습니다.",
                        metadata={
                            "table_name": dg.MetadataValue.text(POSTGRES_NPS_TABLE),
                            "error_message": dg.MetadataValue.text("Table not found")
                        }
                    )

                cursor.execute(f"SELECT COUNT(*) FROM \"{POSTGRES_NPS_TABLE}\"") # 테이블명에 따옴표 추가
                count = cursor.fetchone()[0]
                context.log.info(f"✅ 테이블 '{POSTGRES_NPS_TABLE}' 확인 완료: {count:,}건")
                
                return dg.AssetCheckResult(
                    passed=True,
                    metadata={
                        "total_records": dg.MetadataValue.int(count),
                        "table_name": dg.MetadataValue.text(POSTGRES_NPS_TABLE)
                    }
                )
                
    except Exception as e:
        context.log.error(f"💥 PostgreSQL 체크 실패: {str(e)}")
        context.log.error(f"🚨 연결 정보를 확인하고 데이터베이스 상태를 점검하세요")
        
        return dg.AssetCheckResult(
            passed=False,
            description=f"PostgreSQL 연결/테이블 확인 실패: {str(e)}",
            metadata={
                "error_message": dg.MetadataValue.text(str(e))
            }
        )
