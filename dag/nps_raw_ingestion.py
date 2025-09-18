"""
NPS Raw Data Ingestion - Bronze Tier
국민연금 원시 데이터 다운로드 및 수집
"""

from dataclasses import dataclass, field
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
import re
import threading
import concurrent.futures
import logging
from typing import Any
from contextlib import contextmanager
import time
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
from dagster import AssetExecutionContext, asset
from dagster_duckdb import DuckDBResource
import pandas as pd
import shutil
import dagster as dg
import chardet
import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor, as_completed
import traceback

# =============================================================================
# 1단계: 전역 상수 및 설정
# =============================================================================

@dataclass
class NPSConfig:
    """NPS 데이터 처리 관련 설정"""
    # 다운로드 설정
    max_concurrent_downloads: int = 3
    request_timeout: int = 10
    retry_attempts: int = 3
    retry_backoff_factor: float = 1.0
    
    # 파일 시스템 설정
    required_disk_space_gb: float = 10.0
    min_file_size_kb: int = 100
    batch_size: int = 1000
    
    # 날짜 범위 검증
    min_year: int = 2015
    max_year: int = 2025
    
    # 특수 케이스 및 메타데이터 설정
    skip_label: str = "..."

@dataclass
class NPSFilePaths:
    """NPS 파일 경로 관리"""
    base_dir: Path = Path("data") / "nps"
    
    @property
    def raw_history_dir(self) -> Path:
        return self.base_dir / "history" / "in"
    
    @property
    def processed_history_dir(self) -> Path:
        return self.base_dir / "history" / "out"

@dataclass
class NPSURLConfig:
    """NPS 관련 URL 설정"""
    data_gov_base: str = "https://www.data.go.kr"
    
    @property
    def main_url(self) -> str:
        return f"{self.data_gov_base}/data/15083277/fileData.do"
    
    @property
    def hist_url(self) -> str:
        return f"{self.data_gov_base}/tcs/dss/selectHistAndCsvData.do"
    
    @property
    def detail_url(self) -> str:
        return f"{self.data_gov_base}/tcs/dss/selectDpkDetailInfo.do"
    
    @property
    def download_json_url(self) -> str:
        return f"{self.data_gov_base}/tcs/dss/selectFileDataDownload.do"
    
    @property
    def file_download_url(self) -> str:
        return f"{self.data_gov_base}/cmm/cmm/fileDownload.do"

@dataclass
class NPSSpecialCases:
    """NPS 특수 케이스 설정"""
    special_case_label: str = "국민연금공단_국민연금 가입 사업장 내역_20200624"
    special_version_202007: str = "국민연금공단_국민연금 가입 사업장 내역_20200717"
    special_version_202006: str = "국민연금공단_국민연금 가입 사업장 내역_20200619"
    
    @property
    def special_cases_config(self) -> dict:
        return {
            self.special_case_label: {
                "reason": "2020년 6월 데이터 중복 업로드 문제",
                "versions": [
                    {
                        "pattern": "2020-07",
                        "filename": self.special_version_202007,
                        "description": "수정본 (최신)"
                    },
                    {
                        "pattern": "2020-06",
                        "filename": self.special_version_202006, 
                        "description": "초기본"
                    }
                ]
            }
        }
    
    @property
    def metadata_file_patterns(self) -> list[str]:
        return [
            "컬럼정보", "항목정보", "파일정보", "데이터정보", "스키마",
            "schema", "column", "metadata", "설명서", "가이드", "manual"
        ]

@dataclass
class NPSSchema:
    """NPS 데이터 스키마 정의"""
    original_columns: list[str] = field(default_factory=lambda: [
        "사업장명", "사업자등록번호", "우편번호", "사업장지번상세주소", "사업장도로명상세주소", 
        "고객법정동주소코드", "고객행정동주소코드", "사업장형태구분코드", "사업장업종코드", 
        "사업장업종코드명", "적용일자", "재등록일자", "탈퇴일자", "가입자수", "당월고지금액", 
        "신규취득자수", "상실가입자수"
    ])
    
    column_mapping: dict[str, str] = field(default_factory=lambda: {
        '자료생성년월': 'date_extracted',
        '사업장명': 'company_name',
        '사업자등록번호': 'business_reg_num',
        '우편번호': 'postal_code',
        '사업장지번상세주소': 'address_jibun',
        '사업장도로명상세주소': 'address_road',
        '고객법정동주소코드': 'legal_dong_code',
        '고객행정동주소코드': 'admin_dong_code',
        '사업장형태구분코드': 'company_type_code',
        '사업장업종코드': 'industry_code',
        '사업장업종코드명': 'industry_name',
        '적용일자': 'application_date',
        '재등록일자': 'reregistration_date',
        '탈퇴일자': 'withdrawal_date',
        '가입자수': 'subscriber_count',
        '당월고지금액': 'monthly_fee',
        '신규취득자수': 'new_members',
        '상실가입자수': 'lost_members'
    })
    
    numeric_columns: list[str] = field(default_factory=lambda: [
        '가입자수', '당월고지금액', '신규취득자수', '상실가입자수'
    ])
    
    string_columns: list[str] = field(default_factory=lambda: [
        '사업장명', '사업장지번상세주소', '사업장도로명상세주소', '사업장업종코드명'
    ])
    
    @property
    def processing_dtypes(self) -> dict[str, Any]:
        return {
            "사업자등록번호": "Int64",
            "가입자수": "Int64",
            "당월고지금액": "Int64",
            "신규취득자수": "Int64",
            "상실가입자수": "Int64",
            "적용일자": str,
            "재등록일자": str,
            "탈퇴일자": str,
            "자료생성년월": str,
        }

@dataclass
class NPSDatabase:
    """NPS 데이터베이스 관련 설정"""
    metadata_table: str = "nps_file_metadata"
    processed_table: str = "nps_processed_data"
    
    @property
    def postgres_column_order(self) -> list[str]:
        return [
            "date", "company_id", "company_name", "business_reg_num", "postal_code", 
            "address_jibun", "address_road", "legal_dong_code", "admin_dong_code", 
            "company_type_code", "industry_code", "industry_name", "application_date", 
            "reregistration_date", "withdrawal_date", "subscriber_count", "monthly_fee", 
            "new_members", "lost_members", "avg_fee"
        ]

# 전역 설정 인스턴스들
CONFIG = NPSConfig()
PATHS = NPSFilePaths()
URLS = NPSURLConfig()
SPECIAL_CASES = NPSSpecialCases()
SCHEMA = NPSSchema()
DATABASE = NPSDatabase()

# 정규식 패턴들
YYYYMMDD_PATTERN = re.compile(r"(\d{4})(\d{2})(\d{2})")
MMDDYYYY_PATTERN = re.compile(r"(\d{1,2})[/_-](\d{1,2})[/_-](\d{4})")  
YYYYMM_PATTERN = re.compile(r"(\d{4})\D*(\d{1,2})\D")  
KOREAN_DATE_PATTERN = re.compile(r"(\d{4})년\s*(\d{1,2})월")
UNDERLINE_DATE_PATTERN = re.compile(r"(\d{1,2})_(\d{1,2})_(\d{4})")

# =============================================================================
# 2단계: 다운로드를 위한 헬퍼 함수들
# =============================================================================

def _is_metadata_file(filename: str) -> bool:
    """메타데이터/설명서 파일인지 판단"""
    filename_lower = filename.lower()
    return any(pattern in filename_lower for pattern in SPECIAL_CASES.metadata_file_patterns)

def _sanitize_filename(filename: str) -> str:
    """파일명에서 안전하지 않은 문자들을 제거하거나 대체"""
    filename = filename.replace("/", "_")
    filename = filename.replace("\\", "_")
    filename = re.sub(r'[<>:"|?*]', "", filename)
    filename = re.sub(r'\s+', " ", filename)
    filename = filename.strip()
    return filename

def _check_disk_space(path: Path, required_gb: float = None) -> bool:
    """디스크 공간 체크"""
    if required_gb is None:
        required_gb = CONFIG.required_disk_space_gb
        
    try:
        total, used, free = shutil.disk_usage(path)
        free_gb = free / (1024**3)  # GB 변환
        
        logging.getLogger(__name__).info(
            f"디스크 공간 체크: {free_gb:.2f}GB 여유 공간 (필요: {required_gb}GB)"
        )
        
        return free_gb >= required_gb
    except Exception as e:
        logging.getLogger(__name__).warning(f"디스크 공간 체크 실패: {e}")
        return True

def _get_expected_file_size(
    session: requests.Session,
    file_info: dict[str, str],
    timeout: int = None
) -> int | None:
    """파일의 예상 크기를 HEAD 요청으로 확인"""
    if timeout is None:
        timeout = CONFIG.request_timeout
        
    try:
        # Step 1: 상세 페이지에서 파라미터 추출
        detail_response = session.get(URLS.detail_url, params={
            "publicDataDetailPk": file_info['detail_pk'],
            "publicDataHistSn": file_info['hist_sn']
        }, timeout=timeout)
        detail_response.raise_for_status()
        
        detail_soup = BeautifulSoup(detail_response.text, "html.parser")
        download_btn = detail_soup.find("a", onclick=re.compile(r"fn_fileDataDown"))
        
        if not download_btn:
            return None
        
        onclick_params = re.findall(r"'([^']+)'", download_btn["onclick"])
        if len(onclick_params) < 5:
            return None
        
        pub_pk, detail_pk_2, atch_id, sn, ext = onclick_params[:5]
        
        # Step 2: JSON API 호출
        json_response = session.get(URLS.download_json_url, params={
            "publicDataPk": pub_pk,
            "publicDataDetailPk": detail_pk_2,
            "fileExtsn": ext
        }, timeout=timeout)
        json_response.raise_for_status()
        
        json_data = json_response.json()
        if "atchFileId" not in json_data:
            return None
        
        # Step 3: HEAD 요청으로 파일 크기 확인
        head_response = session.head(URLS.file_download_url, params={
            "atchFileId": json_data["atchFileId"]
        }, timeout=timeout)
        head_response.raise_for_status()
        
        # Content-Length 헤더에서 파일 크기 추출
        content_length = head_response.headers.get('Content-Length')
        if content_length:
            return int(content_length)
        
        return None
        
    except Exception as e:
        logging.getLogger(__name__).debug(f"파일 크기 확인 실패: {e}")
        return None

# =============================================================================
# 3단계: 다운로드 후보 추출 함수 (수정됨 - nps_download.py에서 가져옴)
# =============================================================================

def _extract_download_candidates(
    session: requests.Session, 
    main_pk: str, 
    detail_pk: str
) -> list[dict[str, str]]:
    """NPS 히스토리 페이지에서 다운로드 후보들 추출 (nps_download.py 작동 버전 사용)"""
    logger = logging.getLogger(__name__)
    
    try:
        # 히스토리 URL 사용 (nps_download.py와 동일)
        response = session.get(URLS.hist_url, params={
            "publicDataPk": main_pk,
            "publicDataDetailPk": detail_pk
        }, timeout=CONFIG.request_timeout)
        logger.info(f"히스토리 페이지 요청: {URLS.hist_url}")
        logger.info(f"응답 상태 코드: {response.status_code}")
        response.raise_for_status()
        
        # HTML 파싱 및 히스토리 링크 찾기 (nps_download.py와 동일한 방식)
        soup = BeautifulSoup(response.text, "html.parser")
        history_links = soup.select("div.data-history a[onclick*='fn_fileDataDetail']")
        
        logger.info(f"📋 전체 히스토리 링크 발견: {len(history_links)}개")
        
        if not history_links:
            logger.error("히스토리 링크를 찾을 수 없습니다")
            
            # 디버깅을 위해 페이지 내용의 일부를 로깅
            logger.error(f"페이지 내용 미리보기 (처음 1000자): {response.text[:1000]}")
            
            # 전체 페이지에서 'data-history' 텍스트 검색
            if 'data-history' in response.text:
                logger.error("페이지에 'data-history' 텍스트가 존재합니다")
            else:
                logger.error("페이지에 'data-history' 텍스트가 없습니다")
            
            # div.data-history 요소 찾기 시도
            data_history_div = soup.find("div", class_="data-history")
            if data_history_div:
                logger.error(f"data-history div 발견, 하위 링크 수: {len(data_history_div.find_all('a'))}")
                logger.error(f"data-history div 내용: {str(data_history_div)[:500]}")
            else:
                logger.error("data-history div를 찾을 수 없습니다")
                
                # 다른 history 관련 클래스 찾기
                history_divs = soup.find_all("div", class_=lambda x: x and "history" in x.lower())
                if history_divs:
                    logger.error(f"다른 history 관련 div들: {[div.get('class') for div in history_divs]}")
                else:
                    logger.error("history 관련 div가 없습니다")
            
            return []
        
        candidates = []
        processed_links = set()
        metadata_files_skipped = []
        unprocessed_links = []
        
        # 🔥 특수 케이스 설정 (동적 처리 가능)
        special_cases = {
            SPECIAL_CASES.special_case_label: {
                "versions": [
                    {
                        "pattern": "2020-07",
                        "filename": SPECIAL_CASES.special_version_202007,
                        "processed": False,
                        "description": "2020년 7월 수정본"
                    },
                    {
                        "pattern": "2020-06", 
                        "filename": SPECIAL_CASES.special_version_202006,
                        "processed": False,
                        "description": "2020년 6월 초기본"
                    }
                ]
            }
        }
        
        # 🔍 모든 링크 상세 분석
        for i, link in enumerate(history_links, 1):
            text = link.text.strip()
            onclick_attr = link.get("onclick", "")
            parent_text = link.parent.get_text(strip=True) if link.parent else ""
            
            logger.debug(f"📎 링크 {i}: '{text}' (부모: '{parent_text}') [onclick: {onclick_attr[:50]}...]")
            
            # 빈 텍스트나 건너뛸 라벨 체크
            if not text or text == CONFIG.skip_label:
                logger.debug(f"⏭️  빈 텍스트 또는 건너뛸 라벨: '{text}'")
                continue
                
            # 메타데이터 파일 필터링
            if _is_metadata_file(text):
                metadata_files_skipped.append({
                    "text": text,
                    "parent_text": parent_text,
                    "link_index": i
                })
                logger.info(f"📄 메타데이터 파일 건너뜀: '{text}'")
                continue
                
            # onclick 파라미터 추출
            onclick_matches = re.findall(r"'([^']+)'", onclick_attr)
            if len(onclick_matches) < 2:
                logger.warning(f"⚠️  onclick 파라미터 부족 ({len(onclick_matches)}개): '{text}' - {onclick_attr}")
                unprocessed_links.append({
                    "text": text,
                    "reason": f"onclick 파라미터 부족 ({len(onclick_matches)}개)",
                    "onclick": onclick_attr
                })
                continue
                
            detail_pk_candidate = onclick_matches[0]
            hist_sn_candidate = onclick_matches[1]
            
            # 🎯 특수 케이스 처리 (강화된 로직)
            special_case_processed = False
            
            if text in special_cases:
                logger.info(f"🔍 특수 케이스 후보 발견: '{text}'")
                logger.info(f"   📝 전체 텍스트: '{parent_text}'")
                
                special_case = special_cases[text]
                
                # 각 버전별로 패턴 매칭 시도
                for version in special_case["versions"]:
                    if version["processed"]:
                        continue
                    
                    pattern = version["pattern"]
                    filename = version["filename"]
                    
                    # 다양한 패턴 매칭 시도
                    pattern_found = False
                    
                    # 1. 부모 텍스트에서 패턴 찾기
                    if pattern in parent_text:
                        pattern_found = True
                        logger.info(f"   ✅ 부모 텍스트에서 패턴 '{pattern}' 발견")
                    
                    # 2. 링크 텍스트에서 패턴 찾기
                    elif pattern in text:
                        pattern_found = True
                        logger.info(f"   ✅ 링크 텍스트에서 패턴 '{pattern}' 발견")
                    
                    # 3. 날짜 형식 변환 후 찾기 (2020-07 → 202007, 20200701 등)
                    elif any(date_variant in parent_text or date_variant in text 
                            for date_variant in [
                                pattern.replace("-", ""),  # 2020-07 → 202007
                                pattern.replace("-", "") + "01",  # 2020-07 → 20200701
                                pattern.replace("-", "/"),  # 2020-07 → 2020/07
                            ]):
                        pattern_found = True
                        logger.info(f"   ✅ 날짜 변형에서 패턴 '{pattern}' 발견")
                    
                    if pattern_found:
                        candidates.append({
                            "filename": filename,
                            "detail_pk": detail_pk_candidate,
                            "hist_sn": hist_sn_candidate,
                            "is_special_case": True
                        })
                        
                        version["processed"] = True
                        special_case_processed = True
                        
                        logger.info(f"   🎯 특수 케이스 추가 완료: {filename} (패턴: {pattern})")
                        break
                
                # 특수 케이스 라벨이지만 패턴이 매칭되지 않은 경우
                if not special_case_processed:
                    logger.warning(f"⚠️  특수 케이스 라벨 '{text}'이지만 패턴 매칭 실패")
                    logger.warning(f"   📝 확인된 텍스트: '{parent_text}'")
                    unprocessed_links.append({
                        "text": text,
                        "reason": "특수 케이스 패턴 매칭 실패",
                        "parent_text": parent_text
                    })
            
            # 일반 파일 처리 (특수 케이스가 아닌 경우)
            if not special_case_processed:
                if text in processed_links:
                    logger.debug(f"🔄 중복 파일명 건너뜀: '{text}'")
                    continue
                    
                candidates.append({
                    "filename": text,
                    "detail_pk": detail_pk_candidate,
                    "hist_sn": hist_sn_candidate,
                    "is_special_case": False
                })
                processed_links.add(text)
                logger.debug(f"✅ 일반 파일 추가: '{text}'")
        
        # 처리 결과 로깅
        logger.info(f"총 {len(candidates)}개 다운로드 후보 발견")
        logger.info(f"메타데이터 제외: {len(metadata_files_skipped)}개")
        logger.info(f"처리 실패: {len(unprocessed_links)}개")
        
        return candidates
        
    except Exception as e:
        logger.error(f"다운로드 후보 추출 실패: {e}")
        logger.error(f"에러 타입: {type(e).__name__}")
        logger.error(f"에러 상세: {str(e)}")
        import traceback
        logger.error(f"스택 트레이스: {traceback.format_exc()}")
        return []

# =============================================================================
# 4단계: 단일 파일 다운로드 함수
# =============================================================================

def _download_single_nps_file(
    session: requests.Session,
    file_info: dict[str, str],
    thread_id: int,
    stop_event: threading.Event = None
) -> dict[str, Any]:
    """단일 파일 다운로드"""
    if stop_event and stop_event.is_set():
        return {"success": False, "error": "중단됨"}
    
    filename = file_info["filename"]
    detail_pk = file_info["detail_pk"]
    hist_sn = file_info["hist_sn"]
    is_special_case = file_info.get("is_special_case", False)
    
    try:
        # Step 1: 상세 페이지에서 다운로드 파라미터 추출
        detail_response = session.get(URLS.detail_url, params={
            "publicDataDetailPk": detail_pk,
            "publicDataHistSn": hist_sn
        }, timeout=CONFIG.request_timeout)
        detail_response.raise_for_status()
        
        detail_soup = BeautifulSoup(detail_response.text, "html.parser")
        download_btn = detail_soup.find("a", onclick=re.compile(r"fn_fileDataDown"))
        
        if not download_btn:
            raise RuntimeError("다운로드 버튼을 찾을 수 없습니다")
        
        onclick_params = re.findall(r"'([^']+)'", download_btn["onclick"])
        if len(onclick_params) < 5:
            raise RuntimeError("다운로드 파라미터가 부족합니다")
        
        pub_pk, detail_pk_2, atch_id, sn, ext = onclick_params[:5]
        
        # Step 2: JSON API로 실제 다운로드 URL 획득
        json_response = session.get(URLS.download_json_url, params={
            "publicDataPk": pub_pk,
            "publicDataDetailPk": detail_pk_2,
            "fileExtsn": ext
        }, timeout=CONFIG.request_timeout)
        json_response.raise_for_status()
        
        json_data = json_response.json()
        if "atchFileId" not in json_data:
            raise RuntimeError("첨부파일 ID를 찾을 수 없습니다")
        
        # Step 3: 실제 파일 다운로드
        download_response = session.get(URLS.file_download_url, params={
            "atchFileId": json_data["atchFileId"]
        }, timeout=CONFIG.request_timeout, stream=True)
        download_response.raise_for_status()
        
        # 파일 저장
        sanitized_filename = _sanitize_filename(filename)
        if not sanitized_filename.endswith('.csv'):
            sanitized_filename += '.csv'
        
        file_path = PATHS.raw_history_dir / sanitized_filename
        
        # 파일 쓰기
        with open(file_path, 'wb') as f:
            for chunk in download_response.iter_content(chunk_size=8192):
                if stop_event and stop_event.is_set():
                    f.close()
                    file_path.unlink(missing_ok=True)  # 부분 파일 삭제
                    return {"success": False, "error": "중단됨"}
                f.write(chunk)
        
        # 파일 크기 확인
        file_size = file_path.stat().st_size
        if file_size < CONFIG.min_file_size_kb * 1024:
            file_path.unlink(missing_ok=True)
            raise RuntimeError(f"다운로드된 파일이 너무 작습니다: {file_size}B")
        
        logging.getLogger(__name__).info(
            f"[Thread-{thread_id}] 다운로드 완료: {filename} → {sanitized_filename} ({file_size:,}B)"
        )
        
        return {
            "success": True,
            "filename": filename,
            "sanitized_filename": sanitized_filename,
            "path": str(file_path),
            "file_size": file_size,
            "thread_id": thread_id,
            "is_special_case": is_special_case,
            "integrity_check": "size_verified"
        }
        
    except Exception as e:
        error_msg = f"[Thread-{thread_id}] 다운로드 실패: {filename} - {e}"
        
        if is_special_case:
            logging.getLogger(__name__).warning(f"{error_msg} (특수 케이스)")
        else:
            logging.getLogger(__name__).error(error_msg)
        
        # Fail-Fast: 모든 예외를 즉시 재발생
        raise RuntimeError(error_msg) from e

@contextmanager
def _graceful_executor(max_workers: int):
    """ThreadPoolExecutor를 안전하게 관리하는 컨텍스트 매니저"""
    executor = ThreadPoolExecutor(max_workers=max_workers)
    try:
        yield executor
    except KeyboardInterrupt:
        logging.getLogger(__name__).warning("키보드 중단 감지 - 모든 작업 종료 중...")
        executor.shutdown(wait=False, cancel_futures=True)
        raise
    except Exception:
        executor.shutdown(wait=False, cancel_futures=True)
        raise
    finally:
        executor.shutdown(wait=True)

def _get_existing_files_from_db(conn) -> set[str]:
    """DuckDB에서 기존 처리된 파일들의 목록을 조회"""
    try:
        tables_query = f"""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_name = '{DATABASE.metadata_table}'
        """
        result = conn.execute(tables_query).fetchone()
        
        if not result:
            return set()
        
        existing_files_query = f"""
        SELECT DISTINCT original_filename, sanitized_filename
        FROM {DATABASE.metadata_table}
        WHERE success = true
        """
        
        result = conn.execute(existing_files_query).fetchall()
        
        existing_files = set()
        for row in result:
            existing_files.add(row[0])  # original_filename
            existing_files.add(row[1])  # sanitized_filename
        
        return existing_files
        
    except Exception as e:
        logging.getLogger(__name__).warning(f"기존 파일 조회 실패: {e}")
        return set()

def _get_existing_files_from_filesystem() -> set[str]:
    """파일 시스템에서 기존 파일들의 목록을 조회"""
    logger = logging.getLogger(__name__)
    existing_files = set()
    
    try:
        # raw_history_dir에 있는 모든 CSV 파일들 확인
        if PATHS.raw_history_dir.exists():
            for file_path in PATHS.raw_history_dir.glob("*.csv"):
                if file_path.is_file() and file_path.stat().st_size > CONFIG.min_file_size_kb * 1024:
                    # 파일명에서 확장자 제거한 버전과 원본 파일명 모두 추가
                    existing_files.add(file_path.name)  # 실제 파일명 (sanitized)
                    existing_files.add(file_path.stem)  # 확장자 제거한 파일명
                    
                    # 날짜 접두사가 있는 경우 제거한 버전도 추가
                    stem = file_path.stem
                    if '_' in stem:
                        # 예: "20200619_국민연금공단_국민연금 가입 사업장 내역_20200717" 
                        # → "국민연금공단_국민연금 가입 사업장 내역_20200717"
                        parts = stem.split('_', 1)
                        if len(parts) > 1 and parts[0].isdigit():
                            existing_files.add(parts[1])
        
        # processed_history_dir에 있는 파일들도 확인
        if PATHS.processed_history_dir.exists():
            for file_path in PATHS.processed_history_dir.glob("*.csv"):
                if file_path.is_file():
                    existing_files.add(file_path.name)
                    existing_files.add(file_path.stem)
                    
                    # 날짜 접두사 제거 버전
                    stem = file_path.stem
                    if '_' in stem:
                        parts = stem.split('_', 1)
                        if len(parts) > 1 and parts[0].isdigit():
                            existing_files.add(parts[1])
        
        logger.info(f"파일 시스템에서 {len(existing_files)}개 기존 파일명 패턴 발견")
        return existing_files
        
    except Exception as e:
        logger.warning(f"파일 시스템 조회 실패: {e}")
        return set()

def _get_combined_existing_files(conn) -> tuple[set[str], list[dict[str, Any]]]:
    """파일시스템과 DuckDB에서 기존 파일들의 목록을 조회 (무결성 검증 포함)"""
    logger = logging.getLogger(__name__)
    
    # 1. 파일시스템에서 기존 파일들 조회 및 무결성 검증
    filesystem_files = set()
    corrupted_files = []
    
    if PATHS.raw_history_dir.exists():
        logger.info(f"파일시스템 검사 중: {PATHS.raw_history_dir}")
        csv_files = list(PATHS.raw_history_dir.glob("*.csv"))
        logger.info(f"발견된 CSV 파일 수: {len(csv_files)}개")
        
        for file_path in csv_files:
            validation_result = _validate_file_integrity(file_path)
            
            if validation_result["valid"]:
                # 다양한 파일명 패턴 추가 (중복 체크 개선)
                filename = file_path.name
                stem = file_path.stem
                
                filesystem_files.add(filename)  # 전체 파일명
                filesystem_files.add(stem)      # 확장자 제거
                
                # 날짜 접두사 제거 패턴들
                if '_' in stem:
                    parts = stem.split('_', 1)
                    if len(parts) > 1 and parts[0].isdigit():
                        filesystem_files.add(parts[1])  # 날짜 접두사 제거
                        filesystem_files.add(parts[1] + '.csv')  # 날짜 접두사 제거 + 확장자
                
                # 특수 문자 제거/치환된 버전도 추가
                sanitized = _sanitize_filename(stem)
                filesystem_files.add(sanitized)
                filesystem_files.add(sanitized + '.csv')
                
                logger.debug(f"유효한 파일 등록: {filename}")
            else:
                corrupted_files.append({
                    "filename": file_path.name,
                    "path": str(file_path),
                    "validation_result": validation_result
                })
                logger.warning(
                    f"손상된 파일 발견: {file_path.name} - {validation_result.get('reason', 'unknown')}"
                )
    
    # 2. DuckDB에서 기존 파일들 조회
    db_files = set()
    try:
        tables_query = f"""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_name = '{DATABASE.metadata_table}'
        """
        result = conn.execute(tables_query).fetchone()
        
        if result:
            existing_files_query = f"""
            SELECT DISTINCT original_filename, sanitized_filename
            FROM {DATABASE.metadata_table}
            WHERE success = true
            """
            
            result = conn.execute(existing_files_query).fetchall()
            
            for row in result:
                original_filename = row[0]
                sanitized_filename = row[1]
                
                # 다양한 패턴 추가
                db_files.add(original_filename)
                db_files.add(sanitized_filename)
                
                # 확장자 관련 패턴들
                if original_filename.endswith('.csv'):
                    db_files.add(original_filename[:-4])  # 확장자 제거
                if not sanitized_filename.endswith('.csv'):
                    db_files.add(sanitized_filename + '.csv')  # 확장자 추가
            
            logger.info(f"DB에서 {len(result)}개 성공 레코드 발견")
    except Exception as e:
        logger.warning(f"DB 조회 실패: {e}")
    
    # 3. 통합 결과 반환 (손상된 파일은 제외)
    valid_files = filesystem_files.union(db_files)
    
    logger.info(
        f"파일 무결성 검증 완료: 파일시스템 {len(filesystem_files)}개 패턴, "
        f"DB {len(db_files)}개 패턴, 손상된 파일 {len(corrupted_files)}개"
    )
    logger.info(f"통합된 유효 파일 패턴: {len(valid_files)}개")
    
    return valid_files, corrupted_files

def _update_metadata_batch(conn, successful_downloads: list[dict[str, Any]]) -> None:
    """성공한 다운로드들의 메타데이터를 DuckDB에 배치 저장"""
    if not successful_downloads:
        return
    
    try:
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {DATABASE.metadata_table} (
            id BIGINT,
            original_filename VARCHAR,
            sanitized_filename VARCHAR,
            file_path VARCHAR,
            success BOOLEAN,
            download_timestamp TIMESTAMP,
            file_size_bytes BIGINT,
            thread_id INTEGER,
            is_special_case BOOLEAN,
            special_version VARCHAR,
            integrity_check VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (id)
        )
        """
        conn.execute(create_table_sql)
        
        max_id_result = conn.execute(f"SELECT COALESCE(MAX(id), 0) FROM {DATABASE.metadata_table}").fetchone()
        next_id = max_id_result[0] + 1 if max_id_result else 1
        
        insert_data = []
        current_timestamp = datetime.now(timezone.utc)
        
        for i, download in enumerate(successful_downloads):
            insert_data.append((
                next_id + i,
                download["filename"],
                download["sanitized_filename"],
                download["path"],
                True,
                current_timestamp,
                download["file_size"],
                download["thread_id"],
                download.get("is_special_case", False),
                "",  # special_version은 추후 확장 가능
                download.get("integrity_check", "")
            ))
        
        insert_sql = f"""
        INSERT INTO {DATABASE.metadata_table} (
            id, original_filename, sanitized_filename, file_path, success,
            download_timestamp, file_size_bytes, thread_id, is_special_case,
            special_version, integrity_check, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        conn.executemany(insert_sql, insert_data)
        
        logging.getLogger(__name__).info(
            f"메타데이터 배치 업데이트 완료: {len(insert_data)}개 레코드"
        )
        
    except Exception as e:
        logging.getLogger(__name__).error(f"메타데이터 업데이트 실패: {e}")

def _validate_file_integrity(file_path: Path) -> dict[str, Any]:
    """파일 무결성 검증"""
    try:
        if not file_path.exists():
            return {"valid": False, "reason": "file_not_found"}
        
        file_size = file_path.stat().st_size
        
        # 1. 최소 크기 검증
        if file_size < CONFIG.min_file_size_kb * 1024:
            return {
                "valid": False, 
                "reason": "file_too_small", 
                "size_bytes": file_size,
                "min_size_bytes": CONFIG.min_file_size_kb * 1024
            }
        
        # 2. CSV 헤더 검증 (첫 몇 줄만 읽어서 확인)
        try:
            with open(file_path, 'r', encoding='utf-8-sig', errors='replace') as f:
                first_line = f.readline().strip()
                if not first_line:
                    return {"valid": False, "reason": "empty_file"}
                
                # CSV 헤더가 있는지 간단히 확인
                if ',' not in first_line and '\t' not in first_line:
                    return {"valid": False, "reason": "invalid_csv_format"}
        
        except Exception as read_error:
            return {"valid": False, "reason": "read_error", "error": str(read_error)}
        
        # 3. 파일 확장자 검증
        if not file_path.name.lower().endswith('.csv'):
            return {"valid": False, "reason": "invalid_extension"}
        
        return {
            "valid": True, 
            "size_bytes": file_size,
            "size_mb": round(file_size / (1024 * 1024), 2)
        }
        
    except Exception as e:
        return {"valid": False, "reason": "validation_error", "error": str(e)}

# =============================================================================
# NPS 파일 다운로드 Asset
# =============================================================================

@asset(
    description="NPS 히스토리 데이터를 병렬 다운로드 - Bronze Tier",
    group_name="NPS",
    kinds={"csv"},
    tags={
        "domain": "finance", 
        "data_tier": "bronze",
        "source": "national_pension"
    },
    metadata={"source": "data.go.kr"}
)
def nps_raw_ingestion(
    context: AssetExecutionContext, 
    nps_duckdb: DuckDBResource
) -> dg.MaterializeResult:
    """NPS 히스토리 데이터를 다운로드 - Fail-Fast 정책"""
    context.log.info("NPS 히스토리 데이터 다운로드 시작 (Fail-Fast 정책)")
    
    PATHS.raw_history_dir.mkdir(parents=True, exist_ok=True)
    
    # 디스크 공간 사전 체크
    if not _check_disk_space(PATHS.raw_history_dir, CONFIG.required_disk_space_gb):
        raise RuntimeError(f"디스크 공간 부족: 최소 {CONFIG.required_disk_space_gb}GB 필요")
    
    max_workers = CONFIG.max_concurrent_downloads
    stop_event = threading.Event()
    successful_downloads = []
    
    # 세션 초기화
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    })
    
    try:
        # Step 1: 메인 페이지에서 PK 추출
        context.log.info("메인 페이지에서 PK 추출")
        main_response = session.get(URLS.main_url, timeout=CONFIG.request_timeout)
        main_response.raise_for_status()
        
        main_soup = BeautifulSoup(main_response.text, "html.parser")
        main_pk = main_soup.find(id="publicDataPk")["value"]
        detail_pk = main_soup.find(id="publicDataDetailPk")["value"]
        
        context.log.info(f"PKs 추출 완료 - Main: {main_pk}, Detail: {detail_pk}")
        
        # Step 2: 히스토리 다운로드 후보들 추출
        context.log.info("히스토리 페이지에서 다운로드 후보들 추출")
        download_candidates = _extract_download_candidates(session, main_pk, detail_pk)
        
        # 상세 분석을 위한 로깅
        total_candidates = len(download_candidates)
        special_candidates = [c for c in download_candidates if c.get('is_special_case', False)]
        general_candidates = [c for c in download_candidates if not c.get('is_special_case', False)]

        context.log.info(f"📊 후보 분석: 총 {total_candidates}개")
        context.log.info(f"   📁 일반 파일: {len(general_candidates)}개")
        context.log.info(f"   🎯 특수 케이스: {len(special_candidates)}개")

        if special_candidates:
            context.log.info(f"특수 케이스 파일들: {[c['filename'] for c in special_candidates]}")
        else:
            context.log.info("특수 케이스 파일 없음")

        # 예상 개수 검증
        expected_count = 113  # 일반 111 + 특수 2
        if total_candidates != expected_count:
            context.log.warning(f"예상과 다른 파일 개수: {total_candidates} (예상: {expected_count})")
        else:
            context.log.info(f"예상한 파일 개수와 일치: {total_candidates}개")
        
        if not download_candidates:
            raise RuntimeError("다운로드할 파일이 없습니다")
        
        context.log.info(f"{len(download_candidates)}개 파일 발견")
        
        # Step 3: 기존 파일들 조회 (무결성 검증 포함)
        context.log.info("🔍 기존 파일 및 무결성 검증 시작")
        with nps_duckdb.get_connection() as conn:
            existing_files, corrupted_files = _get_combined_existing_files(conn)
            
            context.log.info(f"📊 파일 상태 체크 완료:")
            context.log.info(f"   ✅ 유효한 기존 파일: {len(existing_files)}개")
            context.log.info(f"   ⚠️ 손상된 파일: {len(corrupted_files)}개")
            context.log.info(f"   📁 총 후보 파일: {len(download_candidates)}개")
            
            # 기존 파일 목록 로깅 (처음 10개만, 중복 체크 디버깅을 위해)
            if existing_files:
                sample_existing = list(existing_files)[:10]
                context.log.info(f"🔍 기존 파일 샘플 (중복 체크용):")
                for i, file in enumerate(sample_existing, 1):
                    context.log.info(f"   {i}. {file}")
                if len(existing_files) > 10:
                    context.log.info(f"   ... 외 {len(existing_files) - 10}개 더")
            
            # 다운로드 후보 목록 로깅 (중복 체크 디버깅을 위해)
            context.log.info(f"📋 다운로드 후보 파일 샘플:")
            for i, candidate in enumerate(download_candidates[:5], 1):
                context.log.info(f"   {i}. {candidate['filename']}")
            if len(download_candidates) > 5:
                context.log.info(f"   ... 외 {len(download_candidates) - 5}개 더")
            
            # 손상된 파일들 삭제 및 재다운로드 대상에 추가
            corrupted_filenames = []
            if corrupted_files:
                context.log.warning(f"🔧 {len(corrupted_files)}개 손상된 파일 처리 중")
                for corrupted in corrupted_files:
                    try:
                        corrupted_path = Path(corrupted["path"])
                        if corrupted_path.exists():
                            corrupted_path.unlink()
                            context.log.warning(f"   🗑️ 손상된 파일 삭제: {corrupted['filename']}")
                        corrupted_filenames.append(corrupted["filename"])
                    except Exception as e:
                        context.log.error(f"   ❌ 손상된 파일 삭제 실패: {corrupted['filename']} - {e}")
            
            # Step 4: 다운로드 대상 필터링 (신규 + 손상된 파일들)
            context.log.info("🎯 다운로드 대상 필터링 중")
            
            # 개선된 중복 체크 로직
            filtered_candidates = []
            skipped_files = []
            redownload_files = []
            
            for candidate in download_candidates:
                filename = candidate["filename"]
                sanitized_filename = _sanitize_filename(filename)
                if not sanitized_filename.endswith('.csv'):
                    sanitized_filename += '.csv'
                
                # 다양한 파일명 패턴으로 중복 체크
                file_exists = any([
                    filename in existing_files,                          # 원본 파일명
                    sanitized_filename in existing_files,                # 정제된 파일명
                    filename + '.csv' in existing_files,                 # 확장자 추가
                    filename.replace('.csv', '') in existing_files,      # 확장자 제거
                    sanitized_filename.replace('.csv', '') in existing_files  # 정제된 파일명 확장자 제거
                ])
                
                # 재다운로드 체크 (손상된 파일인지)
                is_corrupted = any([
                    filename in corrupted_filenames,
                    sanitized_filename in corrupted_filenames
                ])
                
                if is_corrupted:
                    # 손상된 파일은 재다운로드
                    filtered_candidates.append(candidate)
                    redownload_files.append(filename)
                    context.log.info(f"🔄 재다운로드 대상: {filename} (손상된 파일)")
                elif not file_exists:
                    # 존재하지 않는 새 파일은 다운로드
                    filtered_candidates.append(candidate)
                    context.log.debug(f"📥 신규 다운로드: {filename}")
                else:
                    # 이미 존재하는 파일은 건너뛰기
                    skipped_files.append(filename)
                    context.log.debug(f"⏭️ 건너뛰기: {filename} (이미 존재)")
            
            # 필터링 결과 상세 로깅
            context.log.info(f"📋 필터링 결과:")
            context.log.info(f"   📥 다운로드 대상: {len(filtered_candidates)}개")
            context.log.info(f"   ⏭️ 건너뛸 파일: {len(skipped_files)}개 (이미 존재)")
            context.log.info(f"   🔄 재다운로드: {len(redownload_files)}개 (손상된 파일)")
            
            context.log.info(
                f"필터링 결과: {len(download_candidates)}개 → {len(filtered_candidates)}개 "
                f"(중복 제외: {len(download_candidates) - len(filtered_candidates)}개, "
                f"손상 재다운: {len(corrupted_filenames)}개)"
            )
            
            if not filtered_candidates:
                if corrupted_files:
                    context.log.info(
                        f"✅ 모든 파일이 이미 다운로드되었으나 {len(corrupted_files)}개 손상된 파일이 재다운로드되었습니다"
                    )
                else:
                    context.log.info("✅ 모든 파일이 이미 다운로드되었습니다")
                
                return dg.MaterializeResult(
                    metadata={
                        "status": "All files already downloaded",
                        "total_candidates": dg.MetadataValue.int(len(download_candidates)),
                        "existing_files_count": dg.MetadataValue.int(len(existing_files)),
                        "corrupted_files_found": dg.MetadataValue.int(len(corrupted_files)),
                        "corrupted_files_redownloaded": dg.MetadataValue.int(len(corrupted_filenames)),
                        "new_downloads": dg.MetadataValue.int(0)
                    }
                )
            
            # 다운로드 대상 파일 목록 로깅
            context.log.info(f"🚀 {len(filtered_candidates)}개 파일 다운로드 시작")
            if len(filtered_candidates) <= 10:
                for i, candidate in enumerate(filtered_candidates, 1):
                    is_redownload = candidate["filename"] in corrupted_filenames
                    status_icon = "🔄" if is_redownload else "📥"
                    context.log.info(f"   {status_icon} {i}. {candidate['filename']}")
            else:
                # 처음 5개와 마지막 5개만 표시
                for i, candidate in enumerate(filtered_candidates[:5], 1):
                    is_redownload = candidate["filename"] in corrupted_filenames
                    status_icon = "🔄" if is_redownload else "📥"
                    context.log.info(f"   {status_icon} {i}. {candidate['filename']}")
                
                context.log.info(f"   ... (중간 {len(filtered_candidates) - 10}개 파일 생략)")
                
                for i, candidate in enumerate(filtered_candidates[-5:], len(filtered_candidates) - 4):
                    is_redownload = candidate["filename"] in corrupted_filenames
                    status_icon = "🔄" if is_redownload else "📥"
                    context.log.info(f"   {status_icon} {i}. {candidate['filename']}")
            
            # Step 5: 병렬 다운로드 실행
            context.log.info(f"⚡ {max_workers}개 스레드로 병렬 다운로드 시작")
            
            with _graceful_executor(max_workers) as executor:
                future_to_info = {
                    executor.submit(_download_single_nps_file, session, file_info, i % max_workers, stop_event): file_info
                    for i, file_info in enumerate(filtered_candidates)
                }
                
                for future in tqdm(as_completed(future_to_info), total=len(future_to_info), 
                                desc="NPS 다운로드", unit="files"):
                    result = future.result()
                    if result["success"]:
                        successful_downloads.append(result)
            
            # Step 5: 메타데이터 배치 업데이트
            if successful_downloads:
                _update_metadata_batch(conn, successful_downloads)
        
        # 성공 시에만 결과 반환
        total_new = len(successful_downloads)
        total_size = sum(d.get("file_size", 0) for d in successful_downloads)
        
        context.log.info(
            f"모든 다운로드 성공: {total_new}개 신규 다운로드 "
            f"({total_size:,}B = {total_size / (1024**2):.1f}MB)"
        )
        
        # 성능 통계
        performance_stats = {
            "parallel_strategy": "threading",
            "max_workers_used": max_workers,
            "total_candidates": len(download_candidates),
            "successful_downloads": total_new,
            "worker_efficiency": round(total_new / max_workers, 2) if max_workers > 0 else 0,
            "avg_file_size_mb": round(total_size / (1024**2) / max(1, total_new), 2)
        }
        
        return dg.MaterializeResult(
            metadata={
                "dagster/row_count": dg.MetadataValue.int(total_new),
                "total_candidates": dg.MetadataValue.int(len(download_candidates)),
                "successful_downloads": dg.MetadataValue.int(total_new),
                "total_size_bytes": dg.MetadataValue.int(total_size),
                "total_size_mb": dg.MetadataValue.float(round(total_size / (1024**2), 1)),
                "performance_stats": dg.MetadataValue.json(performance_stats),
                "special_cases_found": dg.MetadataValue.int(len(special_candidates)),
                "general_files_found": dg.MetadataValue.int(len(general_candidates)),
                "corrupted_files_found": dg.MetadataValue.int(len(corrupted_files)) if 'corrupted_files' in locals() else 0,
                "corrupted_files_redownloaded": dg.MetadataValue.int(len(corrupted_filenames)) if 'corrupted_filenames' in locals() else 0
            }
        )
        
    except KeyboardInterrupt:
        context.log.error("키보드 중단으로 인한 실행 중단")
        stop_event.set()
        raise KeyboardInterrupt("사용자가 NPS 다운로드를 중단했습니다")
        
    except Exception as e:
        context.log.error(f"NPS 다운로드 실패: {e}")
        stop_event.set()
        raise RuntimeError(f"NPS 히스토리 다운로드 실패: {e}") from e
        
    finally:
        # 확실한 정리
        stop_event.set()
        session.close()
        context.log.info("세션과 모든 리소스가 정리되었습니다.")

@asset(
    description="NPS 파일들의 무결성을 검증하고 손상된 파일을 보고 - Quality Assurance",
    group_name="NPS",
    kinds={"python"},
    tags={
        "domain": "finance", 
        "data_tier": "quality",
        "source": "national_pension",
        "validation": "integrity_check"
    },
    metadata={"validation_type": "file_integrity"}
)
def nps_file_integrity_check(
    context: AssetExecutionContext,
    nps_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    """NPS 파일들의 무결성을 검증하고 손상된 파일을 식별"""
    context.log.info("NPS 파일 무결성 검증 시작")
    
    if not PATHS.raw_history_dir.exists():
        context.log.warning("NPS raw history 디렉토리가 존재하지 않습니다")
        return dg.MaterializeResult(
            metadata={
                "status": "Directory not found",
                "total_files_checked": 0,
                "valid_files": 0,
                "corrupted_files": 0
            }
        )
    
    # 모든 CSV 파일들을 검증
    all_files = list(PATHS.raw_history_dir.glob("*.csv"))
    valid_files = []
    corrupted_files = []
    
    context.log.info(f"총 {len(all_files)}개 파일에 대해 무결성 검증 수행")
    
    for file_path in all_files:
        validation_result = _validate_file_integrity(file_path)
        
        if validation_result["valid"]:
            valid_files.append({
                "filename": file_path.name,
                "size_mb": validation_result["size_mb"],
                "size_bytes": validation_result["size_bytes"]
            })
        else:
            corrupted_files.append({
                "filename": file_path.name,
                "reason": validation_result["reason"],
                "details": validation_result
            })
            context.log.warning(
                f"손상된 파일: {file_path.name} - {validation_result['reason']}"
            )
    
    # 결과 요약
    total_files = len(all_files)
    valid_count = len(valid_files)
    corrupted_count = len(corrupted_files)
    
    context.log.info(f"무결성 검증 완료: {valid_count}개 유효, {corrupted_count}개 손상")
    
    # 큰 파일과 작은 파일 분석
    if valid_files:
        file_sizes = [f["size_mb"] for f in valid_files]
        avg_size = sum(file_sizes) / len(file_sizes)
        min_size = min(file_sizes)
        max_size = max(file_sizes)
        
        size_stats = {
            "average_size_mb": round(avg_size, 2),
            "min_size_mb": round(min_size, 2),
            "max_size_mb": round(max_size, 2),
            "total_size_gb": round(sum(file_sizes) / 1024, 2)
        }
    else:
        size_stats = {}
    
    # DuckDB에 검증 결과 저장
    try:
        with nps_duckdb.get_connection() as conn:
            integrity_table = f"{DATABASE.metadata_table}_integrity"
            
            create_integrity_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {integrity_table} (
                check_id INTEGER PRIMARY KEY,
                filename VARCHAR,
                is_valid BOOLEAN,
                reason VARCHAR,
                size_bytes BIGINT,
                size_mb REAL,
                check_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
            conn.execute(create_integrity_table_sql)
            
            # 기존 레코드 삭제 (최신 상태 유지)
            conn.execute(f"DELETE FROM {integrity_table}")
            
            # 새로운 검증 결과 저장
            insert_data = []
            for i, file_info in enumerate(valid_files + corrupted_files):
                is_valid = file_info in valid_files
                insert_data.append((
                    i + 1,
                    file_info["filename"],
                    is_valid,
                    file_info.get("reason", "valid") if not is_valid else "valid",
                    file_info.get("size_bytes", 0),
                    file_info.get("size_mb", 0.0)
                ))
            
            if insert_data:
                conn.executemany(f"""
                    INSERT INTO {integrity_table} 
                    (check_id, filename, is_valid, reason, size_bytes, size_mb)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, insert_data)
                
                context.log.info(f"무결성 검증 결과를 DB에 저장: {len(insert_data)}개 레코드")
                
    except Exception as db_error:
        context.log.warning(f"DB 저장 실패: {db_error}")
    
    return dg.MaterializeResult(
        metadata={
            "total_files_checked": dg.MetadataValue.int(total_files),
            "valid_files": dg.MetadataValue.int(valid_count),
            "corrupted_files": dg.MetadataValue.int(corrupted_count),
            "integrity_rate_percent": dg.MetadataValue.float(
                round((valid_count / total_files) * 100, 1) if total_files > 0 else 0
            ),
            "size_statistics": dg.MetadataValue.json(size_stats) if size_stats else dg.MetadataValue.json({}),
            "corrupted_file_details": dg.MetadataValue.json(corrupted_files[:10] if corrupted_files else []),
            "sample_valid_files": dg.MetadataValue.json(valid_files[:5] if valid_files else [])
        }
    )
