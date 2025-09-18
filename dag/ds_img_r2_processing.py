"""
DS 프로젝트 R2 이미지 처리 파이프라인
PNG 파일을 WebP 형식으로 변환하여 다른 버킷에 저장
"""

import os
import io
import re
import uuid
from typing import List, Dict, Any
from datetime import datetime
import time  # Added import
from dataclasses import dataclass
from pathlib import Path

import boto3
from PIL import Image
import dagster as dg
import duckdb
import psycopg2
from psycopg2.extras import RealDictCursor

from dagster_duckdb import DuckDBResource
from .resources import PostgresResource




# ──── 환경 변수 로드 ────
R2_ACCESS_KEY_ID = os.getenv("DS_R2_ACCESS_KEY_ID")
R2_SECRET_ACCESS_KEY = os.getenv("DS_R2_SECRET_ACCESS_KEY")
R2_ENDPOINT = os.getenv("DS_R2_ENDPOINT")
R2_SOURCE_BUCKET = os.getenv("DS_R2_SOURCE_BUCKET")  # PNG 파일이 있는 소스 버킷
R2_TARGET_BUCKET = os.getenv("DS_R2_TARGET_BUCKET")  # WebP 파일을 저장할 타겟 버킷
DS_PUBLIC_URL = os.getenv("DS_PUBLIC_URL")  # 공개 URL


# ──── DS 이미지 처리 경로 관리 ────
@dataclass
class DSImagePaths:
    """DS 이미지 처리 파일 경로 관리"""
    base_dir: Path = Path("data") / "images" / "ds"
    
    @property
    def source_png_dir(self) -> Path:
        """R2에서 다운로드한 PNG 파일 임시 저장 디렉토리"""
        return self.base_dir / R2_SOURCE_BUCKET
        
    @property
    def converted_webp_dir(self) -> Path:
        """변환된 WebP 파일 임시 저장 디렉토리"""
        return self.base_dir / R2_TARGET_BUCKET
    
    def ensure_directories(self) -> None:
        """필요한 디렉토리 생성"""
        self.source_png_dir.mkdir(parents=True, exist_ok=True)
        self.converted_webp_dir.mkdir(parents=True, exist_ok=True)

# DS 이미지 경로 인스턴스 생성
ds_image_paths = DSImagePaths()



def get_r2_client():
    """R2 클라이언트 생성 헬퍼 함수"""
    if not all([R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_ENDPOINT]):
        raise ValueError("R2 자격 증명이 .env 파일에 완전히 구성되지 않았습니다")
    
    return boto3.client(
        service_name='s3',
        aws_access_key_id=R2_ACCESS_KEY_ID,
        aws_secret_access_key=R2_SECRET_ACCESS_KEY,
        endpoint_url=R2_ENDPOINT,
        region_name='auto'
    )


@dg.asset(
    group_name="DS",
    kinds={"source"},
    deps=["ds_img_r2_docker", "ds_img_r2_node"],  
    tags={
        "domain": "image_processing",
        "data_tier": "bronze", 
        "source": "cloudflare_r2"
    },
    description="DS 프로젝트 - R2 소스 버킷에서 PNG 파일을 다운로드하여 로컬에 저장 (Bronze Tier)"
)
def ds_r2_download(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """
    R2 소스 버킷에서 PNG 파일을 다운로드하여 로컬 디렉토리에 저장합니다.
    로컬에 저장된 PNG 파일 경로 목록을 반환합니다.
    """
    if not R2_SOURCE_BUCKET:
        raise ValueError("R2_SOURCE_BUCKET이 .env 파일에 설정되지 않았습니다")

    ds_image_paths.ensure_directories()
    for existing_file in ds_image_paths.source_png_dir.glob("*.png"):
        existing_file.unlink()

    s3_client = get_r2_client()
    downloaded_files = []
    all_png_objects = []
    
    total_download_time_seconds = 0.0 # Initialize here for scope

    try:
        context.log.info(f"R2 소스 버킷 '{R2_SOURCE_BUCKET}'에서 PNG 파일 목록 가져오는 중...")
        paginator = s3_client.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=R2_SOURCE_BUCKET):
            if "Contents" in page:
                for obj in page["Contents"]:
                    if obj["Key"].lower().endswith(".png"):
                        all_png_objects.append(obj)
        
        total_files = len(all_png_objects)

        if total_files == 0:
            context.log.info("다운로드할 PNG 파일이 없습니다.")
            return dg.MaterializeResult(
                metadata={
                    "dagster/row_count": dg.MetadataValue.int(0),
                    "source_bucket": dg.MetadataValue.text(R2_SOURCE_BUCKET),
                    "downloaded_files_count": dg.MetadataValue.int(0),
                    "total_files_in_bucket_png": dg.MetadataValue.int(0),
                    "total_download_time_seconds": dg.MetadataValue.float(0.0),
                    "message": dg.MetadataValue.text("다운로드할 PNG 파일이 없습니다.")
                }
            )
        
        context.log.info(f"총 {total_files}개의 PNG 파일을 다운로드합니다.")

        start_time = time.time()
        downloaded_count = 0

        for obj in all_png_objects:
            try:
                response = s3_client.get_object(Bucket=R2_SOURCE_BUCKET, Key=obj["Key"])
                png_data = response['Body'].read()
                safe_filename = obj["Key"].replace("/", "_").replace("\\", "_")
                local_file_path = ds_image_paths.source_png_dir / safe_filename
                with open(local_file_path, 'wb') as f:
                    f.write(png_data)
                downloaded_files.append(str(local_file_path))
                
                downloaded_count += 1

                elapsed_time = time.time() - start_time
                avg_time_per_file = elapsed_time / downloaded_count if downloaded_count > 0 else 0
                remaining_files = total_files - downloaded_count
                estimated_time_remaining_seconds = avg_time_per_file * remaining_files
                
                formatted_eta = "계산 중..."
                if downloaded_count > 0 and remaining_files > 0 : # Start estimating after first file and if files remain
                    eta_minutes = int(estimated_time_remaining_seconds // 60)
                    eta_seconds = int(estimated_time_remaining_seconds % 60)
                    formatted_eta = f"{eta_minutes}분 {eta_seconds}초"
                elif remaining_files == 0:
                    formatted_eta = "거의 완료"

                context.log.info(
                    f"진행: {downloaded_count}/{total_files} ('{obj['Key']}') | 경과: {elapsed_time:.2f}초 | 예상 남은 시간: {formatted_eta}"
                )

            except Exception as e_file:
                context.log.error(f"파일 '{obj['Key']}' 다운로드 중 오류 발생: {e_file}")
                # Optionally, re-raise or collect failed files if needed
                continue # Continue with the next file

        total_download_time_seconds = time.time() - start_time
        context.log.info(f"🎯 총 {len(downloaded_files)}개의 PNG 파일을 {total_download_time_seconds:.2f}초 만에 다운로드했습니다. (버킷 내 총 PNG: {total_files}개)")

    except Exception as e:
        context.log.error(f"R2 버킷 '{R2_SOURCE_BUCKET}'에서 파일 다운로드 중 주요 오류 발생: {e}")
        # Ensure total_download_time_seconds is set if error occurs before loop finishes
        if 'start_time' in locals():
             total_download_time_seconds = time.time() - start_time
        else:
             total_download_time_seconds = 0.0 # Or handle as appropriate
        raise

    return dg.MaterializeResult(
        metadata={
            "dagster/row_count": dg.MetadataValue.int(len(downloaded_files)),
            "source_bucket": dg.MetadataValue.text(R2_SOURCE_BUCKET),
            "downloaded_files_count": dg.MetadataValue.int(len(downloaded_files)),
            "total_files_in_bucket_png": dg.MetadataValue.int(total_files if 'total_files' in locals() else 0),
            "total_download_time_seconds": dg.MetadataValue.float(total_download_time_seconds),
            "local_storage_dir": dg.MetadataValue.text(str(ds_image_paths.source_png_dir)),
            "preview_files": dg.MetadataValue.json([Path(f).name for f in downloaded_files[:10]]),
            "processing_timestamp": dg.MetadataValue.text(datetime.now().isoformat())
        }
    )


@dg.asset(
    name="ds_png2webp",
    group_name="DS",
    kinds={"compute"},
    tags={
        "domain": "image_processing",
        "data_tier": "silver",
        "source": "internal"
    },
    deps=["ds_r2_download"],
    description="DS 프로젝트 - 로컬 PNG 파일을 WebP 형식으로 변환 및 저장 (Silver Tier)"
)
def ds_png2webp(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """
    로컬 PNG 디렉토리에서 파일을 읽어 WebP 형식으로 변환하여 저장합니다.
    변환된 WebP 파일 경로 목록을 반환합니다.
    """
    ds_image_paths.ensure_directories()
    png_files = list(ds_image_paths.source_png_dir.glob("*.png"))
    if not png_files:
        context.log.info("변환할 PNG 파일이 없습니다")
        return dg.MaterializeResult(
            metadata={
                "dagster/row_count": dg.MetadataValue.int(0),
                "converted_files_count": dg.MetadataValue.int(0),
                "message": dg.MetadataValue.text("변환할 PNG 파일이 없습니다")
            }
        )
    # 기존 WebP 파일 정리
    for existing_file in ds_image_paths.converted_webp_dir.glob("*.webp"):
        existing_file.unlink()
    converted_files = []
    for png_path in png_files:
        try:
            with Image.open(png_path) as img:
                webp_filename = png_path.stem + ".webp"
                webp_path = ds_image_paths.converted_webp_dir / webp_filename
                img.save(webp_path, format="WEBP", quality=80, optimize=True)
                converted_files.append(str(webp_path))
                context.log.info(f"✓ 변환 완료: '{png_path.name}' → '{webp_filename}'")
        except Exception as e:
            context.log.error(f"❌ 파일 '{png_path.name}' 변환 실패: {e}")
            continue
    return dg.MaterializeResult(
        metadata={
            "dagster/row_count": dg.MetadataValue.int(len(converted_files)),
            "converted_files_count": dg.MetadataValue.int(len(converted_files)),
            "preview_converted_files": dg.MetadataValue.json([Path(f).name for f in converted_files[:10]]),
            "processing_timestamp": dg.MetadataValue.text(datetime.now().isoformat())
        }
    )


@dg.asset(
    name="ds_img_finalize",
    group_name="DS",
    kinds={"compute"},
    tags={
        "domain": "image_processing",
        "data_tier": "silver",
        "source": "internal"
    },
    deps=["ds_png2webp"],
    description="DS 프로젝트 - PNG 파일을 최종 디렉토리로 이동하고 company.logo 컬럼을 public URL로 업데이트, DuckDB 기록 (webp도 함께 기록)"
)
def ds_img_finalize(context: dg.AssetExecutionContext, ds_duckdb: DuckDBResource, ds_postgres: PostgresResource) -> dg.MaterializeResult: 
    """
    PNG 파일을 최종 디렉토리로 이동, webp도 함께 이동. company.logo 컬럼은 png public url로만 업데이트. DuckDB에는 png/webp 모두 기록.
    """
    if not DS_PUBLIC_URL:
        raise ValueError("DS_PUBLIC_URL이 .env 파일에 설정되지 않았습니다")
    ds_image_paths.ensure_directories()
    final_dir = ds_image_paths.base_dir / "final"
    final_dir.mkdir(parents=True, exist_ok=True)
    png_files = list(ds_image_paths.source_png_dir.glob("*.png"))
    if not png_files:
        context.log.info("이동할 PNG 파일이 없습니다")
        return dg.MaterializeResult(
            metadata={
                "dagster/row_count": dg.MetadataValue.int(0),
                "updated_companies": dg.MetadataValue.int(0),
                "message": dg.MetadataValue.text("이동할 PNG 파일이 없습니다")
            }
        )
    # postgres_config = {
    #     "host": POSTGRES_HOST,
    #     "port": int(POSTGRES_PORT) if POSTGRES_PORT else 5432,
    #     "user": POSTGRES_USER,
    #     "password": POSTGRES_PASSWORD,
    #     "dbname": POSTGRES_DB
    # }
    updated_companies = []
    failed_updates = []
    context.log.info(f"{len(png_files)}개 PNG 파일 이동 및 company.logo 업데이트 시작 (webp도 함께 기록)")
    try:
        with ds_postgres.get_connection() as pg_conn:
            with pg_conn.cursor(cursor_factory=RealDictCursor) as cur:
                for png_path in png_files:
                    try:
                        company_id = extract_cid(png_path.name)
                        # PNG 파일 이동
                        final_png_path = final_dir / png_path.name
                        png_path.rename(final_png_path)
                        # webp 파일도 있으면 같이 이동
                        webp_path =ds_image_paths.converted_webp_dir / (png_path.stem + ".webp")
                        final_webp_path = None
                        if webp_path.exists():
                            final_webp_path = final_dir / webp_path.name
                            webp_path.rename(final_webp_path)
                        # logo_url은 png만 사용
                        logo_url = f"{DS_PUBLIC_URL.rstrip('/')}/{final_png_path.name}"
                        cur.execute(
                            "SELECT id, name FROM company WHERE id = %s",
                            (company_id,)
                        )
                        company_record = cur.fetchone()
                        # --- R2 타겟 버킷 업로드 ---
                        s3_client = get_r2_client()
                        # PNG 업로드
                        try:
                            s3_client.upload_file(str(final_png_path), R2_TARGET_BUCKET, final_png_path.name)
                            context.log.info(f"✓ PNG 업로드: {final_png_path.name} → {R2_TARGET_BUCKET}")
                        except Exception as e:
                            context.log.error(f"❌ PNG 업로드 실패: {final_png_path.name} - {e}")
                        # WebP 업로드
                        if final_webp_path:
                            try:
                                s3_client.upload_file(str(final_webp_path), R2_TARGET_BUCKET, final_webp_path.name)
                                context.log.info(f"✓ WebP 업로드: {final_webp_path.name} → {R2_TARGET_BUCKET}")
                            except Exception as e:
                                context.log.error(f"❌ WebP 업로드 실패: {final_webp_path.name} - {e}")
                        # --- 기존 DB 처리 계속 ---
                        if company_record:
                            cur.execute(
                                "UPDATE company SET thumbnail = %s WHERE id = %s",
                                (logo_url, company_id)
                            )
                            if cur.rowcount > 0:
                                updated_companies.append({
                                    "id": company_id,
                                    "name": company_record["name"],
                                    "thumbnail": logo_url,
                                    "filename_png": final_png_path.name,
                                    "filename_webp": final_webp_path.name if final_webp_path else None,
                                    "updated_at": datetime.now().isoformat()
                                })
                                context.log.info(f"✓ 회사 로고(png) 업데이트 완료: '{company_record['name']}' (ID: {company_id}) → {logo_url}")
                            else:
                                failed_updates.append({
                                    "id": id,
                                    "filename": final_png_path.name,
                                    "error": "UPDATE 쿼리가 0개 행에 영향을 줌"
                                })
                                context.log.error(f"❌ FAST-FAIL: UPDATE 실패 - id '{id}' (파일: {final_png_path.name})")
                                raise Exception(f"UPDATE 실패 - id '{id}'에 대한 업데이트가 0개 행에 영향을 줌")
                        else:
                            failed_updates.append({
                                "id": id,
                                "filename": final_png_path.name,
                                "error": f"PostgreSQL에서 id '{id}'를 찾을 수 없음"
                            })
                            context.log.error(f"❌ FAST-FAIL: 회사를 찾을 수 없음 - id '{id}' (파일: {final_png_path.name})")
                            raise Exception(f"PostgreSQL에서 id '{id}'를 찾을 수 없음")
                    except Exception as e:
                        context.log.error(f"❌ FAST-FAIL: 파일 '{png_path.name}' 처리 중 치명적 오류: {e}")
                        failed_updates.append({
                            "id": id if 'id' in locals() else 'unknown',
                            "filename": png_path.name,
                            "error": str(e)
                        })
                        raise
                pg_conn.commit()
    except Exception as e:
        context.log.error(f"PostgreSQL 연결 또는 처리 실패: {e}")
        raise
    try:
        with ds_duckdb.get_connection() as duck_conn:
            duck_conn.execute("""
                CREATE TABLE IF NOT EXISTS ds_logo_updates (
                    id VARCHAR,
                    name VARCHAR,
                    thumbnail VARCHAR,
                    filename_png VARCHAR,
                    filename_webp VARCHAR,
                    updated_at TIMESTAMP,
                    processing_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            if updated_companies:
                duck_conn.executemany(
                    """
                    INSERT INTO ds_logo_updates (
                        id, name, thumbnail, filename_png, filename_webp, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    [
                        (
                            record["id"],
                            record["name"],
                            record["thumbnail"],
                            record["filename_png"],
                            record["filename_webp"],
                            record["updated_at"]
                        )
                        for record in updated_companies
                    ]
                )
                context.log.info(f"📝 DuckDB에 {len(updated_companies)}개 업데이트 기록 저장 완료")
    except Exception as e:
        context.log.error(f"DuckDB 기록 저장 실패: {e}")
    context.log.info(
        f"🎯 로고 업데이트 완료 요약: {len(updated_companies)}개 성공, {len(failed_updates)}개 실패"
    )
    return dg.MaterializeResult(
        metadata={
            "dagster/row_count": dg.MetadataValue.int(len(updated_companies)),
            "updated_companies": dg.MetadataValue.int(len(updated_companies)),
            "failed_updates": dg.MetadataValue.int(len(failed_updates)),
            "ds_public_url": dg.MetadataValue.text(DS_PUBLIC_URL),
            "processed_files": dg.MetadataValue.int(len(png_files)),
            "preview_updated_companies": dg.MetadataValue.json(
                [f"{c['name']} ({c['id']})" for c in updated_companies[:5]]
            ),
            "processing_timestamp": dg.MetadataValue.text(datetime.now().isoformat())
        }
    )


@dg.asset(
    group_name="DS",
    kinds={"cleanup"},
    tags={
        "domain": "image_processing",
        "data_tier": "utility",
        "source": "cloudflare_r2"
    },
    description="DS 프로젝트 - R2 소스 버킷의 모든 이미지를 삭제 (의존성 없음, 위험: 되돌릴 수 없음)"
)
def ds_r2_clear_src(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """
    R2 소스 버킷의 모든 PNG, WEBP 이미지를 삭제합니다.
    """
    if not R2_SOURCE_BUCKET:
        raise ValueError("R2_SOURCE_BUCKET이 .env 파일에 설정되지 않았습니다")
    s3_client = get_r2_client()
    deleted = []
    errors = []
    try:
        context.log.info(f"R2 소스 버킷 '{R2_SOURCE_BUCKET}'에서 이미지 삭제 시작")
        paginator = s3_client.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=R2_SOURCE_BUCKET):
            if "Contents" in page:
                for obj in page["Contents"]:
                    key = obj["Key"]
                    if key.lower().endswith(('.png', '.webp')):
                        try:
                            s3_client.delete_object(Bucket=R2_SOURCE_BUCKET, Key=key)
                            deleted.append(key)
                            context.log.info(f"✓ 삭제: {key}")
                        except Exception as e:
                            errors.append({"key": key, "error": str(e)})
                            context.log.error(f"❌ 삭제 실패: {key} - {e}")
        context.log.info(f"🎯 총 {len(deleted)}개 이미지 삭제 완료, 실패 {len(errors)}개")
    except Exception as e:
        context.log.error(f"R2 버킷 '{R2_SOURCE_BUCKET}' 이미지 삭제 실패: {e}")
        raise
    return dg.MaterializeResult(
        metadata={
            "dagster/row_count": dg.MetadataValue.int(len(deleted)),
            "deleted_files": dg.MetadataValue.json(deleted[:10]),
            "error_count": dg.MetadataValue.int(len(errors)),
            "errors": dg.MetadataValue.json(errors[:5]),
            "bucket": dg.MetadataValue.text(R2_SOURCE_BUCKET),
            "processing_timestamp": dg.MetadataValue.text(datetime.now().isoformat())
        }
    )


@dg.asset(
    name="ds_r2_clear_tgt",
    group_name="DS",
    kinds={"cleanup"},
    tags={
        "domain": "image_processing",
        "data_tier": "utility",
        "source": "cloudflare_r2"
    },
    description="DS 프로젝트 - R2 타겟 버킷의 모든 이미지를 삭제 (의존성 없음, 위험: 되돌릴 수 없음)"
)
def ds_r2_clear_tgt(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """
    R2 타겟 버킷의 모든 PNG, WEBP 이미지를 삭제합니다.
    """
    if not R2_TARGET_BUCKET:
        raise ValueError("R2_TARGET_BUCKET이 .env 파일에 설정되지 않았습니다")
    s3_client = get_r2_client()
    deleted = []
    errors = []
    try:
        context.log.info(f"R2 타겟 버킷 '{R2_TARGET_BUCKET}'에서 이미지 삭제 시작")
        paginator = s3_client.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=R2_TARGET_BUCKET):
            if "Contents" in page:
                for obj in page["Contents"]:
                    key = obj["Key"]
                    if key.lower().endswith(('.png', '.webp')):
                        try:
                            s3_client.delete_object(Bucket=R2_TARGET_BUCKET, Key=key)
                            deleted.append(key)
                            context.log.info(f"✓ 삭제: {key}")
                        except Exception as e:
                            errors.append({"key": key, "error": str(e)})
                            context.log.error(f"❌ 삭제 실패: {key} - {e}")
        context.log.info(f"🎯 총 {len(deleted)}개 이미지 삭제 완료, 실패 {len(errors)}개")
    except Exception as e:
        context.log.error(f"R2 버킷 '{R2_TARGET_BUCKET}' 이미지 삭제 실패: {e}")
        raise
    return dg.MaterializeResult(
        metadata={
            "dagster/row_count": dg.MetadataValue.int(len(deleted)),
            "deleted_files": dg.MetadataValue.json(deleted[:10]),
            "error_count": dg.MetadataValue.int(len(errors)),
            "errors": dg.MetadataValue.json(errors[:5]),
            "bucket": dg.MetadataValue.text(R2_TARGET_BUCKET),
            "processing_timestamp": dg.MetadataValue.text(datetime.now().isoformat())
        }
    )


@dg.asset_check(
    asset="ds_r2_download",
)
def ds_r2_src_check(context: dg.AssetCheckExecutionContext):
    """R2 소스 버킷 연결 상태 검증"""
    try:
        s3_client = get_r2_client()
        s3_client.head_bucket(Bucket=R2_SOURCE_BUCKET)
        return dg.AssetCheckResult(
            passed=True,
            metadata={"bucket_name": dg.MetadataValue.text(R2_SOURCE_BUCKET)}
        )
    except Exception as e:
        return dg.AssetCheckResult(
            passed=False,
            metadata={
                "bucket_name": dg.MetadataValue.text(R2_SOURCE_BUCKET),
                "error": dg.MetadataValue.text(str(e))
            }
        )


@dg.asset_check(
    asset="ds_upload_webp_to_r2",
)
def ds_r2_tgt_check(context: dg.AssetCheckExecutionContext):
    """R2 타겟 버킷 연결 상태 검증"""
    try:
        s3_client = get_r2_client()
        s3_client.head_bucket(Bucket=R2_TARGET_BUCKET)
        return dg.AssetCheckResult(
            passed=True,
            metadata={"bucket_name": dg.MetadataValue.text(R2_TARGET_BUCKET)}
        )
    except Exception as e:
        return dg.AssetCheckResult(
            passed=False,
            metadata={
                "bucket_name": dg.MetadataValue.text(R2_TARGET_BUCKET),
                "error": dg.MetadataValue.text(str(e))
            }
        )


def extract_cid(filename: str) -> str:
    """
    파일명에서 company_id를 추출합니다.
    예: clogo-0038de98-fbe5-4ce4-a139-0b2e10ac028f.webp → 0038de98-fbe5-4ce4-a139-0b2e10ac028f
    """
    # 파일명에서 확장자 제거
    name_without_ext = Path(filename).stem
    
    # clogo- 접두사 제거하고 company_id 추출
    if name_without_ext.startswith("clogo-"):
        return name_without_ext[6:]  # "clogo-" 제거
    else:
        # 만약 다른 패턴이면 그대로 반환
        return name_without_ext

