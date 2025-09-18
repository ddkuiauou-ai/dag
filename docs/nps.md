# NPS (국민연금) 프로젝트

## 개요

NPS (국민연금) 프로젝트는 공공데이터포털의 국민연금 사업장 가입자 CSV 데이터를 다운로드해 정제한 뒤 PostgreSQL 및 SaaS 레이어 테이블로 적재하는 Dagster 파이프라인입니다. `requests`와 `BeautifulSoup`로 원본 파일을 병렬 수집하고, DuckDB를 메타데이터 저장소로 활용해 다운로드 및 처리 현황을 추적합니다. 후속 단계에서는 멀티프로세싱으로 CSV를 UTF-8 포맷으로 정규화한 뒤, PostgreSQL의 `public.pension` 테이블과 SaaS용 `dim_company`/`fact_pension_monthly` 계층으로 데이터를 전송합니다.

## 목표

- 공공데이터포털(data.go.kr)의 사업장 가입자 CSV를 안정적으로 전량 수집합니다.
- DuckDB에 파일 메타데이터(다운로드, 무결성, 처리 결과)를 기록하여 장애 지점을 추적합니다.
- PostgreSQL `public.pension` 테이블에 중복 없는 원본 스냅샷을 적재하고, SaaS 분석을 위한 차원/팩트 테이블을 유지합니다.
- 인덱스, 정규화 함수, SaaS ETL 파이프라인을 자동화해 사후 분석 및 고객 매칭 품질을 보장합니다.

## 오케스트레이션 및 스케줄링

### 작업 (Jobs)

- **`nps_history_job`**: `NPS` 그룹 내 자산(`nps_raw_ingestion`, `nps_file_integrity_check`, `nps_data_processing`, `nps_to_postgres_simple`, `nps_pension_staging_indexes`)을 대상으로 하는 일괄 실행 작업입니다. 전체 히스토리를 새로 내려받거나 재처리할 때 사용합니다.
- SaaS 계층 자산(`ensure_saas_tables`, `nps_saas_etl_run`, `nps_full_rebuild_from_pension`)은 현재 별도 작업에 묶여 있지 않아 필요 시 수동으로 실행합니다.

### 파티셔닝

현재 NPS 파이프라인은 파티션을 사용하지 않습니다. 각 실행은 파일 시스템과 DuckDB에 존재하는 전체 CSV 세트를 기준으로 처리합니다.

### 리소스 (Resources)

- **`nps_duckdb`** (`DuckDBResource`): `data/nps.duckdb` 파일을 통해 다운로드/처리 메타데이터를 기록합니다.
- **`nps_postgres`** (`PostgresResource`): `public.pension` 및 SaaS 계층 테이블을 읽고 쓰는 데 사용합니다.

## 데이터 흐름 및 에셋 종속성

```mermaid
graph TD
    subgraph "NPS 그룹"
        A[nps_raw_ingestion <br> (Bronze)] --> B[nps_data_processing <br> (Silver)];
        A --> C[nps_file_integrity_check <br> (Quality)];
        B --> D[nps_to_postgres_simple <br> (Warehouse)];
        D --> E[nps_pension_staging_indexes <br> (Infra)];
    end
    subgraph "NPS_SaaS 그룹"
        E --> F[ensure_saas_tables <br> (DDL)];
        F --> G[nps_saas_etl_run <br> (Incremental ETL)];
        F --> H[nps_full_rebuild_from_pension <br> (Full Rebuild)];
    end
```

### 에셋 세부 정보

#### NPS 그룹

- **`nps_raw_ingestion`**
  - **티어**: Bronze
  - **출력**: `data/nps/history/in/*.csv`
  - **주요 기능**: 세션 쿠키와 추출한 PK를 이용해 모든 히스토리 CSV를 병렬 다운로드합니다. 파일 이름/크기/다운로드 결과를 DuckDB의 `nps_file_metadata` 테이블에 기록하고 손상된 파일은 재다운로드합니다.【F:dag/nps_raw_ingestion.py†L102-L205】【F:dag/nps_raw_ingestion.py†L917-L1103】
- **`nps_file_integrity_check`**
  - **티어**: Quality
  - **출력**: DuckDB `nps_file_metadata_integrity`
  - **주요 기능**: 다운로드된 CSV의 크기, 헤더, 확장자를 점검해 손상 여부를 기록합니다.【F:dag/nps_raw_ingestion.py†L1187-L1333】
- **`nps_data_processing`**
  - **티어**: Silver
  - **출력**: `data/nps/history/out/*.csv`
  - **주요 기능**: 원본 CSV를 멀티프로세싱으로 읽어 UTF-8-SIG로 재저장하고 처리 통계를 DuckDB `nps_file_metadata_processed`에 저장합니다.【F:dag/nps_data_processing.py†L20-L121】【F:dag/nps_data_processing.py†L187-L352】
- **`nps_to_postgres_simple`**
  - **티어**: Warehouse
  - **출력**: PostgreSQL `public.pension`
  - **주요 기능**: 처리된 CSV를 스캔해 파일별 날짜를 추출하고, COPY 기반 일괄 적재 전에 보조 인덱스를 삭제합니다. 로드 시 `(data_created_ym, company_name, business_reg_num, zip_code, subscriber_count, monthly_notice_amount)` 조합으로 중복을 제거한 뒤 `copy_from`을 사용해 빠르게 적재합니다.【F:dag/nps_postgres_simple.py†L17-L191】【F:dag/nps_postgres_simple.py†L208-L340】
- **`nps_pension_staging_indexes`**
  - **티어**: Infrastructure
  - **출력**: PostgreSQL 내 확장/함수/인덱스
  - **주요 기능**: `pg_trgm`·`unaccent` 확장과 회사명 정규화 함수, 사업장 번호/주소/업종 기반 보조 인덱스를 생성해 SaaS 전환 쿼리를 가속화합니다.【F:dag/nps_index_optimization.py†L1-L195】【F:dag/nps_index_optimization.py†L244-L468】

#### NPS_SaaS 그룹

- **`ensure_saas_tables`**
  - **티어**: DDL
  - **주요 기능**: `dim_company`, `company_alias`, `fact_pension_monthly`, `etl_control`, `etl_run_log` 등 SaaS 계층 테이블과 필수 인덱스/트리거를 생성합니다.【F:dag/nps_kyc_etl.py†L1-L214】
- **`nps_saas_etl_run`**
  - **티어**: SaaS ETL
  - **주요 기능**: `public.pension`에서 최근 월 데이터를 TEMP 테이블로 추출해 후보 매칭을 수행하고, dim/alias/fact 테이블을 업서트한 뒤 실행 로그와 감사 데이터를 기록합니다.【F:dag/nps_kyc_etl.py†L217-L620】
- **`nps_full_rebuild_from_pension`**
  - **티어**: SaaS Rebuild
  - **주요 기능**: SaaS 계층 전체를 비우고 `public.pension`의 최신 데이터를 사용해 차원·팩트·롤업·계보 테이블을 재구축합니다.【F:dag/nps_kyc_etl.py†L688-L918】

### 에셋 검사 (Asset Checks)

- **`simple_postgres_check`**: `nps_to_postgres_simple` 실행 후 테이블 존재 여부와 최근 데이터 시점, 레코드 수를 검증합니다.【F:dag/nps_postgres_simple.py†L508-L569】
- **`check_pension_staging_indexes`**: 필수 확장/함수/인덱스가 모두 준비됐는지 확인합니다.【F:dag/nps_index_optimization.py†L469-L528】
- **`check_timeseries_uniqueness`**, **`check_recent_freshness`**, **`check_etl_coverage`**: SaaS 팩트 테이블의 중복, 최신성, 최근 실행 결과를 점검합니다.【F:dag/nps_kyc_etl.py†L630-L687】

## 데이터 저장소

### DuckDB 메타데이터

- `nps_file_metadata`: 다운로드한 파일의 경로, 크기, 상태를 보관합니다.
- `nps_file_metadata_integrity`: 무결성 검사 결과를 저장합니다.
- `nps_file_metadata_processed`: 멀티프로세싱 처리 결과(행 수, 처리 시간, 인코딩 등)를 기록합니다.

### 메인 테이블: `public.pension`

`nps_to_postgres_simple` 자산이 COPY로 적재하는 기본 테이블 구조 예시는 아래와 같습니다. `id` 컬럼은 식별자 자동 증가용이며, 나머지 컬럼은 CSV 스키마와 일치합니다.

```sql
CREATE TABLE IF NOT EXISTS public.pension (
  id BIGSERIAL PRIMARY KEY,
  data_created_ym TIMESTAMP WITHOUT TIME ZONE NOT NULL,
  company_name TEXT NOT NULL,
  business_reg_num VARCHAR(10),
  join_status VARCHAR(5),
  zip_code VARCHAR(10),
  lot_number_address TEXT,
  road_name_address TEXT,
  legal_dong_addr_code VARCHAR(15),
  admin_dong_addr_code VARCHAR(15),
  addr_sido_code VARCHAR(5),
  addr_sigungu_code VARCHAR(5),
  addr_emdong_code VARCHAR(5),
  workplace_type VARCHAR(5),
  industry_code VARCHAR(10),
  industry_name VARCHAR(50),
  applied_at TIMESTAMP WITHOUT TIME ZONE,
  re_registered_at TIMESTAMP WITHOUT TIME ZONE,
  withdrawn_at TIMESTAMP WITHOUT TIME ZONE,
  subscriber_count INTEGER,
  monthly_notice_amount BIGINT,
  new_subscribers INTEGER,
  lost_subscribers INTEGER,
  created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT now(),
  updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT now()
);
```

- **중복 제거 전략**: 적재 전 `(data_created_ym, company_name, business_reg_num, zip_code, subscriber_count, monthly_notice_amount)` 조합을 기반으로 DataFrame 수준에서 중복을 제거합니다.【F:dag/nps_postgres_simple.py†L255-L276】

### SaaS 계층 테이블

`ensure_saas_tables`는 다음 객체들을 생성/보강합니다.

- `dim_company`: 사업장 BRN6와 주소 키를 기준으로 고유 회사를 관리하며, 정규화된 이름/회사 유형 배열과 관련 인덱스를 제공합니다.【F:dag/nps_kyc_etl.py†L41-L108】
- `company_alias`: 다양한 이름 변형을 저장해 후보 매칭 품질을 높입니다.【F:dag/nps_kyc_etl.py†L108-L133】
- `fact_pension_monthly`: 월별 가입자 집계를 보관하며 `(company_id, ym)`이 기본키입니다.【F:dag/nps_kyc_etl.py†L133-L151】
- `etl_control`, `etl_run_log`, `match_audit_log`, `match_review_queue`: 증분 ETL 구간 제어, 실행 로그, 후보 감사, 검토 큐를 추적합니다.【F:dag/nps_kyc_etl.py†L151-L214】

### 인덱스 및 함수

- 회사명 정규화 함수 `nps_normalize_company_name`, 회사 유형 추출 함수 `nps_extract_company_types`, 롤업용 `nps_canonicalize_company_name` 등이 생성됩니다.【F:dag/nps_index_optimization.py†L200-L447】
- 주요 인덱스: `idx_pension_name_norm_trgm_gin`, `idx_pension_name_norm_trgm_gist`, `idx_pension_name_norm_prefix`, `idx_pension_brn`, `idx_pension_block_addr_ind`, `idx_pension_ym_brin` (CONCURRENTLY 미사용).【F:dag/nps_index_optimization.py†L68-L85】【F:dag/nps_index_optimization.py†L244-L468】

## 개발 및 디버깅

### 🔧 유용한 확인 명령어

**NPS 그룹 에셋 나열:**

```bash
cd /workspace/dag
python -c "
from dag.definitions import defs
from dagster import AssetSelection

asset_graph = defs.get_asset_graph()
nps_assets = AssetSelection.groups('NPS').resolve(asset_graph)
print('🎯 Assets in the \'NPS\' group:')
for asset_key in nps_assets:
    print(f'  - {asset_key.to_user_string()}')
print(f'\n📊 Total {len(nps_assets)} assets.')
"
```

**에셋 종속성 확인:**

```bash
cd /workspace/dag
python -c "
from dag.definitions import defs
from dagster import AssetKey

asset_graph = defs.get_asset_graph()
asset_names = ['nps_raw_ingestion', 'nps_file_integrity_check', 'nps_data_processing',
               'nps_to_postgres_simple', 'nps_pension_staging_indexes',
               'ensure_saas_tables', 'nps_saas_etl_run', 'nps_full_rebuild_from_pension']

for name in asset_names:
    key = AssetKey([name])
    if key in asset_graph.all_asset_keys:
        deps = asset_graph.get_upstream_asset_keys(key)
        print(f"{name} depends on: {[dep.to_user_string() for dep in deps]}")
"
```
