# NPS (국민연금) 프로젝트

## 개요

NPS (국민연금) 프로젝트는 대한민국 국민연금공단의 사업장 가입자 데이터를 수집, 처리 및 분석하기 위해 설계된 데이터 파이프라인입니다. 이 프로젝트는 Dagster를 사용하여 관리되며, 원시 데이터 수집부터 최종 분석용 데이터셋 생성까지 데이터의 전체 생명주기를 처리합니다.

## 목표

-   공공데이터포털(data.go.kr)에서 월별 NPS 사업장 가입자 데이터를 수집하고 처리합니다.
-   가입자 수, 산업 분류, 지역 정보를 포함한 연금 데이터의 깨끗하고 구조화된 쿼리 가능한 데이터베이스를 유지합니다.
-   KYC(Know Your Customer) 프로세스를 통해 기존 회사 정보로 연금 데이터를 보강합니다.
-   분석 쿼리에 최적화된 데이터베이스 성능을 제공합니다.

## 오케스트레이션 및 스케줄링

### 작업 (Jobs)

NPS 에셋은 Dagster 작업을 통해 실행됩니다. 주요 작업은 다음과 같습니다.

-   **`nps_history_job`**: 이 작업은 "NPS" 그룹 내의 모든 에셋(`nps_raw_ingestion`, `nps_data_processing`, `nps_postgres_simple`)을 대상으로 합니다. 지정된 파티션에 대한 기록 데이터를 가져와 처리하도록 설계되었습니다.

### 파티셔닝

NPS 파이프라인은 월별로 데이터를 처리하기 위해 월 단위로 파티션됩니다.

-   **`MonthlyPartitionsDefinition`**: `nps_raw_ingestion` 에셋은 **2018-01-01**부터 **2024-07-01**까지의 월별 파티션을 정의합니다. 이를 통해 Dagster는 이 범위 내의 특정 월에 대해 파이프라인을 실행할 수 있으므로 데이터 백필 및 업데이트에 대한 세분화된 제어가 가능합니다.

### 리소스 (Resources)

파이프라인은 Dagster에 구성된 외부 리소스에 의존합니다.

-   **`DataGovClient`**: `nps_raw_ingestion`이 공공데이터포털(data.go.kr) API와 상호 작용하고 소스 CSV 파일을 다운로드하는 데 사용하는 클라이언트입니다.
-   **`PostgresResource`**: `nps_postgres_simple` 및 `nps_kyc_etl`이 데이터 웨어하우스에서 읽고 쓰는 데 사용하는 PostgreSQL 데이터베이스에 대한 연결을 제공합니다.

## 데이터 흐름 및 에셋 종속성

이 프로젝트는 각각 변환 단계를 나타내는 여러 에셋으로 구성됩니다. 에셋 간의 종속성은 아래에 시각화된 데이터 흐름을 정의합니다.

```mermaid
graph TD
    subgraph "NPS 그룹"
        A[nps_raw_ingestion <br> (Bronze)] --> B[nps_data_processing <br> (Silver)];
        B --> C[nps_postgres_simple <br> (Gold)];
    end
    subgraph "NPS_KYC 그룹"
        C --> D[nps_kyc_etl <br> (Gold)];
    end
```

### 에셋 세부 정보

#### 1. `nps_raw_ingestion`
-   **티어**: Bronze
-   **그룹**: NPS
-   **업스트림 종속성**: 없음
-   **설명**: 공공데이터포털에서 특정 월의 원시 NPS 데이터를 CSV 파일로 다운로드합니다.
-   **파티션**: `MonthlyPartitionsDefinition`
-   **리소스**: `DataGovClient`를 사용합니다.

#### 2. `nps_data_processing`
-   **티어**: Silver
-   **그룹**: NPS
-   **업스트림 종속성**: `nps_raw_ingestion`
-   **설명**: 원시 CSV를 가져와 정리하고, 열 이름을 영어로 바꾸고, 코드를 표준화하고, `avg_fee`와 같은 새 필드를 계산합니다. 출력은 처리된 Parquet 파일입니다.
-   **에셋 검사**:
    -   `check_subscriber_count_not_zero`: 가입자 수가 양수인지 확인합니다.
    -   `check_company_name_not_empty`: 회사 이름이 null이거나 비어 있지 않은지 확인합니다.

#### 3. `nps_postgres_simple`
-   **티어**: Gold
-   **그룹**: NPS
-   **업스트림 종속성**: `nps_data_processing`
-   **설명**: 처리된 Parquet 데이터를 PostgreSQL의 `pension` 테이블에 로드합니다. 동일한 월에 대한 중복 레코드가 생성되는 것을 방지하기 위해 `upsert` 메커니즘을 사용합니다.
-   **리소스**: `PostgresResource`를 사용합니다.
-   **에셋 검사**:
    -   `check_nps_postgres_simple_row_count`: 데이터베이스에 로드된 행 수가 0보다 큰지 확인합니다.

#### 4. `nps_kyc_etl`
-   **티어**: Gold
-   **그룹**: NPS_KYC
-   **업스트림 종속성**: `nps_postgres_simple`
-   **설명**: 주 NPS 데이터가 로드된 후 이 에셋은 `pension` 테이블을 읽고 사업자 등록 번호를 기준으로 내부 `company` 테이블과 조인한 다음 보강된 결과를 `pension_kyc` 테이블에 저장합니다.
-   **리소스**: `PostgresResource`를 사용합니다.

### 독립 실행형 유지 관리 작업

-   **`nps_index_optimization_job`**: 주요 데이터베이스 인덱스를 삭제하고 다시 생성하여 `pension` 테이블의 쿼리 성능을 최적화하기 위해 독립적으로 실행되는 유지 관리 작업입니다.

## 데이터베이스

### 메인 테이블: `pension`

처리된 NPS 사업장 데이터를 저장하는 중앙 테이블입니다.

#### 주요 필드

-   **기본 정보**: `company_name`, `business_reg_num`
-   **위치 데이터**: `zip_code`, `lot_number_address`, `road_name_address`, `addr_sido_code`, `addr_sigungu_code`
-   **산업 정보**: `industry_code`, `industry_name`
-   **가입 통계**: `subscriber_count`, `monthly_notice_amount`, `new_subscribers`, `lost_subscribers`
-   **시간 정보**: `data_created_ym` (데이터가 해당하는 월)

### 성능 및 무결성

-   **데이터 무결성**: `nps_postgres_simple` 에셋은 (`data_created_ym`, `company_name`, `zip_code` 등) 필드의 조합을 기반으로 `upsert` 전략을 사용하여 동일한 월에 대한 중복 레코드를 방지합니다.
-   **인덱스 최적화**: `nps_index_optimization_job`은 일반적인 쿼리 패턴을 가속화하기 위해 다음 인덱스를 생성합니다.
    -   `idx_pension_company_name_trgm`: 회사 이름에 대한 빠른 유사 문자열 검색용 (`pg_trgm` 확장 필요).
    -   `idx_pension_region_industry`: 지역 및 산업별 필터링을 위한 복합 인덱스.
    -   `idx_pension_data_created_ym`: 시계열 기반 쿼리용.
    -   `idx_pension_business_reg_num`: 사업자 등록 번호로 빠른 조회를 위한 인덱스.

## 개발 및 디버깅

### 🔧 유용한 확인 명령어

**NPS 그룹의 에셋 나열:**

```bash
cd /Users/craigchoi/silla/dag

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
python -c "
from dag.definitions import defs
from dagster import AssetKey

asset_graph = defs.get_asset_graph()
nps_assets = ['nps_raw_ingestion', 'nps_data_processing', 'nps_postgres_simple', 'nps_kyc_etl']

for asset_name in nps_assets:
    asset_key = AssetKey([asset_name])
    if asset_key in asset_graph.all_asset_keys:
        deps = asset_graph.get_upstream_asset_keys(asset_key)
        print(f'{asset_name} depends on: {[dep.to_user_string() for dep in deps]}')
"
```

## 부록: 데이터베이스 스키마 (PostgreSQL)

```sql
-- 1) 유사 검색을 위한 필수 확장
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- 2) 메인 테이블 생성
CREATE TABLE IF NOT EXISTS public.pension (
  id                  BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  data_created_ym     TIMESTAMP WITHOUT TIME ZONE NOT NULL,
  company_name        VARCHAR(100) NOT NULL,
  business_reg_num    VARCHAR(10),
  join_status         VARCHAR(5),
  zip_code            VARCHAR(10),
  lot_number_address  VARCHAR(50),
  road_name_address   VARCHAR(50),
  legal_dong_addr_code VARCHAR(15),
  admin_dong_addr_code VARCHAR(15),
  addr_sido_code      VARCHAR(5),
  addr_sigungu_code   VARCHAR(5),
  addr_emdong_code    VARCHAR(5),
  workplace_type      VARCHAR(5),
  industry_code       VARCHAR(10),
  industry_name       VARCHAR(50),
  applied_at          TIMESTAMP WITHOUT TIME ZONE,
  re_registered_at    TIMESTAMP WITHOUT TIME ZONE,
  withdrawn_at        TIMESTAMP WITHOUT TIME ZONE,
  subscriber_count    INTEGER,
  monthly_notice_amount BIGINT,
  new_subscribers     INTEGER,
  lost_subscribers    INTEGER,
  avg_fee             INTEGER,
  created_at          TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT now(),
  updated_at          TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT now()
);

-- 3) 인덱스 생성 (nps_index_optimization_job에 의해 관리됨)
-- 참고: 이 작업은 이러한 인덱스를 삭제하고 다시 생성합니다.

-- 유사 회사 이름 검색용
CREATE INDEX IF NOT EXISTS idx_pension_company_name_trgm
  ON public.pension USING gin (company_name gin_trgm_ops);

-- 결합된 지역 및 산업 쿼리용
CREATE INDEX IF NOT EXISTS idx_pension_region_industry
  ON public.pension (addr_sido_code, addr_sigungu_code, industry_code);

-- 시계열 쿼리용
CREATE INDEX IF NOT EXISTS idx_pension_data_created_ym
  ON public.pension (data_created_ym);

-- 사업자 등록 번호로 조회용
CREATE INDEX IF NOT EXISTS idx_pension_business_reg_num
  ON public.pension (business_reg_num);

-- 4) upsert 로직을 위한 고유 제약 조건 (개념적)
-- `nps_postgres_simple`의 upsert 로직은
-- (data_created_ym, company_name, zip_code, subscriber_count, monthly_notice_amount)의 조합을 기반으로 중복을 방지합니다.
-- 로더의 유연성을 허용하기 위해 공식적인 UNIQUE INDEX는 기본적으로 생성되지 않습니다.
```