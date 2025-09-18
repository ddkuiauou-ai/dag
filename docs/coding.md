# DAG Project Coding Guidelines

Comprehensive coding guidelines for developing data pipelines using Dagster in the DAG project.

## Table of Contents

1. [File Management and Organization](#file-management-and-organization)
2. [Asset Management and Definitions Registration](#asset-management-and-definitions-registration)
3. [Job Management and Registration](#job-management-and-registration)
4. [Data Tier Tagging System (Medallion Architecture)](#data-tier-tagging-system-medallion-architecture)
5. [Basic Asset Definition Style](#basic-asset-definition-style)
6. [Schema Metadata Definition](#schema-metadata-definition)
7. [Row Count Metadata](#row-count-metadata)
8. [Data Validation Pattern](#data-validation-pattern)
9. [Kinds Configuration](#kinds-configuration)
10. [Development Philosophy](#development-philosophy)
11. [Best Practices Summary](#best-practices-summary)
12. [Environment and Configuration Management](#environment-and-configuration-management)
13. [Utility Functions and Helpers](#utility-functions-and-helpers)
14. [Testing Guidelines](#testing-guidelines)
15. [Performance and Optimization Guidelines](#performance-and-optimization-guidelines)
16. [Security and Compliance](#security-and-compliance)
17. [Monitoring and Observability](#monitoring-and-observability)

## 1. File Management and Organization

### File Naming Conventions

- **Project Prefixes**: All new files must include appropriate project prefixes so Dagster modules stay organized by pipeline.
  - `cd_` prefix for the CD (Chundan) stock orchestration code (including history and Turso variants such as `cd_history_*` and `cd_*_turso`).
  - `ds_` prefix for the DS (DataStory) computer-vision pipelines.
  - `is_` prefix for the IS (Isshoo) crawler and enrichment pipelines.
  - `ss_` prefix for the SS (Silla-Store) crawler pipelines and dictionary assets.
  - `nps_` prefix for the National Pension Service (NPS) workloads.
  - Example: `cd_price_processing.py`, `ds_img_r2_processing.py`, `is_data_sync.py`, `ss_data_sync.py`, `nps_raw_ingestion.py`.

### File Size Management

- **Line Limit**: Keep Python files between 1000-1500 lines for maintainability
- **File Splitting**: If files exceed 1500 lines, split into additional files
- **Logical Separation**: Split files based on functional boundaries and asset groups
- **Example Split Strategy**:
  ```
  nps_raw_ingestion.py      # Raw history ingestion assets (downloads)
  nps_data_processing.py    # CSV 정제 및 DuckDB 적재
  nps_postgres_simple.py    # DuckDB → Postgres 적재 및 증분 로딩
  nps_index_optimization.py # 분석/모델링 자산
  nps_kyc_etl.py            # SaaS 및 KYC 동기화 자산
  ```

> **Future improvement**: 이전 가이드에서는 분석·리포트 전용 모듈(`nps_reporting.py`, `cd_analytics.py`)이나 공용 유틸리티 모듈(`shared_utils.py`, `common_analytics.py`)을 별도로 두어 재사용 로직을 분리할 것을 제안했습니다. 현재 트리에서는 아직 반영되지 않았지만, 대용량 분석 또는 다중 프로젝트에서 공유되는 계산 로직이 늘어날 경우 이러한 파일명을 미리 예약해 두고 분리 계획을 유지하세요.

### Asset Organization Strategy

- **Helper Functions as Assets**: Convert significant helper functions into separate assets when:

  - The function performs substantial data processing
  - The result could be reused by multiple downstream assets
  - The function has clear business value or represents an important intermediate step
  - The function processes data that should be cached or monitored

- **Keep as Helper Functions**: When functions are:
  - Simple utility operations (formatting, validation)
  - Used only within a single asset
  - Perform minimal data transformation
  - Are purely computational without data dependencies

### Example Asset Separation

```python
# GOOD: Split into separate assets for reusability and monitoring
@dg.asset(
    group_name="NPS",
    tags={"data_tier": "bronze", "domain": "pension"}
)
def nps_his_download() -> dg.MaterializeResult:
    """Raw CSV 다운로드 (history/in 폴더 저장)"""
    pass

@dg.asset(
    deps=["nps_his_download"],
    group_name="NPS",
    tags={"data_tier": "silver", "domain": "pension"}
)
def nps_his_digest() -> dg.MaterializeResult:
    """CSV 인코딩 변환 및 DuckDB 정제"""
    pass

@dg.asset(
    deps=["nps_his_digest"],
    group_name="NPS",
    tags={"data_tier": "gold", "domain": "pension"}
)
def nps_his_postgres_append() -> dg.MaterializeResult:
    """정제된 데이터를 Postgres로 적재"""
    pass

# AVOID: Monolithic asset doing everything
@dg.asset
def nps_complete_pipeline() -> dg.MaterializeResult:
    """원시 다운로드부터 적재까지 단일 에셋으로 구현하지 마세요"""
    pass
```

## 2. Asset Management and Definitions Registration

### Overview

When adding new assets to the DAG project, proper registration in `definitions.py` is essential for Dagster to recognize and manage your assets. This section provides guidelines for organizing assets across modules and maintaining the central definitions file.

### Adding New Asset Modules

#### Step 1: Create Asset Module

When creating new asset modules, follow the established naming conventions:

```python
# Example: nps_postgres_simple.py
"""
NPS Postgres Sync - Gold Tier
국민연금 정제 데이터를 Postgres에 적재
"""

import dagster as dg
from dagster import AssetExecutionContext
from dagster_duckdb import DuckDBResource
from dag.resources import PostgresResource


@dg.asset(
    description="정제된 NPS 데이터를 Postgres warehouse로 적재",
    group_name="NPS",
    kinds={"python", "postgres"},
    tags={
        "domain": "finance",
        "data_tier": "gold",
        "source": "national_pension",
    },
    deps=["nps_his_digest"],
)
def nps_his_postgres_append(
    context: AssetExecutionContext,
    nps_duckdb: DuckDBResource,
    nps_postgres: PostgresResource,
) -> dg.MaterializeResult:
    """DuckDB에서 변환된 데이터를 Postgres 테이블에 병합"""
    # 적재 로직 구현
    pass
```

#### Step 2: Update definitions.py

새로운 모듈을 생성한 후에는 `dag/definitions.py`에 있는 중앙 정의를 함께 업데이트해야 합니다. 이 파일은 다섯 개의 프로젝트(CD, DS, IS, SS, NPS)와 공유 리소스/스케줄/센서를 모두 한곳에서 묶어 줍니다.

```python
# dag/definitions.py (발췌)
from dagster import (
    Definitions,
    EnvVar,
    build_last_update_freshness_checks,
    build_sensor_for_freshness_checks,
    load_asset_checks_from_modules,
    load_assets_from_modules,
)
from datetime import timedelta as _td
from dagster_duckdb import DuckDBResource
from dagster_docker import PipesDockerClient
from dagster_openai import OpenAIResource

from .resources import PostgresResource, TursoResource
from . import (  # noqa: TID252
    cd_raw_ingestion,
    cd_bppedd_processing,
    cd_display_processing,
    cd_marketcap_processing,
    cd_metrics_processing,
    cd_price_processing,
    cd_img_r2_processing,
    cd_img_r2_docker,
    cd_raw_ingestion_turso,
    cd_price_processing_turso,
    cd_bppedd_processing_turso,
    cd_marketcap_processing_turso,
    cd_metrics_processing_turso,
    cd_history_prices,
    cd_history_marketcaps,
    cd_history_bppedds,
    cd_img_r2_node,
)
from . import (  # noqa: TID252
    ds_img_r2_docker,
    ds_img_r2_processing,
    ds_img_r2_node,
)
from . import (  # noqa: TID252
    nps_raw_ingestion,
    nps_data_processing,
    nps_postgres_simple,
    nps_index_optimization,
    nps_kyc_etl,
)
from . import (  # noqa: TID252
    is_node,
    is_data_sync,
    is_data_unsync,
    is_data_llm,
    is_data_vlm,
    is_data_cluster,
)
from . import ss_node, ss_data_sync, ss_data_llm, ss_data_dictionary

from .jobs import (  # noqa: TID252
    nps_history_job,
    cd_daily_update_job,
    cd_daily_update_turso_job,
    cd_history_job,
    cd_docker_job,
    cd_node_job,
    ds_docker_job,
    ds_node_job,
    is_ingest_job,
    is_crawler_10min_job,
    is_crawler_interval_job,
    ss_ingest_job,
)
from .schedules import (  # noqa: TID252
    cd_daily_update_schedule,
    is_crawler_schedule_10min,
)
from .is_sensor_output_data import is_output_data_sensor
from .ss_data_sync import ss_crawl_sources
from .is_node import is_crawler_executor_interval_schedule
from .is_data_unsync import post_trends_asset, keyword_trends_asset
from .is_data_cluster import cluster_rotation_asset
```

프로젝트별로 `load_assets_from_modules`와 `load_asset_checks_from_modules` 호출이 준비되어 있으므로 새로운 모듈을 추가할 때는 해당 리스트에 모듈을 반드시 포함시켜야 합니다.

```python
all_cd_assets = load_assets_from_modules([
    cd_raw_ingestion,
    cd_bppedd_processing,
    cd_display_processing,
    cd_marketcap_processing,
    cd_metrics_processing,
    cd_price_processing,
    cd_img_r2_processing,
    cd_img_r2_docker,
    cd_raw_ingestion_turso,
    cd_price_processing_turso,
    cd_bppedd_processing_turso,
    cd_marketcap_processing_turso,
    cd_metrics_processing_turso,
    cd_history_prices,
    cd_history_marketcaps,
    cd_history_bppedds,
    cd_img_r2_node,
])

all_ds_assets = load_assets_from_modules([
    ds_img_r2_docker,
    ds_img_r2_processing,
    ds_img_r2_node,
])

all_nps_assets = load_assets_from_modules([
    nps_raw_ingestion,
    nps_data_processing,
    nps_postgres_simple,
    nps_index_optimization,
    nps_kyc_etl,
])

all_nps_asset_checks = load_asset_checks_from_modules([
    nps_raw_ingestion,
    nps_data_processing,
    nps_postgres_simple,
    nps_index_optimization,
    nps_kyc_etl,
])

all_is_assets = load_assets_from_modules([
    is_node,
    is_data_sync,
    is_data_unsync,
    is_data_llm,
    is_data_vlm,
    is_data_cluster,
])

all_ss_assets = load_assets_from_modules([
    ss_node,
    ss_data_sync,
    ss_data_llm,
    ss_data_dictionary,
])

post_trends_freshness_checks = build_last_update_freshness_checks(
    assets=[post_trends_asset],
    lower_bound_delta=_td(minutes=10),
)
cluster_rotation_freshness_checks = build_last_update_freshness_checks(
    assets=[cluster_rotation_asset],
    lower_bound_delta=_td(minutes=10),
)
keyword_trends_freshness_checks = build_last_update_freshness_checks(
    assets=[keyword_trends_asset],
    lower_bound_delta=_td(minutes=10),
)
_all_freshness_checks = (
    post_trends_freshness_checks
    + cluster_rotation_freshness_checks
    + keyword_trends_freshness_checks
)
freshness_checks_sensor = build_sensor_for_freshness_checks(
    freshness_checks=_all_freshness_checks,
)
```

`Definitions` 객체는 자산, 에셋 체크, 잡, 스케줄, 센서, 리소스를 모두 한 번에 등록합니다. 새로운 리소스를 도입할 때는 아래 블록을 참고해 동일한 패턴을 유지하세요.

```python
defs = Definitions(
    assets=[
        *all_cd_assets,
        *all_ds_assets,
        *all_nps_assets,
        *all_is_assets,
        *all_ss_assets,
    ],
    asset_checks=[
        *all_nps_asset_checks,
        *_all_freshness_checks,
    ],
    jobs=[
        nps_history_job,
        cd_history_job,
        cd_docker_job,
        cd_node_job,
        ds_docker_job,
        ds_node_job,
        cd_daily_update_job,
        cd_daily_update_turso_job,
        is_ingest_job,
        ss_ingest_job,
        is_crawler_10min_job,
        is_crawler_interval_job,
    ],
    schedules=[
        cd_daily_update_schedule,
        is_crawler_schedule_10min,
        is_crawler_executor_interval_schedule,
    ],
    sensors=[
        is_output_data_sensor,
        freshness_checks_sensor,
    ],
    resources={
        "docker_pipes_client": PipesDockerClient(),
        "cd_duckdb": DuckDBResource(database="data/cd.duckdb"),
        "nps_duckdb": DuckDBResource(database="data/nps.duckdb"),
        "ds_duckdb": DuckDBResource(database="data/ds.duckdb"),
        "cd_postgres": PostgresResource(
            host=EnvVar("CD_POSTGRES_HOST"),
            port=EnvVar.int("CD_POSTGRES_PORT"),
            user=EnvVar("CD_POSTGRES_USER"),
            password=EnvVar("CD_POSTGRES_PASSWORD"),
            database=EnvVar("CD_POSTGRES_DB"),
        ),
        "ds_postgres": PostgresResource(
            host=EnvVar("DS_POSTGRES_HOST"),
            port=EnvVar.int("DS_POSTGRES_PORT"),
            user=EnvVar("DS_POSTGRES_USER"),
            password=EnvVar("DS_POSTGRES_PASSWORD"),
            database=EnvVar("DS_POSTGRES_DB"),
        ),
        "nps_postgres": PostgresResource(
            host=EnvVar("NPS_POSTGRES_HOST"),
            port=EnvVar.int("NPS_POSTGRES_PORT"),
            user=EnvVar("NPS_POSTGRES_USER"),
            password=EnvVar("NPS_POSTGRES_PASSWORD"),
            database=EnvVar("NPS_POSTGRES_DB"),
        ),
        "is_postgres": PostgresResource(
            host=EnvVar("IS_POSTGRES_HOST"),
            port=EnvVar.int("IS_POSTGRES_PORT"),
            user=EnvVar("IS_POSTGRES_USER"),
            password=EnvVar("IS_POSTGRES_PASSWORD"),
            database=EnvVar("IS_POSTGRES_DB"),
        ),
        "cd_turso": TursoResource(
            url=EnvVar("TURSO_DATABASE_URL"),
            auth_token=EnvVar("TURSO_AUTH_TOKEN"),
        ),
        "openai": OpenAIResource(
            base_url="http://192.168.50.204:1234/v1",
            api_key="ollama",
        ),
        "omen": OpenAIResource(
            base_url="http://192.168.50.12:1234/v1",
            api_key="ollama",
        ),
        "oss": OpenAIResource(
            base_url="http://192.168.50.107:1234/v1",
            api_key="oss",
        ),
        "qwen25": OpenAIResource(
            base_url="http://192.168.50.107:1234/v1",
            api_key="qwen",
        ),
        "gemini": OpenAIResource(
            base_url="https://generativelanguage.googleapis.com/v1beta/openai/",
            api_key="AIzaSyD50H5babYxOodY-saVsGOk-1hS3Oo--Dw",
        ),
    },
)
```

> **필수 체크**: 모듈을 추가하거나 이름을 변경했다면 `load_assets_from_modules` 리스트, 관련 잡/스케줄/센서 등록, 그리고 리소스 매핑까지 모두 반영되었는지 반드시 확인하세요. Dagster UI에서 새 모듈이 표시되지 않으면 대부분 이 단계가 누락된 것입니다.

### Module Organization Strategies

#### Strategy 1: Project-Based Grouping (권장)

프로젝트별로 애셋을 그룹화하여 관리합니다. 실제 `definitions.py`에서는 아래와 같이 도메인/백엔드별 리스트를 유지합니다.

```python
# NPS 프로젝트 애셋들
nps_assets = load_assets_from_modules([
    nps_raw_ingestion,      # Bronze ingestion
    nps_data_processing,    # Silver cleansing
    nps_postgres_simple,    # Warehouse sync
    nps_index_optimization, # Analytics/optimizer
    nps_kyc_etl,            # SaaS ETL
])

# CD 프로젝트 애셋들 (DuckDB + 이미지 파이프라인)
cd_assets = load_assets_from_modules([
    cd_raw_ingestion,
    cd_bppedd_processing,
    cd_display_processing,
    cd_marketcap_processing,
    cd_metrics_processing,
    cd_price_processing,
    cd_img_r2_processing,
    cd_img_r2_docker,
    cd_img_r2_node,
])

# CD 프로젝트 Turso 미러링 애셋들
cd_turso_assets = load_assets_from_modules([
    cd_raw_ingestion_turso,
    cd_price_processing_turso,
    cd_bppedd_processing_turso,
    cd_marketcap_processing_turso,
    cd_metrics_processing_turso,
])

# CD 과거 데이터 백필 애셋들
cd_history_assets = load_assets_from_modules([
    cd_history_prices,
    cd_history_marketcaps,
    cd_history_bppedds,
])

# DS (DataStory) 컴퓨터 비전 파이프라인
ds_assets = load_assets_from_modules([
    ds_img_r2_docker,
    ds_img_r2_processing,
    ds_img_r2_node,
])

# IS (Isshoo) 크롤러 및 파생 데이터
is_assets = load_assets_from_modules([
    is_node,
    is_data_sync,
    is_data_unsync,
    is_data_llm,
    is_data_vlm,
    is_data_cluster,
])

# SS (Silla-Store) 크롤러 및 사전 데이터
ss_assets = load_assets_from_modules([
    ss_node,
    ss_data_sync,
    ss_data_llm,
    ss_data_dictionary,
])
```

#### Strategy 2: Backend/Environment Grouping

특정 백엔드나 실행환경(Turso, Docker, Node 등)에 따라 리스트를 분리합니다. 예를 들어 CD 파이프라인은 DuckDB·Turso·히스토리·이미지
실행 환경별로 별도 리스트를 유지하여 잡과 리소스가 올바르게 연결되도록 합니다. 새로운 모듈이 Turso를 사용한다면 `cd_turso_assets`와 같이
해당 백엔드 전용 리스트에 추가해야 합니다.

#### Strategy 3: Data Tier-Based Grouping (Future Enhancement)

이전 버전의 가이드에서는 Bronze/Silver/Gold 계층별 리스트를 유지해 **Medallion Architecture** 단위로 애셋을 제어하는 방식을 추천했습니다. 현재 레포에서는 프로젝트/백엔드 기준으로 그룹화하지만, 계층별 재처리가 필요해질 때를 대비해 아래 전략을 염두에 두세요.

```python
# Bronze 계층 모듈 예시 (원시 수집 중심)
bronze_modules = [
    nps_raw_ingestion,
    cd_raw_ingestion,
    ss_node,
    is_node,
]
bronze_assets = load_assets_from_modules(bronze_modules)

# Silver 계층 모듈 예시 (정제/정규화)
silver_modules = [
    nps_data_processing,
    cd_bppedd_processing,
    cd_marketcap_processing,
    cd_price_processing,
    ds_img_r2_processing,
    ss_data_sync,
    is_data_sync,
]
silver_assets = load_assets_from_modules(silver_modules)

# Gold 계층 모듈 예시 (분석/웨어하우스 적재)
gold_modules = [
    nps_postgres_simple,
    nps_index_optimization,
    cd_metrics_processing,
    cd_img_r2_docker,
    cd_img_r2_node,
    ss_data_llm,
    is_data_llm,
]
gold_assets = load_assets_from_modules(gold_modules)
```

> **Tip**: `data_tier` 태그가 이미 모든 자산에 붙어 있으므로, 위와 같은 리스트를 유지하면 `AssetSelection.tag("data_tier", "silver")`와 같은 잡 정의를 손쉽게 복원할 수 있습니다.

### Best Practices for Asset Registration

#### 1. Consistent Import Naming

```python
# GOOD: 명확한 모듈명 사용
from dag import cd_raw_ingestion, cd_marketcap_processing

# AVOID: 혼란스러운 별칭
from dag import cd_raw_ingestion as cri
```

#### 2. Logical Grouping

```python
# GOOD: 논리적 그룹화
all_nps_assets = load_assets_from_modules([
    nps_raw_ingestion,
    nps_data_processing,
    nps_postgres_simple,
    nps_index_optimization,
    nps_kyc_etl,
])

# AVOID: 무작위 순서
random_assets = load_assets_from_modules([
    cd_img_r2_docker,
    nps_raw_ingestion,
    cd_metrics_processing_turso,
])
```

#### 3. Documentation and Comments

```python
# 각 모듈의 역할을 명확히 문서화
nps_assets = load_assets_from_modules([
    nps_raw_ingestion,        # Raw history ingestion (Bronze)
    nps_data_processing,      # CSV 정제 및 변환 (Silver)
    nps_postgres_simple,      # DuckDB → Postgres 적재 (Gold)
    nps_index_optimization,   # 지수 최적화 분석 (Analytics)
    nps_kyc_etl,              # SaaS KYC 테이블 동기화
])
```

### Asset Discovery Verification

새로운 애셋이 올바르게 등록되었는지 확인:

```bash
# 현재 프로젝트 디렉토리로 이동
cd /workspace/dag

# 터미널에서 애셋 목록 확인
python -c "from dag.definitions import defs; print(f'Total assets: {len(defs.assets)}'); [print(f'- {asset.key}') for asset in defs.assets[:10]]"

# 개발 서버에서 확인
dagster dev
# → http://localhost:3000 에서 애셋 목록 확인

# 특정 모듈의 애셋만 확인
python -c "
from dagster import load_assets_from_modules
from dag import nps_raw_ingestion, nps_data_processing
assets = load_assets_from_modules([nps_raw_ingestion, nps_data_processing])
print(f'NPS Assets: {len(assets)}')
for asset in assets:
    print(f'- {asset.key}')
"
```

### 실제 프로젝트 예시 - NPS 모듈 분할 사례

**분할 전 (nps_download.py - 1596 lines):**

```python
# 단일 파일에 모든 기능이 포함됨
# - 설정 클래스들
# - 다운로드 로직
# - 데이터 처리 로직
# - 2개의 큰 애셋
```

**분할 후:**

```python
# nps_raw_ingestion.py (Bronze tier)
# - NPSConfig, NPSFilePaths, NPSURLConfig 등 설정 클래스
# - nps_his_download 애셋 (원시 데이터 다운로드)

# nps_data_processing.py (Silver tier)
# - nps_his_digest 애셋 (CSV 처리 및 인코딩 변환)
# - 데이터 정제 로직
```

**definitions.py 업데이트:**

```python
# 이전 (예시)
from dag import nps_example_module
all_example_assets = load_assets_from_modules([nps_example_module])

# 현재
from dag import nps_raw_ingestion, nps_data_processing
all_nps_assets = load_assets_from_modules([nps_raw_ingestion, nps_data_processing])
```

### Common Issues and Solutions

#### 문제 1: 모듈 임포트 에러

```python
# 문제: 모듈 경로 오류
from dag.nps_data_processing import *  # ❌

# 해결: 올바른 임포트
from dag import nps_data_processing    # ✅
```

#### 문제 2: 중복 애셋 이름

```python
# 문제: 여러 모듈에서 같은 애셋 이름 사용
# nps_data_processing.py에 'clean_data'
# cd_price_processing.py에 'clean_data'

# 해결: 고유한 애셋 이름 사용
# nps_data_processing.py에 'nps_clean_data'
# cd_price_processing.py에 'cd_clean_data'
```

#### 문제 3: 의존성 오류

```python
# 문제: 존재하지 않는 애셋 의존성
@asset(deps=["nonexistent_asset"])  # ❌

# 해결: 올바른 의존성 명시
@asset(deps=["nps_his_download"])   # ✅
```

#### 문제 4: 파일 분할 후 설정 클래스 누락

```python
# 문제: 설정 클래스가 분할된 모듈에서 접근 불가
# nps_data_processing.py에서 NPSConfig를 찾을 수 없음

# 해결: 설정 클래스를 올바른 모듈에서 임포트
from dag.nps_raw_ingestion import NPSConfig, NPSFilePaths
```

#### 문제 5: load_assets_from_modules 오류

```python
# 문제: 잘못된 모듈 리스트
all_assets = load_assets_from_modules([
    "nps_raw_ingestion",  # ❌ 문자열이 아닌 모듈 객체여야 함
])

# 해결: 올바른 모듈 객체 전달
from dag import nps_raw_ingestion
all_assets = load_assets_from_modules([
    nps_raw_ingestion,    # ✅ 모듈 객체
])
```

#### 디버깅 도구

```python
# 애셋 등록 상태 확인
def debug_assets():
    from dag.definitions import defs
    print(f"Total assets: {len(defs.assets)}")
    for asset in defs.assets:
        print(f"- {asset.key}: {asset.tags}")

# 모듈별 애셋 확인
def debug_module_assets(module_name):
    from dagster import load_assets_from_modules
    import importlib

    module = importlib.import_module(f"dag.{module_name}")
    assets = load_assets_from_modules([module])
    print(f"{module_name} assets: {len(assets)}")
    for asset in assets:
        print(f"- {asset.key}")
```

### File Split Decision Matrix

새로운 애셋을 추가할 때 파일 분할 결정 가이드:

| 기준          | 새 파일 생성 | 기존 파일에 추가 |
| ------------- | ------------ | ---------------- |
| 파일 크기     | >1500줄      | <1000줄          |
| 기능적 관련성 | 다른 도메인  | 같은 도메인      |
| 의존성 패턴   | 독립적       | 강하게 연결됨    |
| 데이터 계층   | 다른 계층    | 같은 계층        |
| 팀 소유권     | 다른 팀      | 같은 팀          |

### Asset Naming Conventions

```python
# 애셋 이름 패턴: {project}_{noun}_{verb}
nps_his_download           # NPS 원시 사업장 데이터 다운로드
nps_his_digest             # NPS CSV 정제 및 인코딩 변환
nps_his_postgres_append    # NPS Postgres 적재
cd_prices                  # CD 일별 OHLCV 수집
cd_digest_price            # CD 가격 정제 및 영구 테이블 적재
cd_populate_security_ranks # CD 종목 지표 순위 계산
```

### Function and Asset Name Consistency

**핵심 원칙**: 함수명과 asset명은 반드시 일치해야 합니다.

```python
# ✅ 올바른 예시 - 함수명과 asset명이 일치
@dg.asset(
    name="nps_his_download",  # Asset명
    group_name="NPS",
    tags={"data_tier": "bronze", "project": "nps"}
)
def nps_his_download(context: AssetExecutionContext) -> dg.MaterializeResult:
    # 함수명과 asset명이 정확히 일치
    pass

# ❌ 잘못된 예시 - 함수명과 asset명이 불일치
@dg.asset(
    name="nps_his_download",  # Asset명
    group_name="NPS"
)
def fetch_nps_data(context: AssetExecutionContext):  # 다른 함수명
    pass
```

**의존성 참조 시 주의사항**:

```python
# ✅ Asset명으로 의존성 참조
@dg.asset(
    deps=["nps_his_download"],  # Asset명 사용
    name="nps_his_digest"
)
def nps_his_digest(context: AssetExecutionContext):
    pass

# ❌ 함수명으로 의존성 참조하면 안됨
@dg.asset(
    deps=["fetch_nps_data"],  # 이렇게 하면 안됨
    name="nps_his_digest"
)
def nps_his_digest_wrong(context: AssetExecutionContext):
    pass
```

**일관성 체크리스트**:

- [ ] 함수명과 `name` 파라미터가 정확히 일치
- [ ] 다른 asset에서 의존성 참조 시 asset명 사용
- [ ] 파일명도 가능한 한 주요 asset명과 일치시키기
- [ ] 변경 시 관련된 모든 참조 업데이트

### Quick Reference - Asset Module 추가 체크리스트

#### ✅ 새로운 애셋 모듈 추가 시 확인 사항

1. **파일 생성**

   - [ ] 적절한 프로젝트 접두사 사용 (`nps_`, `cd_`)
   - [ ] 파일 크기 1000-1500줄 범위 유지
   - [ ] 명확한 데이터 계층 분리 (Bronze/Silver/Gold)

2. **애셋 정의**

   - [ ] `@asset` 데코레이터에 적절한 메타데이터 포함
   - [ ] `group_name`, `tags`, `description` 설정
   - [ ] 의존성(`deps`) 올바르게 명시
   - [ ] `MaterializeResult` 반환

3. **definitions.py 업데이트**

   - [ ] 새 모듈 임포트 추가
   - [ ] `load_assets_from_modules`에 모듈 추가
   - [ ] 그룹화 전략에 따라 적절히 배치

4. **검증**
   - [ ] `python -c "from dag.definitions import defs; print(len(defs.assets))"` 실행
   - [ ] `dagster dev`로 UI에서 애셋 확인
   - [ ] 의존성 그래프 확인

#### 🔧 자주 사용하는 명령어

```bash
# 현재 등록된 애셋 수 확인
python -c "from dag.definitions import defs; print(f'Assets: {len(defs.assets)}')"

# 특정 모듈의 애셋 확인
python -c "
from dagster import load_assets_from_modules
from dag import nps_raw_ingestion
assets = load_assets_from_modules([nps_raw_ingestion])
[print(asset.key) for asset in assets]
"

# 개발 서버 시작
dagster dev

# 에러 체크
python -m py_compile dag/새로운모듈.py
```

#### 📝 모듈 템플릿

```python
"""
{Project} {Tier} - {Purpose}
{한글 설명}
"""

from dagster import AssetExecutionContext, MaterializeResult, asset
import dagster as dg
from dagster_duckdb import DuckDBResource

@asset(
    description="{상세 설명} - {Tier} Tier",
    group_name="{PROJECT}",
    kinds={"python", "{domain}"},
    tags={
        "domain": "{domain}",
        "data_tier": "{tier}",
        "source": "{source}"
    },
    deps=["{dependency_asset}"]  # 의존성이 있는 경우
)
def {project}_{tier}_{name}(
    context: AssetExecutionContext,
    {project}_duckdb: DuckDBResource,
) -> MaterializeResult:
    """{한글 설명}"""

    # 구현 로직

    return MaterializeResult(
        metadata={
            "dagster/row_count": row_count,
            "processing_time": processing_time,
            # 기타 메타데이터
        }
    )
```

## 3. Job Management and Registration

### Overview

Dagster Jobs allow you to group related assets and execute them together as a cohesive unit. Jobs are particularly useful for creating data pipelines that process multiple assets in a specific sequence or executing all assets within a domain area.

### Job Definition Strategy

현재 `dag/jobs.py`에서는 AssetSelection의 downstream 기능과 그룹 기반 선택을 조합해 각 파이프라인 실행 단위를 정의합니다.

```python
import dagster as dg
from .partitions import daily_exchange_category_partition

cd_targets = dg.AssetSelection.assets("cd_stockcode").downstream()
cd_targets_turso = dg.AssetSelection.assets("cd_stockcode_turso").downstream()
cd_history_targets = dg.AssetSelection.assets(
    "historical_prices",
    "historical_marketcaps",
    "historical_bppedds",
).downstream()
cd_docker_targets = dg.AssetSelection.assets("cd_img_r2_docker").downstream()
cd_node_targets = dg.AssetSelection.assets("cd_img_r2_node").downstream()
ds_docker_targets = dg.AssetSelection.assets("ds_img_r2_docker").downstream()
ds_node_targets = dg.AssetSelection.assets("ds_img_r2_node").downstream()
is_targets = dg.AssetSelection.assets("posts_asset").downstream()
ss_targets = dg.AssetSelection.assets("ss_crawl_sources").downstream()
nps_targets = dg.AssetSelection.groups("NPS")
```

#### Tier-Based & Cross-Domain Jobs (Future Enhancement)

이전 가이드에서는 `data_tier` 태그나 공통 태그를 활용해 다중 프로젝트 자산을 한 번에 실행하는 잡을 제안했습니다. 현재 레포에서는 프로젝트별 잡에 집중하지만, 계층별 백필이나 교차 분석이 필요해질 때 아래 패턴을 재도입할 수 있도록 문서를 유지합니다.

```python
# Bronze 계층 전체를 주기적으로 재수집하는 Job
bronze_job = dg.define_asset_job(
    name="bronze_ingestion_job",
    selection=dg.AssetSelection.tag("data_tier", "bronze"),
    description="모든 Bronze 계층 데이터를 일괄 재수집",
    tags={"tier": "bronze", "type": "ingestion"},
)

# Gold 계층 교차 분석 Job
cross_domain_job = dg.define_asset_job(
    name="cross_domain_analytics",
    selection=dg.AssetSelection.tag("data_tier", "gold"),
    description="프로젝트 전반의 Gold 자산을 결합 분석",
    tags={"type": "analytics", "priority": "high"},
)
```

> **Reminder**: 이러한 잡을 정의할 경우 `dag/jobs.py`와 `definitions.py` 모두에 등록하고, `data_tier` 태그가 일관되게 유지되는지 확인하세요.

### Creating Job Modules

모든 job 정의는 `dag/jobs.py`에 위치합니다. 실제 파일 구조는 다음과 같습니다.

```python
# dag/jobs.py
import dagster as dg
from .partitions import daily_exchange_category_partition

nps_history_job = dg.define_asset_job(
    name="nps_history_job",
    selection=nps_targets,
    description="NPS 과거 데이터 한번에 가지고 오기",
    tags={"source": "NPS", "type": "history"},
)

cd_daily_update_job = dg.define_asset_job(
    name="cd_daily_update_job",
    partitions_def=daily_exchange_category_partition,
    selection=cd_targets,
    description="매일 주식 가격 업데이트",
    tags={"type": "daily_update"},
)

cd_daily_update_turso_job = dg.define_asset_job(
    name="cd_daily_update_turso_job",
    partitions_def=daily_exchange_category_partition,
    selection=cd_targets_turso,
    description="매일 주식 가격 업데이트 (TursoDB)",
    tags={"type": "daily_update"},
)

cd_history_job = dg.define_asset_job(
    name="cd_history_job",
    selection=cd_history_targets,
    description="과거 데이터 한번에 가지고 오기",
    tags={"type": "history"},
)

cd_docker_job = dg.define_asset_job(
    name="cd_docker_job",
    selection=cd_docker_targets,
    description="CD 이미지 처리용 도커 컨테이너를 실시간 로그 스트리밍으로 실행합니다.",
    tags={"source": "docker", "type": "realtime"},
)

cd_node_job = dg.define_asset_job(
    name="cd_node_job",
    selection=cd_node_targets,
    description="CD 이미지 처리용 노드 컨테이너를 실시간 로그 스트리밍으로 실행합니다.",
    tags={"source": "node", "type": "realtime"},
)

ds_docker_job = dg.define_asset_job(
    name="ds_docker_job",
    selection=ds_docker_targets,
    description="DS 이미지 처리용 도커 컨테이너를 실시간 로그 스트리밍으로 실행합니다.",
    tags={"source": "docker", "type": "realtime"},
)

ds_node_job = dg.define_asset_job(
    name="ds_node_job",
    selection=ds_node_targets,
    description="DS 이미지 처리용 노드 컨테이너를 실시간 로그 스트리밍으로 실행합니다.",
    tags={"source": "node", "type": "realtime"},
)

is_ingest_job = dg.define_asset_job(
    name="is_ingest_job",
    selection=is_targets,
    description="IS 크롤러 output_data 적재 전체 파이프라인",
    tags={"source": "IS", "type": "ingest", "dagster/priority": "3"},
)

ss_ingest_job = dg.define_asset_job(
    name="ss_ingest_job",
    selection=ss_targets,
    description="SS 크롤러 ssdata 적재 전체 파이프라인",
    tags={"source": "SS", "type": "ingest", "dagster/priority": "3"},
)

is_crawler_10min_job = dg.define_asset_job(
    name="is_crawler_job_10min",
    selection=dg.AssetSelection.assets("is_crawler_executor"),
    description="IS 크롤러 executor 을 매 10분마다 실행합니다. 신규 게시글만 크롤링",
    tags={"type": "schedule", "interval": "10min"},
)

is_crawler_interval_job = dg.define_asset_job(
    name="is_crawler_interval_job",
    selection=dg.AssetSelection.assets("is_crawler_executor_interval"),
    description="IS 크롤러 executor 을 매 30분, 1시간, 3시간, 6시간, 12시간, 24시간, 48시간마다 실행합니다.",
    tags={"type": "schedule", "interval": "30min, 1h, 3h, 6h, 12h, 24h, 48h"},
)
```

새로운 Job을 추가할 때는 위 패턴을 따라 `dag/jobs.py`에 정의하고, 앞서 설명한 `Definitions`의 `jobs` 리스트에 동일한 이름을 추가합니다.

```python
# 신규 Job 예시: DS 백필 파이프라인 (필요 시 새로운 시드 애셋 정의 후 사용)
ds_backfill_targets = dg.AssetSelection.assets("ds_backfill_seed").downstream()
ds_backfill_job = dg.define_asset_job(
    name="ds_backfill_job",
    selection=ds_backfill_targets,
    description="DS 백필 파이프라인",
    tags={"type": "backfill"},
)

# dag/definitions.py
from .jobs import (
    nps_history_job,
    cd_history_job,
    cd_docker_job,
    cd_node_job,
    ds_docker_job,
    ds_node_job,
    cd_daily_update_job,
    cd_daily_update_turso_job,
    is_ingest_job,
    ss_ingest_job,
    is_crawler_10min_job,
    is_crawler_interval_job,
    ds_backfill_job,
)

defs = Definitions(
    ...,
    jobs=[
        nps_history_job,
        cd_history_job,
        cd_docker_job,
        cd_node_job,
        ds_docker_job,
        ds_node_job,
        cd_daily_update_job,
        cd_daily_update_turso_job,
        is_ingest_job,
        ss_ingest_job,
        is_crawler_10min_job,
        is_crawler_interval_job,
        ds_backfill_job,
    ],
    ...,
)
```

### Asset Group Name 설정 (CRITICAL)

Job이 올바르게 Asset을 선택하려면 Asset의 `group_name`이 정확히 설정되어야 합니다. 현재 프로젝트에서 사용하는 대표적인 그룹명은 다음과 같습니다.

- `CD`, `CD_TURSO`, `CD_HISTORY`
- `DS`
- `IS`
- `SS`, `SS_Dictionary`
- `NPS`, `NPS_SaaS`

각 Asset은 실제로 해당 그룹 중 하나를 사용하고 있으며, Job의 AssetSelection과 반드시 일치해야 합니다.

```python
# nps_raw_ingestion.py
@asset(
    description="NPS 히스토리 데이터를 병렬 다운로드 - Bronze Tier",
    group_name="NPS",  # ← Job에서 참조하는 그룹명과 일치해야 함
    kinds={"csv"},
    tags={
        "domain": "finance",
        "data_tier": "bronze",
        "source": "national_pension"
    },
)
def nps_his_download(context: AssetExecutionContext) -> dg.MaterializeResult:
    pass

# nps_data_processing.py
@asset(
    description="다운로드된 NPS CSV 파일들을 UTF-8로 인코딩 변환 - Silver Tier",
    group_name="NPS",  # ← 같은 그룹명 사용
    kinds={"python", "csv"},
    tags={
        "domain": "finance",
        "data_tier": "silver",
        "source": "national_pension"
    },
    deps=["nps_his_download"]
)
def nps_his_digest(context: AssetExecutionContext) -> dg.MaterializeResult:
    pass
```

### Job 실행 및 검증

#### 개발 환경에서 Job 확인

```bash
# 현재 등록된 Job 확인
python -c "
from dag.definitions import defs
print(f'Total jobs: {len(defs.jobs)}')
for job in defs.jobs:
    print(f'- {job.name}: {job.description}')
"

# Job에 포함된 Asset 확인
python -c "
from dag.jobs import nps_history_job
from dag.definitions import defs
job = nps_history_job
asset_keys = job.asset_selection.resolve(defs.assets)
print(f'Job {job.name} includes {len(asset_keys)} assets:')
for key in asset_keys:
    print(f'- {key}')
"

# Dagster UI에서 확인
dagster dev
# → http://localhost:3000/jobs 에서 Job 목록 확인
```

### Job 실행 방법

#### 1. Dagster UI에서 실행

- http://localhost:3000/jobs 접속
- 원하는 Job 선택
- "Launch Run" 버튼 클릭

#### 2. CLI에서 실행

```bash
# 특정 Job 실행
dagster job execute nps_history_job

# Job 실행 계획 확인 (dry-run)
dagster job execute nps_history_job --dry-run
```

#### 3. Python 코드에서 실행

```python
from dagster import execute_job
from dag.definitions import defs
from dag.jobs import nps_history_job

# Job 실행
result = execute_job(nps_history_job, resources=defs.resources)
print(f"Job success: {result.success}")
```

### Job 설계 Best Practices

#### 1. 논리적 그룹화

```python
# GOOD: 시드 Asset을 기준으로 downstream 선택
cd_targets = dg.AssetSelection.assets("cd_stockcode").downstream()
is_targets = dg.AssetSelection.assets("posts_asset").downstream()

# AVOID: 태그 기반으로만 선택하면 필요한 자산이 누락될 수 있음
bronze_only = dg.AssetSelection.tag("data_tier", "bronze")
```

#### 2. 명확한 Job 이름 및 설명

```python
# GOOD: 목적이 명확한 이름과 설명
cd_daily_update_job = dg.define_asset_job(
    name="cd_daily_update_job",
    partitions_def=daily_exchange_category_partition,
    selection=cd_targets,
    description="매일 주식 가격 업데이트",
    tags={"type": "daily_update"},
)

# AVOID: 모호하거나 목적을 알 수 없는 이름
data_job = dg.define_asset_job(name="data_job")
```

#### 3. 적절한 태그 활용

```python
# 운영/모니터링 목적 태그 예시
is_crawler_10min_job = dg.define_asset_job(
    name="is_crawler_job_10min",
    selection=dg.AssetSelection.assets("is_crawler_executor"),
    description="IS 크롤러 executor 을 매 10분마다 실행",
    tags={"type": "schedule", "interval": "10min"},
)

# 실시간 로그 수집 태그 예시
cd_docker_job = dg.define_asset_job(
    name="cd_docker_job",
    selection=cd_docker_targets,
    description="CD 이미지 처리용 도커 컨테이너를 실시간 로그 스트리밍으로 실행",
    tags={"source": "docker", "type": "realtime"},
)
```

### Common Issues and Solutions

#### 문제 1: Job이 Asset을 찾지 못함

```python
# 문제: Asset의 group_name과 Job의 selection이 불일치
# Asset: group_name="nps"
# Job: AssetSelection.groups("NPS")

# 해결: 대소문자 및 명칭 일치
@asset(group_name="NPS")  # Job에서 사용하는 그룹명과 정확히 일치
def my_asset():
    pass
```

#### 문제 2: Job이 definitions.py에서 인식되지 않음

```python
# 문제: Job 임포트 누락
from .jobs import nps_history_job  # ← 임포트 필요

defs = Definitions(
    jobs=[nps_history_job],  # ← 등록 필요
)
```

#### 문제 3: 순환 의존성 에러

```python
# 문제: Job이 존재하지 않는 Asset을 참조
# 해결: Asset이 먼저 정의되고 등록되었는지 확인
from dag import nps_raw_ingestion, nps_data_processing  # Asset 모듈 먼저 임포트
from .jobs import nps_history_job  # Job 나중에 임포트
```

### Quick Reference - Job 추가 체크리스트

#### ✅ 새로운 Job 추가 시 확인 사항

1. **Job 정의**

   - [ ] `dag/jobs.py`에 Job 정의 추가
   - [ ] 명확한 이름과 설명 설정
   - [ ] 적절한 AssetSelection 방식 선택
   - [ ] 의미있는 태그 추가

2. **Asset 그룹 설정**

   - [ ] 관련 Asset들의 `group_name` 일치 확인
   - [ ] Asset이 올바른 태그를 가지고 있는지 확인

3. **definitions.py 등록**

   - [ ] Job 임포트 추가
   - [ ] `jobs` 리스트에 Job 추가

4. **검증**
   - [ ] Job 목록 확인: `python -c "from dag.definitions import defs; print([j.name for j in defs.jobs])"`
   - [ ] Job 내 Asset 확인: UI 또는 CLI에서 확인
   - [ ] Job 실행 테스트

#### 🔧 Job 관련 자주 사용하는 명령어

```bash
# Job 목록 확인
python -c "from dag.definitions import defs; [print(f'{j.name}: {j.description}') for j in defs.jobs]"

# 특정 Job의 Asset 목록 확인
python -c "
from dag.jobs import nps_history_job
from dag.definitions import defs
assets = nps_history_job.asset_selection.resolve(defs.assets)
[print(asset) for asset in assets]
"

# Job 실행 (CLI)
dagster job execute nps_history_job

# Job 실행 계획 확인
dagster job execute nps_history_job --dry-run
```

#### 🎯 Job-Asset 연결 검증 방법 (검증된 방법)

**특정 그룹에 포함된 에셋들 확인 (가장 유용):**

```bash
# NPS 그룹의 모든 에셋들 확인 - nps_history_job에 포함되는 에셋들
python -c "
from dag.definitions import defs
from dagster import AssetSelection

asset_graph = defs.get_asset_graph()
nps_assets = AssetSelection.groups('NPS').resolve(asset_graph)
print('🎯 NPS 그룹에 포함된 에셋들:')
for asset_key in nps_assets:
    print(f'  - {asset_key}')
print(f'\n📊 총 {len(nps_assets)}개 에셋이 nps_history_job에 포함됩니다')
"
```

**전체 에셋 등록 상태 확인:**

```bash
# 현재 등록된 총 에셋 수 확인
python -c "from dag.definitions import defs; print(f'등록된 에셋 수: {len(defs.assets)}')"

# 모든 에셋 목록과 그룹 확인
python -c "
from dag.definitions import defs
for asset in defs.assets:
    asset_def = defs.get_asset_graph().get(asset)
    group = getattr(asset_def, 'group_name', 'No Group')
    print(f'{asset}: 그룹[{group}]')
"
```

**Asset 의존성 관계 확인:**

```bash
# 특정 에셋의 의존성 확인
python -c "
from dag.definitions import defs
from dagster import AssetKey

asset_graph = defs.get_asset_graph()
target_key = AssetKey(['nps_data_processing'])

if target_key in asset_graph.all_asset_keys:
    deps = asset_graph.get(target_key).dependency_keys
    print(f'nps_data_processing 의존성: {list(deps)}')
else:
    print('nps_data_processing 에셋을 찾을 수 없습니다')
"
```

**Job 정의 및 선택 로직 확인:**

```bash
# Job의 AssetSelection 검증
python -c "
from dag.jobs import nps_history_job
print(f'Job 이름: {nps_history_job.name}')
print(f'Selection 타입: {type(nps_history_job.asset_selection)}')
print(f'Description: {nps_history_job.description}')
"
```

## 4. Data Tier Tagging System (Medallion Architecture)

### Architecture Overview

The DAG project uses Bronze, Silver, and Gold tags representing **Medallion Architecture** data quality layers for systematic data pipeline organization.

### 🥉 Bronze Layer - Raw Data Collection

**Purpose**: Store raw data from external sources without transformation

**Characteristics**:

- Original data preserved as-is
- No data validation or cleansing
- Formats: CSV, JSON, API responses
- Direct ingestion from sources

**Examples**:

- `nps_his_download` - Raw NPS workplace history download
- `cd_prices` - Daily OHLCV ingestion from KRX

```python
@dg.asset(
    tags={"data_tier": "bronze", "domain": "pension"},
    group_name="NPS"
)
def nps_his_download() -> dg.MaterializeResult:
    """Raw NPS workplace history download"""
    pass
```

### 🥈 Silver Layer - Data Cleansing and Standardization

**Purpose**: Transform Bronze data into clean, standardized format for analysis

**Characteristics**:

- Data type conversion (string → number, date)
- Null value handling and duplicate removal
- Column name standardization (snake_case)
- Basic data validation

**Examples**:

- `nps_his_digest` - Cleansed and re-encoded NPS history
- `cd_digest_price` - Standardized daily stock price snapshot

```python
@dg.asset(
    deps=["nps_his_download"],
    tags={"data_tier": "silver", "domain": "pension"},
    group_name="NPS"
)
def nps_his_digest() -> dg.MaterializeResult:
    """Cleanse and standardize workplace data"""
    pass
```

### 🥇 Gold Layer - Business Logic and Analytics

**Purpose**: Apply business logic to create analysis-ready data

**Characteristics**:

- Complex calculations and aggregations
- Multi-source data integration
- Business rule implementation
- Dashboard/report ready data

**Examples**:

- `nps_his_postgres_append` - Warehouse sync of cleansed NPS data
- `cd_populate_security_ranks` - Stock ranking analytics
- `nps_pension_staging_indexes` - SaaS 인덱스/함수 생성 및 최적화

```python
@dg.asset(
    deps=["nps_his_digest"],
    tags={"data_tier": "gold", "domain": "pension"},
    group_name="NPS"
)
def nps_his_postgres_append() -> dg.MaterializeResult:
    """Append cleansed data into Postgres"""
    pass
```

### Data Flow Examples

#### NPS Project Data Pipeline:

```
nps_his_download (Bronze)
    ↓ data cleansing
nps_his_digest (Silver)
    ↓ warehouse load
nps_his_postgres_append (Gold)
```

#### CD Project Data Pipeline:

```
cd_prices (Bronze)
    ↓ data standardization
cd_digest_price (Silver)
    ↓ analytics enrichment
cd_populate_security_ranks (Gold)
```

### Tag Usage Guidelines

```python
# Bronze - Raw data
tags={"data_tier": "bronze", "domain": "pension", "source": "government"}

# Silver - Cleaned data
tags={"data_tier": "silver", "domain": "pension", "quality": "validated"}

# Gold - Analytics data
tags={"data_tier": "gold", "domain": "analytics", "business_critical": "true"}
```

### Benefits of Layered Architecture

1. **Data Traceability**: Easy identification of data processing stage
2. **Reusability**: Each layer serves different use cases
3. **Debugging**: Quick identification of issues at specific stages
4. **Performance**: Selective reprocessing of required layers only

## 5. Basic Asset Definition Style

### Use @dg.asset Decorator

- All data assets should be defined using the `@dg.asset` style.
- Utilize rich metadata to facilitate search and filtering capabilities.

### Tags Guidelines

- **Important**: Tags are the primary way to organize assets in Dagster.
- They appear in the UI and can be used for searching and filtering assets in the Asset catalog in Dagster+.
- Tags are structured as key-value pairs of strings.
- Write meaningful tags as they can be used for searching and various other purposes later.

### Result Return Pattern

- Always return results using `dg.MaterializeResult` to make them easily viewable in the Dagster UI during the day.
- **For small data**: Display the entire dataset
- **For large data**: Show data samples from the beginning and end

### Basic Example Code

```python
import dagster as dg

@dg.asset(
    kinds={"python", "duckdb"},
    tags={"domain": "marketing", "pii": "true"},
    group_name="ingestion",
)
def products(duckdb: DuckDBResource) -> dg.MaterializeResult:
    with duckdb.get_connection() as conn:
        conn.execute(
            """
            create or replace table products as (
                select * from read_csv_auto('data/products.csv')
            )
            """
        )

        preview_query = "select * from products limit 10"
        preview_df = conn.execute(preview_query).fetchdf()
        row_count = conn.execute("select count(*) from products").fetchone()
        count = row_count[0] if row_count else 0

        return dg.MaterializeResult(
            metadata={
                "row_count": dg.MetadataValue.int(count),
                "preview": dg.MetadataValue.md(preview_df.to_markdown(index=False)),
            }
        )
```

## 6. Schema Metadata Definition

### Overview

If the schema of your asset is pre-defined, you can attach it as definition metadata. If the schema is only known when an asset is materialized, you can attach it as metadata to the materialization.

### How to Attach Schema Metadata to an Asset

- Construct a TableSchema object with TableColumn entries describing each column in the table
- Attach the TableSchema object to the asset as part of the metadata parameter under the dagster/column_schema key
- Attachment location: This can be attached to your asset definition, or to the MaterializeResult object returned by the asset function

### Definition Metadata Example

```python
from dagster import AssetKey, MaterializeResult, TableColumn, TableSchema, asset

@asset(
    deps=[AssetKey("source_bar"), AssetKey("source_baz")],
    metadata={
        "dagster/column_schema": TableSchema(
            columns=[
                TableColumn(
                    "name",
                    "string",
                    description="The name of the person",
                ),
                TableColumn(
                    "age",
                    "int",
                    description="The age of the person",
                ),
            ]
        )
    },
)
def my_asset():
    # Asset implementation logic
    pass
```

## 7. Row Count Metadata

### Purpose

- Display the latest row count
- Track changes in row count over time
- Use this information to monitor data quality

### Implementation Method

Attach a numerical value to the dagster/row_count key in the metadata parameter of the MaterializeResult object returned by the asset function.

```python
import pandas as pd
from dagster import AssetKey, MaterializeResult, asset

@asset(deps=[AssetKey("source_bar"), AssetKey("source_baz")])
def my_asset():
    my_df: pd.DataFrame = ...

    yield MaterializeResult(metadata={"dagster/row_count": 374})
```

## 8. Data Validation Pattern

### Use @dg.asset_check

For data validation, use dg.AssetCheckResult and implement it in the following style:

### Validation Function Example

```python
from collections import Counter
import dagster as dg

@dg.asset_check(
    asset=fine_tuned_model,
    additional_ins={"data": dg.AssetIn("enriched_graphic_novels")},
    description="Compare fine-tuned model against base model accuracy",
)
def fine_tuned_model_accuracy(
    context: dg.AssetCheckExecutionContext,
    openai: OpenAIResource,
    fine_tuned_model,
    data,
) -> dg.AssetCheckResult:
    validation = data.sample(n=constants.VALIDATION_SAMPLE_SIZE)

    models = Counter()
    base_model = constants.MODEL_NAME
    with openai.get_client(context) as client:
        for data in [row for _, row in validation.iterrows()]:
            for model in [fine_tuned_model, base_model]:
                model_answer = model_question(
                    client,
                    model,
                    data,
                    categories=constants.CATEGORIES,
                )
                if model_answer == data["category"]:
                    models[model] += 1

    model_accuracy = {
        fine_tuned_model: models[fine_tuned_model] / constants.VALIDATION_SAMPLE_SIZE,
        base_model: models[base_model] / constants.VALIDATION_SAMPLE_SIZE,
    }

    if model_accuracy[fine_tuned_model] < model_accuracy[base_model]:
        return dg.AssetCheckResult(
            passed=False,
            severity=dg.AssetCheckSeverity.WARN,
            description=f"{fine_tuned_model} has lower accuracy than {base_model}",
            metadata=model_accuracy,
        )
    else:
        return dg.AssetCheckResult(
            passed=True,
            metadata=model_accuracy,
        )
```

## 9. Kinds Configuration

### Limitations and Usage

- Maximum 3 kinds are allowed per asset
- Like tags, kinds are also used for search and filtering
- Only the following predefined values are allowed:

### Allowed Kinds Values

airbyte, airflow, airliftmapped, airtable, athena, atlan, aws, awsstepfunction,
awsstepfunctions, axioma, azure, azureml, bigquery, cassandra, catboost, celery,
census, chalk, claude, clickhouse, cockroachdb, collibra, cplus, cplusplus,
csharp, cube, dask, databricks, datadog, datahub, db2, dbt, dbtcloud, deepseek,
deltalake, denodo, dify, dingtalk, discord, dlt, dlthub, docker, doris, doubao,
druid, duckdb, elasticsearch, evidence, excel, facebook, fivetran, flink, gcp,
gcs, gemini, github, gitlab, go, google, googlecloud, googledrive, googlesheets,
grafana, graphql, greatexpectations, hackernews, hackernewsapi, hadoop, hashicorp,
hex, hightouch, hudi, huggingface, huggingfaceapi, iceberg, icechunk, impala,
instagram, ipynb, java, javascript, json, jupyter, k8s, kafka, kedro, kubernetes,
lakefs, lightgbm, linear, linkedin, llama, looker, mariadb, matplotlib, meltano,
meta, metabase, microsoft, minio, mistral, mlflow, modal, mongodb, montecarlo,
mysql, net, notdiamond, noteable, notion, numpy, omni, openai, openmetadata,
optuna, oracle, pagerduty, pandas, pandera, papermill, papertrail, parquet,
pinot, plotly, plural, polars, postgres, postgresql, powerbi, prefect, presto,
pulsar, pydantic, pyspark, python, pytorch, pytorchlightning, qwen, r, r2,
rabbitmq, ray, react, reddit, redis, redpanda, redshift, rockset, rust, s3,
sagemaker, salesforce, scala, scikitlearn, scipy, scylladb, sdf, secoda, segment,
sharepoint, shell, shopify, sigma, slack, sling, snowflake, snowpark, soda,
spanner, spark, sql, sqlite, sqlmesh, sqlserver, starrocks, stepfunction,
stepfunctions, stitch, stripe, supabase, superset, tableau, talend, teams,
tecton, tensorflow, teradata, thoughtspot, tiktok, toml, treasuredata, trino,
twilio, twitter, typescript, vercel, volcengine, wandb, weaviate, wechat, x,
xgboost, youtube, bronze, silver, gold, dag, task, table, view, dataset,
semanticmodel, source, seed, file, dashboard, report, notebook, workbook, csv,

## 10. Development Philosophy

### Core Design Principles

#### Fast-Fail & Simplicity First

- **Fail Fast Philosophy**: Let errors occur early and visibly rather than masking them with excessive error handling
- **Trust Dagster**: Rely on Dagster's built-in retry mechanisms instead of building custom retry logic
- **Simple Process Design**: Design straightforward, linear data processing flows
- **Avoid Over-Engineering**: Don't build multiple fallback layers or complex error recovery systems

#### Performance-Oriented Development

- **Speed as Priority**: Optimize for processing speed and system performance
- **Efficient Data Operations**: Use vectorized operations, efficient data types, and optimized queries
- **Minimize Memory Footprint**: Process data in chunks, release resources promptly
- **Profile Critical Paths**: Identify and optimize bottlenecks in data processing

#### Smart Error Handling Strategy

```python
# GOOD: Simple, fast-fail approach
@dg.asset
def nps_process_data(raw_data) -> dg.MaterializeResult:
    """Process data with simple error handling."""
    # Let pandas/SQL errors bubble up - Dagster will handle retries
    processed_data = raw_data.dropna().astype({'amount': 'float64'})

    if processed_data.empty:
        raise ValueError("No valid data after processing")

    return dg.MaterializeResult(
        metadata={"row_count": dg.MetadataValue.int(len(processed_data))}
    )

# AVOID: Over-engineered error handling
@dg.asset
def nps_over_engineered_process(raw_data) -> dg.MaterializeResult:
    """Don't do this - too much defensive coding."""
    try:
        try:
            # Multiple try-catch layers
            processed_data = raw_data.dropna()
        except Exception as e:
            # Custom retry logic (Dagster already provides this)
            time.sleep(1)
            processed_data = raw_data.dropna()
    except Exception as e:
        # Fallback to alternative processing (unnecessary complexity)
        processed_data = alternative_processing(raw_data)

    # Too many validation layers
    if processed_data is not None and not processed_data.empty and len(processed_data) > 0:
        # This level of checking is excessive
        pass
```

#### Data Analysis Encouragement

- **Deep Analytics Welcome**: Comprehensive data analysis and statistical computations are encouraged
- **Insight Generation**: Build rich analytics assets for business insights
- **Statistical Processing**: Perform complex statistical analysis when it adds business value
- **Visualization Support**: Create assets that support dashboards and reporting

### Dagster Asset Utilization Strategy

#### Maximizing Asset Concepts

- **Asset-First Thinking**: Design around data assets rather than traditional functions
- **Rich Metadata**: Use comprehensive metadata for observability and monitoring
- **Asset Dependencies**: Create meaningful dependencies that reflect business logic
- **Asset Grouping**: Organize related assets into logical groups

#### Context7 MCP Integration

When working with Dagster and need guidance:

```python
# When unsure about Dagster patterns, use Context7 MCP
# Example questions to ask Context7:
# - "How to implement asset partitioning in Dagster?"
# - "Best practices for Dagster asset metadata?"
# - "How to handle large datasets efficiently in Dagster?"
# - "Dagster asset dependency patterns for data pipelines?"

@dg.asset
def example_asset_with_best_practices():
    """
    If unsure about implementation:
    1. Check docs/coding-guidelines.md first
    2. Consult Context7 MCP for Dagster-specific guidance
    3. Follow established patterns in existing assets
    """
    pass
```

## 11. Best Practices Summary

### File Management and Organization

- **Project Prefixes**: Always use `nps_` or `cd_` prefixes for new files
- **File Size Limits**: Keep files between 1000-1500 lines for maintainability
- **Logical File Splitting**: Split files by data tier (bronze/silver/gold) or functionality
- **Asset Decomposition**: Convert significant helper functions to assets when they process substantial data or have reuse potential

### Code Quality and Organization

- Follow PEP 8 coding standards and use type hints consistently
- Write comprehensive docstrings and maintain clear documentation
- Organize code into logical modules with clear responsibilities
- Use meaningful variable and function names that express intent
- Implement proper error handling with specific exception types

### Dagster Asset Development

- Use `@dg.asset` decorator with rich metadata and meaningful tags
- Always return `dg.MaterializeResult` with appropriate metadata
- Implement data validation using `@dg.asset_check` pattern
- Use proper resource management and dependency definition
- Follow consistent naming conventions for assets and groups

### Data Pipeline Best Practices

- Implement incremental processing using partitioning where appropriate
- Use appropriate data tiers (bronze, silver, gold) for organization
- Ensure clear data lineage through asset dependencies
- Handle data quality issues proactively with validation checks
- Use chunking and efficient queries for large dataset processing

### Performance and Scalability

- Optimize database queries and use appropriate indexes
- Process large datasets in manageable chunks
- Use efficient file formats (Parquet for analytics)
- Implement caching for expensive computations
- Monitor memory usage and execution times

### Security and Compliance

- Mark PII data with appropriate tags and handle securely
- Use proper access controls and environment separation
- Implement audit logging for sensitive data operations
- Use secret management systems for credentials
- Follow data protection regulations (GDPR, etc.)

### Monitoring and Observability

- Include comprehensive metadata in MaterializeResult
- Implement health checks and data quality monitoring
- Use structured logging with appropriate levels
- Set up alerts for critical pipeline failures
- Track performance metrics and trends over time

### Testing and Reliability

- Write unit tests for business logic and asset functions
- Implement integration tests for end-to-end pipeline validation
- Use mock resources for testing asset functionality
- Test error conditions and edge cases
- Maintain test coverage for critical data transformations

### Documentation Standards

- Provide clear docstrings for all functions and assets
- Document expected data formats and schema definitions
- Include business context and domain knowledge
- Maintain up-to-date examples and templates
- Reference project-specific documentation for context

## 12. Environment and Configuration Management

### Environment Variables

Use environment variables for configuration that varies between development, staging, and production environments:

```python
import os
from typing import Optional

# Database configuration
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "dag_project")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")

# API keys and secrets
API_KEY = os.getenv("API_KEY")
SECRET_KEY = os.getenv("SECRET_KEY")

# Feature flags
ENABLE_FEATURE_X = os.getenv("ENABLE_FEATURE_X", "false").lower() == "true"
```

### Configuration Files

For complex configurations, use configuration files with proper validation:

```python
from pydantic import BaseSettings, Field
from typing import Optional

class DatabaseConfig(BaseSettings):
    host: str = Field(default="localhost", env="DB_HOST")
    port: int = Field(default=5432, env="DB_PORT")
    database: str = Field(default="dag_project", env="DB_NAME")
    username: str = Field(default="postgres", env="DB_USER")
    password: str = Field(default="", env="DB_PASSWORD")

    class Config:
        env_file = ".env"

class AppConfig(BaseSettings):
    database: DatabaseConfig = DatabaseConfig()
    debug: bool = Field(default=False, env="DEBUG")
    log_level: str = Field(default="INFO", env="LOG_LEVEL")
```

## 13. Utility Functions and Helpers

### Data Transformation Utilities

Create reusable utility functions for common data operations:

```python
import pandas as pd
from typing import List, Dict, Any, Optional
import dagster as dg

def standardize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardize column names to snake_case format.

    Args:
        df: Input DataFrame

    Returns:
        DataFrame with standardized column names
    """
    df.columns = df.columns.str.lower().str.replace(' ', '_').str.replace('[^a-z0-9_]', '', regex=True)
    return df

def validate_required_columns(df: pd.DataFrame, required_columns: List[str]) -> None:
    """
    Validate that all required columns exist in DataFrame.

    Args:
        df: Input DataFrame
        required_columns: List of required column names

    Raises:
        ValueError: If any required columns are missing
    """
    missing_columns = set(required_columns) - set(df.columns)
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")

def get_data_quality_metrics(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Calculate basic data quality metrics for a DataFrame.

    Args:
        df: Input DataFrame

    Returns:
        Dictionary containing data quality metrics
    """
    return {
        "total_rows": len(df),
        "total_columns": len(df.columns),
        "null_counts": df.isnull().sum().to_dict(),
        "duplicate_rows": df.duplicated().sum(),
        "memory_usage_mb": df.memory_usage(deep=True).sum() / 1024 / 1024,
    }

def create_materialization_metadata(
    df: pd.DataFrame,
    description: Optional[str] = None,
    include_preview: bool = True,
    preview_rows: int = 10
) -> Dict[str, dg.MetadataValue]:
    """
    Create standard materialization metadata for DataFrames.

    Args:
        df: Input DataFrame
        description: Optional description of the data
        include_preview: Whether to include data preview
        preview_rows: Number of rows to include in preview

    Returns:
        Dictionary of metadata values for MaterializeResult
    """
    metadata = {
        "dagster/row_count": dg.MetadataValue.int(len(df)),
        "columns": dg.MetadataValue.int(len(df.columns)),
        "memory_usage_mb": dg.MetadataValue.float(
            df.memory_usage(deep=True).sum() / 1024 / 1024
        ),
    }

    if description:
        metadata["description"] = dg.MetadataValue.text(description)

    if include_preview and len(df) > 0:
        preview_df = df.head(preview_rows)
        metadata["preview"] = dg.MetadataValue.md(
            preview_df.to_markdown(index=False)
        )

    # Add data quality metrics
    quality_metrics = get_data_quality_metrics(df)
    metadata["duplicate_rows"] = dg.MetadataValue.int(quality_metrics["duplicate_rows"])

    return metadata
```

### Database Connection Utilities

```python
from contextlib import contextmanager
from typing import Generator
import dagster as dg

@contextmanager
def get_database_connection(resource: dg.ConfigurableResource) -> Generator:
    """
    Context manager for database connections with proper error handling.

    Args:
        resource: Dagster database resource

    Yields:
        Database connection object
    """
    conn = None
    try:
        conn = resource.get_connection()
        yield conn
    except Exception as e:
        if conn:
            try:
                conn.rollback()
            except:
                pass
        raise e
    finally:
        if conn:
            conn.close()

def execute_sql_with_retry(
    conn,
    sql: str,
    params: Optional[Dict] = None,
    max_retries: int = 3,
    delay: float = 1.0
) -> Any:
    """
    Execute SQL with retry logic for transient failures.

    Args:
        conn: Database connection
        sql: SQL query to execute
        params: Query parameters
        max_retries: Maximum number of retry attempts
        delay: Delay between retries in seconds

    Returns:
        Query result
    """
    import time

    for attempt in range(max_retries + 1):
        try:
            if params:
                return conn.execute(sql, params)
            else:
                return conn.execute(sql)
        except Exception as e:
            if attempt == max_retries:
                raise e
            time.sleep(delay * (2 ** attempt))  # Exponential backoff
```

## 14. Testing Guidelines

### Asset Testing

Write tests for your Dagster assets to ensure reliability:

```python
import dagster as dg
from dagster import build_asset_context
import pytest
import pandas as pd

def test_my_asset():
    """Test asset functionality with mock data."""
    # Create mock resource
    mock_duckdb = MockDuckDBResource()

    # Build asset context
    context = build_asset_context()

    # Execute asset
    result = my_asset(context, mock_duckdb)

    # Assertions
    assert isinstance(result, dg.MaterializeResult)
    assert "dagster/row_count" in result.metadata
    assert result.metadata["dagster/row_count"].value > 0

def test_asset_validation():
    """Test asset validation logic."""
    # Create test data with known issues
    test_df = pd.DataFrame({
        'business_registration_number': ['1234567890', None, '123'],
        'business_name': ['Test Corp', 'Another Corp', 'Third Corp']
    })

    # Test validation function
    with pytest.raises(ValueError, match="Missing required columns"):
        validate_required_columns(test_df, ['missing_column'])
```

### Integration Testing

```python
def test_pipeline_integration():
    """Test full pipeline execution."""
    from dagster import materialize

    # Define test job with assets
    test_job = dg.define_asset_job(
        "test_pipeline",
        selection=["asset1", "asset2", "asset3"]
    )

    # Execute with test resources
    result = materialize(
        [asset1, asset2, asset3],
        resources={
            "duckdb": test_duckdb_resource,
            "postgres": test_postgres_resource
        }
    )

    assert result.success
```

### Module Integration Verification

When integrating new asset modules into `definitions.py`, use these verification methods:

#### 1. Asset Registration Verification

```python
# Check total asset count
python -c "from dag.definitions import defs; print(f'총 에셋: {len(defs.assets)}개')"

# Verify specific assets are registered
python -c "from dag.definitions import defs; print([asset.key for asset in defs.assets])"
```

#### 2. Asset Group Selection Testing

```python
# Test asset group selection for jobs
python -c "
from dag.definitions import defs
from dagster import AssetSelection

asset_graph = defs.get_asset_graph()
nps_assets = AssetSelection.groups('NPS').resolve(asset_graph)
print(f'NPS 그룹 에셋들: {[key.to_user_string() for key in nps_assets]}')
"
```

#### 3. Job Asset Selection Verification

```python
# Verify job selects all intended assets
python -c "
from dag.definitions import defs
from dag.jobs import nps_history_job

asset_graph = defs.get_asset_graph()
job_assets = nps_history_job.asset_selection.resolve(asset_graph)
print(f'Job이 선택한 에셋들: {[key.to_user_string() for key in job_assets]}')
"
```

#### 4. Asset Checks Registration

```python
# Verify asset checks are properly loaded
python -c "from dag.definitions import defs; print(f'Asset checks: {len(defs.asset_checks)}개')"
```

#### 5. Module Import Validation

```python
# Test module imports work correctly
python -c "
try:
    from dag import (
        nps_raw_ingestion,
        nps_data_processing,
        nps_postgres_simple,
        nps_index_optimization,
    )
    print('✅ 모든 모듈 임포트 성공')
except ImportError as e:
    print(f'❌ 임포트 에러: {e}')
"
```

#### 6. Complete Integration Test

```python
# Full integration verification script
python -c "
from dag.definitions import defs
from dagster import AssetSelection

print('=== DAG 통합 검증 ===')
print(f'총 에셋: {len(defs.assets)}개')
print(f'Asset checks: {len(defs.asset_checks)}개')
print(f'Jobs: {len(defs.jobs)}개')

asset_graph = defs.get_asset_graph()
nps_assets = AssetSelection.groups('NPS').resolve(asset_graph)
print(f'NPS 그룹 에셋: {len(nps_assets)}개')

for key in nps_assets:
    print(f'  - {key.to_user_string()}')
print('✅ 통합 검증 완료')
"
```

## 15. Performance and Optimization Guidelines

### Performance-First Development Philosophy

#### Speed as Primary Concern

- **Performance Over Safety**: Prioritize processing speed over defensive programming
- **Efficient Data Types**: Use appropriate data types (int32 vs int64, category for strings)
- **Vectorized Operations**: Prefer pandas/numpy vectorized operations over loops
- **Memory Efficiency**: Process large datasets in chunks, release resources promptly

#### Avoiding Over-Engineering

- **No Excessive Error Handling**: Don't build multiple fallback mechanisms
- **Trust the Framework**: Let Dagster handle retries and error recovery
- **Simple Algorithms**: Choose straightforward algorithms over complex optimizations
- **Profile Before Optimizing**: Measure performance before adding complexity

### Database Query Optimization

- **Use Appropriate Indexes**: Create indexes on frequently queried columns
- **Limit Data Transfer**: Select only necessary columns, use LIMIT for development
- **Batch Operations**: Use bulk inserts/updates instead of row-by-row operations
- **Query Analysis**: Use EXPLAIN to understand query execution plans

```python
# Good: Efficient query with specific columns and filters
efficient_query = """
    SELECT business_name, subscription_status_code, subscriber_count
    FROM national_pension_businesses
    WHERE data_creation_month >= %s
    AND subscription_status_code = 1
    ORDER BY subscriber_count DESC
    LIMIT 1000
"""

# Avoid: Selecting all data without filters
inefficient_query = "SELECT * FROM national_pension_businesses"
```

### Memory Management

- **Process in Chunks**: For large datasets, process data in manageable chunks
- **Use Generators**: Utilize generators for memory-efficient data processing
- **Clean Up Resources**: Explicitly close connections and free memory when done
- **Monitor Memory Usage**: Track memory consumption in asset metadata

```python
def process_large_dataset_in_chunks(
    conn,
    table_name: str,
    chunk_size: int = 10000
) -> Generator[pd.DataFrame, None, None]:
    """Process large dataset in chunks to manage memory usage."""
    offset = 0
    while True:
        chunk_query = f"""
            SELECT * FROM {table_name}
            ORDER BY id
            LIMIT {chunk_size} OFFSET {offset}
        """
        chunk_df = conn.execute(chunk_query).fetchdf()

        if chunk_df.empty:
            break

        yield chunk_df
        offset += chunk_size
```

### Caching and Materialization Strategy

- **Cache Expensive Operations**: Cache results of computationally expensive transformations
- **Incremental Updates**: Use partitioning for time-series data to enable incremental processing
- **Smart Materialization**: Only re-materialize assets when upstream dependencies change
- **Storage Optimization**: Use appropriate file formats (Parquet for analytics, CSV for interoperability)

```python
@dg.asset(
    partitions_def=dg.DailyPartitionsDefinition(start_date="2023-01-01"),
    io_manager_key="parquet_io_manager",  # Use efficient storage format
    tags={"cache": "enabled", "data_tier": "silver"}
)
def daily_stock_analysis(
    context: dg.AssetExecutionContext,
    raw_stock_data
) -> dg.MaterializeResult:
    """Process stock data with daily partitioning for incremental updates."""
    partition_date = context.partition_key

    # Only process data for the specific partition date
    filtered_data = raw_stock_data[
        raw_stock_data['date'] == partition_date
    ]

    # ... processing logic ...

    return dg.MaterializeResult(
        metadata={
            "partition_date": dg.MetadataValue.text(partition_date),
            "processed_records": dg.MetadataValue.int(len(filtered_data))
        }
    )
```

## 16. Security and Compliance

### Data Protection

- **PII Handling**: Mark assets containing PII with appropriate tags
- **Access Control**: Use proper database permissions and role-based access
- **Data Encryption**: Ensure data is encrypted in transit and at rest
- **Audit Logging**: Log data access and modifications for compliance

```python
@dg.asset(
    tags={
        "pii": "true",
        "compliance": "gdpr",
        "access_level": "restricted"
    },
    description="Contains personally identifiable information - handle with care"
)
def customer_personal_data(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Process customer PII data with appropriate security measures."""
    # ... secure processing logic ...

    context.log.info(f"Processed PII data - audit log entry created")
    return dg.MaterializeResult(
        metadata={
            "pii_records_processed": dg.MetadataValue.int(record_count),
            "audit_timestamp": dg.MetadataValue.timestamp(time.time())
        }
    )
```

### Environment Separation

- **Configuration Isolation**: Use separate configurations for dev/staging/prod
- **Resource Isolation**: Use different database instances for different environments
- **Secret Management**: Use proper secret management systems (not plain text)
- **Network Security**: Implement proper network security and VPN access

## 17. Monitoring and Observability

### Asset Monitoring

- **Health Checks**: Implement comprehensive asset checks for data quality
- **Performance Metrics**: Track execution time, memory usage, and row counts
- **Alert Configuration**: Set up alerts for asset failures and data quality issues
- **Dashboard Creation**: Create dashboards for monitoring pipeline health

```python
@dg.asset_check(
    asset=financial_analysis,
    description="Monitor for significant changes in financial metrics"
)
def monitor_financial_trends(
    context: dg.AssetCheckExecutionContext,
    financial_analysis
) -> dg.AssetCheckResult:
    """Monitor for unusual patterns in financial analysis results."""

    # Calculate trend metrics
    current_metrics = calculate_current_metrics(financial_analysis)
    historical_metrics = get_historical_metrics()

    deviation_threshold = 0.15  # 15% threshold
    significant_changes = []

    for metric, current_value in current_metrics.items():
        historical_value = historical_metrics.get(metric, 0)
        if historical_value > 0:
            deviation = abs(current_value - historical_value) / historical_value
            if deviation > deviation_threshold:
                significant_changes.append(f"{metric}: {deviation:.2%} change")

    if significant_changes:
        return dg.AssetCheckResult(
            passed=False,
            severity=dg.AssetCheckSeverity.WARN,
            description=f"Significant changes detected: {', '.join(significant_changes)}",
            metadata={
                "changes_detected": dg.MetadataValue.text(str(significant_changes)),
                "deviation_threshold": dg.MetadataValue.float(deviation_threshold)
            }
        )

    return dg.AssetCheckResult(
        passed=True,
        description="Financial trends within normal ranges"
    )
```

### Logging Best Practices

- **Structured Logging**: Use structured logging with consistent formats
- **Log Levels**: Use appropriate log levels (DEBUG, INFO, WARNING, ERROR)
- **Context Information**: Include relevant context in log messages
- **Performance Logging**: Log execution times for performance monitoring

```python
import logging
import time
from functools import wraps

def log_execution_time(func):
    """Decorator to log function execution time."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        try:
            result = func(*args, **kwargs)
            execution_time = time.time() - start_time
            logging.info(
                f"Function {func.__name__} completed successfully",
                extra={
                    "function": func.__name__,
                    "execution_time": execution_time,
                    "status": "success"
                }
            )
            return result
        except Exception as e:
            execution_time = time.time() - start_time
            logging.error(
                f"Function {func.__name__} failed: {str(e)}",
                extra={
                    "function": func.__name__,
                    "execution_time": execution_time,
                    "status": "error",
                    "error": str(e)
                }
            )
            raise
    return wrapper
```
