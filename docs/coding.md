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

- **Project Prefixes**: All new files must include appropriate project prefixes
  - `nps_` prefix for National Pension Service project files
  - `cd_` prefix for CD (Chundan) stock project files
  - `is_` prefix for IS (Isshoo) community content project files
  - Example: `nps_business_data.py`, `cd_stock_prices.py`, `is_crawler.py`

### File Size Management

- **Line Limit**: Keep Python files between 1000-1500 lines for maintainability
- **File Splitting**: If files exceed 1500 lines, split into additional files
- **Logical Separation**: Split files based on functional boundaries and asset groups
- **Example Split Strategy**:
  ```
  nps_raw_data.py        # Raw data ingestion assets
  nps_processed_data.py  # Data transformation assets
  nps_analytics.py       # Analytics and reporting assets
  nps_utils.py          # Helper functions and utilities
  ```

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
    group_name="nps_processing",
    tags={"data_tier": "bronze", "domain": "pension"}
)
def nps_raw_business_data() -> dg.MaterializeResult:
    # Raw data ingestion
    pass

@dg.asset(
    deps=["nps_raw_business_data"],
    group_name="nps_processing",
    tags={"data_tier": "silver", "domain": "pension"}
)
def nps_cleaned_business_data() -> dg.MaterializeResult:
    # Data cleaning and standardization
    pass

@dg.asset(
    deps=["nps_cleaned_business_data"],
    group_name="nps_analytics",
    tags={"data_tier": "gold", "domain": "pension"}
)
def nps_business_metrics() -> dg.MaterializeResult:
    # Business metrics calculation
    pass

# AVOID: Monolithic asset doing everything
@dg.asset
def nps_complete_pipeline() -> dg.MaterializeResult:
    # Don't combine raw ingestion, cleaning, and analytics in one asset
    pass
```

## 2. Asset Management and Definitions Registration

### Overview

When adding new assets to the DAG project, proper registration in `definitions.py` is essential for Dagster to recognize and manage your assets. This section provides guidelines for organizing assets across modules and maintaining the central definitions file.

### Adding New Asset Modules

#### Step 1: Create Asset Module

When creating new asset modules, follow the established naming conventions:

```python
# Example: nps_analytics.py
"""
NPS Analytics - Gold Tier
국민연금 분석 및 보고서 생성
"""

from dagster import AssetExecutionContext, asset
import dagster as dg

@asset(
    description="NPS 사업장 분석 리포트 생성 - Gold Tier",
    group_name="NPS",
    kinds={"python", "analytics"},
    tags={
        "domain": "finance",
        "data_tier": "gold",
        "source": "national_pension"
    },
    deps=["nps_his_digest"]  # 의존성 명시
)
def nps_business_analytics(
    context: AssetExecutionContext,
    nps_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    """NPS 사업장 데이터 기반 비즈니스 분석"""
    # 분석 로직 구현
    pass
```

#### Step 2: Update definitions.py

새로운 모듈을 생성한 후, 반드시 `definitions.py`에 추가해야 합니다:

**현재 프로젝트 상태 (2025년 5월 기준):**

```python
# dag/definitions.py
from dagster import Definitions, EnvVar, load_assets_from_modules
from dagster_duckdb import DuckDBResource
from .resources import PostgresResource

from dag import nps_raw_ingestion, nps_data_processing  # noqa: TID252

all_nps_assets = load_assets_from_modules([nps_raw_ingestion, nps_data_processing])

defs = Definitions(
    assets=all_nps_assets,
    resources={
        "cd_duckdb": DuckDBResource(database="data/cd.duckdb"),
        "nps_duckdb": DuckDBResource(database="data/nps.duckdb"),
        "postgres": PostgresResource(
            host=EnvVar("POSTGRES_HOST"),
            port=EnvVar.int("POSTGRES_PORT"),
            user=EnvVar("POSTGRES_USER"),
            password=EnvVar("POSTGRES_PASSWORD"),
            database=EnvVar("POSTGRES_DB"),
        ),
    },
)
```

**새로운 애셋 모듈 추가 시 확장 방법:**

```python
# dag/definitions.py
from dagster import Definitions, EnvVar, load_assets_from_modules
from dagster_duckdb import DuckDBResource
from .resources import PostgresResource

# 모든 애셋 모듈을 임포트
from dag import (
    nps_raw_ingestion,     # Bronze tier
    nps_data_processing,   # Silver tier
    nps_analytics,         # Gold tier - 새로 추가된 모듈
    cd_raw_ingestion,      # CD 프로젝트 모듈들
    cd_data_processing,
    # ... 기타 모듈들
)

# 프로젝트별 애셋 그룹화
nps_assets = load_assets_from_modules([
    nps_raw_ingestion,
    nps_data_processing,
    nps_analytics,  # ← 새로 추가
])

cd_assets = load_assets_from_modules([
    cd_raw_ingestion,
    cd_data_processing,
])

# 전체 애셋 통합
all_assets = [*nps_assets, *cd_assets]

defs = Definitions(
    assets=all_assets,
    resources={
        "cd_duckdb": DuckDBResource(database="data/cd.duckdb"),
        "nps_duckdb": DuckDBResource(database="data/nps.duckdb"),
        "postgres": PostgresResource(
            host=EnvVar("POSTGRES_HOST"),
            port=EnvVar.int("POSTGRES_PORT"),
            user=EnvVar("POSTGRES_USER"),
            password=EnvVar("POSTGRES_PASSWORD"),
            database=EnvVar("POSTGRES_DB"),
        ),
    },
)
```

### Module Organization Strategies

#### Strategy 1: Project-Based Grouping (권장)

프로젝트별로 애셋을 그룹화하여 관리:

```python
# NPS 프로젝트 애셋들
nps_assets = load_assets_from_modules([
    nps_raw_ingestion,      # Bronze
    nps_data_processing,    # Silver
    nps_analytics,          # Gold
    nps_reporting,          # Gold+
])

# CD 프로젝트 애셋들
cd_assets = load_assets_from_modules([
    cd_raw_ingestion,       # Bronze
    cd_data_processing,     # Silver
    cd_analytics,           # Gold
])

# 공통/유틸리티 애셋들
utility_assets = load_assets_from_modules([
    shared_utils,
    common_analytics,
])
```

#### Strategy 2: Data Tier-Based Grouping

데이터 계층별로 그룹화 (소규모 프로젝트용):

```python
# 계층별 그룹화
bronze_assets = load_assets_from_modules([
    nps_raw_ingestion,
    cd_raw_ingestion,
])

silver_assets = load_assets_from_modules([
    nps_data_processing,
    cd_data_processing,
])

gold_assets = load_assets_from_modules([
    nps_analytics,
    cd_analytics,
    cross_domain_analytics,
])
```

### Best Practices for Asset Registration

#### 1. Consistent Import Naming

```python
# GOOD: 명확한 모듈명 사용
from dag import nps_raw_ingestion, nps_data_processing

# AVOID: 혼란스러운 별칭
from dag import nps_raw_ingestion as nps_raw
```

#### 2. Logical Grouping

```python
# GOOD: 논리적 그룹화
all_nps_assets = load_assets_from_modules([
    nps_raw_ingestion,
    nps_data_processing,
    nps_analytics,
])

# AVOID: 무작위 순서
random_assets = load_assets_from_modules([
    nps_analytics,
    cd_raw_ingestion,
    nps_raw_ingestion,
])
```

#### 3. Documentation and Comments

```python
# 각 모듈의 역할을 명확히 문서화
nps_assets = load_assets_from_modules([
    nps_raw_ingestion,     # 원시 데이터 수집 (Bronze)
    nps_data_processing,   # 데이터 정제 및 변환 (Silver)
    nps_analytics,         # 비즈니스 분석 (Gold)
])
```

### Asset Discovery Verification

새로운 애셋이 올바르게 등록되었는지 확인:

```bash
# 현재 프로젝트 디렉토리로 이동
cd /Users/craigchoi/silla/dag

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
# cd_data_processing.py에 'clean_data'

# 해결: 고유한 애셋 이름 사용
# nps_data_processing.py에 'nps_clean_data'
# cd_data_processing.py에 'cd_clean_data'
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
# 애셋 이름 패턴: {project}_{tier}_{domain}_{action}
nps_raw_business_data      # NPS 원시 사업장 데이터
nps_silver_clean_data      # NPS 정제된 데이터
nps_gold_analytics_report  # NPS 분석 리포트
cd_raw_stock_prices        # CD 원시 주가 데이터
cd_gold_investment_insight # CD 투자 인사이트
```

### Function and Asset Name Consistency

**핵심 원칙**: 함수명과 asset명은 반드시 일치해야 합니다.

```python
# ✅ 올바른 예시 - 함수명과 asset명이 일치
@dg.asset(
    name="nps_raw_ingestion",  # Asset명
    group_name="nps_bronze",
    tags={"tier": "bronze", "project": "nps"}
)
def nps_raw_ingestion(context: AssetExecutionContext) -> dg.MaterializeResult:
    # 함수명과 asset명이 정확히 일치
    pass

# ❌ 잘못된 예시 - 함수명과 asset명이 불일치
@dg.asset(
    name="nps_raw_ingestion",  # Asset명
    group_name="nps_bronze"
)
def fetch_nps_data(context: AssetExecutionContext):  # 다른 함수명
    pass
```

**의존성 참조 시 주의사항**:

```python
# ✅ Asset명으로 의존성 참조
@dg.asset(
    deps=["nps_raw_ingestion"],  # Asset명 사용
    name="nps_data_processing"
)
def nps_data_processing(context: AssetExecutionContext):
    pass

# ❌ 함수명으로 의존성 참조하면 안됨
@dg.asset(
    deps=["fetch_nps_data"],  # 이렇게 하면 안됨
    name="nps_data_processing"
)
def nps_data_processing(context: AssetExecutionContext):
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

#### Asset Selection Methods

Jobs use `AssetSelection` to define which assets to include:

```python
import dagster as dg

# 1. Group-based selection (권장)
nps_targets = dg.AssetSelection.groups("NPS")

# 2. Tag-based selection
bronze_assets = dg.AssetSelection.tag("data_tier", "bronze")

# 3. Specific asset selection
specific_assets = dg.AssetSelection.assets(["nps_his_download", "nps_his_digest"])

# 4. Upstream/downstream selection
downstream_assets = dg.AssetSelection.assets(["nps_his_download"]).downstream()
```

### Creating Job Modules

#### Step 1: Create jobs.py

모든 job 정의는 `dag/jobs.py`에 중앙 집중화:

```python
# dag/jobs.py
import dagster as dg

# NPS 프로젝트 관련 애셋 그룹
nps_targets = dg.AssetSelection.groups("NPS")

# NPS 히스토리 데이터 처리 Job
nps_history_job = dg.define_asset_job(
    name="nps_history_job",
    selection=nps_targets,
    description="NPS 과거 데이터 한번에 가지고 오기",
    tags={"source": "NPS", "type": "history"},
)

# CD 프로젝트 관련 Job (예시)
cd_targets = dg.AssetSelection.groups("CD")

cd_daily_job = dg.define_asset_job(
    name="cd_daily_job",
    selection=cd_targets,
    description="CD 주식 데이터 일일 처리",
    tags={"source": "CD", "type": "daily"},
)

# 데이터 계층별 Job (예시)
bronze_job = dg.define_asset_job(
    name="bronze_ingestion_job",
    selection=dg.AssetSelection.tag("data_tier", "bronze"),
    description="모든 Bronze 계층 데이터 수집",
    tags={"tier": "bronze", "type": "ingestion"},
)
```

#### Step 2: Update definitions.py

Job을 정의한 후, `definitions.py`에 등록:

```python
# dag/definitions.py
from dagster import Definitions, EnvVar, load_assets_from_modules
from dagster_duckdb import DuckDBResource
from .resources import PostgresResource
from . import nps_raw_ingestion, nps_data_processing  # noqa: TID252
from .jobs import nps_history_job  # ← Job 임포트

all_nps_assets = load_assets_from_modules([nps_raw_ingestion, nps_data_processing])

defs = Definitions(
    assets=all_nps_assets,
    jobs=[nps_history_job],  # ← Job 등록
    resources={
        "cd_duckdb": DuckDBResource(database="data/cd.duckdb"),
        "nps_duckdb": DuckDBResource(database="data/nps.duckdb"),
        "postgres": PostgresResource(
            host=EnvVar("POSTGRES_HOST"),
            port=EnvVar.int("POSTGRES_PORT"),
            user=EnvVar("POSTGRES_USER"),
            password=EnvVar("POSTGRES_PASSWORD"),
            database=EnvVar("POSTGRES_DB"),
        ),
    ),
)
```

### Job 확장 시 예시

새로운 Job을 추가할 때:

```python
# dag/jobs.py에 추가
# 복합 도메인 분석 Job
cross_domain_job = dg.define_asset_job(
    name="cross_domain_analytics",
    selection=dg.AssetSelection.tag("data_tier", "gold"),
    description="모든 Gold 계층 분석 실행",
    tags={"type": "analytics", "priority": "high"},
)

# dag/definitions.py에 등록
from .jobs import nps_history_job, cross_domain_job  # ← 새 Job 임포트

defs = Definitions(
    assets=all_nps_assets,
    jobs=[nps_history_job, cross_domain_job],  # ← 새 Job 추가
    resources={...},
)
```

### Asset Group Name 설정 (CRITICAL)

Job이 올바르게 Asset을 선택하려면 Asset의 `group_name`이 정확히 설정되어야 함:

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
def nps_his_download(...):
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
def nps_his_digest(...):
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
# GOOD: 비즈니스 도메인별 그룹화
nps_targets = dg.AssetSelection.groups("NPS")
cd_targets = dg.AssetSelection.groups("CD")

# AVOID: 기술적 분류로만 그룹화
bronze_only = dg.AssetSelection.tag("data_tier", "bronze")
```

#### 2. 명확한 Job 이름 및 설명

```python
# GOOD: 목적이 명확한 이름
nps_history_job = dg.define_asset_job(
    name="nps_history_job",
    description="NPS 과거 데이터 한번에 가지고 오기",
)

# AVOID: 모호한 이름
data_job = dg.define_asset_job(name="data_job")
```

#### 3. 적절한 태그 활용

```python
# 운영 환경 구분
production_job = dg.define_asset_job(
    name="nps_production_sync",
    selection=nps_targets,
    tags={"env": "production", "schedule": "daily"},
)

# 데이터 품질 체크 Job
quality_job = dg.define_asset_job(
    name="data_quality_checks",
    selection=dg.AssetSelection.tag("quality_check", "true"),
    tags={"type": "validation", "critical": "true"},
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

- `nps_raw_business_data` - Raw NPS workplace data
- `cd_raw_stock_prices` - Raw stock price data

```python
@dg.asset(
    tags={"data_tier": "bronze", "domain": "pension"},
    group_name="nps_ingestion"
)
def nps_raw_business_registrations() -> dg.MaterializeResult:
    """Collect raw NPS workplace registration data"""
    # Read CSV files directly without transformation
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

- `nps_cleaned_business_data` - Cleansed workplace data
- `cd_processed_stock_prices` - Standardized stock price data

```python
@dg.asset(
    deps=["nps_raw_business_registrations"],
    tags={"data_tier": "silver", "domain": "pension"},
    group_name="nps_processing"
)
def nps_cleaned_business_data() -> dg.MaterializeResult:
    """Cleanse and standardize workplace data"""
    # 1. Standardize column names
    # 2. Convert data types
    # 3. Handle null values
    # 4. Remove duplicates
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

- `nps_business_analytics` - Workplace analysis metrics
- `cd_stock_analytics` - Stock investment analysis
- `combined_financial_insights` - Integrated financial insights

```python
@dg.asset(
    deps=["nps_cleaned_business_data", "cd_processed_stock_prices"],
    tags={"data_tier": "gold", "domain": "analytics"},
    group_name="cross_domain_analytics"
)
def combined_financial_insights() -> dg.MaterializeResult:
    """Combine NPS and stock data for financial insights"""
    # 1. Integrate multiple data sources
    # 2. Apply complex business logic
    # 3. Calculate analytical metrics
    # 4. Generate dashboard-ready data
    pass
```

### Data Flow Examples

#### NPS Project Data Pipeline:

```
nps_raw_business_data (Bronze)
    ↓ data cleansing
nps_cleaned_business_data (Silver)
    ↓ business logic application
nps_business_analytics (Gold)
```

#### CD Project Data Pipeline:

```
cd_raw_stock_prices (Bronze)
    ↓ data standardization
cd_processed_stock_prices (Silver)
    ↓ investment analysis logic
cd_investment_insights (Gold)
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
    from dag import nps_raw_ingestion, nps_data_processing, nps_postgres_integration
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
