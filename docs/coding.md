# DAG Project Coding Guidelines — Reorganized (v2025-09, Final)

본 가이드는 **Dagster 기반 데이터 파이프라인(DAG 프로젝트)**의 일관된 개발·운영을 위해 작성되었습니다. 흩어진 내용을 계층적으로 재구성하고, 실제 작업에 바로 적용할 수 있도록 체크리스트·예시·검증 스크립트를 포함합니다. 아래 **결정 사항(Resolved Decisions)**을 문서 전반에 반영했습니다.

✅ 철학 요약: 단순함 · 일관성 · 가시성 — 자산 중심(Asset-first) 설계, 풍부한 메타데이터, Fail Fast(모든 재시도 금지), 과도한 방어 로직 지양.

⸻

## 목차
1. [결정 사항(Resolved Decisions)](#1-결정-사항resolved-decisions)
2. [문서 사용법 & 표기 규칙](#2-문서-사용법--표기-규칙)
3. [리포지토리 & 파일 조직 원칙](#3-리포지토리--파일-조직-원칙)
4. [메달리온 아키텍처 (Bronze/Silver/Gold)](#4-메달리온-아키텍처-bronzesilvergold)
5. [애셋 개발 규약](#5-애셋-개발-규약)
6. [모듈 등록 & definitionspy](#6-모듈-등록--definitionspy)
7. [잡(Jobs)·스케줄·센서](#7-잡jobs스케줄센서)
8. [환경/리소스 & 설정 관리](#8-환경리소스--설정-관리)
9. [유틸리티 & 헬퍼 모음](#9-유틸리티--헬퍼-모음)
10. [테스트 전략](#10-테스트-전략)
11. [성능 & 최적화 가이드](#11-성능--최적화-가이드)
12. [보안 & 컴플라이언스](#12-보안--컴플라이언스)
13. [모니터링·관측성 & 로깅](#13-모니터링관측성--로깅)
14. [검증/디버깅 스크립트 모음](#14-검증디버깅-스크립트-모음)
15. [자주 발생하는 문제 & 해결](#15-자주-발생하는-문제--해결)
16. [베스트 프랙티스 요약](#16-베스트-프랙티스-요약)
17. [부록 A — 허용 kinds 전체 목록](#17-부록-a--허용-kinds-전체-목록)
18. [부록 B — 자주 쓰는 CLI/파이썬 스니펫](#18-부록-b--자주-쓰는-clipython-스니펫)

⸻

## 1. 결정 사항(Resolved Decisions)
1. **Fail Fast 정책**: 재시도 유틸/설정/코드 전면 금지. 오류는 즉시 표면화한다.
2. **메달리온 계층 표기**: 계층은 반드시 `tags.data_tier`에만 표기하며 kinds에는 계층 정보를 넣지 않는다.
3. **리소스 자격증명**: 모든 자격증명·민감정보는 EnvVar 또는 시크릿 매니저에서 로드한다. 예시 코드에서도 하드코딩을 금지한다.
4. **Dagster CLI 표기**: 실행 명령은 최신 표준 `dagster job launch -j <job_name>`으로 통일한다.
5. **함수명 = 애셋명**: 함수명과 애셋명(name)은 항상 동일해야 하며 의존성 참조도 애셋명 기반으로 한다.
6. **그룹명 표준화**: 허용 그룹명은 `CD`, `DS`, `IS`, `SS`, `NPS` 다섯 개다. 이전 표준(`CD_TURSO`, `SS_Dictionary`, `NPS_SaaS` 등)은 사용 금지.

> 본 문서의 예시·체크리스트·스니펫은 위 결정을 모두 준수한다.

⸻

## 2. 문서 사용법 & 표기 규칙
- 코드 블록은 복사·붙여넣기 즉시 실행 가능한 형태로 유지한다.
- **굵은 글씨**는 필수 준수 항목, _기울임_은 참고 사항을 의미한다.
- 체크리스트는 실제 작업 전·후 점검 용도로 작성되었으며 항목을 임의로 삭제하지 않는다.
- 예시는 Dagster 최신 LTS 버전 기준이며, 필요 시 버전 명시 후 업데이트한다.
- 문서 내 모든 경로·명령은 리포지토리 루트(`/Users/.../dag`) 기준 상대 경로를 사용한다.
- 기존 내용보다 간소화하지 말고, 세부 가이드를 추가할 때는 관련 스크립트나 예시를 함께 제공한다.

⸻

## 3. 리포지토리 & 파일 조직 원칙

### 3.1 접두사 규약(프로젝트별)
- `cd_`: Chundan 주식 파이프라인 (히스토리, Turso 변형 포함)
- `ds_`: DataStory 컴퓨터 비전 파이프라인
- `is_`: Isshoo 크롤러·엔리치먼트 파이프라인
- `ss_`: Silla-Store 크롤러·사전 자산
- `nps_`: 국민연금(NPS) 워크로드
- 예: `cd_price_processing.py`, `ds_img_r2_processing.py`, `is_data_sync.py`, `ss_data_sync.py`, `nps_raw_ingestion.py`

### 3.2 파일 크기 & 분할 기준
- **권장 라인 수**: 1,000–1,500줄.
- **1500줄 초과 시**: 기능 경계 또는 데이터 계층 기준으로 분할한다.
- **분할 예시**:
  ```
  nps_raw_ingestion.py      # Raw history ingestion (Bronze)
  nps_data_processing.py    # CSV 정제 및 DuckDB 적재 (Silver)
  nps_postgres_simple.py    # DuckDB → Postgres 적재 (Gold)
  nps_index_optimization.py # 분석/모델링 자산
  nps_kyc_etl.py            # SaaS/KYC 동기화
  ```

> _추가 분리 고려_: 장기적으로 대용량 분석/공유 로직은 `*_analytics.py`, 공용 유틸은 `shared_utils.py`, `common_analytics.py` 등으로 분리 계획을 유지한다.

### 3.3 애셋 vs 헬퍼 분리 기준
- **애셋으로 승격**: 재사용성이 높고, 중요한 중간 산출물이며, 캐시·모니터링이 필요한 경우, 또는 비즈니스적으로 의미 있는 단계일 때.
- **헬퍼 유지**: 단순 유틸리티, 단일 애셋 전용, 미미한 변환, 데이터 의존성이 없는 경우.
- 헬퍼가 라인리지나 메타데이터에 중요한 역할을 한다면 애셋 승격을 재검토한다.

### 3.4 파일 분할 의사결정 매트릭스
| 기준          | 새 파일 생성 | 기존 파일에 추가 |
| ------------- | ------------ | ---------------- |
| 파일 크기     | >1500줄      | <1000줄          |
| 기능적 관련성 | 다른 도메인  | 같은 도메인      |
| 의존성 패턴   | 독립적       | 강하게 연결됨    |
| 데이터 계층   | 다른 계층    | 같은 계층        |
| 팀 소유권     | 다른 팀      | 같은 팀          |

### 3.5 빠른 체크리스트 — 새 모듈 생성 전 점검
1. [ ] 파일명에 프로젝트 접두사 적용 (`cd_`, `nps_`, ...)
2. [ ] 1,500줄 초과 예상 시 분할 전략 수립
3. [ ] 주요 애셋 중심으로 파일 구조 재검토
4. [ ] 기존 모듈과 기능 중복 여부 확인
5. [ ] 데이터 계층(Bronze/Silver/Gold) 정의 명확화

⸻

## 4. 메달리온 아키텍처 (Bronze/Silver/Gold)

### 4.1 핵심 원칙
- **Bronze**: 원시 데이터 수집·보존, 검증/정제 없음.
- **Silver**: 정제·표준화(타입, 결측, 중복, 컬럼 구조).
- **Gold**: 비즈니스 로직·집계·웨어하우스 적재·리포팅 준비.

### 4.2 태깅 규약
- **필수**: `tags["data_tier"]`에 `bronze|silver|gold`를 지정한다.
- `kinds`는 실행/기술 스택 식별용이며 계층 정보는 절대 넣지 않는다.
- 태그 키/값은 검색·필터 가능한 의미 있는 문자열을 사용한다.

```python
@dg.asset(group_name="NPS", tags={"data_tier": "silver", "domain": "pension", "source": "national_pension"})
def nps_his_digest(...):
    ...
```

### 4.3 파이프라인 예시
- **NPS**: `nps_his_download (Bronze)` → `nps_his_digest (Silver)` → `nps_his_postgres_append (Gold)`
- **CD**: `cd_prices (Bronze)` → `cd_digest_price (Silver)` → `cd_populate_security_ranks (Gold)`

### 4.4 계층별 메타데이터 패턴
- Bronze: 수집 소스, 원본 스냅샷 경로
- Silver: 정제 통계, 결측치/중복 제거 수
- Gold: 비즈니스 KPI, 보고서 링크, 품질지표

### 4.5 계층 구조의 장점
1. 데이터 추적성 강화 — 특정 단계에서 문제를 즉시 식별 가능
2. 재사용성 극대화 — 계층별 결과를 다양한 워크로드에서 활용
3. 디버깅 용이 — 실패 지점을 빠르게 좁힐 수 있음
4. 성능 최적화 — 필요한 계층만 부분 재처리 가능

⸻

## 5. 애셋 개발 규약

### 5.1 공통 스타일
- 모든 데이터 애셋은 `@dg.asset` 데코레이터를 사용한다.
- **함수명 = 애셋명(name)** 원칙을 지키며, 의존성(`deps`)도 애셋명으로 지정한다.
- `group_name`은 반드시 `CD`, `DS`, `IS`, `SS`, `NPS` 중 하나로 설정한다.
- 풍부한 메타데이터를 `MaterializeResult`에 첨부해 검색·관측성을 높인다.

```python
@dg.asset(
    name="nps_his_download",
    group_name="NPS",
    tags={"data_tier": "bronze", "domain": "pension", "source": "national_pension"},
)
def nps_his_download(context: AssetExecutionContext) -> dg.MaterializeResult:
    ...
```

### 5.2 태그(tags) 가이드
- 태그는 검색·필터의 1급 도구다. 의미 있는 키/값을 작성한다.
- 예: `{"domain": "pension", "data_tier": "silver", "source": "national_pension"}`
- PII가 포함된 경우 `{"pii": "true"}` 등 명시적으로 표기한다.

### 5.3 kinds 가이드
- 최대 3개까지 지정하며, 허용 값은 [부록 A](#17-부록-a--허용-kinds-전체-목록) 참고.
- 기술 스택/런타임/형태 식별용으로 사용한다.
- **금지**: `bronze`, `silver`, `gold` 등 계층 관련 값을 kinds에 사용하지 않는다.

### 5.4 네이밍 일관성 (CRITICAL)
- 함수명과 애셋명은 완전히 동일해야 한다.
- 다른 애셋을 참조할 때는 반드시 애셋명으로 의존성 지정.

```python
@dg.asset(name="nps_his_digest", group_name="NPS", deps=["nps_his_download"], tags={"data_tier": "silver"})
def nps_his_digest(context: AssetExecutionContext) -> dg.MaterializeResult:
    ...
```

### 5.5 결과 반환 & 메타데이터 패턴
```python
return dg.MaterializeResult(
    metadata={
        "dagster/row_count": dg.MetadataValue.int(count),
        "preview": dg.MetadataValue.md(preview_df.to_markdown(index=False)),
        "execution_time_sec": dg.MetadataValue.float(elapsed_seconds),
    }
)
```

### 5.6 스키마 메타데이터 예시
```python
from dagster import AssetKey, TableColumn, TableSchema, asset

@asset(
    deps=[AssetKey("source_bar"), AssetKey("source_baz")],
    metadata={
        "dagster/column_schema": TableSchema(
            columns=[
                TableColumn("name", "string", description="사람 이름"),
                TableColumn("age", "int", description="나이"),
            ]
        )
    },
)
def my_asset():
    ...
```

### 5.7 데이터 검증: @dg.asset_check
```python
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
    ...
```

### 5.8 파티셔닝 & 증분 처리 예시
```python
@dg.asset(
    partitions_def=dg.DailyPartitionsDefinition(start_date="2023-01-01"),
    io_manager_key="parquet_io_manager",
    group_name="CD",
    tags={"cache": "enabled", "data_tier": "silver"},
)
def daily_stock_analysis(context: dg.AssetExecutionContext, raw_stock_data) -> dg.MaterializeResult:
    partition_date = context.partition_key
    filtered = raw_stock_data[raw_stock_data["date"] == partition_date]
    return dg.MaterializeResult(metadata={
        "partition_date": dg.MetadataValue.text(partition_date),
        "dagster/row_count": dg.MetadataValue.int(len(filtered)),
    })
```

### 5.9 애셋 정의 체크리스트
1. [ ] 함수명과 `name` 파라미터 일치
2. [ ] `group_name`이 허용된 5개 중 하나인지 확인
3. [ ] `tags.data_tier` 지정 및 kinds에서 계층 제거
4. [ ] `MaterializeResult`에 핵심 메타데이터(행수, 미리보기 등) 포함
5. [ ] 의존성은 애셋명 기준으로 설정
6. [ ] PII·도메인 태그 등 관측성 강화를 위한 정보 추가

### 5.10 개발 철학 & Dagster 활용 전략
#### Fast-Fail & 단순성 우선
- 오류는 조기에 드러나도록 두고, 과도한 예외 처리나 다중 방어 로직을 작성하지 않는다.
- Dagster의 실패/재시도 관리 기능을 신뢰하며, 커스텀 재시도 유틸은 금지한다.
- 직선형 파이프라인 설계를 우선하고 복잡한 분기나 우회 경로는 최소화한다.

```python
@dg.asset
def nps_process_data(raw_data) -> dg.MaterializeResult:
    """단순 Fail-Fast 패턴"""
    processed = raw_data.dropna().astype({"amount": "float64"})
    if processed.empty:
        raise ValueError("No valid data after processing")
    return dg.MaterializeResult(
        metadata={"dagster/row_count": dg.MetadataValue.int(len(processed))}
    )

@dg.asset
def nps_over_engineered_process(raw_data) -> dg.MaterializeResult:
    """❌ 금지: 과도한 예외 처리/재시도"""
    # 중첩된 try/except, 수동 재시도, 불필요한 대안 경로는 정책 위반이다.
    processed = raw_data.dropna()
    return dg.MaterializeResult(
        metadata={"dagster/row_count": dg.MetadataValue.int(len(processed))}
    )
```

#### 데이터 분석 및 자산 활용 장려
- 심층 분석과 통계 연산은 자산으로 분리해 재사용성과 관측성을 높인다.
- 풍부한 메타데이터와 의존성 선언으로 비즈니스 가치를 명확히 표현한다.
- 자산 간 의존성은 실제 비즈니스 흐름을 반영하도록 설계한다.

#### Context7 MCP 활용
- Dagster 패턴이 애매할 때는 Context7 MCP에 질문해 최신 API/패턴을 확인한다.
  - 예: "Dagster DailyPartitionsDefinition 활용법?", "대규모 데이터셋 처리 최적화?"
- 질의 전에는 본 문서를 통해 프로젝트 표준을 먼저 확인한다.

⸻

## 6. 모듈 등록 & definitions.py

### 6.1 기본 개요
- 새로운 애셋 모듈을 생성했다면 `dag/definitions.py`를 동반 수정해야 Dagster가 인식한다.
- 모듈 임포트 → `load_assets_from_modules` 등록 → 자산/체크/잡/스케줄/센서/리소스 매핑 순으로 점검한다.

### 6.2 definitions.py 패턴 (발췌)
```python
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
from . import (
    cd_raw_ingestion,
    cd_bppedd_processing,
    cd_display_processing,
    cd_marketcap_processing,
    cd_metrics_processing,
    cd_price_processing,
    cd_img_r2_processing,
    cd_img_r2_docker,
    cd_img_r2_node,
    cd_history_prices,
    cd_history_marketcaps,
    cd_history_bppedds,
    cd_raw_ingestion_turso,
    cd_price_processing_turso,
    cd_bppedd_processing_turso,
    cd_marketcap_processing_turso,
    cd_metrics_processing_turso,
)
from . import (
    ds_img_r2_docker,
    ds_img_r2_processing,
    ds_img_r2_node,
)
from . import (
    nps_raw_ingestion,
    nps_data_processing,
    nps_postgres_simple,
    nps_index_optimization,
    nps_kyc_etl,
)
from . import (
    is_node,
    is_data_sync,
    is_data_unsync,
    is_data_llm,
    is_data_vlm,
    is_data_cluster,
)
from . import (
    ss_node,
    ss_data_sync,
    ss_data_llm,
    ss_data_dictionary,
)
```

### 6.3 자산/체크 로더 구성
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
    cd_img_r2_node,
    cd_raw_ingestion_turso,
    cd_price_processing_turso,
    cd_bppedd_processing_turso,
    cd_marketcap_processing_turso,
    cd_metrics_processing_turso,
    cd_history_prices,
    cd_history_marketcaps,
    cd_history_bppedds,
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
```

### 6.4 Freshness 체크 & 센서
```python
from .is_data_unsync import post_trends_asset, keyword_trends_asset
from .is_data_cluster import cluster_rotation_asset

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

### 6.5 Definitions 객체 템플릿 (EnvVar만 사용)
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
        nps_history_job,
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
        "cd_duckdb": DuckDBResource(database=EnvVar("CD_DUCKDB_PATH")),
        "ds_duckdb": DuckDBResource(database=EnvVar("DS_DUCKDB_PATH")),
        "nps_duckdb": DuckDBResource(database=EnvVar("NPS_DUCKDB_PATH")),
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
        "turso": TursoResource(
            url=EnvVar("TURSO_DATABASE_URL"),
            auth_token=EnvVar("TURSO_AUTH_TOKEN"),
        ),
        "openai": OpenAIResource(
            base_url=EnvVar("OPENAI_BASE_URL"),
            api_key=EnvVar("OPENAI_API_KEY"),
        ),
        "qwen25": OpenAIResource(
            base_url=EnvVar("QWEN_BASE_URL"),
            api_key=EnvVar("QWEN_API_KEY"),
        ),
        "gemini": OpenAIResource(
            base_url=EnvVar("GEMINI_OPENAI_COMPAT_BASE_URL"),
            api_key=EnvVar("GEMINI_API_KEY"),
        ),
    },
)
```

### 6.6 모듈 구성 전략
1. **프로젝트 기준 그룹화 (권장)**: `CD/DS/IS/SS/NPS` 별 리스트 유지.
2. **백엔드/환경 기준**: Turso, Docker, Node 등 실행환경별 리스트를 별도로 유지.
3. **데이터 계층 기준(옵션)**: Bronze/Silver/Gold 별 리스트를 유지해 대규모 재수집 시 활용.

### 6.7 등록 체크리스트
1. [ ] 새 모듈 임포트 추가
2. [ ] `load_assets_from_modules`/`load_asset_checks_from_modules`에 등록
3. [ ] 관련 Jobs/Schedules/Sensors 리스트 업데이트
4. [ ] 신규 Resources 매핑 (EnvVar만 사용)
5. [ ] Dagster UI에서 새 자산 노출 확인 (`dagster dev`)

⸻

## 7. 잡(Jobs)·스케줄·센서

### 7.1 자주 쓰는 패턴
```python
import dagster as dg
from .partitions import daily_exchange_category_partition

cd_targets = dg.AssetSelection.assets("cd_stockcode").downstream()
cd_turso_targets = dg.AssetSelection.assets("cd_stockcode_turso").downstream()
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

### 7.2 Job 정의 예시
```python
cd_daily_update_job = dg.define_asset_job(
    name="cd_daily_update_job",
    partitions_def=daily_exchange_category_partition,
    selection=cd_targets,
    description="매일 주식 가격 업데이트",
    tags={"type": "daily_update"},
)

nps_history_job = dg.define_asset_job(
    name="nps_history_job",
    selection=nps_targets,
    description="NPS 전 계층 자산 materialize",
    tags={"domain": "pension", "data_tier": "mixed"},
)
```

### 7.3 그룹명 표준 (CRITICAL)
- Job이 의도한 애셋을 선택하려면 애셋의 `group_name`이 정확히 `CD`, `DS`, `IS`, `SS`, `NPS` 중 하나여야 한다.
- 과거 그룹명(`CD_TURSO`, `SS_Dictionary`, `NPS_SaaS` 등)은 즉시 수정한다.

### 7.4 실행 방법
- **UI**: `dagster dev` 실행 후 http://localhost:3000/jobs 접속 → Job 선택 → "Launch Run" 클릭
- **CLI** (표준):
  ```bash
  dagster job launch -j nps_history_job
  dagster job launch -j cd_daily_update_job --run-config configs/cd_daily.yml
  ```
- **Python**:
  ```python
  from dagster import materialize
  from dag.definitions import defs

  result = materialize(asset_selection=nps_targets, resources=defs.get_resources())
  print(result.success)
  ```

### 7.5 Fail Fast 정책
- Dagster 설정 및 Job/Op 레벨에서 재시도 옵션을 사용하지 않는다.
- 실패는 즉시 surface하여 원인 분석 후 재실행한다.

### 7.6 확장 아이디어
1. `tags["data_tier"]` 기반 대규모 재수집 Job (예: Bronze 전체)
2. 교차 도메인 Gold 계층 분석 Job
3. 관측성 강화를 위한 Freshness 센서 및 AssetCheck Job

### 7.7 Job 추가 체크리스트
1. [ ] `dag/jobs.py`에 Job 정의 추가 및 설명/태그 작성
2. [ ] 사용 대상 애셋의 `group_name` 일치 여부 확인
3. [ ] `dag/definitions.py`의 `jobs` 리스트에 등록
4. [ ] `dagster job launch -j ...` 로 실행 검증
5. [ ] 재시도 옵션이 활성화되어 있지 않은지 확인

### 7.8 검증 스크립트
```bash
python -c "from dag.definitions import defs; print(f'Total jobs: {len(defs.jobs)}'); [print(f'- {j.name}: {j.description}') for j in defs.jobs]"
python -c "from dag.jobs import nps_history_job; from dag.definitions import defs; g=defs.get_asset_graph(); ks=nps_history_job.asset_selection.resolve(g); [print(k) for k in ks]; print(len(ks))"
```

⸻

## 8. 환경/리소스 & 설정 관리

### 8.1 환경 변수 & 설정 클래스
```python
import os

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
API_KEY = os.getenv("API_KEY")
```

### 8.2 Pydantic 설정 예시
```python
from pydantic import BaseSettings, Field

class DatabaseConfig(BaseSettings):
    host: str = Field(env="DB_HOST")
    port: int = Field(default=5432, env="DB_PORT")
    database: str = Field(env="DB_NAME")
    username: str = Field(env="DB_USER")
    password: str = Field(env="DB_PASSWORD")

    class Config:
        env_file = ".env"
```

### 8.3 리소스 정의 주의사항
- 자격 증명은 반드시 EnvVar/시크릿 매니저 사용.
- 하드코딩 금지 (API 키, DB 비밀번호, 엔드포인트 등).
- 로컬 개발용 기본값이 필요하면 `.env.example`에 문서화한다.

### 8.4 설정 체크리스트
1. [ ] 새로운 리소스는 EnvVar 기반으로 작성했는가?
2. [ ] 민감 정보가 코드/커밋에 노출되지 않았는가?
3. [ ] 환경별 설정(dev/stg/prod) 분리가 명확한가?
4. [ ] 필요한 경우 Pydantic/BaseSettings 등 검증 로직을 추가했는가?

⸻

## 9. 유틸리티 & 헬퍼 모음

### 9.1 데이터 변환/품질 유틸
```python
import pandas as pd

def standardize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_", regex=False)
        .str.replace("[^a-z0-9_]", "", regex=True)
    )
    return df
```

### 9.2 데이터 검증 헬퍼
```python
from typing import List

def validate_required_columns(df: pd.DataFrame, required: List[str]) -> None:
    missing = set(required) - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {missing}")
```

### 9.3 데이터베이스 유틸 (Fail Fast)
```python
from contextlib import contextmanager
from typing import Generator

@contextmanager
def db_conn(resource) -> Generator:
    conn = resource.get_connection()
    try:
        yield conn
    finally:
        conn.close()
```

- 트랜잭션은 최소 범위로 묶고, 실패 시 즉시 예외를 전파한다.

### 9.4 데이터 품질/메타데이터 유틸
```python
import dagster as dg
import pandas as pd
from typing import Any, Dict, Optional

def get_data_quality_metrics(df: pd.DataFrame) -> Dict[str, Any]:
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
    preview_rows: int = 10,
) -> Dict[str, dg.MetadataValue]:
    metadata: Dict[str, dg.MetadataValue] = {
        "dagster/row_count": dg.MetadataValue.int(len(df)),
        "columns": dg.MetadataValue.int(len(df.columns)),
        "memory_usage_mb": dg.MetadataValue.float(
            df.memory_usage(deep=True).sum() / 1024 / 1024
        ),
    }
    if description:
        metadata["description"] = dg.MetadataValue.text(description)
    if include_preview and not df.empty:
        metadata["preview"] = dg.MetadataValue.md(
            df.head(preview_rows).to_markdown(index=False)
        )
    metrics = get_data_quality_metrics(df)
    metadata["duplicate_rows"] = dg.MetadataValue.int(metrics["duplicate_rows"])
    return metadata
```

### 9.5 데이터베이스 커넥션 베스트 프랙티스
```python
from contextlib import contextmanager
from typing import Generator
import dagster as dg

@contextmanager
def get_database_connection(resource: dg.ConfigurableResource) -> Generator:
    conn = resource.get_connection()
    try:
        yield conn
    finally:
        conn.close()
```

- 실패 시 즉시 예외를 전파하며, 수동 재시도나 지수 백오프 코드는 작성하지 않는다.

### 9.6 금지 패턴 (정책 위반 사례)
```python
# ❌ 금지: 재시도 유틸 (기존 execute_sql_with_retry 패턴은 사용하지 않는다.)
def execute_sql_with_retry(*_args, **_kwargs) -> None:
    raise NotImplementedError("Fail Fast 정책으로 제거되었습니다")
```

- 과거 `execute_sql_with_retry`와 같은 재시도/백오프 유틸은 Fail Fast 정책에 맞게 제거하거나 예외를 발생시키도록 수정한다.
- 네트워크/DB 실패는 Dagster 런타임에서 실패로 기록하고 원인 파악 후 재실행한다.

⸻

## 10. 테스트 전략

### 10.1 단위 테스트
- `build_asset_context`, 목 리소스 등을 사용해 단일 애셋 기능을 검증한다.
- 비즈니스 로직과 데이터 변환 함수에 대한 단위 테스트를 작성한다.

### 10.2 통합 테스트
- `materialize` 또는 `JobDefinition.execute_in_process`로 소규모 파이프라인을 실행한다.
- 테스트용 DuckDB/SQLite 등 임시 리소스를 활용해 환경을 격리한다.

### 10.3 모듈 등록 검증
```bash
python -c "from dag.definitions import defs; print(f'Total assets: {len(defs.assets)}')"
python -c "from dag.definitions import defs; print(f'Asset checks: {len(defs.asset_checks)}')"
```

### 10.4 테스트 체크리스트
1. [ ] 핵심 비즈니스 로직에 대한 단위 테스트 존재
2. [ ] 주요 파이프라인에 대한 통합 테스트 작성 여부
3. [ ] 신규 모듈 추가 시 등록 수·목록 검증 스크립트 실행
4. [ ] 실패 케이스(엣지, 누락 데이터 등) 테스트 포함

### 10.5 테스트 예시 코드
```python
import dagster as dg
from dagster import build_asset_context
import pandas as pd
import pytest


def test_my_asset():
    """Asset 기능 단위 테스트 예시"""
    context = build_asset_context()
    mock_duckdb = MockDuckDBResource()  # 테스트용 목 리소스/fixture
    result = my_asset(context, mock_duckdb)
    assert isinstance(result, dg.MaterializeResult)
    assert "dagster/row_count" in result.metadata
    assert result.metadata["dagster/row_count"].value > 0


def test_asset_validation():
    """검증 로직 테스트 예시"""
    frame = pd.DataFrame({
        "business_registration_number": ["1234567890", None, "123"],
        "business_name": ["Test Corp", "Another Corp", "Third Corp"],
    })
    with pytest.raises(ValueError, match="Missing required columns"):
        validate_required_columns(frame, ["missing_column"])
```

```python
from dagster import materialize


def test_pipeline_integration():
    """전체 파이프라인 통합 테스트 예시"""
    result = materialize(
        [asset1, asset2, asset3],
        resources={
            "duckdb": test_duckdb_resource,
            "postgres": test_postgres_resource,
        },
    )
    assert result.success
```

⸻

## 11. 성능 & 최적화 가이드
### 11.1 성능 우선 개발 원칙
- **Speed First**: 처리 속도와 시스템 성능을 최우선으로 고려한다.
- **벡터화**: 가능한 경우 pandas/numpy 벡터 연산을 사용하고 반복문을 지양한다.
- **메모리 절약**: 데이터 타입을 명확히 지정하고 불필요한 컬럼을 즉시 제거한다.
- **과공학 금지**: 측정 이전의 과도한 최적화는 피하고, 단순한 알고리즘으로 시작한다.

### 11.2 DB 쿼리 최적화 예시
```python
# GOOD: 필요한 컬럼만 조회하고 필터/정렬을 명확히 지정
EFFICIENT_QUERY = """
    SELECT business_name, subscription_status_code, subscriber_count
    FROM national_pension_businesses
    WHERE data_creation_month >= %s
      AND subscription_status_code = 1
    ORDER BY subscriber_count DESC
    LIMIT 1000
"""

# ❌ AVOID: 전체 컬럼을 제한 없이 읽어오는 패턴
INEFFICIENT_QUERY = "SELECT * FROM national_pension_businesses"
```

- 인덱스가 필요한 컬럼은 사전에 정의하고 `EXPLAIN`으로 실행 계획을 확인한다.
- 개발 환경에서는 LIMIT/OFFSET을 활용해 데이터 전송량을 줄인다.

### 11.3 메모리 관리 & 청크 처리
```python
import pandas as pd
from typing import Generator

def process_large_dataset_in_chunks(conn, table: str, chunk_size: int = 10_000) -> Generator[pd.DataFrame, None, None]:
    offset = 0
    while True:
        query = f"""
            SELECT *
            FROM {table}
            ORDER BY id
            LIMIT {chunk_size} OFFSET {offset}
        """
        chunk = conn.execute(query).fetchdf()
        if chunk.empty:
            break
        yield chunk
        offset += chunk_size
```

- 큰 테이블은 청크 단위로 처리하고, 처리 후 즉시 메모리를 해제한다.
- 파티션 키 기준으로 데이터를 분할하여 부분 materialization을 지원한다.

### 11.4 캐싱 & 증분 Materialization
```python
import dagster as dg

@dg.asset(
    partitions_def=dg.DailyPartitionsDefinition(start_date="2023-01-01"),
    io_manager_key="parquet_io_manager",
    tags={"cache": "enabled", "data_tier": "silver"},
)
def daily_stock_analysis(context: dg.AssetExecutionContext, raw_stock_data) -> dg.MaterializeResult:
    partition_key = context.partition_key
    sliced = raw_stock_data[raw_stock_data["date"] == partition_key]
    return dg.MaterializeResult(
        metadata={
            "partition_date": dg.MetadataValue.text(partition_key),
            "dagster/row_count": dg.MetadataValue.int(len(sliced)),
        }
    )
```

- 비용이 큰 계산 결과는 Parquet/Delta 등 효율적 포맷으로 캐싱한다.
- 태그(`{"cache": "enabled"}`)를 활용해 캐시 정책을 명시한다.

⸻

## 12. 보안 & 컴플라이언스
- PII는 `tags` 또는 `metadata`에 명시하고 접근 통제를 설정한다(`{"pii": "true"}` 등).
- 민감 데이터는 암호화, 데이터 마스킹, 감사 로깅을 통해 보호한다.
- 환경 분리(dev/stg/prod)를 유지하고 네트워크·시크릿 관리 정책을 지킨다.
- GDPR 등 적용 규정을 식별하고 관련 문서 링크를 유지한다.

### 12.1 PII 자산 정의 예시
```python
import time

@dg.asset(
    description="Contains personally identifiable information - handle with care",
    group_name="SS",
    tags={
        "pii": "true",
        "compliance": "gdpr",
        "access_level": "restricted",
        "data_tier": "gold",
    },
)
def customer_personal_data(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    record_count = 0
    # ... 보안 처리 로직 ...
    context.log.info("Processed PII data - audit log entry created")
    return dg.MaterializeResult(
        metadata={
            "pii_records_processed": dg.MetadataValue.int(record_count),
            "audit_timestamp": dg.MetadataValue.timestamp(time.time()),
        }
    )
```

- 접근 제어, 감사 로깅, 암호화 여부를 메타데이터로 노출해 감사 가능성을 높인다.

⸻

## 13. 모니터링·관측성 & 로깅
### 13.1 AssetCheck & 신선도 모니터링
```python
@dg.asset_check(
    asset=financial_analysis,
    description="Monitor for significant changes in financial metrics",
)
def monitor_financial_trends(
    context: dg.AssetCheckExecutionContext,
    financial_analysis,
) -> dg.AssetCheckResult:
    current_metrics = calculate_current_metrics(financial_analysis)
    historical_metrics = get_historical_metrics()
    deviation_threshold = 0.15
    significant = []
    for metric, current in current_metrics.items():
        baseline = historical_metrics.get(metric, 0)
        if baseline > 0:
            deviation = abs(current - baseline) / baseline
            if deviation > deviation_threshold:
                significant.append(f"{metric}: {deviation:.2%} change")
    if significant:
        return dg.AssetCheckResult(
            passed=False,
            severity=dg.AssetCheckSeverity.WARN,
            description=", ".join(significant),
            metadata={
                "changes_detected": dg.MetadataValue.text(str(significant)),
                "deviation_threshold": dg.MetadataValue.float(deviation_threshold),
            },
        )
    return dg.AssetCheckResult(passed=True, description="Financial trends within normal ranges")
```

### 13.2 구조적 로깅 & 실행 시간 계측
```python
import logging
import time
from functools import wraps

def log_execution_time(func):
    """실행 시간을 측정해 구조적 로그로 남긴다."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time.time()
        try:
            result = func(*args, **kwargs)
            elapsed = time.time() - start
            logging.info(
                "Function completed",
                extra={"function": func.__name__, "execution_time": elapsed, "status": "success"},
            )
            return result
        except Exception as exc:
            elapsed = time.time() - start
            logging.error(
                "Function failed",
                extra={
                    "function": func.__name__,
                    "execution_time": elapsed,
                    "status": "error",
                    "error": str(exc),
                },
            )
            raise
    return wrapper
```

- AssetCheck/Freshness 센서를 통해 품질·신선도를 모니터링하고, Slack/Webhook 등으로 알림을 연동한다.
- 성능 메트릭(행수, 실행 시간, 메모리)을 `MaterializeResult`에 기록해 추세를 관찰한다.

⸻

## 14. 검증/디버깅 스크립트 모음
```bash
# 총 자산 수 확인
python -c "from dag.definitions import defs; print(f'Total assets: {len(defs.assets)}')"

# 특정 그룹(NPS) 애셋 확인
python -c "from dag.definitions import defs; from dagster import AssetSelection; g=defs.get_asset_graph(); ks=AssetSelection.groups('NPS').resolve(g); [print(k) for k in ks]; print(len(ks))"

# 잡 목록 확인
python -c "from dag.definitions import defs; [print(f"{j.name}: {j.description}") for j in defs.jobs]"

# Job이 포함하는 애셋들 확인
python -c "from dag.jobs import nps_history_job; from dag.definitions import defs; g=defs.get_asset_graph(); ks=nps_history_job.asset_selection.resolve(g); [print(k) for k in ks]"

# 애셋 체크 수 확인
python -c "from dag.definitions import defs; print(f'Asset checks: {len(defs.asset_checks)}')"
```

⸻

## 15. 자주 발생하는 문제 & 해결
1. **모듈 임포트 오류** → `from dag import nps_data_processing` 등 정확한 패키지 경로로 임포트.
2. **중복 애셋명** → 프로젝트 접두사를 포함한 고유 명명 규칙 유지.
3. **존재하지 않는 의존성** → `deps`는 항상 애셋명으로 지정 (`"nps_his_download"`).
4. **파일 분할 후 설정 누락** → 설정 클래스/리소스를 새 모듈에서 올바르게 임포트.
5. **`load_assets_from_modules` 타입 오류** → 문자열이 아닌 모듈 객체를 전달.
6. **Job이 애셋을 못 찾음** → `group_name`이 허용된 표준명인지, 대소문자 일치 여부 확인.
7. **definitions.py 미등록** → 모듈/잡/센서/스케줄/리소스 리스트에 모두 추가.
8. **순환 의존성** → 애셋 정의 순서를 점검하고 최소 의존 원칙을 따른다.
9. **재시도 로직 도입** → 조직 정책 위반. 관련 코드/설정을 즉시 제거.

⸻

## 16. 베스트 프랙티스 요약
- 접두사 규약, 파일 크기, 기능 단위 분할을 준수한다.
- `@dg.asset` + 풍부한 메타데이터 + `group_name` 표준(5종) 조합을 유지한다.
- 메달리온 계층은 `tags.data_tier`로 통일하고 kinds에는 계층 정보를 넣지 않는다.
- 파티션/증분 처리·Parquet·캐싱으로 성능을 극대화한다.
- 테스트·검증 스크립트로 등록·선택·의존 관계를 상시 점검한다.
- 보안·시크릿·환경 분리를 기본값으로 삼는다.
- Fail Fast: 재시도 전면 금지(코드/설정/유틸).

⸻

## 17. 부록 A — 허용 kinds 전체 목록

> 최대 3개까지 사용 가능. 아래 목록에 `bronze`, `silver`, `gold`가 포함되어 있어도 **kinds에는 사용 금지**이며 계층 표기는 오직 `tags.data_tier`로 수행한다.

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
semanticmodel, source, seed, file, dashboard, report, notebook, workbook, csv

⸻

## 18. 부록 B — 자주 쓰는 CLI/파이썬 스니펫

```bash
# 애셋 등록 상태 요약
python -c "from dag.definitions import defs; print(f'등록된 애셋 수: {len(defs.assets)}')"

# 특정 모듈만 로드해 보기
python -c "from dagster import load_assets_from_modules; from dag import nps_raw_ingestion, nps_data_processing; assets = load_assets_from_modules([nps_raw_ingestion, nps_data_processing]); print(len(assets)); [print(x.key) for x in assets]"

# 개발 서버
dagster dev

# Job 실행 (최신 표준 CLI)
dagster job launch -j nps_history_job

# Job 실행 계획 (dry run)
dagster job launch -j cd_daily_update_job --dry-run

# 새 모듈 문법 검사
python -m py_compile dag/new_module.py
```

⸻

_이 문서는 v2025-09 기준으로 확정(최종)되었으며, 변경 시 반드시 본 문서를 갱신하고 변경 이력과 함께 배포한다._
