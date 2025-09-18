# Silla-DAG 통합 프로젝트 기술 명세서

## 1. 개요

**Silla-DAG**는 Dagster를 기반으로 주식, 커머스, 커뮤니티, 공공데이터 등 서로 다른 도메인의 파이프라인을 한 곳에서 실행·감시·자동화하기 위한 중앙 데이터 오케스트레이션 레이어입니다. 프로젝트별 자산 모듈은 `dag/` 패키지에 공존하며, `definitions.py`가 모든 에셋, 체크, 잡, 스케줄, 센서, 리소스를 하나의 `Definitions` 객체로 결합합니다.【F:dag/dag/definitions.py†L8-L215】

### 1.1 관리 대상 프로젝트 요약

| 프로젝트 | 핵심 목적 | 주요 기동 방법 | 주요 스토리지/리소스 | 상세 문서 |
| --- | --- | --- | --- | --- |
| **CD (Coin-Data)** | 국내 상장사 가격·시총·재무·로고 데이터를 정규화해 지표와 순위를 제공 | `cd_daily_update_job`, `cd_daily_update_turso_job`, `cd_history_job`, 이미지 파이프라인용 `cd_docker_job`/`cd_node_job` | PostgreSQL, TursoDB, DuckDB, Cloudflare R2, Docker/Node 크롤러 | [`docs/cd.md`](docs/cd.md)【F:dag/dag/jobs.py†L25-L60】【F:dag/docs/cd.md†L5-L143】 |
| **SS (Social-Scraping)** | Crawlee로 수집한 네이버 카페/커뮤니티 딜 정보를 규칙·LLM 기반으로 정제·집계 | `ss_ingest_job`이 `ss_crawl_sources` 이후 전 파이프라인을 구동 | PostgreSQL, OpenAI 호환 LLM 리소스 | [`docs/ss.md`](docs/ss.md)【F:dag/dag/jobs.py†L83-L88】【F:dag/docs/ss.md†L5-L83】 |
| **NPS (National Pension Service)** | 국민연금 사업장 가입자 CSV를 다운로드·정제해 Postgres/SaaS 계층으로 적재 | `nps_history_job`으로 브론즈~인프라 자산 일괄 실행 | DuckDB, PostgreSQL | [`docs/nps.md`](docs/nps.md)【F:dag/dag/jobs.py†L16-L22】【F:dag/docs/nps.md†L5-L88】 |
| **DS (Downstream)** | 네이버 쇼핑 라이브 로고 이미지를 수집해 Cloudflare R2와 사내 DB에 반영 | `ds_node_job`/`ds_docker_job`으로 Crawlee 크롤러를 실행 후 후속 에셋 호출 | PostgreSQL, DuckDB, Cloudflare R2, Docker/Node 런타임 | [`docs/ds.md`](docs/ds.md)【F:dag/dag/jobs.py†L62-L74】【F:dag/docs/ds.md†L5-L83】 |
| **IS (Isshoo)** | 다중 커뮤니티 인기 글을 실시간 수집·정규화·랭킹·클러스터링하고 AI 후처리 | `is_crawler_*` 잡과 `is_ingest_job`, 파일 센서, Freshness 센서로 자동화 | PostgreSQL, OpenAI 호환 LLM/VLM 리소스 | [`docs/is.md`](docs/is.md)【F:dag/dag/jobs.py†L76-L105】【F:dag/dag/is_sensor_output_data.py†L10-L79】【F:dag/dag/definitions.py†L150-L216】【F:dag/docs/is.md†L5-L104】 |

### 1.2 문서 구조

- `docs/cd.md`: CD 프로젝트 상세 명세
- `docs/ss.md`: SS 프로젝트 상세 명세
- `docs/nps.md`: NPS 프로젝트 상세 명세
- `docs/ds.md`: DS 프로젝트 상세 명세
- `docs/is.md`: IS 프로젝트 상세 명세
- `docs/coding.md`: 코드 스타일 및 공통 가이드
- `AGENTS.md`: 리포지토리 공통 기여 가이드라인

### 1.3 Dagster 구성 스냅샷

- **에셋 & 체크**: `definitions.py`가 각 프로젝트 모듈에서 에셋과 체크를 로드하여 단일 그래프를 구성합니다.【F:dag/dag/definitions.py†L82-L195】
- **잡 & 스케줄**: 프로젝트별 잡은 `dag/dag/jobs.py`에 정의되며, CD·IS 잡 일부는 크론 스케줄로 자동화됩니다.【F:dag/dag/jobs.py†L16-L105】【F:dag/dag/schedules.py†L13-L48】
- **센서**: IS 파일 센서(`is_output_data_sensor`)와 ISShoo 핵심 자산의 Freshness 센서가 등록되어 실시간 감시를 수행합니다.【F:dag/dag/is_sensor_output_data.py†L10-L79】【F:dag/dag/definitions.py†L150-L216】
- **리소스**: DuckDB, PostgreSQL, Turso, 다중 OpenAI 호환 엔드포인트, Docker Pipes 클라이언트 등을 공용 리소스로 주입합니다.【F:dag/dag/definitions.py†L217-L285】

---

## 2. CD (Coin-Data) 프로젝트 – 주식 데이터 & 이미지 파이프라인

CD 프로젝트는 대한민국 주식 시장 데이터를 다각도로 수집하고, 파티션 기반 파이프라인으로 가격·시총·재무 지표를 정제한 뒤 회사 단위 지표와 순위를 계산합니다. 파이프라인은 PostgreSQL과 TursoDB 이중 타깃을 지원하며, 로고 이미지 파이프라인은 Crawlee 크롤러와 Cloudflare R2를 연계합니다.【F:dag/docs/cd.md†L5-L143】

- **데이터 처리**: `cd_raw_ingestion.py`, `cd_price_processing.py`, `cd_marketcap_processing.py`, `cd_bppedd_processing.py`가 pykrx 데이터를 임시 테이블로 적재하고 정합성 검증 후 본 테이블로 동기화합니다.【F:dag/docs/cd.md†L95-L117】
- **지표/랭킹 생성**: `cd_metrics_processing.py` 계열 자산이 최신 지표 통합, 회사 시총 합산, 순위 계산, `security_rank` 테이블 업서트를 수행합니다.【F:dag/docs/cd.md†L118-L125】
- **검색어/이미지 파이프라인**: `cd_display_processing.py`는 검색어 사전을 확장하고, `cd_img_r2_*` 자산은 R2 다운로드·WebP 변환·최종 업로드 및 DB 갱신을 담당합니다.【F:dag/docs/cd.md†L126-L143】
- **잡 구성**: 일별 파티션 잡과 Turso 전용 잡, 과거 데이터 백필 잡, 이미지 전용 Node/Docker 잡이 제공되어 운영 목적에 맞춰 선택할 수 있습니다.【F:dag/dag/jobs.py†L25-L60】

## 3. SS (Social-Scraping) 프로젝트 – 커뮤니티 딜 파이프라인

SS 프로젝트는 Crawlee TypeScript 크롤러를 빌드·실행해 얻은 JSON을 감시하고, Dagster 에셋으로 정규화/규칙 추출/LLM 추출/집계를 수행해 PostgreSQL 스키마와 API 뷰를 유지합니다.【F:dag/docs/ss.md†L5-L83】

- **크롤러 실행**: `ss_crawler_build`와 `ss_crawler_executor` 에셋이 Crawlee 프로젝트를 빌드 후 순차 실행하면서 로그 기반 통계를 남깁니다.【F:dag/docs/ss.md†L7-L13】
- **데이터 관찰**: `ss_crawl_sources`가 JSON 디렉터리를 동적 파티션으로 추적하고, `ss_raw_posts`가 중복 통계와 함께 원본을 적재합니다.【F:dag/docs/ss.md†L12-L52】
- **정규화 & AI 추출**: `ss_normalized_posts`, `ss_extracted_rules_based`, `ss_extracted_llm`, `ss_deals_upsert`, `ss_aggregates_daily`가 규칙·LLM 추출과 집계를 담당합니다.【F:dag/docs/ss.md†L45-L63】
- **사전 & 스키마 관리**: `ss_bootstrap_schema`, `ss_bootstrap_dictionary_schema`, `ss_seed_dictionary_data`가 핵심 테이블과 데이터 사전을 재구축합니다.【F:dag/docs/ss.md†L47-L70】
- **운영 리소스**: 에셋은 주로 PostgreSQL과 OpenAI 호환 리소스를 사용하며, 환경 변수를 통해 게이트 임계치를 제어합니다.【F:dag/docs/ss.md†L72-L83】
- **잡 구성**: `ss_ingest_job`이 `ss_crawl_sources` 이후 전 에셋을 실행하도록 설정되어 있습니다.【F:dag/dag/jobs.py†L83-L88】

## 4. NPS 프로젝트 – 국민연금 CSV ETL & SaaS 계층

NPS 파이프라인은 공공데이터포털 CSV를 병렬로 다운로드한 뒤 무결성 검증, 인코딩 정제, PostgreSQL 적재, 인덱스 최적화, SaaS 계층 ETL까지 일괄 자동화합니다.【F:dag/docs/nps.md†L5-L88】

- **브론즈 계층**: `nps_raw_ingestion`이 CSV를 병렬 수집하고 DuckDB에 메타데이터를 기록하며, `nps_file_integrity_check`가 파일 상태를 검증합니다.【F:dag/docs/nps.md†L51-L58】
- **실버/웨어하우스**: `nps_data_processing`이 UTF-8-SIG 정규화를 수행하고, `nps_to_postgres_simple`이 중복 제거 후 `public.pension`으로 COPY 적재합니다.【F:dag/docs/nps.md†L59-L67】
- **인프라 & SaaS**: `nps_pension_staging_indexes`가 확장·함수·인덱스를 구축하고, `ensure_saas_tables`/`nps_saas_etl_run`/`nps_full_rebuild_from_pension`이 SaaS 계층을 유지·재구축합니다.【F:dag/docs/nps.md†L68-L82】
- **에셋 체크**: Postgres 적재, 인덱스 준비, SaaS 신선도를 검증하는 체크가 함께 로드됩니다.【F:dag/docs/nps.md†L84-L88】
- **잡 구성**: `nps_history_job`이 NPS 그룹 전체 자산을 한 번에 실행하도록 정의되어 있습니다.【F:dag/dag/jobs.py†L16-L22】

## 5. DS (Downstream) 프로젝트 – 쇼핑 라이브 로고 파이프라인

DS 파이프라인은 Crawlee 기반 `ds-naver-crawler`가 Cloudflare R2 소스 버킷에 업로드한 PNG를 내려받아 WebP 변환 후 타깃 버킷과 내부 DB에 반영합니다.【F:dag/docs/ds.md†L5-L83】

- **크롤러 기동**: `ds_node_job`/`ds_docker_job`이 각각 Node.js 또는 Docker 방식으로 크롤러를 실행하며, 성공 시 다운로드·변환·최종 반영 에셋 전체가 실행됩니다.【F:dag/dag/jobs.py†L62-L74】【F:dag/docs/ds.md†L11-L17】
- **에셋 체인**: `ds_r2_download` → `ds_png2webp` → `ds_img_finalize`가 로컬 임시 저장, WebP 변환, Cloudflare R2 업로드 및 Postgres/DuckDB 갱신을 담당합니다.【F:dag/docs/ds.md†L31-L83】
- **유틸리티 & 체크**: `ds_r2_clear_src`/`ds_r2_clear_tgt`는 버킷 정리를, `ds_r2_src_check`/`ds_r2_tgt_check`는 연결성 검증을 수행합니다.【F:dag/docs/ds.md†L43-L50】
- **리소스**: 파이프라인은 `ds_postgres`, `ds_duckdb`, Cloudflare R2 자격 증명, 공개 URL 환경 변수를 요구합니다.【F:dag/docs/ds.md†L18-L28】

## 6. IS (Isshoo) 프로젝트 – 실시간 커뮤니티 이슈 파이프라인

IS 프로젝트는 국내 주요 커뮤니티의 인기 게시물을 JSON으로 수집해 정규화하고, 트렌드/클러스터/키워드/AI 후처리를 수행해 Isshoo 웹 서비스에 공급합니다.【F:dag/docs/is.md†L5-L104】

- **수집 & 센서**: Crawlee 크롤러가 `output_data/*.json`을 생성하면 `is_output_data_sensor`가 30초 간격으로 감시하여 `is_ingest_job`을 트리거합니다.【F:dag/dag/is_sensor_output_data.py†L10-L79】【F:dag/docs/is.md†L11-L42】
- **동기 자산**: `posts_asset` 이하 동기 자산이 게시물, 댓글, 이미지, 임베드, 스냅샷, 시그니처를 정규화합니다.【F:dag/docs/is.md†L40-L60】
- **골드 & 운영 자산**: 트렌드·클러스터·키워드·큐 승격 자산은 AutomationCondition(`ON2`, `ON10`, `ON30`, `ON1H`)을 통해 주기적으로 실행되며, Freshness 체크로 SLA를 감시합니다.【F:dag/dag/schedules.py†L13-L31】【F:dag/dag/definitions.py†L150-L179】【F:dag/docs/is.md†L61-L103】
- **AI 후처리**: `is_data_llm.py`와 `is_data_vlm.py` 자산이 텍스트/이미지 큐를 소비해 LLM/VLM 분석 결과를 적재합니다.【F:dag/docs/is.md†L88-L93】
- **잡 & 스케줄**: `is_crawler_schedule_10min`이 10분 간격 크롤러를 실행하고, `is_crawler_interval_job`은 장기 백필을 지원합니다.【F:dag/dag/jobs.py†L90-L105】【F:dag/dag/schedules.py†L42-L48】

---

## 7. 기술 스택 및 운영 원칙

- **오케스트레이션**: Dagster + Dagster Cloud, AutomationCondition을 이용한 선언적 스케줄 운영.【F:dag/dag/schedules.py†L13-L31】
- **언어 & 프레임워크**: Python 3.12, Pandas, DuckDB, Requests, BeautifulSoup, Playwright/Crawlee(TypeScript) 등 프로젝트별 스택 혼합.【F:dag/docs/cd.md†L11-L19】【F:dag/docs/nps.md†L5-L6】【F:dag/docs/ds.md†L5-L7】【F:dag/docs/ss.md†L5-L10】
- **데이터베이스**: PostgreSQL, Turso(libSQL), DuckDB를 공용 리소스로 정의하고 프로젝트별로 주입합니다.【F:dag/dag/definitions.py†L221-L257】
- **AI/LLM 리소스**: OpenAI 호환 엔드포인트를 여러 개 등록해 IS·SS 프로젝트의 텍스트/이미지 추출에 활용합니다.【F:dag/dag/definitions.py†L258-L285】【F:dag/docs/is.md†L88-L93】【F:dag/docs/ss.md†L56-L76】
- **오브젝트 스토리지**: Cloudflare R2를 CD/DS 이미지 파이프라인에서 사용합니다.【F:dag/docs/cd.md†L132-L143】【F:dag/docs/ds.md†L18-L83】

## 8. 프로젝트 구조

```
├── pyproject.toml             # Python 프로젝트 설정
├── setup.cfg                  # 설치 설정
├── setup.py                   # 설치 스크립트
├── README.md                  # 최상위 문서
├── dag/                       # 메인 패키지 디렉터리
│   ├── definitions.py         # Dagster Definitions 등록부
│   ├── jobs.py / schedules.py # 잡·스케줄 정의
│   ├── resources.py           # Postgres/Turso/OpenAI 등 리소스
│   ├── cd_*.py                # CD 자산 모듈 (pykrx, 이미지 등)
│   ├── ds_*.py                # DS 자산 및 Crawlee 연동 모듈
│   ├── ss_*.py                # SS 파이프라인 및 LLM 모듈
│   ├── is_*.py                # IS 파이프라인, 센서, 크롤러 스케줄러
│   ├── nps_*.py               # NPS ETL 및 SaaS 자산
│   └── ...                    # 기타 공용 유틸리티
├── dag_tests/                 # 테스트 디렉터리 (자산 단위 테스트)
├── data/                      # 로컬 DuckDB/임시 파일 저장소
└── docs/                      # 문서 (본 개요 및 프로젝트별 상세)
```

## 9. 개발 환경 및 실행

- **의존성 설치**: `pip install -e ".[dev]"`
- **로컬 Dagster UI**: `dagster dev` (기본 포트 3000)
- **테스트 실행**: `python -m pytest`

## 10. 운영 시 참고 명령어

프로젝트별 에셋 그래프를 확인하려면 아래 패턴을 사용할 수 있습니다.

```bash
python -c "
from dag.definitions import defs
from dagster import AssetSelection

asset_graph = defs.get_asset_graph()
for group in ['CD', 'DS', 'SS', 'NPS', 'IS']:
    assets = AssetSelection.groups(group).resolve(asset_graph)
    print(f'[{group}] total {len(assets)} assets')
    for asset_key in sorted(assets, key=lambda k: k.to_user_string()):
        print('  -', asset_key.to_user_string())
    print()
"
```

이 명령은 `definitions.py`에 등록된 그룹별 에셋을 나열하여 파이프라인 구성을 빠르게 점검할 수 있습니다.【F:dag/dag/definitions.py†L82-L195】
