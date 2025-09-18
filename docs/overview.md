# Silla-DAG 통합 프로젝트 기술 명세서

## 1. 개요

**Silla-DAG**는 [Dagster](https://dagster.io/)를 기반으로 구축된 중앙 데이터 엔지니어링 플랫폼입니다. 이 저장소는 각기 다른 목적과 기술 스택을 가진 여러 데이터 파이프라인 프로젝트를 통합 관리하며, 데이터의 수집, 처리, 분석, 저장을 자동화하고 모니터링하는 것을 목표로 합니다.

### 1.1. 관리 대상 프로젝트

본 플랫폼은 다음과 같은 5개의 핵심 데이터 프로젝트를 관리합니다.

1.  **[CD (Coin-Data)](#2-cd-coin-data-프로젝트-주식-정보)**: 대한민국 주식 시장 데이터 서비스
2.  **[SS (Social-Scraping)](#3-ss-social-scraping-프로젝트-커뮤니티-요약)**: 소셜 커뮤니티 게시물 수집 및 LLM 요약 서비스
3.  **[NPS (National Pension Service)](#4-nps-프로젝트-국민연금-데이터)**: 국민연금공단 사업장 가입자 데이터 파이프라인
4.  **[DS (Downstream)](#5-ds-downstream-프로젝트-쇼핑-라이브-크롤링)**: 네이버 쇼핑 라이브 데이터 수집 파이프라인
5.  **[IS (Isshoo)](#6-is-isshoo-프로젝트-커뮤니티-이슈-집계)**: 국내 여러 커뮤니티의 인기 게시물 집계 및 랭킹 서비스

### 1.2. 문서 구조

각 프로젝트의 상세 내용은 해당 문서를 참조하십시오.

-   `cd.md`: CD 프로젝트 상세 명세
-   `ss.md`: SS 프로젝트 상세 명세
-   `nps.md`: NPS 프로젝트 상세 명세
-   `ds.md`: DS 프로젝트 상세 명세
-   `is.md`: IS 프로젝트 상세 명세
-   `coding-guidelines.md`: DAG 프로젝트 코딩 가이드라인

---

## 2. CD (Coin-Data) 프로젝트 (주식 정보)

-   **문서**: `docs/cd.md`
-   **목적**: 대한민국 주식 시장(KOSPI, KOSDAQ, KONEX)의 종목, 시세, 시가총액, 재무 지표 등 데이터를 수집, 가공하여 종합적인 금융 정보를 제공합니다.
-   **핵심 기능**: `pykrx`를 이용한 데이터 수집, 회사/증권 정보 정규화, 지표 통합 및 순위 산정, `Crawlee`를 이용한 로고 이미지 수집 등.
-   **데이터 흐름**: `pykrx`/`Crawlee` → `DuckDB`/`PostgreSQL`/`TursoDB` → `Cloudflare R2`

## 3. SS (Social-Scraping) 프로젝트 (커뮤니티 요약)

-   **문서**: `docs/ss.md`
-   **목적**: 특정 온라인 커뮤니티(뽐뿌)의 게시물을 수집하고, LLM을 활용해 내용을 자동 요약하며, 데이터 구조를 문서화합니다.
-   **핵심 기능**: `Crawlee`를 사용한 스크래핑, `TursoDB`에 동기화, `ChatOpenAI`를 사용한 본문 요약, 데이터 사전 자동 생성.
-   **데이터 흐름**: `Crawlee` → `JSON` → `TursoDB` → `OpenAI` → `Markdown`

## 4. NPS (National Pension Service) 프로젝트 (국민연금 데이터)

-   **문서**: `docs/nps.md`
-   **목적**: 공공데이터포털에서 국민연금공단의 월별 사업장 가입자 데이터를 수집, 정제하고, 내부 `company` 정보와 결합(KYC)하여 분석용 데이터셋을 구축합니다.
-   **핵심 기능**: 월별 파티션 기반 데이터 수집, 데이터 클리닝 및 `Parquet` 변환, `PostgreSQL` 적재, KYC를 통한 데이터 보강.
-   **데이터 흐름**: `data.go.kr` → `CSV` → `Parquet` → `PostgreSQL`

## 5. DS (Downstream) 프로젝트 (쇼핑 라이브 크롤링)

-   **문서**: `docs/ds.md`
-   **목적**: 네이버 쇼핑 라이브의 커머스 데이터를 크롤링하여 원시 데이터를 Cloudflare R2 객체 스토리지에 저장하는 간단한 파이프라인입니다.
-   **핵심 기능**: `Crawlee`를 사용한 스크래핑 후 `Cloudflare R2`에 업로드. 로컬(`Node.js`) 및 `Docker` 실행 환경 지원.
-   **데이터 흐름**: `Crawlee` (`Node.js`/`Docker`) → `JSON` → `Cloudflare R2`

## 6. IS (Isshoo) 프로젝트 (커뮤니티 이슈 집계)

-   **문서**: `docs/is.md`
-   **서비스 URL**: [https://www.isshoo.xyz](https://www.isshoo.xyz)
-   **목적**: 여러 국내 커뮤니티의 인기 게시물을 실시간으로 수집, 집계하여 공정하고 즉시성 있는 이슈 큐레이션 서비스를 제공합니다.
-   **핵심 기능**: 사이트별 `Crawlee` 프로젝트, `FileSensor`를 통한 자동화된 데이터 수집, `PostgreSQL`에 정제 데이터 저장, 트렌드 집계.
-   **데이터 흐름**: `Crawlee` → `JSON` → `Dagster FileSensor` → `PostgreSQL` → `Isshoo Web`

---

## 7. 기술 스택 및 아키텍처

### 7.1. 코어 프레임워크
-   **데이터 오케스트레이션**: `Dagster`, `Dagster Cloud`
-   **프로그래밍 언어**: `Python 3.12`

### 7.2. 데이터베이스
-   **관계형 데이터베이스**: `PostgreSQL` (메인 DB, `psycopg2-binary`)
-   **원격 데이터베이스**: `TURSO` (libSQL 기반, `libsql-experimental`)
-   **분석용 임베디드 DB**: `DuckDB` (로컬 분석 및 임시 데이터 처리)

### 7.3. 데이터 처리 및 저장
-   **데이터프레임**: `Pandas` (v2.0+)
-   **파일 포맷**: `FastParquet` (컬럼 기반 스토리지)

### 7.4. 데이터 수집 (Crawling & Scraping)
-   **웹 크롤링 프레임워크**: `Crawlee` (Node.js/TypeScript)
-   **브라우저 자동화**: `Playwright`
-   **Python 라이브러리**: `pykrx` (한국 주식 시장)

### 7.5. 아키텍처 원칙
-   **Medallion Architecture**: 대부분의 프로젝트가 `Bronze(원시) -> Silver(정제) -> Gold(분석)` 데이터 계층 모델을 따릅니다.
-   **하이브리드 스택**: 데이터 수집은 `Node.js/TypeScript` 생태계의 `Crawlee`를, 데이터 변환(ETL)은 `Python` 생태계의 `Dagster`와 `Pandas`를 중심으로 구성합니다.
-   **다중 DB 지원**: `PostgreSQL`과 `TursoDB`를 동시에 지원하여 유연성을 확보합니다.

## 8. 프로젝트 구조

```
├── pyproject.toml             # Python 프로젝트 설정
├── setup.cfg                  # 설치 설정
├── setup.py                   # 설치 스크립트
├── README.md                  # 프로젝트 문서
├── dag/                       # 메인 패키지 디렉터리
│   ├── __init__.py            # 패키지 초기화
│   ├── definitions.py         # Dagster 정의 (Assets, Jobs, Schedules...)
│   └── ...                    # 각 프로젝트별 모듈 (cd_..., nps_... 등)
├── dag_tests/                 # 테스트 디렉터리
│   └── test_assets.py         # 에셋 테스트
├── data/                      # 데이터 저장 디렉터리
│   └── ...                    # 프로젝트별 데이터
└── docs/                      # 문서
    ├── overview.md            # 본 기술 명세서
    └── ...                    # 각 프로젝트별 상세 문서
```

## 9. 개발 환경 및 실행

### 9.1. 의존성 설치

-   **도구**: `pip`
-   **개발 환경 설치**: `pip install -e ".[dev]"`

### 9.2. 로컬 개발 서버 실행

-   **명령어**: `dagster dev`
-   **UI 접속**: [http://localhost:3000](http://localhost:3000)

### 9.3. 테스트

-   **테스트 실행**: `python -m pytest`

### 9.4. 유용한 검증 명령어

```bash
# 특정 그룹의 에셋 목록 확인 (예: NPS)
python -c "
from dag.definitions import defs
from dagster import AssetSelection
asset_graph = defs.get_asset_graph()
assets = AssetSelection.groups('NPS').resolve(asset_graph)
print('NPS 그룹 에셋들:')
[print(f'  - {asset_key}') for asset_key in assets]
"
```
