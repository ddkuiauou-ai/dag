# Isshoo (is.md) 통합 문서

## 1. 개요 (Overview)

Isshoo는 다양한 한국 커뮤니티 사이트(클리앙, 뽐뿌, FM코리아 등)의 인기 게시물을 신속하게 수집 및 집계하여 공정하고 즉시성 있는 이슈 큐레이션을 제공하는 데이터 파이프라인 및 서비스입니다. (서비스 URL: [https://www.isshoo.xyz](https://www.isshoo.xyz))

본 문서는 Isshoo 프로젝트의 아키텍처, 데이터 파이프라인, 크롤러 구현 현황, 그리고 향후 로드맵을 종합적으로 다루는 단일 소스 진실(Single Source of Truth) 문서입니다.

## 2. 아키텍처 (Architecture)

### 2.1. 데이터 흐름

Isshoo의 전체 데이터 흐름은 다음과 같습니다.

```
[Crawlers] --(JSON)--> [File Sensor] --(Dagster Asset)--> [Database] --> [API/Web]
```

1.  **Crawlers**: `Crawlee`와 `Playwright`를 사용하여 각 커뮤니티 사이트의 데이터를 수집하고, 표준화된 JSON 파일로 출력합니다.
2.  **File Sensor**: 지정된 디렉터리를 모니터링하여 새로운 JSON 파일 생성을 감지하고, Dagster 파이프라인 실행을 트리거합니다.
3.  **Dagster Assets**: 감지된 파일을 처리하여 데이터베이스(PostgreSQL)에 게시물, 댓글, 이미지, 트렌드 등 정제된 데이터를 저장하고 집계합니다.
4.  **API/Web**: 데이터베이스의 정보를 기반으로 사용자에게 랭킹, 최신 글, 키워드 트렌드 등을 제공합니다.

### 2.2. 프로젝트 구조

-   **Dagster 파이프라인 (`/dag/dag/`)**:
    -   `is_*.py`: 데이터 수집, 변환, 집계, 랭킹 로직을 포함하는 핵심 Dagster asset들이 정의되어 있습니다.
    -   `is_data_sql.sql`: 참조용 DDL 및 쿼리를 포함합니다.
-   **Crawlers (`/dag/dag/is_crawlee/`)**: 각 커뮤니티 사이트별로 **독립된 TypeScript 기반 Crawlee 프로젝트**가 각각의 하위 디렉터리(`clien-park/`, `fmkorea/` 등)에 위치합니다. 각 프로젝트는 자체 `package.json`과 `src/` 디렉터리를 가집니다.

## 3. 데이터 파이프라인 (Dagster Assets)

프로젝트 초기 설계 내용을 기반으로, 현재 코드 구현 상태를 반영하여 파이프라인을 기술합니다.

### 3.1. 핵심 자산 (Assets) 및 의존성

```
[File Sensor] -> posts_asset
 ├─ post_versions_asset
 ├─ post_images_asset
 ├─ post_embeds_asset
 ├─ post_comments_asset
 ├─ post_signatures_asset
 ├─ post_keywords_asset
 └─ post_categorize_asset

post_snapshots_asset -> post_trends_asset

post_signatures_asset -> clusters_build_asset
clusters_merge_asset

post_trends_asset + clusters_build_asset -> cluster_rotation_asset
keyword_trends_asset
```

-   **`posts_asset`**: 크롤러가 생성한 JSON 파일을 읽어 초기 데이터를 적재하는 핵심 자산입니다. (구현 완료)
-   **`post_trends_asset`**: 게시물의 조회수, 댓글, 추천수 변화량을 집계하여 트렌드를 분석합니다. (구현 완료)
-   **`clusters_build_asset`**: 텍스트/이미지 유사도를 기반으로 중복 및 재업로드 게시물을 클러스터링합니다. (구현 중)
-   **`cluster_rotation_asset`**: 클러스터의 랭킹 및 노출을 관리합니다. (구현 계획)
-   **`keyword_trends_asset`**: 인기 키워드를 집계합니다. (구현 계획)

### 3.2. 실행 주기 및 자동화 (Automation)

-   **파일 센서**: `is_sensor_output_data.py`에 구현되어 있으며, 새로운 크롤러 결과 파일(`.json`)을 감지하여 `is_ingest_job`을 실행합니다. (구현 완료)
-   **자동화 조건 (AutomationCondition)**: 계획된 `EAGER`, `ON_CRON` 기반의 조건부 실행은 현재 코드에 **적용되지 않았습니다.** 현재는 파일 센서에 의한 단일 파이프라인 실행만 구성되어 있습니다.
-   **Freshness Checks**: 계획된 모니터링 기능은 현재 코드에 **구현되지 않았습니다.**

## 4. 크롤러 (Crawlers)

`dag/dag/is_crawlee` 디렉터리의 소스 코드를 분석하여 크롤러 현황을 기술합니다.

### 4.1. 지원 사이트 현황

| 사이트 | 상태 | 대상 게시판 | 코드 경로 | 비고 |
| --- | --- | --- | --- | --- |
| `clien` | ✅ 정상 작동 | 모두의공원 (`park`) | `clien-park` | `po` 파라미터 기반 페이지네이션 |
| `ppomppu` | ✅ 정상 작동 | 핫게 (`hot`) | `ppomppu-hot` | EUC-KR 인코딩 처리, AJAX 댓글 |
| `fmkorea` | ✅ 정상 작동 | 베스트 (`best`) | `fmkorea` | `camoufox-js` 사용, AJAX 댓글 |
| `damoang` | ✅ 정상 작동 | 자유게시판 (`free`) | `damoang` | 표준 리스트-상세 구조 |
| `theqoo` | ❌ 지원 중단 | - | - | 과거 지원이 계획되었으나 현재 관련 코드가 존재하지 않습니다. |

### 4.2. 크롤러별 특징

-   **`clien-park`**:
    -   대상: 클리앙 모두의공원 (`/service/board/park`)
    -   특징: `data-board-sn` 속성을 `postId`로 사용하며, 페이지네이션은 `po` 쿼리 파라미터를 직접 증가시키는 방식으로 처리합니다. 댓글은 깊이(depth)를 클래스명(`reN`)과 `margin-left` 스타일로 추정하여 트리 구조를 만듭니다.
-   **`damoang`**:
    -   대상: 다모앙 자유게시판 (`/free`)
    -   특징: 표준적인 리스트-상세 구조를 가집니다. `skipDeleted` 규칙을 사용하여 "삭제된 게시물" 텍스트가 포함된 항목을 목록에서 건너뜁니다.
-   **`fmkorea`**:
    -   대상: FM코리아 베스트 게시판 (`/best`)
    -   특징: `camoufox-js` 라이브러리를 사용하여 브라우저 핑거프린팅을 우회합니다. 댓글은 AJAX로 페이지별로 비동기 로드되며, 모든 페이지를 순회하여 전체 댓글을 수집합니다.
-   **`ppomppu-hot`**:
    -   대상: 뽐뿌 핫게 (`/hot.php`)
    -   특징: EUC-KR 인코딩을 사용하는 레거시 사이트 구조에 대응합니다. `iconv-lite`를 사용하여 응답을 UTF-8로 변환하며, 본문 내 상대 경로를 절대 경로로 변환하는 로직이 포함되어 있습니다. 댓글은 AJAX로 로드됩니다.

## 5. 랭킹 및 클러스터링

프로젝트 계획과 `is_data_cluster.py` 코드를 비교 분석하여 랭킹 및 클러스터링 구현 현황을 기술합니다.

### 5.1. 랭킹 로직 (구현 계획)

-   **정규화 랭킹 (`ranked`)**:
    1.  `post_trends` 델타(조회수, 댓글, 추천수)를 30분 단위로 집계
    2.  사이트별 Z-score 정규화
    3.  시간 감쇠(Time Decay) 적용: `exp(-age_hours / 6)`
    4.  댓글 깊이에 따른 페널티 적용
-   **최신순 (`fresh`)**:
    -   간단한 가중치(`3*likes + 2*comments + views`)만 적용

> **현황**: `post_trends_asset`은 구현되어 있으나, Z-score 정규화, 시간 감쇠, 페널티 등을 적용하는 최종 랭킹 로직은 아직 구현되지 않았습니다.

### 5.2. 클러스터링 (중복 컨텐츠 처리)

-   **유사도 측정**:
    -   텍스트: SimHash 또는 MinHash (계획)
    -   이미지/임베드: MinHash (계획)
-   **클러스터링 조건 (계획)**:
    -   `text Jaccard ≥ 0.75`
    -   `(gallery ≥ 0.80) & (text ≥ 0.55)`
    -   `(embed ≥ 0.85) & (text ≥ 0.50 or gallery ≥ 0.60)`

> **현황**: `is_data_cluster.py` 파일에 `post_signatures`, `clusters`, `cluster_posts` 테이블을 생성하는 DDL이 포함되어 있어 **스키마는 정의**되어 있습니다. 하지만, 실제 유사도를 계산하고 데이터를 채우는 핵심 로직은 아직 **구현되지 않았습니다.**

## 6. 로드맵 및 향후 계획

프로젝트의 향후 계획을 현재 코드베이스 기준으로 정리했습니다.

-   [x] **파일 센서 구현**: `is_sensor_output_data.py`에 구현 완료.
-   [ ] **AutomationCondition 적용**: `EAGER`, `ON_CRON` 기반의 조건부 실행 로직 추가 필요.
-   [ ] **Freshness Checks 추가**: 주요 자산의 최신성 모니터링 기능 구현 필요.
-   [ ] **`post_trends_asset` 버그 수정**: 알려진 버그(`dislike_delta` NOT NULL 위반) 수정 필요.
-   [ ] **랭킹 및 클러스터링 로직 구현**: `cluster_build`, `cluster_rotation` 등 핵심 랭킹/클러스터링 자산의 로직 구현.
-   [ ] **키워드 추출 및 트렌드 집계**: `post_keywords_asset`, `keyword_trends_asset` 구현.
-   [ ] **프런트엔드 연동**: 인기 키워드 섹션 등 추가 기능 구현.
-   [ ] **운영 및 모니터링**: Dagster 스케줄 설정, 대시보드 구성, 유닛 테스트 보강.

## 7. 결론

-   **현재 상태**: 각 커뮤니티 사이트에서 데이터를 수집하고 데이터베이스에 적재하는 기본적인 데이터 파이프라인(`posts_asset` 및 하위 자산)과 파일 센서가 구현되어 있습니다.
-   **주요 과제**: 프로젝트 계획에 명시된 핵심 기능인 **정규화 랭킹, 중복 컨텐츠 클러스터링, 키워드 트렌드 집계** 로직의 구현이 필요합니다. 또한, 파이프라인의 안정적이고 효율적인 운영을 위해 Dagster의 자동화 및 모니터링 기능(AutomationCondition, Freshness Checks)을 코드에 적용해야 합니다.

---
_문서 최종 업데이트: 2025-09-19 (Gemini Agent)_