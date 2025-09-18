# SS (소셜 스크래핑) 프로젝트

## 개요

SS (소셜 스크래핑) 프로젝트는 온라인 커뮤니티에서 게시물을 크롤링하고, 수집된 데이터를 처리하며, 대규모 언어 모델(LLM)을 사용하여 데이터를 보강하고, 구조화된 데이터베이스와 데이터 사전을 유지 관리하도록 설계된 데이터 파이프라인입니다.

이 프로젝트는 Node.js 기반 Crawlee 애플리케이션을 사용하여 뽐뿌 휴대폰 포럼(`ppomppu.co.kr/zboard/zboard.php?id=phone`)에서 데이터를 스크래핑하는 것으로 시작합니다.

## 오케스트레이션 및 스케줄링

### 작업 (Jobs)

전체 SS 파이프라인을 실행하기 위한 주요 작업은 다음과 같습니다.

-   **`ss_ingest_job`**: 이 작업은 `"SS"` 그룹 내의 모든 에셋을 대상으로 하며, 전체 데이터 수집 및 처리 워크플로우를 수행하기 위해 올바른 종속성 순서로 실행합니다.

### 리소스 (Resources)

파이프라인은 몇 가지 주요 리소스에 의존합니다.

-   **Node.js/Crawlee 환경**: `ss_crawl_sources` 에셋은 웹 스크레이퍼를 실행하기 위해 `ss_crawlee/ppomppu-phone` 프로젝트가 올바르게 구성된 Node.js 환경이 필요합니다.
-   **`TursoResource`**: 데이터 저장 및 검색을 위해 Turso/SQLite 데이터베이스에 연결하는 클라이언트를 제공하는 사용자 지정 리소스입니다.
-   **`ChatOpenAI`**: `ss_summarize_llm` 에셋이 게시물 내용의 요약을 생성하는 데 사용하는 LLM 리소스입니다.

## 데이터 흐름 및 에셋 종속성

이 프로젝트는 각각 이전 에셋을 기반으로 구축되는 4개의 에셋 체인으로 구성됩니다.

```mermaid
graph TD
    subgraph "SS 그룹"
        A[ss_crawl_sources <br> (Bronze)] --> B[ss_sync_rds <br> (Silver)];
        B --> C[ss_summarize_llm <br> (Gold)];
        C --> D[ss_data_dictionary <br> (Gold)];
    end
```

### 에셋 세부 정보

#### 1. `ss_crawl_sources`
-   **티어**: Bronze
-   **그룹**: SS
-   **업스트림 종속성**: 없음
-   **설명**: `ppomppu-phone` Crawlee 프로젝트를 하위 프로세스로 실행합니다. 이는 뽐뿌 휴대폰 포럼에서 게시물을 스크랩하고 원시 데이터를 로컬 디렉토리에 JSON 파일 모음으로 저장합니다. 에셋의 출력은 이 디렉토리의 경로입니다.

#### 2. `ss_sync_rds`
-   **티어**: Silver
-   **그룹**: SS
-   **업스트림 종속성**: `ss_crawl_sources`
-   **설명**: 크롤러가 생성한 원시 JSON 파일을 읽습니다. 그런 다음 Turso 데이터베이스에 연결하여 `upsert` 작업을 수행하여 게시물 데이터를 `posts` 테이블에 로드하여 중복을 생성하지 않고 데이터가 최신 상태인지 확인합니다.
-   **리소스**: `TursoResource`를 사용합니다.

#### 3. `ss_summarize_llm`
-   **티어**: Gold
-   **그룹**: SS
-   **업스트림 종속성**: `ss_sync_rds`
-   **설명**: 아직 요약되지 않은 `posts` 테이블에서 게시물을 가져옵니다. `ChatOpenAI` 리소스를 사용하여 각 게시물 내용에 대한 간결한 요약을 생성하고 데이터베이스의 해당 행을 업데이트합니다.
-   **리소스**: `TursoResource`, `ChatOpenAI`를 사용합니다.

#### 4. `ss_data_dictionary`
-   **티어**: Gold
-   **그룹**: SS
-   **업스트림 종속성**: `ss_summarize_llm`
-   **설명**: 데이터베이스에 연결하여 `posts` 테이블의 최종 스키마를 검사합니다. Markdown 형식으로 데이터 사전을 자동으로 생성하고 `data_dictionary.md`로 저장하여 데이터 구조에 대한 최신 문서를 제공합니다.
-   **리소스**: `TursoResource`를 사용합니다.

## 데이터베이스

-   **테이블**: `posts`
-   **설명**: 이 테이블은 뽐뿌 포럼에서 스크랩한 모든 데이터를 저장합니다. 주요 열은 다음과 같습니다.
    -   `url` (기본 키)
    -   `title`
    -   `author`
    -   `content`
    -   `timestamp`
    -   `is_summarized` (LLM 요약 완료 여부 플래그)
    -   `summary` (LLM에서 생성된 요약)
    -   크롤러가 추출한 기타 필드.