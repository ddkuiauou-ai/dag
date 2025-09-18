# Isshoo (is.md) 통합 문서

## 1. 개요 (Overview)

Isshoo는 국내 커뮤니티(클리앙, 뽐뿌, FM코리아, 다모앙 등)의 인기 게시글을 초 단위에 가깝게 수집하고, 랭킹·클러스터·키워드 트렌드를 계산하여 Isshoo 웹 서비스([https://www.isshoo.xyz](https://www.isshoo.xyz))에 제공합니다. 본 문서는 Dagster 파이프라인, 크롤러, AI 후처리, 모니터링 설정을 포함한 최신 코드베이스의 싱글 소스 문서입니다.

## 2. 아키텍처 (Architecture)

### 2.1 데이터 흐름

```
[Crawlee 기반 Crawlers] --(JSON)--> [output_data 디렉터리]
    \--(is_output_data_sensor)--> [is_ingest_job]
        \--(posts_asset 및 하위 자산)--> [PostgreSQL]
            \--(post_snapshots/post_signatures 등)--> [골드/AI 자산]
                \--(Materialized Views & API)
```

- **크롤러**: TypeScript + Crawlee/Playwright 조합으로 각 커뮤니티의 리스트/상세 페이지를 순회하고 표준 JSON을 생성합니다.
- **파일 센서**: `output_data/*.json`을 감시하여 신규·수정 파일이 감지되면 Dagster `is_ingest_job`을 실행합니다.
- **Dagster 자산**: `is_data_sync.py`(동기, 실버 계층) → `is_data_unsync.py`/`is_data_cluster.py`/`is_data_llm.py`/`is_data_vlm.py`(비동기, 골드·AI 계층) 순으로 파생 데이터를 적재합니다.
- **API/Web**: PostgreSQL 테이블과 머티리얼라이즈드 뷰(`mv_post_trends_*`, `cluster_trends`, `keyword_trends`)를 조회하여 랭킹, 트렌드, 클러스터 결과를 제공합니다.

### 2.2 프로젝트 구조

- **Dagster 파이프라인 (`/dag/dag/`)**
  - `definitions.py`: 자산/체크/잡/스케줄/센서를 종합 등록하고 Freshness 센서를 구성합니다.
  - `is_data_sync.py`: JSON → 관계형 테이블 정규화(Posts/Comments/Images/Embeds/Snapshots/Signatures).
  - `is_data_unsync.py`: 트렌드, 키워드, 큐 승격 등 비동기 골드 계층 자산.
  - `is_data_cluster.py`: 유사도 기반 클러스터링, 병합, 회전(rank) 로직.
  - `is_data_llm.py` & `is_data_vlm.py`: 텍스트/이미지 LLM 워커 및 인큐 자산.
  - `is_node.py`: Crawlee TypeScript 빌드/실행 자산과 동적 스케줄.
  - `is_sensor_output_data.py`: 파일 센서.
- **크롤러 (`/dag/dag/is_crawlee/`)**: 사이트별 Crawlee 프로젝트(`clien-park`, `ppomppu-hot`, `fmkorea`, `damoang`). 각 폴더는 독립 `package.json` + `src/` 구조를 가집니다.

## 3. Dagster 파이프라인 (Dagster Assets)

### 3.1 브론즈/실버 계층 – 동기 자산 (`is_data_sync.py`)

```
is_output_data_sensor → is_ingest_job
  └─ posts_asset (EAGER)
       ├─ post_versions_asset
       ├─ post_comments_asset
       ├─ post_images_asset
       ├─ post_embeds_asset
       ├─ post_snapshots_asset
       ├─ sites_asset
       └─ post_signatures_asset
```

- **posts_asset**: 수집 JSON을 읽어 `posts` 테이블에 멱등 upsert. 변경 여부를 판단하고 텍스트 해시(`text_rev`)를 관리합니다.
- **post_versions_asset**: 제목/본문/태그 변동 시 버전 스냅샷을 기록합니다.
- **post_comments_asset**: 중첩 댓글을 DFS로 평탄화하여 `post_comments`에 일괄 upsert하고, 중복/깊이 히스토그램을 로깅합니다.
- **post_images_asset**: URL 해시 기반으로 이미지 메타를 upsert하며 신규 이미지를 `media_enrichment_jobs` 큐에 배치 등록합니다.
- **post_embeds_asset**: 임베드/동영상/링크를 upsert하고 이미지형 임베드에 대한 VLM 큐잉을 수행합니다.
- **post_snapshots_asset**: 조회·댓글·추천 카운트를 시간 스냅샷으로 적재하여 후속 트렌드 계산의 기본 재료를 제공합니다.
- **sites_asset**: 사이트·게시판별 마지막 크롤 시각을 갱신합니다.
- **post_signatures_asset**: SimHash/MinHash 기반 텍스트·갤러리·임베드 시그니처를 계산해 `post_signatures`에 저장합니다.

### 3.2 골드 계층 및 운영 자산 (`is_data_unsync.py`, `is_data_cluster.py`)

```
post_snapshots_asset ─┬─> post_trends_asset (ON10)
                       ├─> refresh_mv_post_trends_30m (ON10)
                       └─> refresh_mv_post_trends_agg (ON10)

post_trends_asset ─┬─> cluster_rotation_asset (ON10 & BLOCKS_OK)
                    └─> clusters_build_asset (ON30)

post_signatures_asset ──> clusters_build_asset ──> clusters_merge_asset (ON1H)

posts_asset ─┬─> keyword_trends_asset (ON10)
             └─> text_only_enqueue_asset (ON2)

cluster_rotation_asset ──> promote_p0_from_frontpage_asset (ON2)
```

- **post_trends_asset**: 최근 30분 윈도우에서 조회/댓글/추천 증감을 계산하고 `hot_score=views+3*comments+2*likes`를 저장합니다.
- **refresh_mv_post_trends_30m / _agg**: 실시간/집계 머티리얼라이즈드 뷰를 10분 주기로 `REFRESH MATERIALIZED VIEW CONCURRENTLY` 합니다.
- **clusters_build_asset**: 72시간 내 게시글을 대상으로 SimHash+MinHash LSH → 후보 생성 → 조건부 유사도 검증으로 클러스터를 구성하고 대표글을 선정합니다.
- **clusters_merge_asset**: 14일 내 생성된 중복 클러스터를 top-k 멤버 교차 비교로 병합합니다.
- **cluster_rotation_asset**: 클러스터 hot_score를 백분위 정규화 후 τ=48h 시간 감쇠, 3회 연속 노출 시 24h 쿨다운을 적용하여 랭킹을 기록합니다.
- **keyword_trends_asset**: `post_enrichment` 키워드를 3h/6h/24h/1w 윈도우로 집계하여 `keyword_trends`에 upsert합니다.
- **promote_p0_from_frontpage_asset**: 최근 프론트 노출 글의 LLM/VLM 잡을 P0 우선순위로 승격하고, 필요 시 이미지 잡을 신규 생성합니다.
- **text_only_enqueue_asset**: 이미지 ETA를 추정하여 이미지가 없는 글은 즉시, 이미지가 있는 글은 상황에 따라 홀드 또는 즉시 LLM 큐에 넣습니다.

### 3.3 AI Enrichment 파이프라인 (`is_data_llm.py`, `is_data_vlm.py`)

- **fusion_worker_asset (ON10)**: `fusion_jobs` 큐를 소비하여 텍스트·이미지 블록을 통합한 LLM 프롬프트를 호출하고 카테고리/키워드를 `post_enrichment`에 저장합니다.
- **fusion_backfill_enqueue_asset**: 프롬프트 버전 변경 시 텍스트/이미지 리비전을 기준으로 대량 재큐잉합니다.
- **vlm_worker_asset (ON3)**: `media_enrichment_jobs` 큐를 SKIP LOCKED로 가져와 이미지 캡션/OCR/객체/색상/안전성 분석을 수행하고 `post_image_enrichment`에 저장합니다.
- 두 워커는 성공/실패 건수를 Dagster 메타데이터로 노출하고, 실패 시 `dagster.Failure`를 발생시켜 재시도 제어가 가능합니다.

## 4. 자동화 및 모니터링

- **센서**: `is_output_data_sensor`는 30초 간격으로 `output_data`를 감시해 JSON 변경 시 `is_ingest_job`을 트리거합니다.
- **AutomationCondition**: `schedules.py`에 정의된 `EAGER`, `ON2`, `ON10`, `ON30`, `ON1H` 등 크론 기반 조건을 자산에 직접 부여하여 파이프라인을 선언적 cadence로 운영합니다.
- **스케줄/잡**:
  - `is_crawler_schedule_10min`: 10분마다 Crawlee 실행(`is_crawler_executor`).
  - `is_crawler_executor_interval_schedule`: 30분~48시간 간격의 백필 잡을 동적 매핑으로 실행합니다.
  - `is_ingest_job`: `posts_asset` 이하 전체 자산을 한 번에 재실행하는 기본 잡.
- **Freshness Checks**: `definitions.py`에서 `post_trends_asset`, `cluster_rotation_asset`, `keyword_trends_asset`에 대해 10분 SLA 모니터링을 생성하고, `freshness_checks_sensor`로 감시합니다.

## 5. 크롤러 (Crawlers)

| 사이트 | 상태 | 대상 게시판 | 코드 경로 | 주요 특징 |
| --- | --- | --- | --- | --- |
| `clien` | ✅ 운영 중 | 모두의공원 (`park`) | `clien-park` | `po` 파라미터 페이지네이션, 댓글 depth 파싱, TypeScript Crawlee |
| `ppomppu` | ✅ 운영 중 | 핫게 (`hot.php`) | `ppomppu-hot` | EUC-KR 응답 → `iconv-lite` UTF-8 변환, AJAX 댓글 수집 |
| `fmkorea` | ✅ 운영 중 | 베스트 (`/best`) | `fmkorea` | `camoufox-js`로 브라우저 우회, 댓글 페이지네이션 대응 |
| `damoang` | ✅ 운영 중 | 자유게시판 (`/free`) | `damoang` | 삭제 게시물 스킵, 표준 리스트/상세 구조 |
| `theqoo` | ❌ 미구현 | - | - | 계획만 존재하며 코드 없음 |

- `is_crawler_build` 자산이 TypeScript 프로젝트를 `npm install && npm run build`로 미리 빌드합니다.
- `is_crawler_executor` 자산은 빌드된 JS(`dist/main.js`)를 순차 실행하며, 로그에서 요청/성공/실패/수집 건수를 추출해 메타데이터로 기록합니다.
- 로그 후처리: ANSI 코드 제거, `Final request statistics`/`상세 페이지 처리` 문자열 기반 통계를 수집하고, 임베디드 MIME 타입 빈도를 계산합니다.

## 6. 랭킹 및 클러스터링

- **랭킹 파이프라인**: `post_trends_asset`이 저장한 raw hot_score를 기반으로 `mv_post_trends_30m`(즉시 노출)과 `mv_post_trends_agg`(3h/6h/24h/1w 집계)를 갱신합니다. 웹 계층은 뷰 정규화·사이트별 인터리빙을 적용합니다.
- **클러스터링**: `post_signatures_asset`의 시그니처를 사용해 LSH 후보 생성 → 조건부 유사도 검증으로 클러스터를 구성하고, `cluster_rotation_asset`이 감쇠·쿨다운 규칙을 적용해 섹션별 랭킹을 계산합니다.
- **중복 제어**: `clusters_merge_asset`이 서로 다른 윈도우에서 생성된 유사 클러스터를 주기적으로 병합하여 대표글과 멤버를 정리합니다.
- **Frontpage 회전**: `promote_p0_from_frontpage_asset`과 `cluster_rotation_asset`이 연동되어 프론트 노출 글의 LLM/VLM 큐를 즉시 승격하고, 연속 노출에 따른 suppress/cooldown을 관리합니다.

## 7. 로드맵 및 향후 계획

- [x] 파일 센서 및 `is_ingest_job` 연동 완료.
- [x] AutomationCondition (`ON2/ON10/ON30/ON1H`)을 자산에 적용하여 선언형 스케줄 운영.
- [x] Freshness 체크 및 센서 구축.
- [x] 트렌드/클러스터/키워드 파이프라인 구현 및 머티리얼라이즈드 뷰 자동 갱신.
- [x] LLM/VLM 큐 기반 후처리 파이프라인 운영.
- [ ] Crawlee 커버리지 확장(더쿠 등 미구현 사이트) 및 증분 모드 고도화.
- [ ] 웹/API 계층의 정규화 랭킹/인터리빙 로직과 Dagster 메타데이터 연동.
- [ ] 모니터링 고도화: 큐 대기 시간, 워커 실패율, API SLA 지표 대시보드화.

## 8. 결론

현재 Isshoo 파이프라인은 JSON 수집 → 관계형 정규화 → 트렌드/클러스터/키워드 계산 → AI 후처리를 완결성 있게 구현하고 있으며, Dagster의 자동화 조건과 Freshness 체크로 운영 안정성을 확보했습니다. 향후에는 크롤러 커버리지 확대와 웹 노출 로직/모니터링 고도화를 통해 서비스 품질을 강화할 예정입니다.

---
_문서 최종 업데이트: 2025-09-18 (OpenAI Assistant)_
