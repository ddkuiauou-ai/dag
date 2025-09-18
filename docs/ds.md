# DS (Downstream) 프로젝트

## 개요

DS (Downstream) 프로젝트는 네이버 쇼핑 라이브 플랫폼에서 라이브 커머스 데이터를 크롤링하는 데 중점을 둔 데이터 파이프라인입니다. 라이브 쇼핑 이벤트에 대한 정보를 추출하고, 수집된 데이터를 처리하며, 결과를 클라우드 객체 스토리지(Cloudflare R2)에 저장합니다.

이 파이프라인은 초기 크롤링 단계를 실행하기 위한 두 가지 대안적인 방법을 사용하여 설계되었습니다. 하나는 로컬 Node.js 환경을 사용하는 것이고 다른 하나는 Docker 컨테이너를 사용하는 것으로, 개발 및 프로덕션 시나리오 모두에 유연하게 대응할 수 있습니다.

## 오케스트레이션 및 스케줄링

### 작업 (Jobs)

DS 파이peline은 크롤러에 대한 다양한 실행 방법에 해당하는 두 가지 주요 작업을 통해 오케스트레이션됩니다.

-   **`ds_node_job`**: 이 작업은 로컬 Node.js 환경에서 크롤러를 실행하는 `ds_img_r2_node` 에셋으로 시작하여 파이프라인을 실행합니다.
-   **`ds_docker_job`**: 이 작업은 관리되는 Docker 컨테iner 내에서 크롤러를 실행하는 `ds_img_r2_docker` 에셋으로 시작하여 파이프라인을 실행합니다.

두 작업 모두 크롤링 단계가 성공적으로 완료되면 다운스트림 `ds_img_r2_processing` 에셋을 트리거합니다.

### 리소스 (Resources)

파이프라인은 몇 가지 주요 리소스에 의존합니다.

-   **`DockerResource`**: `ds_img_r2_docker` 에셋이 크롤러의 Docker 컨테이너 수명 주기를 관리하는 데 사용됩니다.
-   **`R2Resource`**: `ds_img_r2_processing` 에셋이 처리된 데이터를 업로드하는 데 사용하는 Cloudflare R2 객체 스토리지에 대한 연결을 제공하는 사용자 지정 리소스입니다.
-   **Node.js/Crawlee 환경**: `ds_img_r2_node` 에셋은 `ds-naver-crawler` 프로젝트가 올바르게 구성된 로컬 Node.js 환경이 필요합니다.

## 데이터 흐름 및 에셋 종속성

이 프로젝트는 단일 처리 에셋으로 수렴되는 두 개의 가능한 시작점을 가진 Bronze-to-Silver 파이프라인으로 구성됩니다.

```mermaid
graph TD
    subgraph "DS 그룹 (Bronze Tier)"
        A[ds_img_r2_docker] -- triggers --> C[ds_img_r2_processing <br> (Silver)];
        B[ds_img_r2_node] -- triggers --> C;
    end
```

### 에셋 세부 정보

#### 1. `ds_img_r2_docker` / `ds_img_r2_node`
-   **티어**: Bronze
-   **그룹**: DS
-   **업스트림 종속성**: 없음
-   **설명**: 이 두 에셋은 파이peline의 대안적인 시작점 역할을 합니다.
    -   `ds_img_r2_docker`: Docker 컨테iner 내에서 `ds-naver-crawler`를 실행합니다. 이는 일반적으로 프로덕션 또는 격리된 환경에서 사용됩니다.
    -   `ds_img_r2_node`: 로컬 Node.js 환경에서 `npm start`를 사용하여 `ds-naver-crawler`를 직접 실행합니다. 이는 로컬 개발 및 디버깅에 적합합니다.
-   **출력**: 두 에셋 모두 크롤러를 트리거하여 스크랩된 원시 데이터를 로컬 `storage/datasets` 디렉토리에 JSON 파일로 저장합니다.

#### 2. `ds_img_r2_processing`
-   **티어**: Silver
-   **그룹**: DS
-   **업스트림 종속성**: `ds_img_r2_docker` 또는 `ds_img_r2_node`
-   **설명**: 이 에셋은 크롤링 단계가 완료된 후 활성화됩니다. 크롤러가 생성한 최신 데이터셋 디렉토리를 찾아 원시 JSON 파일을 읽고 필요한 처리를 수행한 다음 최종 결과를 Cloudflare R2의 지정된 버킷에 업로드합니다.
-   **리소스**: `R2Resource`를 사용합니다.

## 개발 및 디버깅

### 🔧 유용한 확인 명령어

**DS 그룹의 에셋 나열:**

```bash
cd /Users/craigchoi/silla/dag

python -c "
from dag.definitions import defs
from dagster import AssetSelection

asset_graph = defs.get_asset_graph()
ds_assets = AssetSelection.groups('DS').resolve(asset_graph)
print('🎯 Assets in the \'DS\' group:')
for asset_key in ds_assets:
    print(f'  - {asset_key.to_user_string()}')
print(f'\n📊 Total {len(ds_assets)} assets.')
"
```

**에셋 종속성 확인:**

```bash
python -c "
from dag.definitions import defs
from dagster import AssetKey

asset_graph = defs.get_asset_graph()
ds_assets = ['ds_img_r2_docker', 'ds_img_r2_node', 'ds_img_r2_processing']

for asset_name in ds_assets:
    asset_key = AssetKey([asset_name])
    if asset_key in asset_graph.all_asset_keys:
        deps = asset_graph.get_upstream_asset_keys(asset_key)
        print(f'{asset_name} depends on: {[dep.to_user_string() for dep in deps]}')
"
```

## 스토리지

-   **중간 스토리지**: 스크랩된 원시 데이터는 크롤러가 실행되는 로컬 파일 시스템에 JSON 파일로 임시 저장됩니다.
-   **영구 스토리지**: 최종 처리된 데이터 및/이미지는 장기적인 지속성 및 다운스트림 사용을 위해 Cloudflare R2 버킷에 저장됩니다.

```