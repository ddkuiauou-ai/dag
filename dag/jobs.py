import dagster as dg
from .partitions import daily_exchange_category_partition



cd_targets = dg.AssetSelection.assets("cd_stockcode").downstream()
cd_targets_turso = dg.AssetSelection.assets("cd_stockcode_turso").downstream()
cd_history_targets = dg.AssetSelection.assets("historical_prices", "historical_marketcaps", "historical_bppedds").downstream()
cd_docker_targets = dg.AssetSelection.assets("cd_img_r2_docker").downstream()
cd_node_targets = dg.AssetSelection.assets("cd_img_r2_node").downstream()
ds_docker_targets = dg.AssetSelection.assets("ds_img_r2_docker").downstream()
ds_node_targets = dg.AssetSelection.assets("ds_img_r2_node").downstream()
is_targets = dg.AssetSelection.assets("posts_asset").downstream()
ss_targets = dg.AssetSelection.assets("ss_crawl_sources").downstream()

nps_targets = dg.AssetSelection.groups("NPS")
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
    selection= is_targets,
    description="IS 크롤러 output_data 적재 전체 파이프라인",
    tags={"source": "IS", "type": "ingest", "dagster/priority": "3"},
)

ss_ingest_job = dg.define_asset_job(
    name="ss_ingest_job",
    selection= ss_targets,
    description="SS 크롤러 ssdata 적재 전체 파이프라인",
    tags={"source": "SS", "type": "ingest", "dagster/priority": "3"},
)

# 매 10분마다 IS 크롤러 executor 애셋을 실행하는 Job
is_crawler_10min_job = dg.define_asset_job(
    name="is_crawler_job_10min",
    selection=dg.AssetSelection.assets("is_crawler_executor"),
    description="IS 크롤러 executor 을 매 10분마다 실행합니다. 신규 게시글만 크롤링",
    tags={"type": "schedule", "interval": "10min"},
)

# 매 30분, 1시간, 3시간, 6시간, 12시간, 24시간, 48시간마다 IS 크롤러 executor 애셋을 실행하는 Job
# 매 30분, 1시간, 3시간, 6시간, 12시간, 24시간, 48시간마다 IS 크롤러 executor 애셋을 실행하는 Job
is_crawler_interval_job = dg.define_asset_job(
    name="is_crawler_interval_job",
    selection=dg.AssetSelection.assets("is_crawler_executor_interval"),
    description="IS 크롤러 executor 을 매 30분, 1시간, 3시간, 6시간, 12시간, 24시간, 48시간마다 실행합니다.",
    tags={"type": "schedule", "interval": "30min, 1h, 3h, 6h, 12h, 24h, 48h"},
)
