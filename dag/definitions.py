"""Dagster Definitions for the Silla project.

This module wires up assets, jobs, schedules, sensors, and shared resources.
The goal of these edits is to improve readability without removing or changing
any existing functionality.
"""

from dagster import Definitions, EnvVar, load_assets_from_modules, load_asset_checks_from_modules, build_last_update_freshness_checks, build_sensor_for_freshness_checks
from dagster_duckdb import DuckDBResource
from dagster_docker import PipesDockerClient
from dagster_openai import OpenAIResource
from .resources import PostgresResource, TursoResource
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
    cd_raw_ingestion,
    cd_bppedd_processing,
    cd_display_processing,
    cd_marketcap_processing,
    cd_metrics_processing,
    cd_price_processing,
    cd_img_r2_docker,
    cd_img_r2_processing,
    cd_raw_ingestion_turso,
    cd_price_processing_turso,
    cd_bppedd_processing_turso,
    cd_marketcap_processing_turso,
    cd_metrics_processing_turso,
    cd_img_r2_node,
)
from . import (
    is_node,
    is_data_unsync,
    is_data_sync,
    is_data_llm,
    is_data_vlm,
    is_data_cluster,
)
from . import ss_data_llm
from . import cd_history_prices, cd_history_marketcaps, cd_history_bppedds # noqa: TID252
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
from .cd_metrics_processing import cd_populate_security_ranks # Add this import
from .is_sensor_output_data import is_output_data_sensor  # output_data 센서 등록
from .ss_data_sync import ss_crawl_sources  # ss_data_sync 센서 등록
from .is_node import is_crawler_executor_interval_schedule
from .is_data_unsync import post_trends_asset, keyword_trends_asset
from .is_data_cluster import cluster_rotation_asset
from . import ss_node, ss_data_sync, ss_data_llm, ss_data_dictionary


# =============================================================
# Asset & Check Collection
# =============================================================

all_ds_assets = load_assets_from_modules(
    [
        ds_img_r2_docker,
        ds_img_r2_processing,
        ds_img_r2_node,
    ]
)

all_cd_assets = load_assets_from_modules(
    [
        cd_raw_ingestion,
        cd_bppedd_processing,
        cd_display_processing,
        cd_marketcap_processing,
        cd_metrics_processing,  # This module now contains cd_populate_security_ranks
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
    ]
)

all_nps_assets = load_assets_from_modules(
    [
        nps_raw_ingestion,
        nps_data_processing,
        nps_postgres_simple,
        nps_index_optimization,
        nps_kyc_etl,
    ]
)
all_nps_asset_checks = load_asset_checks_from_modules(
    [
        nps_raw_ingestion,
        nps_data_processing,
        nps_postgres_simple,
        nps_index_optimization,
        nps_kyc_etl,
    ]
)
all_is_assets = load_assets_from_modules(
    [
        is_node,
        is_data_sync,
        is_data_unsync,
        is_data_llm,
        is_data_vlm,
        is_data_cluster,
    ]
)
all_ss_assets = load_assets_from_modules(
    [
        ss_node,
        ss_data_sync,
        ss_data_llm,
        ss_data_dictionary
    ]
)

# =============================================================
# Freshness Checks (SLA monitoring)
# =============================================================
# These checks monitor if key downstream assets are updated at least every 10 minutes.

# Use a short alias for timedelta to keep lines readable and avoid name shadowing
from datetime import timedelta as _td

# Posts trend data should update at least every 10 minutes
post_trends_freshness_checks = build_last_update_freshness_checks(
    assets=[post_trends_asset],
    lower_bound_delta=_td(minutes=10),
)
# Cluster rotation results should update at least every 10 minutes
cluster_rotation_freshness_checks = build_last_update_freshness_checks(
    assets=[cluster_rotation_asset],
    lower_bound_delta=_td(minutes=10),
)
# Keyword trend data should update at least every 10 minutes
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
    freshness_checks=_all_freshness_checks
)

# =============================================================
# Dagster Definitions (assets, checks, jobs, schedules, sensors, resources)
# =============================================================
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
        ss_ingest_job
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
        # Docker client used for running tasks in containers
        "docker_pipes_client": PipesDockerClient(),
        # Local DuckDB databases
        "cd_duckdb": DuckDBResource(database="data/cd.duckdb"),
        "nps_duckdb": DuckDBResource(database="data/nps.duckdb"),
        "ds_duckdb": DuckDBResource(database="data/ds.duckdb"),
        # External Postgres connections
        "cd_postgres": PostgresResource(
            host=EnvVar("CD_POSTGRES_HOST"),
            port=EnvVar.int("CD_POSTGRES_PORT"),
            user=EnvVar("CD_POSTGRES_USER"),
            password=EnvVar("CD_POSTGRES_PASSWORD"),
            database=EnvVar("CD_POSTGRES_DB"),
        ),        
        "is_postgres": PostgresResource(
            host=EnvVar("IS_POSTGRES_HOST"),
            port=EnvVar.int("IS_POSTGRES_PORT"),
            user=EnvVar("IS_POSTGRES_USER"),
            password=EnvVar("IS_POSTGRES_PASSWORD"),
            database=EnvVar("IS_POSTGRES_DB"),
        ),
        "nps_postgres": PostgresResource(
            host=EnvVar("NPS_POSTGRES_HOST"),
            port=EnvVar.int("NPS_POSTGRES_PORT"),
            user=EnvVar("NPS_POSTGRES_USER"),
            password=EnvVar("NPS_POSTGRES_PASSWORD"),
            database=EnvVar("NPS_POSTGRES_DB"),
        ),
        "ss_postgres": PostgresResource(
            host=EnvVar("SS_POSTGRES_HOST"),
            port=EnvVar.int("SS_POSTGRES_PORT"),
            user=EnvVar("SS_POSTGRES_USER"),
            password=EnvVar("SS_POSTGRES_PASSWORD"),
            database=EnvVar("SS_POSTGRES_DB"),
        ),
        "ds_postgres": PostgresResource(
            host=EnvVar("DS_POSTGRES_HOST"),
            port=EnvVar.int("DS_POSTGRES_PORT"),
            user=EnvVar("DS_POSTGRES_USER"),
            password=EnvVar("DS_POSTGRES_PASSWORD"),
            database=EnvVar("DS_POSTGRES_DB"),
        ),
        # Turso (libsql) connection
        "cd_turso": TursoResource(
            url=EnvVar("TURSO_DATABASE_URL"),
            auth_token=EnvVar("TURSO_AUTH_TOKEN"),
        ),
        # LLM clients (pointing to local/OpenAI-compatible endpoints)
        "openai": OpenAIResource(
            # base_url="http://192.168.50.12:11434/v1",  # Local Ollama API
            # base_url="http://192.168.50.124:1234/v1",  # q550
            # base_url="http://192.168.50.12:1234/v1",  # omen
            base_url="http://192.168.50.204:1234/v1",  # mac pro
            api_key="ollama",
        ),
        "omen": OpenAIResource(
            base_url="http://192.168.50.12:1234/v1",  # mac pro
            api_key="ollama",
        ),
        "oss": OpenAIResource(
            # base_url="http://192.168.50.12:1234/v1",  # omen
            # base_url="http://192.168.50.107:11434/v1",  # 3080ti
            base_url="http://192.168.50.107:1234/v1",  # 3080ti
            api_key="oss",
        ),
        "qwen25": OpenAIResource(
            # base_url="http://192.168.50.12:1234/v1",  # omen
            # base_url="http://192.168.50.107:11434/v1",  # 3080ti
            base_url="http://192.168.50.107:1234/v1",  # 3080ti
            api_key="qwen",
        ),
        "gemini": OpenAIResource(
            base_url="https://generativelanguage.googleapis.com/v1beta/openai/",
            api_key=EnvVar("GEMINI_API_KEY"),
        ),
    },
)
