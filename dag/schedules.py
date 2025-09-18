import dagster as dg
from .jobs import cd_daily_update_job, is_crawler_10min_job

# =============================================================
# Declarative Automation conditions (cron + guards)
#
# [STRATEGY] Priority-based Staggering
# Jobs are staggered across the hour to prevent simultaneous execution and reduce server load.
# - High-frequency jobs (e.g., 2-min) run on odd minutes.
# - Core incremental jobs (10-min) run at a consistent offset (e.g., +4 mins).
# - Heavy full-mode jobs (30-min, 1-hour+) are placed in the gaps.
# =============================================================
EAGER = dg.AutomationCondition.eager()

# Tier 1: High-frequency, lightweight tasks
ON2   = dg.AutomationCondition.on_cron("1-59/2 * * * *") # Odd minutes
ON3   = dg.AutomationCondition.on_cron("1-59/3 * * * *") # Odd minutes

# Tier 2: Core incremental crawlers (recency)
ON10  = dg.AutomationCondition.on_cron("4,14,24,34,44,54 * * * *")

# Tier 3: Heavy full-mode crawlers (backfill)
ON30  = dg.AutomationCondition.on_cron("8,38 * * * *")
ON1H  = dg.AutomationCondition.on_cron("18 * * * *")
ON3H  = dg.AutomationCondition.on_cron("28 */3 * * *")
ON6H  = dg.AutomationCondition.on_cron("48 */6 * * *")
ON12H = dg.AutomationCondition.on_cron("58 2,14 * * *") # 2:58 AM/PM
ON24H = dg.AutomationCondition.on_cron("58 3 * * *")   # 3:58 AM
ON48H = dg.AutomationCondition.on_cron("50 4 */2 * *")  # 4:50 AM every 2 days (KST assumed). Chosen to steer clear of :58 heavy runs and avoid :48 (6H) / 10-min ticks.

BLOCKS_OK = dg.AutomationCondition.all_deps_blocking_checks_passed()

cd_daily_update_schedule = dg.build_schedule_from_partitioned_job(
    job=cd_daily_update_job,
    minute_of_hour=00,
    hour_of_day=2,
    description="매일 주식 가격 업데이트 스케줄",
    tags={"type": "daily_update"},
)


# IS 크롤러 executor를 매 10분마다 실행하는 스케줄
is_crawler_schedule_10min = dg.ScheduleDefinition(
    cron_schedule="4,14,24,34,44,54 * * * *",
    job=is_crawler_10min_job,
    execution_timezone="Asia/Seoul",
    description="매 10분마다 IS 크롤러 executor 실행",
)