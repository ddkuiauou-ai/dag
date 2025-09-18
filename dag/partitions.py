from datetime import datetime, timedelta

import dagster as dg

DAYS = 42
# DAYS = 5

# START_DATE = "2023-01-01"
START_DATE = datetime.today() - timedelta(days=DAYS)

daily_partition = dg.DailyPartitionsDefinition(
    start_date=START_DATE, timezone="Asia/Seoul", end_offset=1
)

exchange_category_partition = dg.StaticPartitionsDefinition(
    [
        "KOSPI",
        "KOSDAQ",
        "KONEX",
    ],
)

daily_exchange_category_partition = dg.MultiPartitionsDefinition(
    {
        "date": daily_partition,
        "exchange": exchange_category_partition,
    }
)

