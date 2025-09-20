from datetime import datetime
from typing import List, Tuple, Any, Callable, Optional

DAYS = 90
BEFORE_YEARS = 21
KONEX_START_DATE = "20130701"
BATCH_SIZE = 1000000

EXCHANGES = ["KOSPI", "KOSDAQ", "KONEX"]
DATE_FORMAT = "%Y%m%d"

# 오늘 날짜를 "YYYY-MM-DD" 포맷으로 반환하는 헬퍼 함수
def get_today() -> str:
    return datetime.now().strftime("%Y-%m-%d")


# 오늘 날짜를 datetime.date 객체로 반환하는 헬퍼 함수
def get_today_date() -> datetime.date:
    return datetime.now().date()