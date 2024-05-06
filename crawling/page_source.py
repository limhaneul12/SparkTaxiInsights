import datetime
import create_log
import requests


# 현재 시각하는 시간 설정
start_time = datetime.datetime.now()

# # 로그
log = create_log.log()

log.info(f"사이트 HTML 수집을 시작합니다.")


class GoogleUtilityDriver:
    def __init__(self) -> None:
        self.url = f"https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"

    def page(self) -> str:
        req = requests.get(self.url)
        return req.text
