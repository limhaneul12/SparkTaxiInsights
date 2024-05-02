"""
뉴욕 택시 데이터 크롤링 
"""

import os
import pathlib
import logging
from typing import Final
from collections import deque

import urllib3
from urllib.request import urlretrieve
from bs4 import BeautifulSoup
from page_source import GoogleUtilityDriver as gd
from concurrent.futures import ThreadPoolExecutor


urllib3.PoolManager(num_pools=2)
# 경로 설정
PATH: Final[str] = f"{pathlib.Path(__file__).parent.parent}/sparkAnaliysis/data"

# 파일 생성
try:
    os.mkdir(f"{PATH}")
except FileExistsError:
    logging.info(f"이미 메인 파일이 존재합니다.")


def div_tag_faq20_element(e: BeautifulSoup, year: int) -> list[str]:
    """
    Args:
        e (BeautifulSoup): <div data-answer="faq2019" class="faq-questions collapsed" ~~~ ></div>
        year (int): faq20~~부터 시작하여 원하는 연도 접근 하기 위해서 매개변수 사용

    Returns:
        list[str]: [<p>해당연도</p>, ~~]
    """

    return e.find_all(
        "div", {"data-answer": f"faq20{year}", "class": "faq-questions collapsed"}
    )


def div_tag_answers_element_collect(
    e: BeautifulSoup, year: int, taxi_type: str
) -> list[str]:
    """
    Args:
        e (BeautifulSoup): <div class="faq-answers" id="faq2019" role="region" aria-hidden="false" style="display: block;">
        year (int): faq20~~부터 시작하여 원하는 연도 접근 하기 위해서 매개변수 사용

    Returns:
        list[str]: ["ul 속에 감춰진 li 뭉치들"]
    """
    return e.find("div", {"class": "faq-answers", "id": f"faq{year}"}).find_all(
        "a", {"title": taxi_type}
    )


def a_tag_download_link(e: BeautifulSoup):
    return [data["href"] for data in e]


# <a class="exitlink" href="https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2019-02.parquet" t
# itle="High Volume For-Hire Vehicle Trip Records">High Volume For-Hire Vehicle Trip Records</a>


class FileFolderMakeUtil:
    def __init__(self, taxi_type: str, start_year: int, end_year: int) -> None:
        self.taxi_type = taxi_type
        self.start_year = start_year
        self.end_year = end_year

    def folder_name_extraction(self) -> str:
        """폴더 이름 추출"""
        string_data: list[str] = self.taxi_type.split(" ")[:2]
        return " ".join(string_data).replace(" ", "")

    def file_name_extraction(self, file_name: str) -> str:
        """파일 이름 추출"""
        return file_name.split("/")[4]

    def create_folder(self) -> None:
        try:
            for data in range(self.start_year, self.end_year + 1):
                os.makedirs(
                    f"{PATH}/{self.folder_name_extraction()}/{data}",
                    exist_ok=True,
                )
            self.success = True  # 폴더 생성 성공
        except Exception as e:
            logging.error(f"폴더 생성 실패: {e}")
            self.success = False  # 폴더 생성 실패
        return self.success


class AllTaxiDataDownloadIn(FileFolderMakeUtil):
    def __init__(self, taxi_type: str, start_year: int, end_year: int) -> None:
        super().__init__(taxi_type, start_year, end_year)
        self.bs = BeautifulSoup(gd().page(), "lxml")
        self.starting_queue = deque()
        self.ready_queue = deque()
        self.download_quq = deque()
        if self.bs is None:
            return

    def year_href_collect(self, order: int) -> list[str]:
        return div_tag_answers_element_collect(self.bs, order, self.taxi_type)

    def starting_injection(self) -> None:
        for data in range(self.start_year, self.end_year + 1):
            self.starting_queue.append(self.year_href_collect(data))

    def element_preprocessing(self):
        self.starting_injection()
        for data in self.starting_queue:
            self.ready_queue.append(a_tag_download_link(data))

    def ready_for_down(self):
        self.element_preprocessing()
        for year in range(self.start_year, self.end_year + 1):
            logging.info(f"{year}년도 에 접근합니다")
            while self.ready_queue:
                for data in self.ready_queue.popleft():
                    logging.info(f"{data} 다운로드 시도")
                    urlretrieve(
                        data,
                        f"{PATH}/{self.folder_name_extraction()}/{year}/{self.file_name_extraction(data)}",
                    )

    def start(self) -> None:
        if self.create_folder():  # 폴더 생성 메서드의 반환값 확인
            self.ready_for_down()
            if len(self.starting_queue) == 0 and len(self.ready_queue) == 0:
                exit(0)
        else:
            logging.error("폴더 생성 실패로 인해 작업을 중지합니다.")


if __name__ == "__main__":

    high = "High Volume For-Hire Vehicle Trip Records"
    yellow = "Yellow Taxi Trip Records"

    def high_volume() -> None:
        return AllTaxiDataDownloadIn(high, 2019, 2024).start()

    def yello_volume() -> None:
        return AllTaxiDataDownloadIn(yellow, 2009, 2024).start()

    with ThreadPoolExecutor(2) as pool:
        pool.submit(high_volume)
        pool.submit(yello_volume)
