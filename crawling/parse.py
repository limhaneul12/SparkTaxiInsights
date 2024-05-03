"""
뉴욕 택시 데이터 크롤링 
"""

import os
import time
import pathlib
import logging
from typing import Final
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed

import urllib3
from urllib.error import HTTPError
from urllib.request import urlretrieve
from bs4 import BeautifulSoup
from page_source import GoogleUtilityDriver as gd


# 경로 설정
PATH: Final[str] = f"{pathlib.Path(__file__).parent.parent}/sparkAnaliysis/data"
urllib3.PoolManager(num_pools=4, max_size=10)


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


def a_tag_download_link(e: BeautifulSoup) -> list[str]:
    """href 데이터 뭉치 div_tag_answers_element_collect method 에서 추출"""
    return [data["href"] for data in e]


class __FileFolderMakeUtil:
    def __init__(self, taxi_type: str, start_year: int, end_year: int) -> None:
        """폴더 생성할 클래스

        Args:
            taxi_type (str): 택시 유형
            start_year (int): 시작 년도
            end_year (int): 끝 년도
        """
        self.taxi_type = taxi_type
        self.start_year = start_year
        self.end_year = end_year

    def __folder_name_extraction(self) -> str:
        """폴더 이름 추출"""
        string_data: list[str] = self.taxi_type.split(" ")[:2]
        return " ".join(string_data).replace(" ", "")

    def __file_name_extraction(self, file_name: str) -> str:
        """파일 이름 추출"""
        return file_name.split("/")[4]

    def create_folder(self) -> None:
        """폴더 생성"""
        try:
            for data in range(self.start_year, self.end_year + 1):
                os.makedirs(
                    f"{PATH}/{self.__folder_name_extraction()}/{data}",
                    exist_ok=True,
                )
            self.success = True  # 폴더 생성 성공
        except Exception as e:
            logging.error(f"폴더 생성 실패: {e}")
            self.success = False  # 폴더 생성 실패
        return self.success

    def download_data(self, data: str, year: int) -> None:
        try:
            logging.info(f"{data} 다운로드 시도")
            urlretrieve(
                data,
                f"{PATH}/{self.__folder_name_extraction()}/{year}/{self.__file_name_extraction(data)}",
            )
        except HTTPError as http_err:
            logging.error(
                f"다운로드를 할 수 없습니다 다음으로 이동합니다 --> {http_err}"
            )


class AllTaxiDataDownloadIn(__FileFolderMakeUtil):
    def __init__(self, taxi_type: str, start_year: int, end_year: int) -> None:
        """데이터 다운로드 클래스

        시작 큐 -> 마지막 큐

        Args:
            taxi_type (str): 택시 유형
            start_year (int): 시작 년도
            end_year (int): 끝 년도
        """
        super().__init__(taxi_type, start_year, end_year)
        self.bs = BeautifulSoup(gd().page(), "lxml")
        self._start_queue = deque()
        self._finish_queue = deque()
        if self.bs is None:
            return

    def year_href_collect(self, order: int) -> list[str]:
        """각 년도 별 element 추출 요소

        Args:
            order (int) : 시작 년도
        return:
            - list["a 태그로 감싸져 있는 요소들"]

        """
        return div_tag_answers_element_collect(self.bs, order, self.taxi_type)

    def __element_preprocessing(self) -> None:
        """a 태그 뽑아내어 레디큐에 넣기"""
        for data in range(self.start_year, self.end_year + 1):
            year_links = self.year_href_collect(data)
            self._start_queue.append({data: a_tag_download_link(year_links)})

    def ready_for_down(self) -> None:
        """파일 다운로드하여 폴더에 저장"""
        count = 0
        while self._start_queue:
            item: dict[int, list[str]] = self._start_queue.popleft()
            for year, links in item.items():
                logging.info(f"{year} 접근 시도")
                for data in links:
                    self.download_data(data, year)
                    self._finish_queue.append(data)

                    count += 1
                    if count % 4 == 0:
                        logging.info(f"3초 쉽니다")
                        logging.info(f"현재 큐 크기 --> {len(self._finish_queue)}")
                        time.sleep(3)

                    if count % 30 == 0:
                        logging.info(f"웹서버 보호를 위해 5분 쉽니다")
                        time.sleep(300)

    def start(self) -> None:
        """크롤링 시작"""
        if self.create_folder():  # 폴더 생성 메서드의 반환값 확인
            self.__element_preprocessing()
            self.ready_for_down()
        else:
            logging.error("폴더 생성 실패로 인해 작업을 중지합니다.")


def high_volume(texi_type: str, start: int, end: int) -> None:
    return AllTaxiDataDownloadIn(texi_type, start, end).start()


def yello_volume(texi_type: str, start: int, end: int) -> None:
    return AllTaxiDataDownloadIn(texi_type, start, end).start()


def green_volume(texi_type: str, start: int, end: int) -> None:
    return AllTaxiDataDownloadIn(texi_type, start, end).start()


def for_volume(texi_type: str, start: int, end: int) -> None:
    return AllTaxiDataDownloadIn(texi_type, start, end).start()


if __name__ == "__main__":

    high = "High Volume For-Hire Vehicle Trip Records"
    yellow = "Yellow Taxi Trip Records"
    green = "Green Taxi Trip Records"
    for_hire = "For-Hire Vehicle Trip Records"

    with ThreadPoolExecutor(2) as pool:
        futures = [
            # pool.submit(high_volume, high, 2019, 2024),
            # pool.submit(yello_volume, yellow, 2009, 2024),
            pool.submit(green_volume, green, 2014, 2024),
            pool.submit(for_volume, for_hire, 2015, 2024),
        ]

        # 모든 작업이 완료될 때까지 기다림
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logging.error(f"오류를 발견하여 작업을 중단합니다 --> {e}")
