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


def a_tag_download_link(e: BeautifulSoup) -> list[str]:
    """href 데이터 뭉치 div_tag_answers_element_collect method 에서 추출"""
    return [data["href"] for data in e]


class FileFolderMakeUtil:
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

    def folder_name_extraction(self) -> str:
        """폴더 이름 추출"""
        string_data: list[str] = self.taxi_type.split(" ")[:2]
        return " ".join(string_data).replace(" ", "")

    def file_name_extraction(self, file_name: str) -> str:
        """파일 이름 추출"""
        return file_name.split("/")[4]

    def create_folder(self) -> None:
        """폴더 생성"""
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
        """데이터 다운로드 클래스

        시작 큐 -> 준비큐 -> 다운로드

        Args:
            taxi_type (str): 택시 유형
            start_year (int): 시작 년도
            end_year (int): 끝 년도
        """
        super().__init__(taxi_type, start_year, end_year)
        self.bs = BeautifulSoup(gd().page(), "lxml")
        self.starting_queue = deque()
        self.ready_queue = deque()
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

    def starting_injection(self) -> None:
        """준비큐 에 각 년도마다 a태그 요소 넣기"""
        for data in range(self.start_year, self.end_year + 1):
            self.starting_queue.append({data: self.year_href_collect(data)})

    def data_pop_list_injection(
        self, data_q: deque, target: deque
    ) -> list[dict[int, list[str]]]:
        """각 큐의 작업 과정 통일

        Args:
            data_q (deque): 시작 큐
            target (deque): 타켓 큐

        Returns:
            _type_: [{2019: [다운로드링크]}]
        """
        data_q

        data: list[dict[int, list[str]]] = []
        while target:
            item: dict[int, list[str]] = target.popleft()
            if isinstance(item, dict):
                for y_data, link in item.items():
                    data.append({y_data: link})
        return data

    def element_preprocessing(self) -> None:
        """a 태그 뽑아내어 레디큐에 넣기"""
        data: list[dict[int, list[str]]] = self.data_pop_list_injection(
            self.starting_injection(), self.starting_queue
        )
        for _, data in enumerate(data):
            data_partition: dict[int, list[str]] = {
                list(data.keys())[0]: a_tag_download_link(
                    data for data in list(data.values())[0]
                )
            }
            self.ready_queue.append(data_partition)

    def ready_for_down(self) -> None:
        """파일 다운로드하여 폴더에 저장"""
        data: list[dict[int, list[str]]] = self.data_pop_list_injection(
            self.element_preprocessing(), self.ready_queue
        )
        for element in (down for down in data):
            logging.info(f"{list(element)}년 접근 시도")
            for year, links in element.items():
                for data in links:
                    logging.info(f"{data} 다운로드 시도")
                    urlretrieve(
                        data,
                        f"{PATH}/{self.folder_name_extraction()}/{year}/{self.file_name_extraction(data)}",
                    )

    def start(self) -> None:
        """크롤링 시작"""
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
