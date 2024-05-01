"""
뉴욕 택시 데이터 크롤링 
"""

import os
import pathlib
import logging
from typing import Final
from collections import deque

from urllib.request import urlretrieve
from bs4 import BeautifulSoup
from page_source import GoogleUtilityDriver as gd


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
    e: BeautifulSoup, year: int, taxi_type
) -> list[str]:
    """
    Args:
        e (BeautifulSoup): <div class="faq-answers" id="faq2019" role="region" aria-hidden="false" style="display: block;">
        year (int): faq20~~부터 시작하여 원하는 연도 접근 하기 위해서 매개변수 사용

    Returns:
        list[str]: ["ul 속에 감춰진 li 뭉치들"]
    """
    return e.find("div", {"class": "faq-answers", "id": f"faq20{year}"}).find_all(
        "a", {"title": taxi_type}
    )


class FileFolderMakeUtil:
    def __init__(self, taxi_type: str, start_year: int, end_year: int) -> None:
        self.taxi_type = taxi_type
        self.start_year = start_year
        self.end_year = end_year

    def folder_name_extraction(self) -> str:
        string_data: list[str] = self.taxi_type.split(" ")[:2]
        return " ".join(string_data).replace(" ", "")

    def create_folder(self) -> None:
        return os.makedirs(
            f"{PATH}/{self.folder_name_extraction()}/20{self.start_year}", exist_ok=True
        )


class AllTaxiDataDownloadIn(FileFolderMakeUtil):
    def __init__(self, taxi_type: str, start_year: int, end_year: int) -> None:
        super().__init__(taxi_type, start_year, end_year)
        self.bs = BeautifulSoup(gd().page(), "lxml")
        self.deque = deque()
        if self.bs is None:
            return

    def year_href_collect(self, data):
        return div_tag_answers_element_collect(self.bs, data, self.taxi_type)

    def data_temp(self):
        for i in range(self.start_year, self.end_year):
            self.deque.append(self.year_href_collect(i))

    def try_downlod(self):
        daaa = self.data_temp()
        for data in self.deque:
            print(data)


if __name__ == "__main__":
    a = AllTaxiDataDownloadIn("High Volume For-Hire Vehicle Trip Records", 19, 25).tt()
    print(a)
