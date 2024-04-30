"""
뉴욕 택시 데이터 크롤링 
"""

import os
import pathlib
import logging
from typing import Final
from queue import Queue

from urllib.request import urlretrieve
from bs4 import BeautifulSoup
from page_source import GoogleUtilityDriver as gd


# 경로 설정
PATH: Final[str] = f"{pathlib.Path(__file__).parent.parent}/sparkAnaliysis/data"

# 파일 생성
try:
    os.mkdir(f"{PATH}/")
except FileExistsError:
    logging.info(f"이미 메인 파일이 존재합니다.")


# Element Location
class NewYorkTaxiPageElement:
    """뉴욕 택시 데이터 HTML 접근 요소"""

    @staticmethod
    def a_title_element(e: BeautifulSoup) -> list[str]:
        """
        Args:
            e (BeautifulSoup): <li>
                <a href="다운로드 링크" title="Yellow Taxi Trip Records" class="exitlink">Yellow Taxi Trip Records </a>
            (PARQUET)</li>

        Returns:
            list[str]: ["A태그에 감싸진 다운로드 링크"]
        """
        return e.find_all("a", {"title": "High Volume For-Hire Vehicle Trip Records"})

    @staticmethod
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

    @staticmethod
    def div_tag_answers_element(e: BeautifulSoup, year: int) -> list[str]:
        """
        Args:
            e (BeautifulSoup): <div class="faq-answers" id="faq2019" role="region" aria-hidden="false" style="display: block;">
            year (int): faq20~~부터 시작하여 원하는 연도 접근 하기 위해서 매개변수 사용

        Returns:
            list[str]: ["A태그에 바깥쪽 ul 뭉치들"]
        """
        return e.find_all("div", {"class": "faq-answers", "id": f"faq20{year}"})


class FileDownloadAndFolderMaking(NewYorkTaxiPageElement):
    """파일 다운로드 하기 위한 클래스

    Args:
        NewYorkTaxiPageElement (_type_): 뉴욕택시 데이터 홈페이지 HTML page source
    """

    def __init__(self, start: int, end: int, path: str) -> None:
        """시작 매개변수

        Args:
            start (int): 시작 연도
            end (int): 끝 년도
            path (str): 데이터를 저장할 위치
        """
        super().__init__()
        self.q = Queue()
        self.start = start
        self.end = end
        self.path = path
        self.bs = BeautifulSoup(gd().page(), "lxml")
        if self.bs is None:
            return

    def log_download_progress(self, j: int) -> None:
        """로그 찍기 위한 메서드"""
        logging.info(f"{j}번째 색션에 접근합니다")

    def file_download(self, e: BeautifulSoup) -> list[str]:
        """a tag를 수집하기 위한 메서드 자세한 부분은 a_title_element method에 작성"""
        return [data["href"] for data in self.a_title_element(e)]

    def folder_making(self, path: str, starting: int) -> None:
        """
        Args:
            폴더 만들기 위한 메서드 div_tag_faq20_element method에 P태그 text를 추출하여 폴더를 생성하였음
            path (str): 저장하기 위한 파일경로
            starting (int): 시작하기 위한 년도

            output : (path)/(처음 시작할 년도 ~~ ).parquet
        """
        for final in self.div_tag_faq20_element(self.bs, starting):
            name: str = final.text.replace("\n", "")
            os.mkdir(f"{path}/{name}/")

    def data_select(self, starting: int) -> None:
        """
        Args:
            starting (int): 시작하기 위한 년도
            Queue 에 다운로드할 데이터 URI가 담김
        """
        for num in self.div_tag_answers_element(self.bs, starting):
            data_struct: list[str] = self.file_download(num)
            self.q.put(data_struct)

    def search_injection(self) -> None:
        """URL 서치 시작점"""
        for starting in range(self.start, self.end - 1, -1):
            self.folder_making(PATH, starting)
            self.data_select(starting)

    def download_file(self, path: str) -> None:
        """
        Args:
            path (str): 저장할 위치
            Queue에 데이터를 하나씩 꺼내와 이름과 file_location에 이름을 추출후 다운로드 진행
        """
        for da in self.q.get():
            name: str = da.split("/")[4]
            name_number: int = int(name.split("_")[2].split("-")[0])
            file_location: str = f"{path}/{name_number}/{name}"
            logging.info(f"{file_location} 저장합니다")
            urlretrieve(da, file_location)

    def download(self, n: int, path: str) -> None:
        """시작점"""
        j: int = 0
        while j < n:
            self.log_download_progress(j)
            self.download_file(path)
            j += 1


if __name__ == "__main__":
    down = FileDownloadAndFolderMaking(start=24, end=19, path=PATH)
    down.search_injection()
    down.download(6, PATH)
