import os
import pandas as pd
from pathlib import Path

from concurrent.futures import ThreadPoolExecutor


def parquet_file_all(taxi_type: str) -> list[list[Path]]:
    def location(data: str) -> list[Path]:
        return sorted(
            Path(__file__).parent.joinpath(f"data/{taxi_type}/{data}").glob("*")
        )

    return [location(data) for data in os.listdir(f"{os.getcwd()}/data/{taxi_type}")]


def data_length(taxi_type: str):
    # 데이터 크기를 계산할 변수를 초기화
    total_data_size = 0

    # parquet_file_all() 함수를 한 번 호출하여 결과를 저장
    all_parquet_files: list[list[Path]] = parquet_file_all(taxi_type)

    # 총합 데이터 개수
    for file_group in all_parquet_files:
        for file in file_group:
            print(file)
            total_data_size += len(pd.read_parquet(file))

    return total_data_size


total = 0
with ThreadPoolExecutor(2) as pool:
    task = [
        pool.submit(data_length, "HighVolume"),
        pool.submit(data_length, "YellowTaxi"),
    ]

    try:
        for data in task:
            total += data.result()
    except Exception:
        pass


print(f"데이터의 크기는 다음과 같습니다 ==> {total}")
