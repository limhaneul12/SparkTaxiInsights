import os
from pathlib import Path
from pyspark.sql import DataFrame
from pyspark.sql.session import SparkSession


class FileLoadInParquet:
    def __init__(self, year: int) -> None:
        self.year = year

    def parquet_file_all(self) -> str:
        return str(Path(os.getcwd()).joinpath(f"data/{str(self.year)}"))

    def read_parquet_data(self, spark: SparkSession) -> list[DataFrame]:
        # 파케이 파일 경로
        data = self.parquet_file_all()
        return spark.read.parquet(f"file:///{data}/*")
