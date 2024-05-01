from typing import Union
from utils.util import FileLoadInParquet

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import col
from pyspark.sql.session import SparkSession


MAX_MEMORY = "8g"


class TaxiDateTimeGroupbyAndAverageMile(FileLoadInParquet):
    def __init__(self, year: int) -> None:
        super().__init__(year)

    def spark_init(self) -> SparkSession:
        return (
            SparkSession.builder.master("spark://spark-master:7077")
            .appName("TripAnaliysis")
            .config("spark.excutor.memory", MAX_MEMORY)
            .config("spark.driver.memory", MAX_MEMORY)
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.executor.cores", "8")
            .config("spark.cores.max", "8")
            .config("spark.sql.execution.arrow.pyspark.enabled", "true")
            .config("spark.shuffle.service.enabled", "true")
            .config("spark.dynamicAllocation.enabled", "true")
            .getOrCreate()
        )

    def data_injection(self) -> Union[DataFrame, None]:
        """
        데이터 합치기

        Returns:
            Union[DataFrame]: ex) 2019년 의 모든 데이터합치기
        """
        # 주어진 연도의 parquey data 불러오기
        return self.read_parquet_data(self.spark_init())

    def datetime_groupby(self, data: DataFrame, name: str, agg_name: str) -> DataFrame:
        """datetime groupby

        Args:
            data (DataFrame): groupby 할 데이터
            name (str): groupby 할 컬럼
            agg_name (str): groupby 하고 난 후 데이터 컬럼명

        Returns:
            DataFrame: datetime | agg_name
        """
        return (
            data.select(F.split(col(name), " ")[0].name("pickup"))
            .dropna()
            .groupBy("pickup")
            .agg(F.count("*").name(agg_name))
        )

    def datetime_miles_average(self, data: DataFrame) -> DataFrame:
        """datetime 별 평균적으로 얼마나 이동했는지

        Args:
            data (DataFrame): groupby할 데이터

        Returns:
            DataFrame: datetime | average_miles
        """
        return (
            data.select(
                F.split(col("pickup_datetime"), " ")[0].name("pickup"),
                col("trip_miles"),
            )
            .groupBy("pickup")
            .agg(F.avg("trip_miles").name("average_miles"))
        )

    def taxi_datetime_type(self, datetime_type: str, alias: str) -> DataFrame:
        """택시 datetime datetime type 지정

        Args:
            datetime_type (str): requset_datetime, dropoff_datetime, pickup_datetime
            alias (str): groupby 하고 난 후 데이터 컬럼명

        Returns:
            DataFrame: 각 datetime 마다의 groupby 데이터
        """
        return self.datetime_groupby(self.data_injection(), datetime_type, alias)

    def list_datetime_year(self) -> list[DataFrame]:
        """년도 마다 모든 datetime 형식을 묶기

        Returns:
            DataFrame: 각 datetime + averagemile의 groupby
        """
        datetime_frame: list[DataFrame] = [
            self.taxi_datetime_type(name, count)
            for name, count in [
                ("request_datetime", "request_count"),
                ("dropoff_datetime", "drop_count"),
            ]
        ]
        datetime_frame.append(self.datetime_miles_average(self.data_injection()))
        return datetime_frame

    def join_chain(self) -> DataFrame:
        """조인해주기 pickup -> request -> dropoff -> mile"""
        datetime_data = self.taxi_datetime_type("pickup_datetime", "trip_count")
        data_frames = self.list_datetime_year()
        for df in data_frames:
            datetime_data = datetime_data.join(df, on="pickup", how="left")
        return datetime_data


class SparkPreprocessingPandasChange(TaxiDateTimeGroupbyAndAverageMile):
    def __init__(self, year: int) -> None:
        super().__init__(year)

    def week_data_rtd(self) -> DataFrame:
        """요일마다 번호 부여하기"""
        data = self.join_chain()
        return data.select(
            F.date_format(col("pickup"), "EEEE").alias("week"),
            F.dayofweek(col("pickup")).alias("week_number"),
            col("pickup"),
            col("trip_count"),
            col("request_count"),
            col("drop_count"),
            col("average_miles"),
        )

    def save_processed_data(self) -> None:
        self.week_data_rtd().orderBy("pickup").toPandas().to_csv(
            f"datetime_prepro/total/TripCount_in_{self.year}.csv",
            index=False,
            index_label=False,
        )


if __name__ == "__main__":
    for data in range(2019, 2025):
        SparkPreprocessingPandasChange(data).save_processed_data()
