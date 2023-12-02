import abc
from pyspark.sql import SparkSession, DataFrame


class PySparkJobInterface(abc.ABC):

    def __init__(self):
        self.spark = self.init_spark_session()

    @abc.abstractmethod
    def init_spark_session(self) -> SparkSession:
        """Create spark session"""
        raise NotImplementedError

    @abc.abstractmethod
    def read_csv(self, input_path: str) -> DataFrame:
        raise NotImplementedError

    @abc.abstractmethod
    def calc_average_efficiency(self, observed: DataFrame) -> DataFrame:
        raise NotImplementedError

    @abc.abstractmethod
    def find_faulty_vehicles(self, avg_observed: DataFrame, required: DataFrame) -> DataFrame:
        raise NotImplementedError

    @abc.abstractmethod
    def save_as(self, data: DataFrame, output_path: str) -> None:
        raise NotImplementedError

    def stop(self) -> None:
        self.spark.stop()
