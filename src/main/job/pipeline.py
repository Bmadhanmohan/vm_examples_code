from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql.types import StructType, StructField, DoubleType, StringType

from main.base import PySparkJobInterface
import pyspark.sql.functions as F


vehicles = StructType([
    StructField("vehicleId", StringType()),
    StructField("efficiency", DoubleType())
])


class PySparkJob(PySparkJobInterface):

    def init_spark_session(self) -> SparkSession:
        # TODO: add your code here
        ...

    def read_csv(self, input_path: str) -> DataFrame:
        # TODO: add your code here
        ...

    def calc_average_efficiency(self, observed: DataFrame) -> DataFrame:
        # TODO: add your code here
        ...

    def find_faulty_vehicles(self, avg_observed: DataFrame, required: DataFrame) -> DataFrame:
        # TODO: add your code here
        ...

    def save_as(self, data: DataFrame, output_path: str) -> None:
        # TODO: add your code here
        ...
