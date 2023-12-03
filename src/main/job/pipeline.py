from pyspark.sql import *
from pyspark.sql.types import StructType, StructField, DoubleType, StringType
from pyspark import *
from pyspark.sql.functions import *
from main.base import PySparkJobInterface
import pyspark.sql.functions as F


vehicles = StructType([
    StructField("vehicleId", StringType()),
    StructField("efficiency", DoubleType())
])


class PySparkJob(PySparkJobInterface):

    def init_spark_session(self) -> SparkSession:
        # TODO: add your code here
        self.spark=SparkSession.builder.master('local').appName('Faulty Vehicle Detection').getOrCreate()
        return self.spark
    def read_csv(self, input_path: str) -> DataFrame:
        # TODO: add your code here
        self.sp= SparkSession.builder.getOrCreate().read.format("csv").option("header", "False").schema(vehicles).load(input_path)
        return self.sp

    def calc_average_efficiency(self, observed: DataFrame) -> DataFrame:
        # TODO: add your code here
        data=observed.groupBy(observed.vehicleId)
        data=data.withColumn('efficiency',sum(observed.vehicleId)/observed['vehicleId'].count())
        data=data.select('vehicleId','efficiency')
        #observed.createOrReplaceGlobalTempView('tab')
        #data=SparkSession.builder.getOrCreate().sql("select vehicleId,sum(fuel_efficiency)/count(vehicleId) from tab group by vehicleId")
        print(data.show())

        return 1

    def find_faulty_vehicles(self, avg_observed: DataFrame, required: DataFrame) -> DataFrame:
        # TODO: add your code here
        return 1

    def save_as(self, data: DataFrame, output_path: str) -> None:
        # TODO: add your code here
        return 1
