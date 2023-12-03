from pyspark.sql import *
from pyspark.sql.types import StructType, StructField, DoubleType, StringType
from pyspark import *
from pyspark.sql.functions import lit
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
        #TODO: add your code here
        data=observed.groupBy(observed.vehicleId).agg({"efficiency":"avg"}).withColumnRenamed('avg(efficiency)','fuelEfficiency')
        data=data.select(data.vehicleId,data.fuelEfficiency)
        return data
    def find_faulty_vehicles(self, avg_observed: DataFrame, required: DataFrame) -> DataFrame:
        # TODO: add your code here
        df=avg_observed.join[required,avg_observed.vehicleId==required.vehicleId,"inner join"]
        df=df.withColumn("obs",abs(df.observedfuelefficiency-df.requirefuelEfficiency))
        df=df.withColumn("faultyvehicle",lit("faulty") if df.obs>=5 else lit("Not faulty"))
        df.filter(df.faultyvehicle=='faulty')
        
        return df.select(df.vehicleId,df.faulty_effieicency)
    def save_as(self, data: DataFrame, output_path: str) -> None:
        # TODO: add your code here

        data.saveAsTextFile(output_path)
