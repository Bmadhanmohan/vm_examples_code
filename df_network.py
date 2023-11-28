from pyspark import *
from pyspark.sql import *
def create_spark_session():
    spark=SparkSession.builder.appName("df event").master('local[*]').getOrCreate()
    return spark
if __name__ == '__main__':
    spark=create_spark_session()
    print(spark)