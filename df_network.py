from pyspark import *
from pyspark.sql import *
def create_spark_session():
    spark=SparkSession.builder.appName("df event").master('local[*]').getOrCreate()
    return spark
def read_eventlog():
    event_df=spark.read.format('csv')\
        .load('hdfs://cdhserver:8020/user/labuser/madhan/networkevent_1.log')\
        .toDF('EventLogs')
    return event_df
if __name__ == '__main__':
    spark=create_spark_session()
    #print(read_eventlog().show(truncate=False))
    even_data_log=read_eventlog()
    ip_add=r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}'
    res_df=evnt_data