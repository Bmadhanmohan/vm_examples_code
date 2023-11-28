from cffi.model import StructType
from pyspark import *
from pyspark.sql import *
from pyspark.sql.functions import regexp_extract, substring, col, length
from pyspark.sql.types import *

def create_spark_session():
    spark=SparkSession.builder.appName("df stream event").master('local[*]').getOrCreate()
    return spark
myschema=StructType([StructField("EventLogs",StringType())])
def read_eventlog():
    event_df=spark.readStream.format('csv')\
        .schema(myschema)\
        .load('hdfs://cdhserver:8020/user/labuser/madhan/streaming/')\
        .toDF('EventLogs')
    return event_df
if __name__ == '__main__':
    spark=create_spark_session()
    #print(read_eventlog().show(truncate=False))
    even_data_log=read_eventlog()
    #64.241.88.10 ip pattern
    ip_add=r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}'
    #[07 / Mar / 2004:16:05:49 - 0800]  datepattern
    dt_ext=r'\[(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} -\d{4})\]'
    url_date=r'"([^"]*)"'
    status_col=r'(\d{1,3}\d) \d{1,5}$'
    bytes_col=r'(\d{1,5})$'
    res_df=even_data_log\
        .withColumn('ip_address',regexp_extract('EventLogs', ip_add,idx=0)) \
        .withColumn('ip_Date', regexp_extract('EventLogs', dt_ext, idx=1)) \
        .withColumn('url_address',regexp_extract('EventLogs',url_date , idx=1)) \
        .withColumn('Status_code', substring(regexp_extract('EventLogs',status_col,1),1,3))\
        .withColumn('size', regexp_extract('EventLogs',bytes_col , idx=1)) \
        .select('ip_address','ip_Date','url_address','Status_code','size')
    #print(res_df.show(truncate=False))
    result=res_df.writeStream.format('console').option('truncate',False).outputMode('append').start()
    result.awaitTermination()