from pyspark import *
from pyspark.sql import *
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from wtforms.validators import regexp


def create_spark_context():
    spark: SparkSession = SparkSession.builder.master('local[*]').appName("files and dir").getOrCreate()
    return spark


myschema = StructType([StructField('Data', StringType())])
def read_data_csv():
    read_data: DataFrame = spark.read.format('csv') \
        .schema(myschema) \
        .load('hdfs://cdhserver:8020/user/labuser/madhan/files_and_directories.log')\
        .toDF('Data')

    return read_data


if __name__ == '__main__':
    spark = create_spark_context()
    data = read_data_csv()
    #print(data.show(truncate=False))
    files_no =r'\d{1,2}'
    size_reg =r'\d{}'
    data_final = data\
        .withColumn('Dir/File',substring(data.Data,1,1)) \
        .withColumn('No_Of_Files', regexp_extract('Data',files_no,idx=0)) \
        .withColumn('Size', regexp_extract('Data', size_reg, idx=0)) \
        .select('Dir/File','No_Of_Files')
    print(data_final.show(truncate=False))
