from pyspark import *
from pyspark import RDD
from pyspark.sql import *
from pyspark.sql.functions import *


def create_spark_session():
    sp = SparkSession.builder.master('local[*]').appName('word').getOrCreate()
    return sp


def read_data():
    sp = SparkContext.getOrCreate()
    data: RDD[str] = sp.textFile('hdfs://cdhserver:8020/user/labuser/madhan/paragrapgh.txt')
    return data


if __name__ == '__main__':
    spark = create_spark_session()
    data1 = read_data()
    # split the data by space
    # data=data1.map(lambda a:a.split(" "))
    # split the data by -
    data = data1.flatMap(lambda d: d.upper().split("-"))
    data = data.flatMap(lambda y: y.replace(",", "").replace(".", "").split(" "))
    data_after_map = data.map(lambda xyz: (xyz, 1))
    data = data_after_map.reduceByKey(lambda x, y: x + y)
    print(data.collect())
