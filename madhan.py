from pyspark import *
from pyspark.sql import *
spark=SparkContext.getOrCreate().parallelize([1,2,3,4,5,5,5])
print(spark.collect())