from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,LongType,IntegerType,FloatType,StringType
from pyspark.sql.functions import *

myschema = StructType([
                StructField("message",StringType(),False),
            ])

spark = SparkSession \
    .builder \
    .appName("SSKafka") \
    .config("spark.jars.packages","org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")


df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", "madhan") \
  .option("startingOffsets", "latest") \
  .load()

df.printSchema()

df1 = df.selectExpr("CAST(value AS STRING)").select(from_csv(col("value"),myschema).alias("data")).select("data.*")
df1.printSchema()

df1.writeStream \
  .outputMode("update") \
  .format("console") \
  .option("truncate", False) \
  .start() \
  .awaitTermination()