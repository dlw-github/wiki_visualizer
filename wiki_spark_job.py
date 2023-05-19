scala_version = '2.12'
spark_version = '3.1.2'

import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1,org.apache.kafka:kafka-clients:2.6.1'


from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType, DateType, StringType, StructType, StructField, ArrayType

spark = SparkSession \
    .builder \
    .appName("StructuredNetworkWordCount") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Create DataFrame representing the stream of input lines from connection to localhost:44444
data_schema = StructType() \
  .add("url", StringType(), True) \
  .add("uuid", StringType(), True) \
  .add("dom", StringType(), True) \
  .add("type", StringType(), True) \
  .add("dt", DateType(), True) \
  .add("user", StringType(), True) \
  .add("title", StringType(), True) \
  .add("date_hour", DateType(), True)

df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "PLAINTEXT://172.17.0.1:9092") \
  .option("subscribe", "output_topic") \
  .option("startingOffsets", "latest") \
  .load()

df = df.withColumn("value", regexp_replace(col("value").cast("string"), "\\\\", ""))
df = df.withColumn("value", regexp_replace(col("value"), "^\"|\"$", ""))
df = df.select(col("timestamp"), from_json(col("value"), data_schema).alias("data"))


# query1 = df.withWatermark("timestamp", "2 minutes").groupBy(window("timestamp", "1 minutes"),"data.dom").count() \
#    .select([to_json(struct("window", "dom", "count")).alias("value")]) \
#    .writeStream \
#    .format("kafka") \
#    .option("kafka.bootstrap.servers", "PLAINTEXT://172.17.0.1:9092") \
#    .option("topic", "spark_topic") \
#    .option("checkpointLocation", "temp/dlw/kafka_checkpoint") \
#    .outputMode("update") \
#    .start()

query1.printSchema()

query1 = df.withWatermark("timestamp", "2 minutes").groupBy(window("timestamp", "1 minutes"),"data.dom").count()

query1.writeStream \   
  .format("console") \
  .option("checkpointLocation", "/tmp/dlw/checkpoint") \
  .outputMode("append") \
  .trigger(processingTime="10 seconds") \
  .start()

spark.streams.awaitAnyTermination()