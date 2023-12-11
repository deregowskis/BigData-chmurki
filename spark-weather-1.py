from pyspark.sql import SparkSession
import time
from pyspark.sql.types import StringType, StructField, StructType, ArrayType, LongType
from pyspark.sql.functions import from_json
from pyspark.sql.functions import explode, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

spark = SparkSession \
    .builder \
    .appName("Streaming from Kafka") \
    .config("spark.streaming.stopGracefullyOnShutdown", True) \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1') \
    .config("spark.sql.shuffle.partitions", 4) \
    .master("local[*]") \
    .getOrCreate()
df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "10.186.0.3:9092") \
  .option("startingOffsets", "earliest") \
  .option("subscribe", "weather-nifi") \
  .option("includeHeaders", "false") \
  .load()
df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

json_schema = StructType([
    StructField("lat", StringType(), True),
    StructField("lon", StringType(), True),
    StructField("elevation", IntegerType(), True),
    StructField("timezone", StringType(), True),
    StructField("units", StringType(), True),
    StructField("current", StructType([
        StructField('icon', StringType(), True),
        StructField('icon_num', IntegerType(), True),
        StructField('summary', StringType(), True),
        StructField('temperature', DoubleType(), True),
        StructField("wind", StructType([
            StructField('speed', DoubleType(), True),
            StructField('angle', IntegerType(), True),
            StructField('dir', StringType(), True)
        ]), True),
        StructField("precipitation", StructType([
            StructField('total', DoubleType(), True),
            StructField('type', StringType(), True)
        ]), True),
        StructField('cloud_cover', IntegerType(), True)
    ]), True),
    StructField("hourly", StringType(), True),  
    StructField("daily", StringType(), True)    
])

json_df = df.selectExpr("cast(value as string) as value")
json_expanded_df = json_df.withColumn("value", from_json(json_df["value"], json_schema)).select("value.*") 

json_df.printSchema()



exploded_df = json_df.select(
    "lat",
    "lon",
    "elevation",
    "timezone",
    "units",
    "current.icon",
    "current.icon_num",
    "current.summary",
    "current.temperature",
    "current.wind.speed",
    "current.wind.angle",
    "current.wind.dir",
    "current.precipitation.total",
    "current.precipitation.type",
    "current.cloud_cover",
    "hourly",
    "daily"
)

exploded_df.show(truncate=False)



spark.sparkContext.setLogLevel('WARN')

#query = df.writeStream.format("json").option("path", 'output').option("checkpointLocation","checkpoint_dir") .start()
query = exploded_df.writeStream.format("console").start()
#time.sleep(5) # sleep 10 seconds
query.awaitTermination()
