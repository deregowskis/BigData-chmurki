from pyspark.sql import SparkSession
import time
from pyspark.sql.types import StringType, StructField, StructType, ArrayType, LongType
from pyspark.sql.functions import from_json
from pyspark.sql.functions import explode, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.sql.functions import col, from_utc_timestamp, dayofmonth, hour, minute


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
  .option("startingOffsets", "latest") \
  .option("subscribe", "warsaw-nifi") \
  .option("includeHeaders", "false") \
  .load()
df.selectExpr("CAST(key AS STRING) as key", "CAST(value AS STRING)")
df.printSchema()

json_schema = StructType([StructField("lastUpdate", StringType(), True),\
        StructField("vehicles" , ArrayType(StructType([ \
        StructField('generated', StringType(), True), \
        StructField('routeShortName', StringType(), True), \
        StructField('tripId', StringType(), True), \
        StructField('headsign', StringType(), True),\
        StructField('vehicleCode', StringType(), True),\
        StructField('vehicleService', StringType(), True), \
        StructField('vehicleId', StringType(), True),\
        StructField('speed', StringType(), True) ,\
        StructField('direction', StringType(), True), \
        StructField('delay', StringType(), True),\
        StructField('scheduledTripStartTime', StringType(), True) ,\
        StructField('lat', StringType(), True), \
        StructField('lon', StringType(), True),\
        StructField('gpsQuality', StringType(), True)]), True), True)
        ])



json_df = df.selectExpr("cast(value as string) as value")
json_expanded_df = json_df.withColumn("value", from_json(json_df["value"], json_schema)).select("value.*") 

json_expanded_df.printSchema()

explodedDF=json_expanded_df.withColumn("vehicles",explode('vehicles'))

exploded_df = explodedDF.select(
    "lastUpdate", 
    "vehicles.generated",
    "vehicles.routeShortName", 
    "vehicles.tripId",
    "vehicles.headsign",
    "vehicles.vehicleCode",
    "vehicles.vehicleService",
    "vehicles.vehicleId",
    "vehicles.speed",
    "vehicles.direction",
    "vehicles.delay",
    "vehicles.scheduledTripStartTime",
    "vehicles.lat",
    "vehicles.lon",
    "vehicles.gpsQuality"
)
exploded_df.printSchema()

explodedDF_UTC = exploded_df.withColumn('timestamp_utc',from_utc_timestamp(col("generated"), "UTC"))

explodedDF_UTC.printSchema()
exploded_df = explodedDF_UTC.select(
    'timestamp_utc',
    dayofmonth("timestamp_utc").alias("day"),
    hour("timestamp_utc").alias("hour"),
    minute("timestamp_utc").alias("minute"),
    "lastUpdate", 
    "generated",
    "routeShortName", 
    "tripId",
    "headsign",
    "vehicleCode",
    "vehicleService",
    "vehicleId",
    "speed",
    "direction",
    "delay",
    "scheduledTripStartTime",
    "lat",
    "lon",
    "gpsQuality"
)


df_weather = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "10.186.0.3:9092") \
  .option("startingOffsets", "latest") \
  .option("subscribe", "weather-forecast") \
  .option("includeHeaders", "false") \
  .load()
df_weather.selectExpr("CAST(key AS STRING) as key", "CAST(value AS STRING)")
df_weather.printSchema()


hourly_schema = StructType([
    StructField("date", StringType(), True),
    StructField("weather", StringType(), True),
    StructField("icon", IntegerType(), True),
    StructField("summary", StringType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("wind", StructType([
        StructField("speed", DoubleType(), True),
        StructField("dir", StringType(), True),
        StructField("angle", IntegerType(), True)
    ]), True),
    StructField("cloud_cover", StructType([
        StructField("total", IntegerType(), True)
    ]), True),
    StructField("precipitation", StructType([
        StructField("total", DoubleType(), True),
        StructField("type", StringType(), True)
    ]), True)
])

# Define the main schema
main_schema = StructType([
    StructField("lat", StringType(), True),
    StructField("lon", StringType(), True),
    StructField("elevation", IntegerType(), True),
    StructField("timezone", StringType(), True),
    StructField("units", StringType(), True),
    StructField("current", StringType(), True),
    StructField("hourly", StructType([
        StructField("data", ArrayType(hourly_schema), True)
    ]), True),
    StructField("daily", StringType(), True)
])


json_df_weather = df_weather.selectExpr("cast(value as string) as value")
json_expanded_df_weather = json_df_weather.withColumn("value", from_json(json_df_weather["value"], main_schema)).select("value.*") 

json_expanded_df_weather.printSchema()

explodedDF_weather=json_expanded_df_weather.withColumn("data",explode('hourly.data'))

explodedDF_weather.printSchema()

exploded_df_weather = explodedDF_weather.select(
    "lat",
    "lon",
    "elevation",
    "timezone",
    "units",
    "current",
    "data.date",
    "data.weather",
    "data.icon",
    "data.summary",
    "data.temperature",
    "data.wind.speed",
    "data.wind.angle",
    "data.wind.dir",
    col("data.precipitation.total").alias("precipitation_total"),
    "data.precipitation.type",
    "data.cloud_cover.total",
    "daily"
)

explodedDF_UTC_weather = exploded_df_weather.withColumn('timestamp_utc',from_utc_timestamp(col("date"), "UTC"))

explodedDF_UTC_weather.printSchema()
exploded_df_weather = explodedDF_UTC_weather.select(
    'timestamp_utc',
    dayofmonth("timestamp_utc").alias("day"),
    hour("timestamp_utc").alias("hour"),
    minute("timestamp_utc").alias("minute"),
    "lat",
    "lon",
    "elevation",
    "timezone",
    "units",
    "current",
    "date",
    "weather",
    "icon",
    "summary",
    "temperature",
    "speed",
    "angle",
    "dir",
    "precipitation_total",
    "type",
    "total",
    "daily"
)

df1_watermark = exploded_df.withWatermark("timestamp_utc", "30 seconds")
df2_watermark = exploded_df_weather.withWatermark("timestamp_utc", "30 seconds")
joined_df = exploded_df.alias("in_df").join(exploded_df_weather.alias("out_df"),  df1_watermark["timestamp_utc"] == df2_watermark["timestamp_utc"],'left')


spark.sparkContext.setLogLevel('WARN')

#query = df.writeStream.format("json").option("path", 'output').option("checkpointLocation","checkpoint_dir") .start()
query = joined_df.writeStream.format("console").start()
#time.sleep(5) # sleep 10 seconds
query.awaitTermination()
query.close()
