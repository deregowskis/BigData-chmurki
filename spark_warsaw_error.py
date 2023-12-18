from pyspark.sql import SparkSession
import time
from pyspark.sql.types import StringType, StructField, StructType, ArrayType, LongType
from pyspark.sql.functions import from_json
from pyspark.sql.functions import explode, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, FloatType
from pyspark.sql.functions import col, from_utc_timestamp, dayofmonth, hour, minute
from datetime import datetime, timedelta
from pyspark.sql.functions import *

from pyspark.ml.regression import RandomForestRegressor
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.types import DoubleType

import pickle
from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf, struct
from pyspark.sql.types import DoubleType
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.datasets import make_classification
from sklearn.model_selection import train_test_split



spark = SparkSession \
    .builder \
    .appName("Streaming from Kafka") \
    .config("spark.streaming.stopGracefullyOnShutdown", True) \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1') \
    .config("spark.sql.shuffle.partitions", 4) \
    .master("local[*]") \
    .getOrCreate()
spark.conf.set("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false")
df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "10.186.0.3:9092") \
  .option("startingOffsets", "latest") \
  .option("subscribe", "warsaw-nifi") \
  .option("includeHeaders", "false") \
  .option("maxOffsetsPerTrigger", "1") \
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
  .option("maxOffsetsPerTrigger", "1") \
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
    col("data.cloud_cover.total").alias("cloud_cover"),
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
    "cloud_cover",
    "daily"
    
)

exploded_df = exploded_df.withColumn(
    "rounded_timestamp",
    expr("from_unixtime(round(unix_timestamp(timestamp_utc) / 3600) * 3600 + 3600)") 
).withColumn('rounded_timestamp',from_utc_timestamp(col("rounded_timestamp"), "UTC"))

df_weather_actual = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "10.186.0.3:9092") \
  .option("startingOffsets", "latest") \
  .option("subscribe", "weather-actual") \
  .option("includeHeaders", "false") \
  .option("maxOffsetsPerTrigger", "1") \
  .load()
df_weather_actual.selectExpr("CAST(key AS STRING) as key", "CAST(value AS STRING)")
df_weather_actual.printSchema()

json_schema_actual = StructType([
    StructField("lat", StringType(), True),
    StructField("lon", StringType(), True),
    StructField("elevation", IntegerType(), True),
    StructField("timezone", StringType(), True),
    StructField("units", StringType(), True),
    StructField("current", StructType([
        StructField("icon", StringType(), True),
        StructField("icon_num", IntegerType(), True),
        StructField("summary", StringType(), True),
        StructField("temperature", FloatType(), True),
        StructField("wind", StructType([
            StructField("speed", FloatType(), True),
            StructField("angle", IntegerType(), True),
            StructField("dir", StringType(), True)
        ]), True),
        StructField("precipitation", StructType([
            StructField("total", FloatType(), True),
            StructField("type", StringType(), True)
        ]), True),
        StructField("cloud_cover", IntegerType(), True)
    ]), True),
    StructField("hourly", StringType(), True),  # Zakładam, że "hourly" może być również null, dlatego używam StringType
    StructField("daily", StringType(), True),  # Zakładam, że "daily" może być również null, dlatego używam StringType
    StructField("time", StringType(), True)
])

json_df_weather_actual = df_weather_actual.selectExpr("cast(value as string) as value")
json_expanded_df_weather_actual = json_df_weather_actual.withColumn("value", from_json(json_df_weather_actual["value"], json_schema_actual)).select("value.*") 

json_expanded_df_weather_actual.printSchema()

exploded_df_weather_actual = json_expanded_df_weather_actual.select(
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
    col("current.precipitation.total").alias("precipitation_total"),
    "current.precipitation.type",
    "current.cloud_cover",
    "hourly",
    "daily",
    "time"
)

explodedDF_UTC_weather_actual = exploded_df_weather_actual.withColumn('timestamp_utc',from_utc_timestamp(col("time"), "UTC"))

explodedDF_UTC_weather_actual.printSchema()
exploded_df_weather_actual = explodedDF_UTC_weather_actual.select(
    'timestamp_utc',
    dayofmonth("timestamp_utc").alias("day"),
    hour("timestamp_utc").alias("hour"),
    minute("timestamp_utc").alias("minute"),
    "lat",
    "lon",
    "elevation",
    "timezone",
    "units",
    "icon",
    "summary",
    "temperature",
    "speed",
    "angle",
    "dir",
    "precipitation_total",
    "type",
    "cloud_cover",
    "daily"
    
)

exploded_df_weather_actual = exploded_df_weather_actual.withColumn(
    "rounded_timestamp",
    expr("from_unixtime(round(unix_timestamp(timestamp_utc) / 3600) * 3600 + 3600)") 
).withColumn('rounded_timestamp',from_utc_timestamp(col("rounded_timestamp"), "UTC"))



df1_watermark = exploded_df.withWatermark("rounded_timestamp", "30 seconds")
df2_watermark = exploded_df_weather.withWatermark("timestamp_utc", "30 seconds")
df3_watermark = exploded_df_weather_actual.withWatermark("timestamp_utc", "45 seconds")

#windowed_stream1 = df1_watermark.groupBy(session_window("timestamp_utc", "10 minutes"),"vehicleId").agg({"speed": "avg"},{"delay":"avg"})
#windowed_stream2 = df2_watermark.groupBy(session_window("timestamp_utc", "10 minutes")).agg({"temperature": "avg"})
# joined_df = df1_watermark.alias("in_df").join(df2_watermark.alias("out_df"),(df2_watermark.timestamp_utc.between(df1_watermark.timestamp_utc - timedelta(seconds=300),df1_watermark.timestamp_utc+ timedelta(seconds=300))),'leftOuter')


df2_watermark = df2_watermark.withColumnRenamed("speed", "wind_speed")
joined_df = df1_watermark.alias("in_df").join(df2_watermark.alias("out_df"),(df1_watermark.rounded_timestamp == df2_watermark.timestamp_utc),'full_outer')


# ["routeShortName","summary","temperature","cloud_cover","wind.speed","wind.angle","precipitation.total","precipitation.type","delay"]
#routeShortName	temperature	cloud_cover	speed	angle	precipitation.total	delay	type

joined_df = joined_df.select("routeShortName","temperature","cloud_cover","wind_speed","angle","precipitation_total","type","rounded_timestamp")



joined_df = joined_df.withColumnRenamed("wind_speed", "speed") \
    .withColumnRenamed("precipitation_total", "total")

joined_df.printSchema()

joined_df_2 = joined_df.alias("in_df").join(df3_watermark.alias("out_df"),(joined_df.rounded_timestamp == df3_watermark.rounded_timestamp),'full_outer')

joined_df_2.printSchema()

# wywala sie 

#@pandas_udf(DoubleType())
#def make_prediction_udf(*cols):
    # Convert DataFrame columns to Pandas DataFrame
#    data = pd.concat(cols, axis=1)
    # Make predictions using the scikit-learn model
#    predictions = rf_model.predict_proba(data)[:, 1]  # Change [:, 1] if it's a classification problem
#    return pd.Series(predictions)
#
    # return pd.Series(1.1)


#rf_model = pickle.load(open("model2 (4).sav", 'rb'))
#result_df = rf_model.predict_proba(joined_df).select("*")
#result_df = joined_df.select("*", make_prediction_udf(struct(*joined_df.columns)).alias("prediction"))


#with open("model2 (4).sav", "rb") as model_file:
#    scikit_model = pickle.load(model_file)
    
#@pandas_udf(DoubleType(), PandasUDFType.SCALAR)
#def predict_udf(*cols):
#    # Assuming your input columns are features
#    features = pd.concat(cols, axis=1)
#    predictions = scikit_model.predict(features)
#    return pd.Series(predictions)


#result_df = joined_df.withColumn("predictions", predict_udf(joined_df["routeShortName"], joined_df["temperature"],joined_df["cloud_cover"],joined_df["speed"],joined_df["angle"],joined_df["total"],joined_df["type"] ))
#result_df.printSchema()





spark.sparkContext.setLogLevel('WARN')
spark.conf.set("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false")
#query = df.writeStream.format("json").option("path", 'output').option("checkpointLocation","checkpoint_dir") .start()
query = joined_df_2.writeStream.format("console").start()
query.awaitTermination()
query.close()
