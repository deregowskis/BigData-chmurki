from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType, ArrayType, LongType, DoubleType, IntegerType, FloatType
from pyspark.sql.functions import *
from sklearn.ensemble import RandomForestRegressor
from sklearn.datasets import make_classification
from sklearn.model_selection import train_test_split
from datetime import datetime, timedelta
from google.cloud import storage
from joblib import dump, load
import pandas as pd
import io
import time
import logging
import happybase

### common ###

def download_blob(bucket_name, source_blob_name, destination_file_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)
    blob.download_to_filename(destination_file_name)
    print(f"Blob {source_blob_name} downloaded to {destination_file_name}.")

bucket_name = "bigdata-chmurki-nifi"  
source_blob_name = "model/model.sav"  
model_path = "local_model.sav"    
download_blob(bucket_name, source_blob_name, model_path)

with open(model_path, 'rb') as model_file:
    loaded_model = load(model_file)    
    

spark = SparkSession \
    .builder \
    .appName("Streaming from Kafka") \
    .config("spark.streaming.stopGracefullyOnShutdown", True) \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1') \
    .config("spark.sql.shuffle.partitions", 4) \
    .master("local[*]") \
    .getOrCreate()
spark.conf.set("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false")

java_logger = spark._jvm.org.apache.log4j.Logger.getLogger("org.apache.spark")
java_logger.setLevel(spark._jvm.org.apache.log4j.Level.ERROR)

java_logger_kafka = spark._jvm.org.apache.log4j.Logger.getLogger("org.apache.kafka")
java_logger_kafka.setLevel(spark._jvm.org.apache.log4j.Level.ERROR)


### warsaw-nifi ###

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
spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
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

explodedDF_UTC = exploded_df.withColumn('timestamp_utc',from_utc_timestamp(col("generated"), "UTC"))
exploded_df = explodedDF_UTC.select(
    'timestamp_utc',
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


### srat warsaw-weather-forecast ###

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
explodedDF_weather=json_expanded_df_weather.withColumn("data",explode('hourly.data'))
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
exploded_df_weather = explodedDF_UTC_weather.select(
    'timestamp_utc',
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



### weather-actual ###

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

explodedDF_UTC_weather_actual = exploded_df_weather_actual.withColumn("timestamp_utc_temp", unix_timestamp(col("time"), "EEE MMM dd HH:mm:ss zzz yyyy"))
explodedDF_UTC_weather_actual = explodedDF_UTC_weather_actual.withColumn("timestamp_utc", to_utc_timestamp(from_unixtime(col("timestamp_utc_temp")), "UTC"))
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


### join streams ###

df_bus = exploded_df.withWatermark("rounded_timestamp", "20 seconds")
df_for = exploded_df_weather.withWatermark("timestamp_utc", "20 seconds")
df_act = exploded_df_weather_actual.withWatermark("timestamp_utc", "20 seconds")

for col_name in df_bus.columns:
    df_bus = df_bus.withColumnRenamed(col_name, f"bus_{col_name}")
for col_name in df_for.columns:
    df_for = df_for.withColumnRenamed(col_name, f"for_{col_name}")
for col_name in df_act.columns:
    df_act = df_act.withColumnRenamed(col_name, f"act_{col_name}")    

df_bus = df_bus.dropDuplicates()   
df_for = df_for.dropDuplicates()   
df_act = df_act.dropDuplicates()   
    
df = df_bus.join(df_act,(df_bus.bus_rounded_timestamp == df_act.act_rounded_timestamp),'leftOuter')    
df = df.join(df_for,(df.bus_rounded_timestamp == df_for.for_timestamp_utc),'leftOuter')        

df = df.dropDuplicates()   

### ml ###

df_ml = df.withColumn("timestamp", col("bus_generated").cast("timestamp"))\
    .withColumn("hour", hour(col("timestamp"))) \
    .withColumn("minute", minute(col("timestamp")))\
    .withColumn("time", (24 * 60) - (col("hour") * 60 + col("minute")))\
    .drop("timestamp","hour","minute")    
    
df_ml = df_ml.withColumn('speed',col('bus_speed').cast("float"))\
    .withColumn('delay',col('bus_delay').cast("float"))\
    .withColumn('lat',col('bus_lat').cast("float"))\
    .withColumn('lon',col('bus_lon').cast("float"))\
    .withColumn('temperature_actual',col('act_temperature'))\
    .withColumn('cloud_cover_actual',col('act_cloud_cover'))\
    .withColumn('wind_speed_actual',col('act_speed'))\
    .withColumn('wind_angle_actual',col('act_angle'))\
    .withColumn('precipitation_total_actual',col('act_precipitation_total'))\
    .withColumn('temperature_forecast',col('for_temperature'))\
    .withColumn('cloud_cover_forecast',col('for_cloud_cover'))\
    .withColumn('wind_speed_forecast',col('for_speed'))\
    .withColumn('wind_angle_forecast',col('for_angle'))\
    .withColumn('precipitation_total_forecast',col('for_precipitation_total'))

df_ml.printSchema()
    

list_of_columns = ['speed',
          'delay',
          'lat',
          'lon',
          'time',
          'temperature_actual',
          'cloud_cover_actual',
          'wind_speed_actual',
          'wind_angle_actual',
          'precipitation_total_actual',
          'temperature_forecast',
          'cloud_cover_forecast',
          'wind_speed_forecast',
          'wind_angle_forecast',
          'precipitation_total_forecast']

@udf('double')
def predict_udf(*cols):
    return float(loaded_model.predict((cols,)))

df_prediction = df_ml.withColumn('prediction', predict_udf(*list_of_columns))
df_prediction = df_prediction.drop(*list_of_columns)

# df_prediction.first().writeStream.outputMode("append").format("console").start()

# df_prediction.writeStream.outputMode("append").foreachBatch(lambda df, epochId: df.limit(1).show()).start()

  
df_prediction.writeStream.outputMode("append").format("console").start()    
       
### save to hbase ###        
        
def write_to_hbase(batch_df, batch_id):
    connection = happybase.Connection('10.186.0.20',9090)
    table_name = 'stream_table'
    data_to_write = batch_df.collect()

    with connection.table(table_name).batch() as table_batch:
        for row in data_to_write:
            table_batch.put(
                                (str(row["bus_timestamp_utc"]) + "__" + str(row["bus_vehicleId"] )).encode(), 
                            {
                                'act_bus:bus_timestamp_utc': str(row["bus_timestamp_utc"]).encode(),
                                'act_bus:bus_lastUpdate': str(row["bus_lastUpdate"]).encode(),
                                'act_bus:bus_generated': str(row["bus_generated"]).encode(),
                                'act_bus:bus_routeShortName': str(row["bus_routeShortName"]).encode(),
                                'act_bus:bus_headsign': str(row["bus_headsign"]).encode(),
                                'act_bus:bus_vehicleCode': str(row["bus_vehicleCode"]).encode(),
                                'act_bus:bus_vehicleService': str(row["bus_vehicleService"]).encode(),
                                'act_bus:bus_vehicleId': str(row["bus_vehicleId"]).encode(),
                                'act_bus:bus_speed': str(row["bus_speed"]).encode(),
                                'act_bus:bus_direction': str(row["bus_direction"]).encode(),
                                'act_bus:bus_delay': str(row["bus_delay"]).encode(),
                                'act_bus:bus_scheduledTripStartTime': str(row["bus_scheduledTripStartTime"]).encode(),
                                'act_bus:bus_lat': str(row["bus_lat"]).encode(),
                                'act_bus:bus_lon': str(row["bus_lon"]).encode(),
                                'act_bus:bus_gpsQuality': str(row["bus_gpsQuality"]).encode(),
                                'act_bus:bus_rounded_timestamp': str(row["bus_rounded_timestamp"]).encode(),
                                
                                'act_weather:act_timestamp_utc': str(row["act_timestamp_utc"]).encode(),
                                'act_weather:act_lat': str(row["act_lat"]).encode(),
                                'act_weather:act_lon': str(row["act_lon"]).encode(),
                                'act_weather:act_elevation': str(row["act_elevation"]).encode(),
                                'act_weather:act_timezone': str(row["act_timezone"]).encode(),
                                'act_weather:act_units': str(row["act_units"]).encode(),
                                'act_weather:act_icon': str(row["act_icon"]).encode(),
                                'act_weather:act_summary': str(row["act_summary"]).encode(),
                                'act_weather:act_temperature': str(row["act_temperature"]).encode(),
                                'act_weather:act_speed': str(row["act_speed"]).encode(),
                                'act_weather:act_angle': str(row["act_angle"]).encode(),
                                'act_weather:act_dir': str(row["act_dir"]).encode(),
                                'act_weather:act_precipitation_total': str(row["act_precipitation_total"]).encode(),
                                'act_weather:act_type': str(row["act_type"]).encode(),
                                'act_weather:act_cloud_cover': str(row["act_cloud_cover"]).encode(),
                                'act_weather:act_daily': str(row["act_daily"]).encode(),
                                'act_weather:act_rounded_timestamp': str(row["act_rounded_timestamp"]).encode(),

                                'fc_weather:for_timestamp_utc': str(row["for_timestamp_utc"]).encode(),
                                'fc_weather:for_lat': str(row["for_lat"]).encode(),
                                'fc_weather:for_lon': str(row["for_lon"]).encode(),
                                'fc_weather:for_elevation': str(row["for_elevation"]).encode(),
                                'fc_weather:for_timezone': str(row["for_timezone"]).encode(),
                                'fc_weather:for_units': str(row["for_units"]).encode(),
                                'fc_weather:for_current': str(row["for_current"]).encode(),
                                'fc_weather:for_date': str(row["for_date"]).encode(),
                                'fc_weather:for_weather': str(row["for_weather"]).encode(),
                                'fc_weather:for_icon': str(row["for_icon"]).encode(),
                                'fc_weather:for_summary': str(row["for_summary"]).encode(),
                                'fc_weather:for_temperature': str(row["for_temperature"]).encode(),
                                'fc_weather:for_speed': str(row["for_speed"]).encode(),
                                'fc_weather:for_angle': str(row["for_angle"]).encode(),
                                'fc_weather:for_dir': str(row["for_dir"]).encode(),
                                'fc_weather:for_precipitation_total': str(row["for_precipitation_total"]).encode(),
                                'fc_weather:for_type': str(row["for_type"]).encode(),
                                'fc_weather:for_total': str(row["for_total"]).encode(),
                                'fc_weather:for_cloud_cover': str(row["for_cloud_cover"]).encode(),
                                'fc_weather:for_daily': str(row["for_daily"]).encode(),
                                
                                'prediction:prediction': str(row["prediction"]).encode()
                            })
    connection.close()

query = (
    df_prediction
    .writeStream
    .outputMode("append")
    .foreachBatch(write_to_hbase)
    .start()
)

spark.conf.set("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false")
query.awaitTermination()
query.close()