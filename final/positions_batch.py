from pyspark.sql import SparkSession
from datetime import datetime, timedelta
from google.cloud import storage
from pyspark.sql.functions import explode, col, current_timestamp, expr, date_format, from_utc_timestamp, dayofmonth,concat, lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, FloatType, ArrayType
import io

spark = SparkSession \
    .builder \
    .appName("Batch job upload") \
    .master("yarn") \
    .getOrCreate()


def get_timestamp_and_decompose(spark):
    two_hours_ago_timestamp = spark.range(1).select(
        expr("current_timestamp() - interval 2 hours").alias("two_hours_ago_timestamp")
    )
    decomposed_timestamp = two_hours_ago_timestamp.select(
        date_format("two_hours_ago_timestamp", "yyyy-MM-dd").alias("day"),
        date_format("two_hours_ago_timestamp", "HH").alias("hour")
    ).collect()[0]
    return decomposed_timestamp["day"], decomposed_timestamp["hour"]

date, hour = get_timestamp_and_decompose(spark)

client = storage.Client()
bucket_name="bigdata-chmurki-nifi"

positions = None
    
for blob in client.list_blobs(bucket_name, prefix='warsaw'):
    if blob.name.count(f'Act_position_{date}-{hour}') == 1:
        print(f'Processing file: {blob.name}')
        try:
            parquet_file_path =f'gs://{bucket_name}/{blob.name}'
            df = spark.read.parquet(parquet_file_path)
            df = df.withColumn("vehicles",explode('vehicles'))
            df = df.select(
                    "lastUpdate", 
                    "vehicles.generated",
                    "vehicles.routeShortName",
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
            df = df.withColumn('timestamp_utc',from_utc_timestamp(col("generated"), "UTC"))
            df = df.withColumn('day',dayofmonth(col("timestamp_utc")))
            df = df.withColumn('key',concat(col("timestamp_utc"), lit("_"), col("vehicleId")))
            df = df.select(
                    'key',
                    'timestamp_utc',
                    'day',
                    "lastUpdate", 
                    "generated",
                    "routeShortName", 
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
            if positions is not None:
                positions = positions.union(df)
            else:
                positions = df
            print('Success')
        except:
            print(f'File {parquet_file_path} was not properly processed.')
            continue
    
catalog = ''.join("""{
  "table": {
    "namespace": "default",
    "name": "positions"
  },
  "rowkey": "key",
  "columns": {
    "key": {"cf": "rowkey", "col": "key", "type": "string"},
    "timestamp_utc": {"cf": "position_cols", "col": "timestamp_utc", "type": "timestamp"},
    "day": {"cf": "position_cols", "col": "day", "type": "integer"},
    "lastUpdate": {"cf": "position_cols", "col": "lastUpdate", "type": "string"},
    "generated": {"cf": "position_cols", "col": "generated", "type": "string"},
    "routeShortName": {"cf": "position_cols", "col": "routeShortName", "type": "string"},
    "headsign": {"cf": "position_cols", "col": "headsign", "type": "string"},
    "vehicleCode": {"cf": "position_cols", "col": "vehicleCode", "type": "string"},
    "vehicleService": {"cf": "position_cols", "col": "vehicleService", "type": "string"},
    "vehicleId": {"cf": "position_cols", "col": "vehicleId", "type": "string"},
    "speed": {"cf": "position_cols", "col": "speed", "type": "string"},
    "direction": {"cf": "position_cols", "col": "direction", "type": "string"},
    "delay": {"cf": "position_cols", "col": "delay", "type": "string"},
    "scheduledTripStartTime": {"cf": "position_cols", "col": "scheduledTripStartTime", "type": "string"},
    "lat": {"cf": "position_cols", "col": "lat", "type": "string"},
    "lon": {"cf": "position_cols", "col": "lon", "type": "string"},
    "gpsQuality": {"cf": "position_cols", "col": "gpsQuality", "type": "string"}
  }
}""".split())

positions.write \
    .format('org.apache.hadoop.hbase.spark') \
    .options(catalog=catalog) \
    .option("hbase.spark.use.hbasecontext","false") \
    .mode("append") \
    .save()