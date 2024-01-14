from pyspark.sql import SparkSession
from datetime import datetime, timedelta
from google.cloud import storage
from pyspark.sql.functions import explode, col, current_timestamp, expr, date_format, from_utc_timestamp, dayofmonth, from_unixtime,unix_timestamp,to_utc_timestamp, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, FloatType, ArrayType
import io

spark = SparkSession \
    .builder \
    .appName("Batch job upload for weather") \
    .master("yarn") \
    .getOrCreate()


def get_timestamp_and_decompose(spark):
    two_hours_ago_timestamp = spark.range(1).select(
        expr("current_date() - interval 1 day").alias("one_day_ago_timestamp")
    )
    decomposed_timestamp = two_hours_ago_timestamp.select(
        date_format("one_day_ago_timestamp", "yyyy-MM-dd").alias("day"),
        date_format("one_day_ago_timestamp", "HH").alias("hour")
    ).collect()[0]
    return decomposed_timestamp["day"], decomposed_timestamp["hour"]

date, hour = get_timestamp_and_decompose(spark)


client = storage.Client()
bucket_name="bigdata-chmurki-nifi"

weather = None

    
for blob in client.list_blobs(bucket_name, prefix='weather'):
    if blob.name.count(f'Act_weather_{date}') == 1:
        print(f'Processing file: {blob.name}')
        try:
            parquet_file_path =f'gs://{bucket_name}/{blob.name}'
            df = spark.read.parquet(parquet_file_path)
            df = df.select(
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
            spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
            df = df.withColumn("timestamp", to_timestamp('time', 'EEE MMM d HH:mm:ss z yyyy'))
            df = df.withColumn("timestamp_utc", to_utc_timestamp(col("timestamp"), "UTC"))
            df = df.withColumn('day',dayofmonth(col("timestamp_utc")))
            df = df.select(
                'timestamp_utc',
                'day',
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
            if weather is not None:
                weather = weather.union(df)
            else:
                weather = df
            print('Success')
        except:
            print(f'File {parquet_file_path} was not properly processed.')
            continue
    
catalog = ''.join("""{
  "table": {
    "namespace": "default",
    "name": "weather"
  },
  "rowkey": "timestamp_utc",
  "columns": {
    "timestamp_utc": {"cf": "rowkey", "col": "timestamp_utc", "type": "timestamp"},
    "day": {"cf": "weather_cols", "col": "day", "type": "string"},
    "lat": {"cf": "weather_cols", "col": "lat", "type": "string"},
    "lon": {"cf": "weather_cols", "col": "lon", "type": "string"},
    "elevation": {"cf": "weather_cols", "col": "elevation", "type": "string"},
    "timezone": {"cf": "weather_cols", "col": "timezone", "type": "string"},
    "units": {"cf": "weather_cols", "col": "units", "type": "string"},
    "icon": {"cf": "weather_cols", "col": "icon", "type": "string"},
    "summary": {"cf": "weather_cols", "col": "summary", "type": "string"},
    "temperature": {"cf": "weather_cols", "col": "temperature", "type": "string"},
    "speed": {"cf": "weather_cols", "col": "speed", "type": "string"},
    "angle": {"cf": "weather_cols", "col": "angle", "type": "string"},
    "dir": {"cf": "weather_cols", "col": "dir", "type": "string"},
    "precipitation_total": {"cf": "weather_cols", "col": "precipitation_total", "type": "string"},
    "type": {"cf": "weather_cols", "col": "type", "type": "string"},
    "cloud_cover": {"cf": "weather_cols", "col": "cloud_cover", "type": "string"},
    "daily": {"cf": "weather_cols", "col": "daily", "type": "string"}
  }
}""".split())

weather.write \
    .format('org.apache.hadoop.hbase.spark') \
    .options(catalog=catalog) \
    .option("hbase.spark.use.hbasecontext","false") \
    .mode("append") \
    .save()