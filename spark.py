from pyspark.sql import SparkSession
import time
from pyspark.sql.types import StringType, StructField, StructType, ArrayType, LongType
from pyspark.sql.functions import from_json
from pyspark.sql.functions import explode, col

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
  .option("subscribe", "warsaw-nifi") \
  .option("includeHeaders", "false") \
  .load()
df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
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



exploded_df = json_expanded_df \
    .select("lastUpdate","vehicles") \
    .withColumn("vehicles", explode("vehicles")) 
    
flattened_df = exploded_df \
    .selectExpr("lastUpdate", 
                "vehicles.generated as generated", "vehicles.routeShortName as routeShortName", 
                "vehicles.tripId as tripId", "vehicles.headsign as headsign",
               "vehicles.vehicleCode as vehicleCode", "vehicles.vehicleService as vehicleService",
               "vehicles.vehicleId as vehicleId", "vehicles.speed as speed",
               "vehicles.direction as direction", "vehicles.delay as delay",
               "vehicles.scheduledTripStartTime as scheduledTripStartTime", "vehicles.lat as lat",
               "vehicles.lon as lon", "vehicles.gpsQuality as gpsQuality") 

spark.sparkContext.setLogLevel('WARN')

#query = df.writeStream.format("json").option("path", 'output').option("checkpointLocation","checkpoint_dir") .start()
query = flattened_df.writeStream.format("console").start()
#time.sleep(5) # sleep 10 seconds
query.awaitTermination()
