from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, from_unixtime, col,date_format,to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, FloatType,LongType
from pyspark.sql.streaming import *

# Create a Spark session
spark = SparkSession.builder \
    .appName("weather") \
    .getOrCreate()

# Define the schema for the JSON data
sf_options = {
    "sfURL": "coa02856.east-us-2.azure.snowflakecomputing.com",
    "sfUser": "project",
    "sfPassword": "Project12345@",
    "sfDatabase": "weather",
    "sfSchema": "public",
}



json_schema = StructType([
    StructField("weather", StructType([
        StructField("longitude", FloatType()),
        StructField("latitude", FloatType()),
        StructField("temperature", FloatType()),
        StructField("feels_like", FloatType()),
        StructField("pressure", FloatType()),
        StructField("humidity", FloatType()),
        StructField("rain_1h", FloatType()),
        StructField("clouds", FloatType()),
        StructField("snow", FloatType()),

    ])),
    StructField("date", StringType())
])
snowflake_column_order = ["id", "year", "month", "day", "hour", "minute", "longitude", "latitude", "temperature", "feels_like", "pressure", "humidity", "rain_1h", "clouds", "snow"]

# Read data from Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("checkpointLocation", "/opt/bitnami/spark/checkpoints") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "weather_topic_2") \
     .option("startingOffsets", "earliest") \
    .load()



df = df.withColumn("value", col("value").cast("string"))

# Parse the JSON data
df = df.select(from_json(col("value"), json_schema).alias("data"))



# Flatten the nested structure
df = df.select("data.weather.*", "data.date")

df_date = df.select("date")

df_extracted = df_date.withColumn("year", date_format(to_timestamp("date", "yyyy-MM-dd HH:mm:ss"), "yyyy")) \
                     .withColumn("month", date_format(to_timestamp("date", "yyyy-MM-dd HH:mm:ss"), "MM")) \
                     .withColumn("day", date_format(to_timestamp("date", "yyyy-MM-dd HH:mm:ss"), "dd")) \
                     .withColumn("hour", date_format(to_timestamp("date", "yyyy-MM-dd HH:mm:ss"), "HH")) \
                     .withColumn("minute", date_format(to_timestamp("date", "yyyy-MM-dd HH:mm:ss"), "mm"))

joined_df = df.join(df_extracted, "date", "inner")
joined_df = joined_df.withColumnRenamed("date", "id")
joined_df = joined_df.select(snowflake_column_order)
# date, longitude, latitude, temperature, feels_like,pressure, humidity, rain_1h, clouds,snow, year , month , day , hour , minute

# Write to Snowflake
joined_df.writeStream \
    .foreachBatch(
        # you can see the transformation code up , i basicaly want to do that speceficaly to the data stream ds
        lambda ds , dt: ds.write
                   .format("net.snowflake.spark.snowflake")
                   .options(**sf_options)
                   .option("dbtable", "weather_table")
                   .mode("append")
                   .save()) \
    .start() \
    .awaitTermination()





