#!/usr/bin/python

# Demo - Input Sources - Kafka
#
# This demo will count the records (json objects) placed in a topic test_topic by a 
# Kafka Producer(kafka-console-producer.sh)
# 
#


# Import Libraries
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *

# Create Spark Session
spark = SparkSession\
.builder\
.master("local")\
.appName("Kafka Source")\
.getOrCreate()

# Get the logger and set the log level
spark.sparkContext.setLogLevel("WARN")

# Define Schema
from pyspark.sql.types import *
housing_schema = StructType([
    StructField('longitude', DoubleType(), True),
    StructField('latitude', DoubleType(), True),
    StructField('housing_median_age', DoubleType(), True),
    StructField('total_rooms', DoubleType(), True),
    StructField('total_bedrooms', DoubleType(), True),
    StructField('population', DoubleType(), True),
    StructField('households', DoubleType(), True),
    StructField('median_income', DoubleType(), True),
    StructField('median_house_value', DoubleType(), True),
    StructField('ocean_proximity', StringType(), True),
])
# Create Streaming DataFrame by reading data from File Source.
initDF = spark\
.readStream\
.format("kafka")\
.option("kafka.bootstrap.servers", "127.0.0.1:9092")\
.option("subscribe", "tips_topic")\
.option("startingOffsets", "earliest")\
.load()

json_stream = initDF\
.select(F.from_json(initDF["value"].cast(StringType()), tips_schema).alias("data"))
json_stream = json_stream.select("data.longitude", "data.latitude", "data.housing_median_age", "data.total_rooms",\
"data.total_bedrooms", "data.population",\ "data.households","data.median_income","median_house_value","ocean_proximity").groupBy().count()

# Output to Console
# Try "update" and "complete" mode.
json_stream\
.writeStream\
.outputMode("complete")\
.option("truncate", False)\
.option("numRows", 3)\
.format("console")\
.start()\
.awaitTermination()



