import os
from pyspark import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


# define const variables
KAFKA_SOURCE_TOPIC = "sourceTopic"
KAFKA_SINK_TOPIC = "sinkTopic"
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS")

def test():
    # Create Spark session object
    sparkSession = SparkSession.Builder.appName("Kafka Spark streaming").master("local[*]").getOrCreate()
    # Set log level to ERROR only
    sparkSession.sparkCOntext.setLogLevel("ERROR")

    df_stream = sparkSession.readStream.format('kafka') \
                    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
                    .option("subscribe", KAFKA_SOURCE_TOPIC) \
                    .option("startingOffsets", "earliest") \
                    .load()

    df_stream.printSchema()

    values_df = df_stream.selectExpr("CAST(value as STRING)", "timestamp")

    values_df.printSchema()

    df_stream.selectExpr("CAST(key AS STRING) AS key", "CAST(value AS STRING) AS value") \
        .writeStream \
        .format("kafka") \
        .trigger(processingTime="15 seconds") \
        .outputMode("update") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("topic", KAFKA_SINK_TOPIC) \
        .option("checkpointLocation", "checkpoint") \
        .start() \
        .awaitTermination()

