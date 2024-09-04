

import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

def create_spark_connection():
    s_conn = None

    try:
        s_conn = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config("spark.network.timeout", "800s") \
             .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2")\
            .getOrCreate()

        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception {e}")

    return s_conn

def connect_to_kafka(spark_conn):
    spark_df = None
    try:
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', '44.215.213.113:9092') \
            .option('subscribe', 'e-commerce') \
            .option('startingOffsets', 'earliest') \
            .load()
        logging.info("kafka dataframe created successfully")
    except Exception as e:
        logging.warning(f"kafka dataframe could not be created because: {e}")

    return spark_df

def create_selection_df_from_kafka(spark_df):
    schema = StructType([
        StructField("product_id", StringType(), False),
        StructField("product_name", StringType(), False),
        StructField("category", StringType(), False),
        StructField("discounted_price", StringType(), False),
        StructField("actual_price", StringType(), False),
        StructField("discount_percentage", StringType(), False),
        StructField("rating", StringType(), False),
        StructField("rating_count", StringType(), False),
        StructField("about_product", StringType(), False),
        StructField("user_id", StringType(), False),
        StructField("user_name", StringType(), False),
        StructField("review_id", StringType(), False),
        StructField("review_title", StringType(), False),
        StructField("review_content", StringType(), False),
        StructField("img_link", StringType(), False),
        StructField("product_link", StringType(), False)
    ])

    sel = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*")

    return sel

def write_stream_to_console(selection_df):
    try:
        query = selection_df.writeStream \
            .outputMode("append") \
            .format("console") \
            .start()

        query.awaitTermination()
        logging.info("Stream started successfully and writing to console")
    except Exception as e:
        logging.error(f"Stream could not be started due to exception: {e}")

if __name__ == "__main__":
    spark_conn = create_spark_connection()
    if spark_conn is not None:
        spark_df = connect_to_kafka(spark_conn)
        if spark_df is not None:
            selection_df = create_selection_df_from_kafka(spark_df)
            write_stream_to_console(selection_df)
