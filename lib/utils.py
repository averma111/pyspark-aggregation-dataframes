from pyspark import SparkConf
import configparser
import pyspark.sql.functions


def get_spark_app_config():
    spark_conf = SparkConf()
    config = configparser.ConfigParser()
    config.read("config/spark.conf")
    for (key, val) in config.items("SPARK_APP_CONFIGS"):
        spark_conf.set(key, val)
    return spark_conf


def load_invoice_df(spark):
    return spark.read \
        .format("csv") \
        .option("inferSchema", "true") \
        .option("header", "true") \
        .load("data/invoices.csv")


def load_parquet_data(spark):
    return spark.read \
        .format("parquet") \
        .load("target/parquet/part-*")
