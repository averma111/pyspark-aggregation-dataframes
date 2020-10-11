import sys

from pyspark.sql.functions import col

from lib.utils import get_spark_app_config, load_parquet_data
from lib.utils import load_invoice_df
from pyspark.sql import SparkSession, Window
import pyspark.sql.functions as f
from lib.logger import Log4j

if __name__ == "__main__":
    conf = get_spark_app_config()
    spark = SparkSession.builder \
        .config(conf=conf) \
        .getOrCreate()

    logger = Log4j(spark)

    logger.info("Starting the pyspark application")
    invoice_df = load_invoice_df(spark)

    invoice_df.select(f.countDistinct(col("InvoiceNo")).alias("Count_Of_Invoice"),
       f.sum(col("Quantity")).alias("Sum_Of_Quantity"),
       f.avg(col("UnitPrice")).alias("Avg_Of_UnitPrice"),
       f.count("*").alias("Total_Count"),
       ).show()

    summary_df = invoice_df \
          .groupBy(col("Country"),col("InvoiceNo")) \
          .agg(f.sum(col("Quantity")).alias("Total_Quantity"),
               f.round(f.sum(col("Quantity")*col("UnitPrice")),2).alias("InvoiceValue")
              ).show()

    weekly_summary_df = invoice_df \
       .withColumn("WeekNumber", f.weekofyear(f.to_date(f.substring(col("InvoiceDate"), 1, 10), "M-d-yyyy"))
                  ) \
       .groupBy(col("Country"), col("WeekNumber")) \
      .agg(
      f.countDistinct(col("InvoiceNo")).alias("NumInvoices"),
       f.sum(col("Quantity")).alias("TotalQuantity"),
       f.round(f.sum(col("Quantity") * col("UnitPrice")), 2).alias("InvoiceValue")
    ).write.save("target/parquet", format="parquet", SaveMode="Overwrite")

    parquet_df = load_parquet_data(spark)

    running_window = Window.partitionBy(col("Country")) \
        .orderBy(col("WeekNumber")) \
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    summary_window_df = parquet_df.withColumn("RunningTotal", f.sum(col("InvoiceValue")) \
                                              .over(running_window)) \
        .show()
    logger.info("Completing the pyspark application")
