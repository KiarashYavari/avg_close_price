# scenario
# You have CSV files containing daily stock prices
# with columns: Date, Ticker, Open, High, Low, Close, and Volume.
# You want to compute the daily average closing price for each stock.

# PySpark is the python API for Apache Spark
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg

# A Spark session is the entry point to programming Spark with the Dataset and
# DataFrame API. It allows you to create DataFrames, register DataFrames as tables,
# execute SQL over tables, cache tables, and read data from various sources.

# create spark session
spark = SparkSession.builder \
    .appName("FinanceBatchProcessingExample") \
    .getOrCreate()

try:
    # Read stock data from CSV files
    df = spark.read.csv("stock_data.csv", header=True, inferSchema=True)
    df.show(5)

    # calculate the avg closing price
    daily_avg_close = df.groupBy("Date", "Ticker") \
        .agg(avg("Close").alias("Average_Close"))
    daily_avg_close.show(5)

    # save results
    daily_avg_close.write.csv("daily_avg_close_output", header=True)
    
finally: # execute it here to gaurantee it will execute even if an error happens
    # stop spark
    # topping the Spark session releases the resources (such as memory and CPU) allocated by Spark for the application. This includes freeing up memory on the driver and workers,
    # closing open files,
    # and shutting down any active connections.
    spark.stop()
