from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main():
    spark = SparkSession.builder.appName("delivery-streaming-demo").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    stream = (
        spark.readStream.format("rate")
        .option("rowsPerSecond", 10)
        .load()
    )

    agg = (
        stream
        .withWatermark("timestamp", "10 seconds")
        .groupBy(F.window("timestamp", "5 seconds"))
        .agg(F.count("*").alias("events"),
             F.avg("value").alias("avg_value"))
    )

    query = (
        agg.writeStream
        .outputMode("update")
        .format("console")
        .option("truncate", False)
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()