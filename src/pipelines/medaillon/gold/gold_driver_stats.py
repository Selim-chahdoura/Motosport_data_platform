from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.sql.functions import avg, count, sum

spark = SparkSession.builder.appName("gold_layer").getOrCreate()

df = spark.read.parquet("data_lake/silver/results_cleaned")

# Add new columns for driver performance (Top 10 and podium finishes)
df = df.withColumn("finished_top_10", when(col("position") <= 10, 1).otherwise(0))
df = df.withColumn("finished_top_3", when(col("position") <= 3, 1).otherwise(0))

df = df.withColumn("position_gain", col("grid") - col("position"))

# Aggregate driver performance statistics
driver_stats = df.groupBy("code").agg(
    count("*").alias("num_races"),
    avg("position").alias("avg_position"),
    avg("points").alias("avg_points"),
    sum(when(col("position") == 1, 1).otherwise(0)).alias("wins"),
    sum("finished_top_10").alias("top10_finishes"),
    sum("finished_top_3").alias("podium_finishes"),
    avg("position_gain").alias("avg_position_gain")
)

# Rename columns to indicate they are driver statistics
for col_name in driver_stats.columns:
    if col_name != "code":
        driver_stats = driver_stats.withColumnRenamed(
            col_name,
            f"driver_{col_name}"
        )

print("Number of unique drivers: ", driver_stats.count())
print("Driver performance statistics:")
driver_stats.show(10)

driver_stats.write.mode("overwrite").parquet("data_lake/gold/driver_stats")