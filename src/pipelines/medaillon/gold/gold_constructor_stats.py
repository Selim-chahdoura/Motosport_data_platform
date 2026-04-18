from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, avg, count, sum

spark = SparkSession.builder.appName("gold_constructor_stats").getOrCreate()
df = spark.read.parquet("data_lake/silver/results_cleaned")

# Add new columns for constructor performance (Top 10 and podium finishes, position gain)
df = df.withColumn("finished_top_10", when(col("position") <= 10, 1).otherwise(0))
df = df.withColumn("finished_top_3", when(col("position") <= 3, 1).otherwise(0))
df = df.withColumn("position_gain", col("grid") - col("position"))

# Aggregate constructor performance statistics
constructor_stats = df.groupBy("constructor_id").agg(
    count("*").alias("num_races"),
    avg("position").alias("avg_position"),
    avg("points").alias("avg_points"),
    sum("finished_top_10").alias("wins"),
    sum("finished_top_10").alias("top10_finishes"),
    sum("finished_top_3").alias("podium_finishes"),
    avg("position_gain").alias("avg_position_gain")
)

# Rename columns to indicate they are constructor statistics
for col_name in constructor_stats.columns:
    if col_name != "constructor_id":
        constructor_stats = constructor_stats.withColumnRenamed(
            col_name,
            f"constructor_{col_name}"
        )
print("Number of unique constructors: ", constructor_stats.count())
print("Constructor performance statistics:")
constructor_stats.show(10)

constructor_stats.write.mode("overwrite").parquet("data_lake/gold/constructor_stats")
