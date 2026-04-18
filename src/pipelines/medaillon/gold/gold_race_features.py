from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, avg, count, sum  

spark = SparkSession.builder.appName("gold_race_features").getOrCreate()
df = spark.read.parquet("data_lake/silver/results_cleaned")

# Add new columns
df = df.withColumn("finished_top_10", when(col("position") <= 10, 1).otherwise(0))
df = df.withColumn("finished_top_3", when(col("position") <= 3, 1).otherwise(0))
df = df.withColumn("position_gain", col("grid") - col("position"))

df.show(10)
df.write.mode("overwrite").parquet("data_lake/gold/results_with_features")