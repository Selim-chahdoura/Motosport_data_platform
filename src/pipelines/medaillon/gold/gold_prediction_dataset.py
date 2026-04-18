from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("gold_prediction_dataset").getOrCreate()

race_df = spark.read.parquet("data_lake/gold/results_with_features")
driver_df = spark.read.parquet("data_lake/gold/driver_stats")
constructor_df = spark.read.parquet("data_lake/gold/constructor_stats")

df = race_df.join(driver_df, "code", "left")
df = df.join(constructor_df, "constructor_id", "left")

df.printSchema()
df.show(10)

df.write.mode("overwrite").parquet("data_lake/gold/prediction_dataset")