from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("union_seasons_results") \
    .getOrCreate()

seasons = [2021, 2022, 2023]

dfs = []

for season in seasons:
    path = f"data_lake/bronze/results_{season}"
    df = spark.read.parquet(path)
    dfs.append(df)

df_all = dfs[0]

for df in dfs[1:]:
    df_all = df_all.union(df)

df_all.show(10)


df_all.write.mode("overwrite").parquet("data_lake/bronze/results_all")

print(f"Total rows (Driver positions): {df_all.count()}")
print("All seasons combined!")