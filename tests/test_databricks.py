from pyspark.sql import SparkSession

spark = SparkSession.getActiveSession()

df = spark.createDataFrame(
    [("VER", 1), ("HAM", 2)],
    ["driver", "position"]
)

df.show()
print("Databricks run works")