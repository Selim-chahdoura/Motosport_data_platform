from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import to_date

spark = SparkSession.builder.appName("silver_results").getOrCreate()

df = spark.read.parquet("data_lake/bronze/results_all")

df.printSchema()

# Cast columns to appropriate data types
df = df.withColumn("season", col("season").cast("int")) \
       .withColumn("round", col("round").cast("int")) \
       .withColumn("grid", col("grid").cast("int")) \
       .withColumn("position", col("position").cast("int")) \
       .withColumn("points", col("points").cast("double")) \
       .withColumn("laps", col("laps").cast("int"))

df = df.withColumn("date", to_date(col("date")))

# Rename columns to follow snake_case convention
df = df.withColumnRenamed("raceName", "race_name") \
       .withColumnRenamed("circuitId", "circuit_id") \
       .withColumnRenamed("circuitName", "circuit_name") \
       .withColumnRenamed("driverId", "driver_id") \
       .withColumnRenamed("givenName", "given_name") \
       .withColumnRenamed("familyName", "family_name") \
       .withColumnRenamed("constructorId", "constructor_id")

df.printSchema()    
print("df length before cleaning: ", df.count())

# Remove Duplicates
df = df.dropDuplicates()
print(f"Total rows after cleaning: {df.count()}")
df.show(10)

# Save df to silver layer
df.write.mode("overwrite").parquet("data_lake/silver/results_cleaned")