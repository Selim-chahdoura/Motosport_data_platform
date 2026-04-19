from pyspark.sql import SparkSession

# -----------------------------
# 1. Start Spark
# -----------------------------
spark = SparkSession.builder \
    .appName("FastF1 Debug") \
    .getOrCreate()

# -----------------------------
# 2. Read your Silver table
# -----------------------------
season = 2023

path = "data_lake/silver/fastf1/season_2023/silver_fastf1_laps.parquet"

df = spark.read.parquet(path)

# -----------------------------
# 3. BASIC OVERVIEW
# -----------------------------
print("\n===== SCHEMA =====")
df.printSchema()

print("\n===== COLUMNS =====")
print(df.columns)

print("\n===== ROW COUNT =====")
print(df.count())

# -----------------------------
# 4. SHOW DATA
# -----------------------------
print("\n===== SAMPLE DATA =====")
df.show(20, truncate=False)

# -----------------------------
# 5. DESCRIBE NUMERIC COLUMNS
# -----------------------------
print("\n===== DESCRIBE =====")
df.describe().show()

# -----------------------------
# 6. NULL CHECK
# -----------------------------
from pyspark.sql.functions import col, sum

print("\n===== NULL COUNTS =====")
null_df = df.select([
    sum(col(c).isNull().cast("int")).alias(c)
    for c in df.columns
])
null_df.show(truncate=False)

# -----------------------------
# 7. DISTINCT VALUES (KEY COLS)
# -----------------------------
print("\n===== DISTINCT DRIVERS =====")
df.select("Driver").distinct().show(50, truncate=False)

print("\n===== DISTINCT TEAMS =====")
df.select("Team").distinct().show(50, truncate=False)

print("\n===== DISTINCT RACES =====")
df.select("race_name").distinct().show(50, truncate=False)

# -----------------------------
# 8. CHECK IMPORTANT COLUMNS
# -----------------------------
important_cols = [
    "LapTimeSeconds",
    "GridPosition",
    "FinishPosition",
    "Points",
    "Status"
]

print("\n===== IMPORTANT COLUMNS PREVIEW =====")
df.select(important_cols).show(20, truncate=False)

# -----------------------------
# 9. QUICK AGG CHECK
# -----------------------------
from pyspark.sql.functions import avg, min, max

print("\n===== QUICK STATS =====")
df.select(
    avg("LapTimeSeconds").alias("avg_lap_time"),
    min("LapTimeSeconds").alias("min_lap_time"),
    max("LapTimeSeconds").alias("max_lap_time")
).show()

# -----------------------------
# DONE
# -----------------------------
print("\nDebug finished ✅")