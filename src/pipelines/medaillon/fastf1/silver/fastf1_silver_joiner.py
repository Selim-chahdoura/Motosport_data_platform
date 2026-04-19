import sys
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

sys.path.append(str(Path(__file__).resolve().parents[5]))

from src.services.spark_session import get_spark_session


class FastF1SilverJoiner:
    def join_lap_weather(self, laps_df: DataFrame, lap_weather_df: DataFrame) -> DataFrame:
        weather_df = lap_weather_df.select(
            "season",
            "race",
            "driver",
            "lap_number",
            "airtemp",
            "humidity",
            "pressure",
            "rainfall",
            "tracktemp",
            "winddirection",
            "windspeed",
        )

        return laps_df.join(
            weather_df,
            on=["season", "race", "driver", "lap_number"],
            how="left",
        )

    def join_results(self, laps_df: DataFrame, results_df: DataFrame) -> DataFrame:
        results_df = results_df.select(
            "season",
            "race",
            F.col("abbreviation").alias("driver"),
            F.col("gridposition").alias("grid_position"),
            F.col("position").alias("finish_position"),
            F.col("status").alias("race_status"),
            F.col("teamname").alias("team_name"),
        ).dropDuplicates(["season", "race", "driver"])

        return laps_df.join(
            results_df,
            on=["season", "race", "driver"],
            how="left",
        )
    
def main():
    season = 2023
    race = "australian"

    spark = get_spark_session("TestJoiner")

    base_path = Path("data_lake/bronze/fastf1") / f"season_{season}" / f"race_{race}"

    laps_path = base_path / "bronze_fastf1_laps.parquet"
    lap_weather_path = base_path / "bronze_fastf1_lap_weather.parquet"
    results_path = base_path / "bronze_fastf1_results.parquet"

    # Load data
    laps_df = spark.read.parquet(str(laps_path))
    lap_weather_df = spark.read.parquet(str(lap_weather_path))
    results_df = spark.read.parquet(str(results_path))

    print("\n=== BEFORE JOIN ===")
    print("laps:", laps_df.count(), "rows")
    print("lap_weather:", lap_weather_df.count(), "rows")
    print("results:", results_df.count(), "rows")

    # Fix column name for join
    laps_df = laps_df.withColumnRenamed("lapnumber", "lap_number")

    # Join
    joiner = FastF1SilverJoiner()

    df = joiner.join_lap_weather(laps_df, lap_weather_df)
    df = joiner.join_results(df, results_df)

    print("columns of joined df:", df.columns)

if __name__ == "__main__":
    main()