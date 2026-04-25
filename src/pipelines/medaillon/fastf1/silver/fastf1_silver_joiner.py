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
