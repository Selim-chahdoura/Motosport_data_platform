from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import avg, min, count, sum, when, col, first


class FastF1GoldDriverRaceSummaryPipeline:
    def __init__(
        self,
        spark: SparkSession,
        season: int,
        silver_base_dir: str = "abfss://silver@motorsportdatalake.dfs.core.windows.net/fastf1",
        gold_base_dir: str = "abfss://gold@motorsportdatalake.dfs.core.windows.net/fastf1",
    ):
        self.spark = spark
        self.season = season
        self.input_path = f"{silver_base_dir.rstrip('/')}/season_{season}"
        self.output_path = f"{gold_base_dir.rstrip('/')}/season_{season}/gold_driver_race_summary"

    def read_data(self) -> DataFrame:
        return self.spark.read.format("delta").load(self.input_path)

    def build_driver_race_summary(self, df: DataFrame) -> DataFrame:
        race_date_col = "lapstartdate" if "lapstartdate" in df.columns else "lap_start_time_ms"

        summary = df.groupBy(
            "season",
            "race",
            "driver",
            "team_name"
        ).agg(
            min(race_date_col).alias("race_date"),
            count("*").alias("laps_completed"),
            avg("lap_time_ms").alias("avg_lap_time_ms"),
            min("lap_time_ms").alias("best_lap_time_ms"),
            sum(when(col("is_pit_lap") == True, 1).otherwise(0)).alias("pit_stop_count"),
            first("grid_position", ignorenulls=True).alias("grid_position"),
            first("finish_position", ignorenulls=True).alias("finish_position"),
            first("race_status", ignorenulls=True).alias("race_status")
        )

        summary = summary.withColumn(
            "position_gain",
            col("grid_position") - col("finish_position")
        )

        summary = summary.withColumn(
            "finished_top_10",
            when(col("finish_position") <= 10, 1).otherwise(0)
        )

        summary = summary.withColumn(
            "finished_top_3",
            when(col("finish_position") <= 3, 1).otherwise(0)
        )

        summary = summary.withColumn(
            "dnf_flag",
            when(col("race_status") != "Finished", 1).otherwise(0)
        )

        return summary

    def save_data(self, df: DataFrame) -> None:
        df.write.format("delta").mode("overwrite").save(self.output_path)

    def run(self) -> None:
        df = self.read_data()
        gold_df = self.build_driver_race_summary(df)
        self.save_data(gold_df)
        print(f"Saved Gold table: {self.output_path}")