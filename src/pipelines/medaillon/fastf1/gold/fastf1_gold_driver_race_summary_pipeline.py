from pathlib import Path

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import avg, min, count, sum, when, col, first, lit


class FastF1GoldDriverRaceSummaryPipeline:
    def __init__(
        self,
        spark: SparkSession,
        season: int,
        silver_base_dir: str = "data_lake/silver/fastf1",
        gold_base_dir: str = "data_lake/gold/fastf1",
    ):
        self.spark = spark
        self.season = season
        self.input_path = Path(silver_base_dir) / f"season_{season}" / "silver_fastf1_laps"
        self.output_path = Path(gold_base_dir) / f"season_{season}" / "gold_driver_race_summary"

    def read_data(self) -> DataFrame:
        print(f"Reading Silver data from: {self.input_path}")
        return self.spark.read.parquet(str(self.input_path))

    def build_driver_race_summary(self, df: DataFrame) -> DataFrame:
        optional_columns = {
            "grid_position": None,
            "finish_position": None,
            "race_status": None,
            "team_name": None,
        }

        for column_name, default_value in optional_columns.items():
            if column_name not in df.columns:
                df = df.withColumn(column_name, lit(default_value))

        summary = df.groupBy(
            "season",
            "race",
            "driver",
            "team_name"
        ).agg(
            min("lapstartdate").alias("race_date"),
            count("*").alias("laps_completed"),
            avg("lap_time_ms").alias("avg_lap_time_ms"),
            min("lap_time_ms").alias("best_lap_time_ms"),
            sum(when(col("is_pit_lap") == True, 1).otherwise(0)).alias("pit_stop_count"),
            first("grid_position", ignorenulls=True).alias("grid_position"),
            first("finish_position", ignorenulls=True).alias("finish_position"),
            first("race_status", ignorenulls=True).alias("race_status"),
        )

        summary = (
            summary.withColumn("position_gain", col("grid_position") - col("finish_position"))
            .withColumn("finished_top_10", when(col("finish_position") <= 10, 1).otherwise(0))
            .withColumn("finished_top_3", when(col("finish_position") <= 3, 1).otherwise(0))
            .withColumn("dnf_flag", when(col("race_status") != "Finished", 1).otherwise(0))
        )

        return summary

    def save_data(self, df: DataFrame) -> None:
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        df.write.mode("overwrite").parquet(str(self.output_path))

    def run(self) -> None:
        df = self.read_data()
        gold_df = self.build_driver_race_summary(df)
        self.save_data(gold_df)

        gold_df.show(5, truncate=False)
        print(f"Saved Gold table: {self.output_path}")


def main():
    spark = (
        SparkSession.builder
        .appName("FastF1 Gold Driver Race Summary")
        .getOrCreate()
    )

    seasons = list(range(2018, 2026))

    for season in seasons:
        print(f"\n===== DRIVER RACE SUMMARY SEASON {season} =====")

        try:
            pipeline = FastF1GoldDriverRaceSummaryPipeline(spark, season=season)
            pipeline.run()
        except Exception as e:
            print(f"Failed Driver Race Summary pipeline for season {season}: {e}")

    spark.stop()


if __name__ == "__main__":
    main()