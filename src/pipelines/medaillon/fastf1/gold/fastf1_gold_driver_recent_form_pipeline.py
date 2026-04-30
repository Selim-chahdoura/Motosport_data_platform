from pathlib import Path

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.window import Window
from pyspark.sql.functions import avg, sum, col


class FastF1GoldDriverRecentFormPipeline:
    def __init__(
        self,
        spark: SparkSession,
        season: int,
        gold_base_dir: str = "data_lake/gold/fastf1",
    ):
        self.spark = spark
        self.season = season
        self.input_path = Path(gold_base_dir) / f"season_{season}" / "gold_driver_race_summary"
        self.output_path = Path(gold_base_dir) / f"season_{season}" / "gold_driver_recent_form"

    def read_data(self) -> DataFrame:
        print(f"Reading Driver Race Summary from: {self.input_path}")
        return self.spark.read.parquet(str(self.input_path))

    def build_recent_form(self, df: DataFrame) -> DataFrame:
        required_columns = [
            "driver",
            "race_date",
            "finish_position",
            "position_gain",
            "finished_top_10",
            "finished_top_3",
            "dnf_flag",
        ]

        for column_name in required_columns:
            if column_name not in df.columns:
                raise ValueError(f"Missing required column: {column_name}")

        last_3_races = (
            Window.partitionBy("driver")
            .orderBy("race_date")
            .rowsBetween(-3, -1)
        )

        last_5_races = (
            Window.partitionBy("driver")
            .orderBy("race_date")
            .rowsBetween(-5, -1)
        )

        recent_form = (
            df.withColumn("avg_finish_last_3", avg("finish_position").over(last_3_races))
            .withColumn("avg_finish_last_5", avg("finish_position").over(last_5_races))
            .withColumn("avg_position_gain_last_5", avg("position_gain").over(last_5_races))
            .withColumn("top_10_count_last_5", sum("finished_top_10").over(last_5_races))
            .withColumn("podium_count_last_5", sum("finished_top_3").over(last_5_races))
            .withColumn("dnf_count_last_5", sum("dnf_flag").over(last_5_races))
        )

        return recent_form

    def save_data(self, df: DataFrame) -> None:
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        df.write.mode("overwrite").parquet(str(self.output_path))

    def run(self) -> None:
        df = self.read_data()
        gold_df = self.build_recent_form(df)
        self.save_data(gold_df)

        gold_df.show(5, truncate=False)
        print(f"Saved Gold table: {self.output_path}")


def main():
    spark = (
        SparkSession.builder
        .appName("FastF1 Gold Driver Recent Form")
        .getOrCreate()
    )

    seasons = list(range(2018, 2026))

    for season in seasons:
        print(f"\n===== DRIVER RECENT FORM SEASON {season} =====")

        try:
            pipeline = FastF1GoldDriverRecentFormPipeline(spark, season=season)
            pipeline.run()
        except Exception as e:
            print(f"Failed Driver Recent Form pipeline for season {season}: {e}")

    spark.stop()


if __name__ == "__main__":
    main()