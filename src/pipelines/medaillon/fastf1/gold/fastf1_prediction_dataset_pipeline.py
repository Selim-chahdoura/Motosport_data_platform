from pathlib import Path

from pyspark.sql import SparkSession, DataFrame


class FastF1GoldPredictionDatasetPipeline:
    def __init__(
        self,
        spark: SparkSession,
        season: int,
        gold_base_dir: str = "data_lake/gold/fastf1",
    ):
        self.spark = spark
        self.season = season

        base_path = Path(gold_base_dir) / f"season_{season}"

        self.driver_base_path = base_path / "gold_driver_race_summary"
        self.driver_form_path = base_path / "gold_driver_recent_form"
        self.constructor_form_path = base_path / "gold_constructor_recent_form"

        self.output_path = base_path / "gold_prediction_dataset"

    def read_data(self):
        print(f"Reading driver summary from: {self.driver_base_path}")
        print(f"Reading driver recent form from: {self.driver_form_path}")
        print(f"Reading constructor recent form from: {self.constructor_form_path}")

        driver_df = self.spark.read.parquet(str(self.driver_base_path))
        driver_form_df = self.spark.read.parquet(str(self.driver_form_path))
        constructor_form_df = self.spark.read.parquet(str(self.constructor_form_path))

        return driver_df, driver_form_df, constructor_form_df

    def build_dataset(
        self,
        driver_df: DataFrame,
        driver_form_df: DataFrame,
        constructor_form_df: DataFrame,
    ) -> DataFrame:

        driver_form_features = driver_form_df.select(
            "season",
            "race",
            "driver",
            "avg_finish_last_3",
            "avg_finish_last_5",
            "avg_position_gain_last_5",
            "top_10_count_last_5",
            "podium_count_last_5",
            "dnf_count_last_5",
        )

        constructor_form_features = constructor_form_df.select(
            "season",
            "race",
            "team_name",
            constructor_form_df["avg_finish_last_3"].alias("constructor_avg_finish_last_3"),
            constructor_form_df["avg_finish_last_5"].alias("constructor_avg_finish_last_5"),
            constructor_form_df["avg_position_gain_last_5"].alias("constructor_avg_position_gain_last_5"),
            constructor_form_df["top_10_count_last_5"].alias("constructor_top_10_count_last_5"),
            constructor_form_df["podium_count_last_5"].alias("constructor_podium_count_last_5"),
            constructor_form_df["dnf_count_last_5"].alias("constructor_dnf_count_last_5"),
        )

        df = driver_df.join(
            driver_form_features,
            on=["season", "race", "driver"],
            how="left",
        )

        df = df.join(
            constructor_form_features,
            on=["season", "race", "team_name"],
            how="left",
        )

        final_df = df.select(
            "season",
            "race",
            "race_date",
            "driver",
            "team_name",
            "grid_position",
            "avg_finish_last_3",
            "avg_finish_last_5",
            "avg_position_gain_last_5",
            "top_10_count_last_5",
            "podium_count_last_5",
            "dnf_count_last_5",
            "constructor_avg_finish_last_3",
            "constructor_avg_finish_last_5",
            "constructor_avg_position_gain_last_5",
            "constructor_top_10_count_last_5",
            "constructor_podium_count_last_5",
            "constructor_dnf_count_last_5",
            "finished_top_10",
        )

        return final_df

    def save_data(self, df: DataFrame):
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        df.write.mode("overwrite").parquet(str(self.output_path))

    def run(self):
        driver_df, driver_form_df, constructor_form_df = self.read_data()
        final_df = self.build_dataset(driver_df, driver_form_df, constructor_form_df)
        self.save_data(final_df)

        final_df.show(5, truncate=False)
        print(f"Saved Gold table: {self.output_path}")


def main():
    spark = (
        SparkSession.builder
        .appName("FastF1 Gold Prediction Dataset")
        .getOrCreate()
    )

    seasons = list(range(2018, 2026))

    for season in seasons:
        print(f"\n===== PREDICTION DATASET SEASON {season} =====")

        try:
            pipeline = FastF1GoldPredictionDatasetPipeline(spark, season=season)
            pipeline.run()
        except Exception as e:
            print(f"Failed Prediction Dataset pipeline for season {season}: {e}")

    spark.stop()


if __name__ == "__main__":
    main()