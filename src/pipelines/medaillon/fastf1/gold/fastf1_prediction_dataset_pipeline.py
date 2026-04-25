from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F


class FastF1GoldPredictionDatasetPipeline:
    def __init__(
        self,
        spark: SparkSession,
        season: int,
        gold_base_dir: str = "abfss://gold@motorsportdatalake.dfs.core.windows.net/fastf1",
    ):
        self.spark = spark
        self.season = season

        base = gold_base_dir.rstrip("/")
        self.driver_base_path = f"{base}/season_{season}/gold_driver_race_summary"
        self.driver_form_path = f"{base}/season_{season}/gold_driver_recent_form"
        self.constructor_form_path = f"{base}/season_{season}/gold_constructor_recent_form"
        self.output_path = f"{base}/season_{season}/gold_prediction_dataset"

    def read_data(self):
        driver_df = self.spark.read.format("delta").load(self.driver_base_path)
        driver_form_df = self.spark.read.format("delta").load(self.driver_form_path)
        constructor_form_df = self.spark.read.format("delta").load(self.constructor_form_path)
        return driver_df, driver_form_df, constructor_form_df

    def build_dataset(
        self,
        driver_df: DataFrame,
        driver_form_df: DataFrame,
        constructor_form_df: DataFrame,
    ) -> DataFrame:

        driver_form_features = driver_form_df.select(
            "season", "race", "driver",
            "avg_finish_last_3",
            "avg_finish_last_5",
            "avg_position_gain_last_5",
            "top_10_count_last_5",
            "podium_count_last_5",
            "dnf_count_last_5"
        )

        constructor_form_features = constructor_form_df.select(
            "season", "race", "team_name",
            F.col("avg_finish_last_3").alias("constructor_avg_finish_last_3"),
            F.col("avg_finish_last_5").alias("constructor_avg_finish_last_5"),
            F.col("avg_position_gain_last_5").alias("constructor_avg_position_gain_last_5"),
            F.col("top_10_count_last_5").alias("constructor_top_10_count_last_5"),
            F.col("podium_count_last_5").alias("constructor_podium_count_last_5"),
            F.col("dnf_count_last_5").alias("constructor_dnf_count_last_5"),
        )

        df = driver_df.join(
            driver_form_features,
            on=["season", "race", "driver"],
            how="left"
        )

        df = df.join(
            constructor_form_features,
            on=["season", "race", "team_name"],
            how="left"
        )

        return df.select(
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
            "finished_top_10"
        )

    def save_data(self, df: DataFrame):
        df.write.format("delta").mode("overwrite").save(self.output_path)

    def run(self):
        driver_df, driver_form_df, constructor_form_df = self.read_data()
        final_df = self.build_dataset(driver_df, driver_form_df, constructor_form_df)
        self.save_data(final_df)
        print(f"Saved Gold table: {self.output_path}")