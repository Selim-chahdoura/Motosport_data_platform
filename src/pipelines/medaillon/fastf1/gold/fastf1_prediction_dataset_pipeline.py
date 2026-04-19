from pyspark.sql import SparkSession, DataFrame


class FastF1GoldPredictionDatasetPipeline:
    def __init__(self, spark: SparkSession, season: int):
        self.spark = spark
        self.season = season

        self.driver_base_path = f"data_lake/gold/fastf1/season_{season}/gold_driver_race_summary"
        self.driver_form_path = f"data_lake/gold/fastf1/season_{season}/gold_driver_recent_form"
        self.constructor_form_path = f"data_lake/gold/fastf1/season_{season}/gold_constructor_recent_form"

        self.output_path = f"data_lake/gold/fastf1/season_{season}/gold_prediction_dataset"

    def read_data(self):
        driver_df = self.spark.read.parquet(self.driver_base_path)
        driver_form_df = self.spark.read.parquet(self.driver_form_path)
        constructor_form_df = self.spark.read.parquet(self.constructor_form_path)

        return driver_df, driver_form_df, constructor_form_df

    def build_dataset(self, driver_df: DataFrame,
                      driver_form_df: DataFrame,
                      constructor_form_df: DataFrame) -> DataFrame:

        # join driver recent form
        df = driver_df.join(
            driver_form_df.select(
                "season", "race", "driver",
                "avg_finish_last_3",
                "avg_finish_last_5",
                "avg_position_gain_last_5",
                "top_10_count_last_5",
                "podium_count_last_5",
                "dnf_count_last_5"
            ),
            on=["season", "race", "driver"],
            how="left"
        )

        # join constructor recent form
        df = df.join(
            constructor_form_df.select(
                "season", "race", "team_name",
                "avg_finish_last_3",
                "avg_finish_last_5",
                "avg_position_gain_last_5",
                "top_10_count_last_5",
                "podium_count_last_5",
                "dnf_count_last_5"
            ),
            on=["season", "race", "team_name"],
            how="left"
        )

        # rename constructor columns to avoid confusion
        df = df.withColumnRenamed("avg_finish_last_3", "constructor_avg_finish_last_3") \
               .withColumnRenamed("avg_finish_last_5", "constructor_avg_finish_last_5") \
               .withColumnRenamed("avg_position_gain_last_5", "constructor_avg_position_gain_last_5") \
               .withColumnRenamed("top_10_count_last_5", "constructor_top_10_count_last_5") \
               .withColumnRenamed("podium_count_last_5", "constructor_podium_count_last_5") \
               .withColumnRenamed("dnf_count_last_5", "constructor_dnf_count_last_5")

        # select only ML-safe columns
        final_df = df.select(
            "season",
            "race",
            "race_date",
            "driver",
            "team_name",
            "grid_position",

            # driver features
            "avg_finish_last_3",
            "avg_finish_last_5",
            "avg_position_gain_last_5",
            "top_10_count_last_5",
            "podium_count_last_5",
            "dnf_count_last_5",

            # constructor features
            "constructor_avg_finish_last_3",
            "constructor_avg_finish_last_5",
            "constructor_avg_position_gain_last_5",
            "constructor_top_10_count_last_5",
            "constructor_podium_count_last_5",
            "constructor_dnf_count_last_5",

            # target
            "finished_top_10"
        )

        return final_df

    def save_data(self, df: DataFrame):
        df.write.mode("overwrite").parquet(self.output_path)

    def run(self):
        driver_df, driver_form_df, constructor_form_df = self.read_data()
        final_df = self.build_dataset(driver_df, driver_form_df, constructor_form_df)
        self.save_data(final_df)

        print(f"Saved Gold table: {self.output_path}")


if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("FastF1 Gold Prediction Dataset") \
        .getOrCreate()

    pipeline = FastF1GoldPredictionDatasetPipeline(spark, season=2023)
    pipeline.run()

    spark.stop()