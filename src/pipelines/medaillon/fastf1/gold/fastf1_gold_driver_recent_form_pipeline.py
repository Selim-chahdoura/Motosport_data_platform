from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.window import Window
from pyspark.sql.functions import avg, sum


class FastF1GoldDriverRecentFormPipeline:
    def __init__(self, spark: SparkSession, season: int):
        self.spark = spark
        self.season = season
        self.input_path = f"data_lake/gold/fastf1/season_{season}/gold_driver_race_summary"
        self.output_path = f"data_lake/gold/fastf1/season_{season}/gold_driver_recent_form"

    def read_data(self) -> DataFrame:
        return self.spark.read.parquet(self.input_path)

    def build_recent_form(self, df: DataFrame) -> DataFrame:
        last_3_races = Window.partitionBy("driver").orderBy("race_date").rowsBetween(-3, -1)
        last_5_races = Window.partitionBy("driver").orderBy("race_date").rowsBetween(-5, -1)

        recent_form = df.withColumn(
            "avg_finish_last_3",
            avg("finish_position").over(last_3_races)
        ).withColumn(
            "avg_finish_last_5",
            avg("finish_position").over(last_5_races)
        ).withColumn(
            "avg_position_gain_last_5",
            avg("position_gain").over(last_5_races)
        ).withColumn(
            "top_10_count_last_5",
            sum("finished_top_10").over(last_5_races)
        ).withColumn(
            "podium_count_last_5",
            sum("finished_top_3").over(last_5_races)
        ).withColumn(
            "dnf_count_last_5",
            sum("dnf_flag").over(last_5_races)
        )

        return recent_form

    def save_data(self, df: DataFrame) -> None:
        df.write.mode("overwrite").parquet(self.output_path)

    def run(self) -> None:
        df = self.read_data()
        gold_df = self.build_recent_form(df)
        self.save_data(gold_df)
        print(gold_df.show(5))
        print(f"Saved Gold table: {self.output_path}")


if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("FastF1 Gold Driver Recent Form") \
        .getOrCreate()

    pipeline = FastF1GoldDriverRecentFormPipeline(spark, season=2023)
    pipeline.run()

    spark.stop()