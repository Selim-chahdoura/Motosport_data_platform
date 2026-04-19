from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import avg, min, sum, count
from pyspark.sql.window import Window


class FastF1GoldConstructorRecentFormPipeline:
    def __init__(self, spark: SparkSession, season: int):
        self.spark = spark
        self.season = season
        self.input_path = f"data_lake/gold/fastf1/season_{season}/gold_constructor_race_summary"
        self.output_path = f"data_lake/gold/fastf1/season_{season}/gold_constructor_recent_form"

    def read_data(self) -> DataFrame:
        return self.spark.read.parquet(self.input_path)

    def add_recent_form_features(self, df: DataFrame) -> DataFrame:
        last_3_races = Window.partitionBy("team_name").orderBy("race_date").rowsBetween(-3, -1)
        last_5_races = Window.partitionBy("team_name").orderBy("race_date").rowsBetween(-5, -1)

        df = df.withColumn(
            "avg_finish_last_3",
            avg("avg_finish_position").over(last_3_races)
        ).withColumn(
            "avg_finish_last_5",
            avg("avg_finish_position").over(last_5_races)
        ).withColumn(
            "avg_position_gain_last_5",
            avg("total_position_gain").over(last_5_races)
        ).withColumn(
            "top_10_count_last_5",
            sum("top_10_finishes").over(last_5_races)
        ).withColumn(
            "podium_count_last_5",
            sum("podium_finishes").over(last_5_races)
        ).withColumn(
            "dnf_count_last_5",
            sum("dnf_count").over(last_5_races)
        )

        return df

    def save_data(self, df: DataFrame) -> None:
        df.write.mode("overwrite").parquet(self.output_path)

    def run(self) -> None:
        df = self.read_data()
        gold_df = self.add_recent_form_features(df)
        self.save_data(gold_df)
        print(gold_df.show(5))
        print(f"Saved Gold table: {self.output_path}")


if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("FastF1 Gold Constructor Recent Form") \
        .getOrCreate()

    pipeline = FastF1GoldConstructorRecentFormPipeline(spark, season=2023)
    pipeline.run()

    spark.stop()