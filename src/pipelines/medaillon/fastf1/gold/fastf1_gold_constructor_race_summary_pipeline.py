from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import avg, min, sum, count, first


class FastF1GoldConstructorRaceSummaryPipeline:
    def __init__(self, spark: SparkSession, season: int):
        self.spark = spark
        self.season = season
        self.input_path = f"data_lake/gold/fastf1/season_{season}/gold_driver_race_summary"
        self.output_path = f"data_lake/gold/fastf1/season_{season}/gold_constructor_race_summary"

    def read_data(self) -> DataFrame:
        return self.spark.read.parquet(self.input_path)

    def build_constructor_race_summary(self, df: DataFrame) -> DataFrame:
        summary = df.groupBy(
            "season",
            "race",
            "team_name"
        ).agg(
            first("race_date", ignorenulls=True).alias("race_date"),
            count("*").alias("num_drivers"),
            avg("finish_position").alias("avg_finish_position"),
            min("finish_position").alias("best_finish_position"),
            sum("position_gain").alias("total_position_gain"),
            avg("avg_lap_time_ms").alias("avg_lap_time_ms"),
            min("best_lap_time_ms").alias("best_lap_time_ms"),
            sum("pit_stop_count").alias("total_pit_stops"),
            sum("finished_top_10").alias("top_10_finishes"),
            sum("finished_top_3").alias("podium_finishes"),
            sum("dnf_flag").alias("dnf_count")
        )

        return summary

    def save_data(self, df: DataFrame) -> None:
        df.write.mode("overwrite").parquet(self.output_path)

    def run(self) -> None:
        df = self.read_data()
        gold_df = self.build_constructor_race_summary(df)
        self.save_data(gold_df)
        print(gold_df.show(5))
        print(f"Saved Gold table: {self.output_path}")


if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("FastF1 Gold Constructor Race Summary") \
        .getOrCreate()

    pipeline = FastF1GoldConstructorRaceSummaryPipeline(spark, season=2023)
    pipeline.run()

    spark.stop()