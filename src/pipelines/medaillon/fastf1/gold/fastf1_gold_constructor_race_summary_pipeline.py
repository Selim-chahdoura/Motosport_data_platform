from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import avg, min, sum, count, first


class FastF1GoldConstructorRaceSummaryPipeline:
    def __init__(
        self,
        spark: SparkSession,
        season: int,
        gold_base_dir: str = "abfss://gold@motorsportdatalake.dfs.core.windows.net/fastf1",
    ):
        self.spark = spark
        self.season = season
        base = gold_base_dir.rstrip("/")
        self.input_path = f"{base}/season_{season}/gold_driver_race_summary"
        self.output_path = f"{base}/season_{season}/gold_constructor_race_summary"

    def read_data(self) -> DataFrame:
        return self.spark.read.format("delta").load(self.input_path)

    def build_constructor_race_summary(self, df: DataFrame) -> DataFrame:
        return df.groupBy(
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

    def save_data(self, df: DataFrame) -> None:
        df.write.format("delta").mode("overwrite").save(self.output_path)

    def run(self) -> None:
        df = self.read_data()
        gold_df = self.build_constructor_race_summary(df)
        self.save_data(gold_df)
        print(f"Saved Gold table: {self.output_path}")