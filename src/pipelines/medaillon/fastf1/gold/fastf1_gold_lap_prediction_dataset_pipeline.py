from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.window import Window
from pyspark.sql.functions import col, lag, avg


class FastF1GoldLapPredictionDatasetPipeline:
    def __init__(self, spark: SparkSession, season: int):
        self.spark = spark
        self.season = season
        self.input_path = f"data_lake/silver/fastf1/season_{season}/silver_fastf1_laps.parquet"
        self.output_path = f"data_lake/gold/fastf1/season_{season}/gold_lap_prediction_dataset"

    def read_data(self) -> DataFrame:
        return self.spark.read.parquet(self.input_path)

    def build_dataset(self, df: DataFrame) -> DataFrame:
        lap_window = Window.partitionBy("season", "race", "driver").orderBy("lap_number")
        last_3_window = Window.partitionBy("season", "race", "driver").orderBy("lap_number").rowsBetween(-3, -1)

        df = df.withColumn(
            "last_lap_time_ms",
            lag("lap_time_ms", 1).over(lap_window)
        )

        df = df.withColumn(
            "last_lap_is_green",
            lag("is_green", 1).over(lap_window)
        )

        df = df.withColumn(
            "avg_lap_time_last_3",
            avg("lap_time_ms").over(last_3_window)
        )

        df = df.withColumn(
            "avg_lap_time_last_2",
            avg("lap_time_ms").over(
                Window.partitionBy("season", "race", "driver")
                .orderBy("lap_number")
                .rowsBetween(-2, -1)
            )
        )

        df = df.withColumn(
            "last_lap_is_yellow",
            lag("is_yellow", 1).over(lap_window)
        )

        df = df.withColumn(
            "last_lap_is_safety_car",
            lag("is_safety_car", 1).over(lap_window)
        )

        final_df = df.select(
            "season",
            "race",
            "driver",
            "driver_number",
            "team_name",
            "lap_number",
            "lapstartdate",

            # current lap known context
            "position",
            "stint",
            "compound",
            "tyre_life",
            "fresh_tyre",
            "is_pit_lap",
            "is_pit_in_lap",
            "is_pit_out_lap",
            "is_green",
            "is_yellow",
            "is_safety_car",
            "is_red_flag",
            "is_vsc",
            "air_temp",
            "humidity",
            "pressure",
            "track_temp",
            "wind_direction",
            "wind_speed",
            "is_raining",

            # history features only
            "last_lap_time_ms",
            "last_lap_is_green",
            "last_lap_is_yellow",
            "last_lap_is_safety_car",
            "avg_lap_time_last_2",
            "avg_lap_time_last_3",

            # target
            col("lap_time_ms").alias("target_lap_time_ms")
        )

        final_df = final_df.filter(col("target_lap_time_ms").isNotNull())

        return final_df

    def save_data(self, df: DataFrame) -> None:
        df.write.mode("overwrite").parquet(self.output_path)

    def run(self) -> None:
        df = self.read_data()
        gold_df = self.build_dataset(df)
        self.save_data(gold_df)
        gold_df.printSchema()
        print(gold_df.show(5))
        print(f"Saved Gold table: {self.output_path}")


if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("FastF1 Gold Lap Prediction Dataset") \
        .getOrCreate()

    pipeline = FastF1GoldLapPredictionDatasetPipeline(spark, season=2023)
    pipeline.run()

    spark.stop()