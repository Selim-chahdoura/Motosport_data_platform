from pathlib import Path

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lag, avg


class FastF1GoldLapPredictionDatasetPipeline:
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
        self.output_path = Path(gold_base_dir) / f"season_{season}" / "gold_lap_prediction_dataset"

    def read_data(self) -> DataFrame:
        print(f"Reading Silver data from: {self.input_path}")
        return self.spark.read.parquet(str(self.input_path))

    def build_dataset(self, df: DataFrame) -> DataFrame:
        required_columns = ["season", "race", "driver", "lap_number", "lap_time_ms"]

        for required_col in required_columns:
            if required_col not in df.columns:
                raise ValueError(f"Missing required column in Silver data: {required_col}")

        lap_window = Window.partitionBy("season", "race", "driver").orderBy("lap_number")

        last_3_window = (
            Window.partitionBy("season", "race", "driver")
            .orderBy("lap_number")
            .rowsBetween(-3, -1)
        )

        last_2_window = (
            Window.partitionBy("season", "race", "driver")
            .orderBy("lap_number")
            .rowsBetween(-2, -1)
        )

        df = (
            df.withColumn("last_lap_time_ms", lag("lap_time_ms", 1).over(lap_window))
            .withColumn("last_lap_is_green", lag("is_green", 1).over(lap_window))
            .withColumn("last_lap_is_yellow", lag("is_yellow", 1).over(lap_window))
            .withColumn("last_lap_is_safety_car", lag("is_safety_car", 1).over(lap_window))
            .withColumn("avg_lap_time_last_2", avg("lap_time_ms").over(last_2_window))
            .withColumn("avg_lap_time_last_3", avg("lap_time_ms").over(last_3_window))
        )

        selected_columns = [
            "season",
            "race",
            "driver",
            "driver_number",
            "team_name",
            "lap_number",
            "lapstartdate",
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
            "last_lap_time_ms",
            "last_lap_is_green",
            "last_lap_is_yellow",
            "last_lap_is_safety_car",
            "avg_lap_time_last_2",
            "avg_lap_time_last_3",
        ]

        select_exprs = []

        for column_name in selected_columns:
            if column_name in df.columns:
                select_exprs.append(col(column_name))
            else:
                select_exprs.append(F.lit(None).alias(column_name))

        final_df = df.select(
            *select_exprs,
            col("lap_time_ms").alias("target_lap_time_ms"),
        )

        final_df = final_df.filter(col("target_lap_time_ms").isNotNull())

        return final_df

    def save_data(self, df: DataFrame) -> None:
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        df.write.mode("overwrite").parquet(str(self.output_path))

    def run(self) -> None:
        df = self.read_data()
        gold_df = self.build_dataset(df)

        self.save_data(gold_df)

        gold_df.printSchema()
        gold_df.show(5, truncate=False)

        print(f"Saved Gold table: {self.output_path}")


def main():
    spark = (
        SparkSession.builder
        .appName("FastF1 Gold Lap Prediction Dataset")
        .getOrCreate()
    )

    seasons = list(range(2018, 2026))

    for season in seasons:
        print(f"\n===== GOLD SEASON {season} =====")

        try:
            pipeline = FastF1GoldLapPredictionDatasetPipeline(spark, season=season)
            pipeline.run()
        except Exception as e:
            print(f"Failed Gold pipeline for season {season}: {e}")

    spark.stop()


if __name__ == "__main__":
    main()