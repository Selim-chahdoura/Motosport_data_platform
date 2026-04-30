import sys
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

sys.path.append(str(Path(__file__).resolve().parents[5]))

from src.services.spark_session import get_spark_session
from src.pipelines.medaillon.fastf1.silver.fastf1_silver_joiner import FastF1SilverJoiner
from prepare_silver_data import PrepareSilverData


class FastF1SilverPipeline:
    def __init__(
        self,
        bronze_base_dir: str = "data_lake/bronze/fastf1",
        silver_base_dir: str = "data_lake/silver/fastf1",
    ):
        self.bronze_base_dir = Path(bronze_base_dir)
        self.silver_base_dir = Path(silver_base_dir)
        self.silver_base_dir.mkdir(parents=True, exist_ok=True)

        self.spark = get_spark_session("FastF1SilverPipeline")
        self.joiner = FastF1SilverJoiner()
        self.prepare = PrepareSilverData()

    def process_season(self, season: int) -> None:
        season_dir = self.bronze_base_dir / f"season_{season}"

        if not season_dir.exists():
            print(f"No Bronze data for season {season}: {season_dir}")
            return

        race_dfs = []

        for race_dir in season_dir.iterdir():
            if not race_dir.is_dir():
                continue
            print(f"\nFound {season_dir} with {len(list(season_dir.iterdir()))} items")
            laps_path = race_dir / "bronze_fastf1_laps"
            lap_weather_path = race_dir / "bronze_fastf1_lap_weather"
            results_path = race_dir / "bronze_fastf1_results"

            if not laps_path.exists():
                print(f"Skipping race (no laps): {race_dir.name}")
                continue

            print(f"\nProcessing {race_dir.name}...")
            print(f"laps_path = {laps_path}")
            print(f"lap_weather_path = {lap_weather_path}")
            print(f"results_path = {results_path}")

            try:
                laps_df = self.spark.read.parquet(str(laps_path))
            except Exception as e:
                print(f"Could not read laps for {race_dir.name}")
                print(f"Error: {e}")
                continue

            if "lapnumber" in laps_df.columns and "lap_number" not in laps_df.columns:
                laps_df = laps_df.withColumnRenamed("lapnumber", "lap_number")

            if lap_weather_path.exists():
                try:
                    lap_weather_df = self.spark.read.parquet(str(lap_weather_path))
                    laps_df = self.joiner.join_lap_weather(laps_df, lap_weather_df)
                except Exception as e:
                    print(f"No usable lap_weather for {race_dir.name}")
                    print(f"Error: {e}")

            if results_path.exists():
                try:
                    results_df = self.spark.read.parquet(str(results_path))
                    laps_df = self.joiner.join_results(laps_df, results_df)
                except Exception as e:
                    print(f"No usable results for {race_dir.name}")
                    print(f"Error: {e}")

            try:
                laps_df = self.prepare.prepare(laps_df)
                laps_df = self._normalize_schema(laps_df)
                race_dfs.append(laps_df)
            except Exception as e:
                print(f"Failed preparing Silver data for {race_dir.name}")
                print(f"Error: {e}")

        if not race_dfs:
            print(f"No races found for season {season}")
            return

        print("\nCombining all races...")

        season_df = race_dfs[0]

        for df in race_dfs[1:]:
            df = self._align_schema(season_df, df)
            season_df = season_df.unionByName(df, allowMissingColumns=True)

        output_path = self.silver_base_dir / f"season_{season}" / "silver_fastf1_laps"
        output_path.parent.mkdir(parents=True, exist_ok=True)

        season_df.write.mode("overwrite").parquet(str(output_path))

        print(f"\nSaved Silver table: {output_path}")

    def _normalize_schema(self, df: DataFrame) -> DataFrame:
        boolean_columns = [
            "ispersonalbest",
            "deleted",
            "fastf1generated",
            "isaccurate",
            "freshtyre",
            "rainfall",
        ]

        for col_name in boolean_columns:
            if col_name in df.columns:
                df = df.withColumn(col_name, F.col(col_name).cast("boolean"))

        integer_columns = [
            "season",
            "lap_number",
        ]

        for col_name in integer_columns:
            if col_name in df.columns:
                df = df.withColumn(col_name, F.col(col_name).cast("int"))

        return df

    def _align_schema(self, base_df: DataFrame, new_df: DataFrame) -> DataFrame:
        for field in base_df.schema:
            col_name = field.name
            col_type = field.dataType

            if col_name in new_df.columns:
                new_df = new_df.withColumn(col_name, F.col(col_name).cast(col_type))
            else:
                new_df = new_df.withColumn(col_name, F.lit(None).cast(col_type))

        return new_df.select(base_df.columns)


def main():
    seasons = list(range(2018, 2026))

    pipeline = FastF1SilverPipeline()

    for season in seasons:
        print(f"\n===== SILVER SEASON {season} =====")
        pipeline.process_season(season)


if __name__ == "__main__":
    main()