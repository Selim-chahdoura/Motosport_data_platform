import sys
from pathlib import Path

from pyspark.sql import DataFrame
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
            raise FileNotFoundError(f"No data for season {season}")

        race_dfs = []

        for race_dir in season_dir.iterdir():
            if not race_dir.is_dir():
                continue

            laps_path = race_dir / "bronze_fastf1_laps.parquet"
            lap_weather_path = race_dir / "bronze_fastf1_lap_weather.parquet"
            results_path = race_dir / "bronze_fastf1_results.parquet"

            if not laps_path.exists():
                print(f"Skipping race (no laps): {race_dir.name}")
                continue

            print(f"Processing {race_dir.name}...")

            laps_df = self.spark.read.parquet(str(laps_path))

            laps_df = laps_df.withColumnRenamed("lapnumber", "lap_number")

            if lap_weather_path.exists():
                lap_weather_df = self.spark.read.parquet(str(lap_weather_path))
                laps_df = self.joiner.join_lap_weather(laps_df, lap_weather_df)

            if results_path.exists():
                results_df = self.spark.read.parquet(str(results_path))
                laps_df = self.joiner.join_results(laps_df, results_df)

            laps_df = self.prepare.prepare(laps_df)
            race_dfs.append(laps_df)

        if not race_dfs:
            print(f"No races found for season {season}")
            return

        print("\nCombining all races...")

        season_df = race_dfs[0]
        for df in race_dfs[1:]:
            season_df = season_df.unionByName(df)

        output_dir = self.silver_base_dir / f"season_{season}"
        output_dir.mkdir(parents=True, exist_ok=True)
        
        output_path = output_dir / "silver_fastf1_laps.parquet"
        season_df.write.mode("overwrite").parquet(str(output_path))

        print(f"\nSaved Silver table: {output_path}")

def main():
    season = 2023

    pipeline = FastF1SilverPipeline()
    pipeline.process_season(season)


if __name__ == "__main__":
    main()