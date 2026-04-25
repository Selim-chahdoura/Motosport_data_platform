from pyspark.sql import SparkSession

from fastf1_silver_joiner import FastF1SilverJoiner
from prepare_silver_data import PrepareSilverData


class FastF1SilverPipeline:
    def __init__(
        self,
        spark: SparkSession,
        dbutils,
        bronze_base_dir: str = "abfss://bronze@motorsportdatalake.dfs.core.windows.net/fastf1",
        silver_base_dir: str = "abfss://silver@motorsportdatalake.dfs.core.windows.net/fastf1",
    ):
        self.spark = spark
        self.dbutils = dbutils
        self.bronze_base_dir = bronze_base_dir.rstrip("/")
        self.silver_base_dir = silver_base_dir.rstrip("/")

        self.joiner = FastF1SilverJoiner()
        self.prepare = PrepareSilverData()

    def process_season(self, season: int) -> None:
        season_dir = f"{self.bronze_base_dir}/season_{season}"

        print(f"season_dir = {season_dir}")

        try:
            race_entries = self.dbutils.fs.ls(season_dir)
        except Exception as e:
            raise FileNotFoundError(f"No data for season {season} at {season_dir}. Error: {e}")

        race_dfs = []

        for entry in race_entries:
            if not entry.isDir():
                continue

            race_dir = entry.path.rstrip("/")
            race_name = race_dir.split("/")[-1]

            laps_path = f"{race_dir}/bronze_fastf1_laps"
            lap_weather_path = f"{race_dir}/bronze_fastf1_lap_weather"
            results_path = f"{race_dir}/bronze_fastf1_results"

            print(f"Processing {race_name}...")
            print(f"laps_path = {laps_path}")
            print(f"lap_weather_path = {lap_weather_path}")
            print(f"results_path = {results_path}")

            try:
                laps_df = self.spark.read.format("delta").load(laps_path)
            except Exception as e:
                print(f"Skipping race (no laps or unreadable path): {race_name}")
                print(f"Error: {e}")
                continue

            if "lapnumber" in laps_df.columns and "lap_number" not in laps_df.columns:
                laps_df = laps_df.withColumnRenamed("lapnumber", "lap_number")

            try:
                lap_weather_df = self.spark.read.format("delta").load(lap_weather_path)
                laps_df = self.joiner.join_lap_weather(laps_df, lap_weather_df)
            except Exception as e:
                print(f"No lap_weather for {race_name}, skipping join.")
                print(f"Error: {e}")

            try:
                results_df = self.spark.read.format("delta").load(results_path)
                laps_df = self.joiner.join_results(laps_df, results_df)
            except Exception as e:
                print(f"No results for {race_name}, skipping join.")
                print(f"Error: {e}")

            laps_df = self.prepare.prepare(laps_df)
            race_dfs.append(laps_df)

        if not race_dfs:
            print(f"No races found for season {season}")
            return

        print("\nCombining all races...")

        season_df = race_dfs[0]
        for df in race_dfs[1:]:
            season_df = season_df.unionByName(df, allowMissingColumns=True)

        output_path = f"{self.silver_base_dir}/season_{season}"
        season_df.write.format("delta").mode("overwrite").save(output_path)

        print(f"\nSaved Silver table: {output_path}")