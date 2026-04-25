from pyspark.sql import SparkSession
import sys
from pathlib import Path

_this_file = Path(sys._getframe(0).f_locals.get("filename") or 
                  sys._getframe(1).f_locals.get("filename"))

project_src = _this_file.parents[4]
sys.path.insert(0, str(project_src))

from utils.fastf1_race_config import load_races_for_season
from pipelines.medaillon.fastf1.bronze.fastf1_bronze_pipeline import FastF1BronzePipeline


def main():
    seasons = [2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025]
    session_type = "R"

    spark = SparkSession.getActiveSession()
    if spark is None:
        raise RuntimeError("No active Spark session found.")

    spark.conf.set(
        "fs.azure.account.key.motorsportdatalake.dfs.core.windows.net",
        dbutils.secrets.get(scope="adls-scope", key="storage-key")
    )

    for season in seasons:
        races = load_races_for_season(season)
        pipeline = FastF1BronzePipeline(spark=spark)

        for race in races:
            print(f"Processing {season} - {race}")
            pipeline.process_session(
                season=season,
                race=race,
                session_type=session_type,
            )


if __name__ == "__main__":
    main()