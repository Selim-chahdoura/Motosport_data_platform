import os
import sys
import tempfile
from pathlib import Path

import pandas as pd
from pyspark.sql import SparkSession

_this_file = Path(sys._getframe(0).f_locals.get("filename") or 
                  sys._getframe(1).f_locals.get("filename"))

project_src = _this_file.parents[2]
sys.path.insert(0, str(project_src))

from ingestion.fastf1_dataset_ingestion.fastf1_raw_ingestion import FastF1RawIngestion
from utils.fastf1_race_config import load_races_for_season


def prepare_for_spark(pdf: pd.DataFrame) -> pd.DataFrame:
    pdf = pdf.copy()

    for col in pdf.columns:
        dtype_str = str(pdf[col].dtype)

        if dtype_str.startswith("timedelta"):
            pdf[col] = pdf[col].astype(str)
        elif dtype_str.startswith("datetime"):
            pdf[col] = pd.to_datetime(pdf[col], errors="coerce")
        elif dtype_str == "object":
            pdf[col] = pdf[col].astype(str)

    return pdf


def main():
    print("\n===== START JOB =====")

    spark = SparkSession.getActiveSession()
    if spark is None:
        raise RuntimeError("No active Spark session found.")

    spark.conf.set(
        "fs.azure.account.key.motorsportdatalake.dfs.core.windows.net",
        dbutils.secrets.get(scope="adls-scope", key="storage-key")
    )

    seasons = [2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025]
    session_type = "R"

    

    for season in seasons:
        races = load_races_for_season(season)
        
        for grand_prix in races:
            print(f"{season} - {grand_prix}")
            ingestion = FastF1RawIngestion()
            raw_data = ingestion.ingest_session(
                season=season,
                grand_prix=grand_prix,
                session_type=session_type,
            )

            race_slug = (
                grand_prix.strip()
                .lower()
                .replace(" ", "_")
                .replace("-", "_")
                .replace("/", "_")
            )

            base_raw = (
                f"abfss://raw@motorsportdatalake.dfs.core.windows.net/"
                f"fastf1/season_{season}/race_{race_slug}/"
            )

            laps_spark = spark.createDataFrame(prepare_for_spark(raw_data["laps"]))
            weather_spark = spark.createDataFrame(prepare_for_spark(raw_data["lap_weather"]))
            results_spark = spark.createDataFrame(prepare_for_spark(raw_data["results"]))

            laps_spark.write.mode("overwrite").parquet(base_raw + "laps/")
            weather_spark.write.mode("overwrite").parquet(base_raw + "lap_weather/")
            results_spark.write.mode("overwrite").parquet(base_raw + "results/")

    print("===== END JOB =====\n")


if __name__ == "__main__":
    main()