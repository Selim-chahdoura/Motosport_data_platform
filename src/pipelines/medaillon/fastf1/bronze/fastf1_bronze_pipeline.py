import sys
import json
import os
from pathlib import Path

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

sys.path.append(str(Path(__file__).resolve().parents[5]))

from src.services.spark_session import get_spark_session


class FastF1BronzePipeline:
    def __init__(
        self,
        raw_base_dir: str = "data_lake/raw/fastf1",
        bronze_base_dir: str = "data_lake/bronze/fastf1",
    ):
        self.raw_base_dir = Path(raw_base_dir)
        self.bronze_base_dir = Path(bronze_base_dir)
        self.bronze_base_dir.mkdir(parents=True, exist_ok=True)

        base_dir = Path(__file__).resolve().parents[5]
        python_exe = str(base_dir / ".venv" / "Scripts" / "python.exe")

        os.environ["PYSPARK_PYTHON"] = python_exe
        os.environ["PYSPARK_DRIVER_PYTHON"] = python_exe

        self.spark = get_spark_session("FastF1BronzePipeline")

    def process_session(self, season: int, race: str, session_type: str = "R") -> None:
        race_slug = self._slugify(race)

        raw_dir = self.raw_base_dir / f"season_{season}" / f"race_{race_slug}"
        bronze_dir = self.bronze_base_dir / f"season_{season}" / f"race_{race_slug}"
        bronze_dir.mkdir(parents=True, exist_ok=True)

        print(f"\nProcessing {season} - {race}")
        print(f"raw_dir = {raw_dir}")
        print(f"bronze_dir = {bronze_dir}")

        tables = ["laps", "lap_weather", "results"]

        for table_name in tables:
            raw_path = raw_dir / f"{table_name}.parquet"
            print(f"\nReading table '{table_name}' from: {raw_path}")

            if not raw_path.exists():
                print(f"Missing raw file, skipping: {raw_path}")
                continue

            try:
                pdf = pd.read_parquet(raw_path)
            except Exception as e:
                print(f"Failed to read raw parquet with pandas: {raw_path}")
                print(f"Error: {e}")
                continue

            print(f"Raw pandas shape: {pdf.shape}")
            print("Raw pandas dtypes:")
            print(pdf.dtypes)

            if pdf.empty or len(pdf.columns) == 0:
                print(f"Skipping empty raw table: {raw_path}")
                continue

            pdf = self._prepare_pdf_for_spark(pdf)

            print(f"Prepared pandas shape: {pdf.shape}")
            print("Prepared pandas dtypes:")
            print(pdf.dtypes)
            print("Prepared pandas columns:")
            print(list(pdf.columns))

            try:
                df = self.spark.createDataFrame(pdf)
                print("Spark schema after createDataFrame:")
                df.printSchema()
            except Exception as e:
                print(f"Could not create Spark DataFrame for {season} - {race} - {table_name}")
                print("Columns and dtypes:")
                print(pdf.dtypes)
                print(f"Error: {e}")
                continue

            bronze_df = self._prepare_bronze_df(
                df=df,
                table_name=table_name,
                season=season,
                race=race_slug,
                session_type=session_type,
            )

            output_path = bronze_dir / f"bronze_fastf1_{table_name}"

            print("Bronze schema before write:")
            bronze_df.printSchema()
            print(f"Bronze columns count: {len(bronze_df.columns)}")

            try:
                bronze_df.write.mode("overwrite").parquet(str(output_path))
                print(f"Saved {output_path}")
            except Exception as e:
                print(f"Failed writing {output_path}")
                print(f"Error: {e}")

    def _prepare_pdf_for_spark(self, pdf: pd.DataFrame) -> pd.DataFrame:
        pdf = pdf.copy()

        boolean_cols = {
            "rainfall",
            "freshtyre",
            "deleted",
            "fastf1generated",
            "isaccurate",
            "ispersonalbest",
        }

        for col in pdf.columns:
            dtype_str = str(pdf[col].dtype)
            col_name = col.lower()

            if dtype_str.startswith("timedelta64"):
                pdf[col] = pdf[col].dt.total_seconds() * 1000

            elif dtype_str.startswith("datetime64"):
                pdf[col] = pd.to_datetime(pdf[col], errors="coerce").astype("datetime64[us]")

            elif col_name in boolean_cols:
                pdf[col] = pdf[col].apply(
                    lambda x: None if pd.isna(x) else (
                        True if str(x).lower() == "true" else
                        False if str(x).lower() == "false" else
                        bool(x) if isinstance(x, (bool, int)) else None
                    )
                )

            else:
                pdf[col] = pdf[col].apply(
                    lambda x: None if pd.isna(x)
                    else json.dumps(x) if isinstance(x, (dict, list, tuple, set))
                    else str(x) if not isinstance(x, (str, int, float, bool))
                    else x
                )

        for col in pdf.columns:
            if pdf[col].isna().all():
                pdf[col] = pdf[col].astype("string")

        pdf = pdf.drop(columns=["Q1", "Q2", "Q3"], errors="ignore")
        return pdf

    def _prepare_bronze_df(
        self,
        df: DataFrame,
        table_name: str,
        season: int,
        race: str,
        session_type: str,
    ) -> DataFrame:
        df = df.select(
            *[F.col(col).alias(self._normalize_column_name(col)) for col in df.columns]
        )

        return (
            df.withColumn("source_table", F.lit(table_name))
            .withColumn("season", F.lit(season))
            .withColumn("race", F.lit(race))
            .withColumn("session_type", F.lit(session_type))
        )

    def _normalize_column_name(self, col: str) -> str:
        return (
            str(col)
            .strip()
            .lower()
            .replace(" ", "_")
            .replace("-", "_")
            .replace("/", "_")
        )

    def _slugify(self, value: str) -> str:
        return (
            value.strip()
            .lower()
            .replace(" ", "_")
            .replace("-", "_")
            .replace("/", "_")
        )


if __name__ == "__main__":
    session_type = "R"

    race_config = {
        "2018": ["Australia", "Bahrain", "China", "Azerbaijan", "Spain", "Monaco", "Canada", "France", "Austria", "Great Britain", "Germany", "Hungary", "Belgium", "Italy", "Singapore", "Russia", "Japan", "United States", "Mexico", "Brazil", "United Arab Emirates"],
        "2019": ["Australia", "Bahrain", "China", "Azerbaijan", "Spain", "Monaco", "Canada", "France", "Austria", "Great Britain", "Germany", "Hungary", "Belgium", "Italy", "Singapore", "Russia", "Japan", "Mexico", "United States", "Brazil", "Abu Dhabi"],
        "2020": ["Austria", "Austria", "Hungary", "Great Britain", "Great Britain", "Spain", "Belgium", "Italy", "Italy", "Russia", "Germany", "Portugal", "Italy", "Turkey", "Bahrain", "Bahrain", "Abu Dhabi"],
        "2021": ["Bahrain", "Italy", "Portugal", "Spain", "Monaco", "Azerbaijan", "France", "Austria", "Austria", "Great Britain", "Hungary", "Belgium", "Netherlands", "Italy", "Russia", "Turkey", "United States", "Mexico", "Brazil", "Qatar", "Saudi Arabia", "Abu Dhabi"],
        "2022": ["Bahrain", "Saudi Arabia", "Australia", "Italy", "United States", "Spain", "Monaco", "Azerbaijan", "Canada", "Great Britain", "Austria", "France", "Hungary", "Belgium", "Netherlands", "Italy", "Singapore", "Japan", "United States", "Mexico", "Brazil", "Abu Dhabi"],
        "2023": ["Bahrain", "Saudi Arabia", "Australia", "Azerbaijan", "United States", "Monaco", "Spain", "Canada", "Austria", "Great Britain", "Hungary", "Belgium", "Netherlands", "Italy", "Singapore", "Japan", "Qatar", "United States", "Mexico", "Brazil", "United States", "Abu Dhabi"],
        "2024": ["Bahrain", "Saudi Arabia", "Australia", "Japan", "China", "United States", "Italy", "Monaco", "Canada", "Spain", "Austria", "United Kingdom", "Hungary", "Belgium", "Netherlands", "Italy", "Azerbaijan", "Singapore", "United States", "Mexico", "Brazil", "United States", "Qatar", "United Arab Emirates"],
        "2025": ["Australia", "China", "Japan", "Bahrain", "Saudi Arabia", "United States", "Italy", "Monaco", "Spain", "Canada", "Austria", "United Kingdom", "Belgium", "Hungary", "Netherlands", "Italy", "Azerbaijan", "Singapore", "United States", "Mexico", "Brazil", "United States", "Qatar", "United Arab Emirates"],
    }

    pipeline = FastF1BronzePipeline()

    for season_str, races in race_config.items():
        season = int(season_str)

        print(f"\n===== SEASON {season} =====")

        for race in races:
            print(f"\n➡️ Running Bronze for {season} - {race}")

            try:
                pipeline.process_session(
                    season=season,
                    race=race,
                    session_type=session_type,
                )
            except Exception as e:
                print(f"❌ Failed for {season} - {race}: {e}")
                continue