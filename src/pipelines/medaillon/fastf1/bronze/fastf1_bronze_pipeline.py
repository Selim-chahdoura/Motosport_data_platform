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

        tables = ["laps", "lap_weather", "results"]

        for table_name in tables:
            raw_path = raw_dir / f"{table_name}.parquet"

            if not raw_path.exists():
                print(f"Missing raw file, skipping: {raw_path}")
                continue

            pdf = pd.read_parquet(raw_path)
            pdf = self._prepare_pdf_for_spark(pdf)

            df = self.spark.createDataFrame(pdf)

            bronze_df = self._prepare_bronze_df(
                df=df,
                table_name=table_name,
                season=season,
                race=race_slug,
                session_type=session_type,
            )

            output_path = bronze_dir / f"bronze_fastf1_{table_name}.parquet"
            bronze_df.write.mode("overwrite").parquet(str(output_path))
            print(f"Saved {output_path}")

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
                pdf[col] = pd.to_datetime(pdf[col]).astype("datetime64[us]")

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

        df = (
            df.withColumn("source_table", F.lit(table_name))
            .withColumn("season", F.lit(season))
            .withColumn("race", F.lit(race))
            .withColumn("session_type", F.lit(session_type))
        )

        return df

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

    races = [
        (2023, "Australian"),
        (2023, "Bahrain"),
        (2023, "Saudi Arabia"),
        (2023, "Azerbaijan"),
        (2023, "Miami"),
    ]

    pipeline = FastF1BronzePipeline()

    for season, race in races:
        print(f"Running Bronze pipeline for {season} - {race}")
        pipeline.process_session(
            season=season,
            race=race,
            session_type=session_type,
        )