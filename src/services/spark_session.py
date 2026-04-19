from pyspark.sql import SparkSession
from pathlib import Path
import os


def get_spark_session(app_name: str = "App") -> SparkSession:
    base_dir = Path(__file__).resolve().parents[2]
    python_exe = str(base_dir / ".venv" / "Scripts" / "python.exe")

    os.environ["PYSPARK_PYTHON"] = python_exe
    os.environ["PYSPARK_DRIVER_PYTHON"] = python_exe

    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[1]")
        .config("spark.pyspark.python", python_exe)
        .config("spark.pyspark.driver.python", python_exe)
        .getOrCreate()
    )

    return spark