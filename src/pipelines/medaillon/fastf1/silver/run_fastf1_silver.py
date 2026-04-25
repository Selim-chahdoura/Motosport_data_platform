from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils
from fastf1_silver_pipeline import FastF1SilverPipeline


def main():
    seasons = [2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025]

    spark = SparkSession.getActiveSession()
    if spark is None:
        raise RuntimeError("No active Spark session found.")

    dbutils = DBUtils(spark)

    spark.conf.set(
        "fs.azure.account.key.motorsportdatalake.dfs.core.windows.net",
        dbutils.secrets.get(scope="adls-scope", key="storage-key")
    )

    for season in seasons:
        pipeline = FastF1SilverPipeline(spark=spark, dbutils=dbutils)
        pipeline.process_season(season)


if __name__ == "__main__":
    main()