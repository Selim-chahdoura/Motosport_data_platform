from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


class FastF1BronzePipeline:
    def __init__(
        self,
        spark: SparkSession,
        raw_base_dir: str = "abfss://raw@motorsportdatalake.dfs.core.windows.net/fastf1",
        bronze_base_dir: str = "abfss://bronze@motorsportdatalake.dfs.core.windows.net/fastf1",
    ):
        self.spark = spark
        self.raw_base_dir = raw_base_dir.rstrip("/")
        self.bronze_base_dir = bronze_base_dir.rstrip("/")
        
    def process_session(self, season: int, race: str, session_type: str = "R") -> None:
        race_slug = self._slugify(race)

        raw_dir = f"{self.raw_base_dir}/season_{season}/race_{race_slug}"
        bronze_dir = f"{self.bronze_base_dir}/season_{season}/race_{race_slug}"

        print(f"raw_dir = {raw_dir}")
        print(f"bronze_dir = {bronze_dir}")

        tables = ["laps", "lap_weather", "results"]

        for table_name in tables:
            raw_path = f"{raw_dir}/{table_name}/"
            print(f"Trying to read table '{table_name}' from: {raw_path}")

            try:
                df = self.spark.read.parquet(raw_path)
            except Exception as e:
                print(f"Missing raw file or unreadable path, skipping: {raw_path}")
                print(f"Error: {e}")
                continue

            bronze_df = self._prepare_bronze_df(
                df=df,
                table_name=table_name,
                season=season,
                race=race_slug,
                session_type=session_type,
            )

            output_path = f"{bronze_dir}/bronze_fastf1_{table_name}"
            bronze_df.write.format("delta").mode("overwrite").save(output_path)
            print(f"Saved {output_path}")

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