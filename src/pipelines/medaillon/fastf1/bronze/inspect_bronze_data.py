import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[5]))

from src.services.spark_session import get_spark_session


class BronzeInspector:
    def __init__(self, bronze_base_dir="data_lake/bronze/fastf1"):
        self.base_dir = Path(bronze_base_dir)
        self.spark = get_spark_session("BronzeInspector")

    def inspect_session(self, season: int, race: str):
        race_slug = race.lower().replace(" ", "_")

        bronze_dir = (
            self.base_dir
            / f"season_{season}"
            / f"race_{race_slug}"
        )

        tables = ["laps", "lap_weather", "results"]

        for table in tables:
            path = bronze_dir / f"bronze_fastf1_{table}.parquet"

            print(f"\n=== {table.upper()} ===")

            if not path.exists():
                print("Missing")
                continue

            df = self.spark.read.parquet(str(path))

            print("Columns:", df.columns)
            print("Dtypes:", dict(df.dtypes))

            print("\nSample:")
            df.show(5, truncate=False)

            if table == "lap_weather":
                print("\nCheck join keys:")
                df.select("driver", "lap_number").show(5)


def main():
    inspector = BronzeInspector()
    inspector.inspect_session(season=2023, race="australian")


if __name__ == "__main__":
    main()