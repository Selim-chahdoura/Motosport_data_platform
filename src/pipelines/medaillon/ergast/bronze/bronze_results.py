from pyspark.sql import SparkSession
from pyspark.sql.functions import explode


spark = SparkSession.builder \
    .appName("bronze_results") \
    .getOrCreate()


def process_season(season):
    print(f"\nProcessing season {season}...")

    # load data
    path = f"data_lake/raw/results/results_{season}.json"
    df = spark.read.option("multiline", "true").json(path)

    # Get one row per race
    df = df.select(explode("Races").alias("race"))

    # Get one row per driver(Each row will represent the result of one driver in a race)
    df = df.select(
        "race.*",
        explode("race.Results").alias("result")
    )

    # flatten
    df_flat = df.select(
        "season",
        "round",
        "raceName",
        "date",

        "Circuit.circuitId",
        "Circuit.circuitName",

        "result.Driver.driverId",
        "result.Driver.code",
        "result.Driver.givenName",
        "result.Driver.familyName",
        "result.Driver.nationality",

        "result.Constructor.constructorId",
        "result.Constructor.name",

        "result.grid",
        "result.position",
        "result.points",
        "result.laps",
        "result.status"
    )

    # Save bronze data per season
    output_path = f"data_lake/bronze/results_{season}"
    df_flat.write.mode("overwrite").parquet(output_path)

    print(f"Saved season {season} to {output_path}")


def main():
    seasons = [2021, 2022, 2023]

    for season in seasons:
        process_season(season)


if __name__ == "__main__":
    main()