from fastf1_raw_ingestion import FastF1RawIngestion

races = [
    (2021, "Bahrain"),
    (2021, "Monaco"),
    (2021, "Silverstone"),
]

ingestion = FastF1RawIngestion()

for season, gp in races:
    try:
        ingestion.ingest_session(season, gp, "R")
    except Exception as e:
        print(f"Failed for {season} {gp}: {e}")