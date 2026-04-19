from fastf1_raw_ingestion import FastF1RawIngestion

races = [
    (2023, "Australian"),
    (2023, "Bahrain"),
    (2023, "Saudi Arabia"),
    (2023, "Azerbaijan"),
    (2023, "Miami")
]

ingestion = FastF1RawIngestion()

for season, gp in races:
    try:
        ingestion.ingest_session(season, gp, "R")
    except Exception as e:
        print(f"Failed for {season} {gp}: {e}")