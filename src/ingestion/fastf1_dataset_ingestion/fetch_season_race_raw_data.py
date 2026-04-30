from fastf1_raw_ingestion import FastF1RawIngestion

race_config = {
        "2022": ["Bahrain","Saudi Arabia","Australia","Italy","United States","Spain","Monaco","Azerbaijan","Canada","Great Britain","Austria","France","Hungary","Belgium","Netherlands","Italy","Singapore","Japan","United States","Mexico","Brazil","Abu Dhabi"],
}

ingestion = FastF1RawIngestion()

for season_str, races in race_config.items():
    season = int(season_str)

    for gp in races:
        try:
            print(f"{season} - {gp}")
            ingestion.ingest_session(season, gp, "R")
        except Exception as e:
            print(f"Failed for {season} {gp}: {e}")