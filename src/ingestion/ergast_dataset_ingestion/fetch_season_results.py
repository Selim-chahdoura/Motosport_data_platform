import requests
import json


def fetch_one_season_results(season):
    limit = 100
    offset = 0
    all_races = []
    print(f"Fetching results for season {season}...")
    while True:
        print(f"Fetching data with offset {offset}...")

        url = f"https://api.jolpi.ca/ergast/f1/{season}/results.json?limit={limit}&offset={offset}"
        response = requests.get(url)
        data = response.json()

        races = data["MRData"]["RaceTable"]["Races"]
        all_races.extend(races)

        total = int(data["MRData"]["total"])
        offset += limit

        if offset >= total:
            break

    return all_races


def fetch_seasons_result():
    seasons = [2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025]

    for season in seasons:
        races = fetch_one_season_results(season)

        with open(f"data_lake/raw/results/results_{season}.json", "w", encoding="utf-8") as f:
            json.dump({"Races": races}, f, indent=2)

    print("Done! Full seasons saved.")


if __name__ == "__main__":
    fetch_seasons_result()