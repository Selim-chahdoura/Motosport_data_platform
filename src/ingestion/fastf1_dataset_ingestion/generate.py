import fastf1
import json
from pathlib import Path

script_dir = Path(__file__).parent

seasons = list(range(2018, 2026))
race_config = {}

for season in seasons:
    schedule = fastf1.get_event_schedule(season)

    races_df = schedule[
        schedule["EventName"].str.contains("Grand Prix", na=False)
    ]

    races = races_df["Country"].tolist()

    race_config[str(season)] = races

output_path = script_dir / "fastf1_races.json"

with open(output_path, "w") as f:
    json.dump(race_config, f, indent=2)

print(f"Saved {output_path}")