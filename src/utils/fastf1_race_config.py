import json
from pathlib import Path


def load_races_for_season(
    season: int,
    config_path: str = "src/config/fastf1_races.json"
) -> list[str]:
    project_root = Path(__file__).resolve().parents[2]
    full_config_path = project_root / config_path

    if not full_config_path.exists():
        raise FileNotFoundError(f"Config file not found: {full_config_path}")

    with open(full_config_path, "r", encoding="utf-8") as f:
        config = json.load(f)

    return config.get(str(season), [])