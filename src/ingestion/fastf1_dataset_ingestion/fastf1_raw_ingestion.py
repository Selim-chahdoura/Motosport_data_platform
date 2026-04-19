from pathlib import Path
from datetime import datetime, timezone

import pandas as pd
import fastf1


class FastF1RawIngestion:
    """
    Ingest raw FastF1 session data and store it in the raw layer.

    Saved raw tables:
    - laps
    - lap_weather
    - results
    """

    def __init__(
        self,
        raw_base_dir: str = "data_lake/raw/fastf1",
        cache_dir: str = "data_lake/cache/fastf1",
    ):
        self.raw_base_dir = Path(raw_base_dir)
        self.cache_dir = Path(cache_dir)

        self.raw_base_dir.mkdir(parents=True, exist_ok=True)
        self.cache_dir.mkdir(parents=True, exist_ok=True)

        fastf1.Cache.enable_cache(str(self.cache_dir))

    def ingest_session(
        self,
        season: int,
        grand_prix: str,
        session_type: str = "R",
    ) -> None:
        print(f"Loading FastF1 session: season={season}, gp={grand_prix}, session={session_type}")

        session = fastf1.get_session(season, grand_prix, session_type)
        session.load()

        ingestion_ts = datetime.now(timezone.utc).isoformat()

        race_slug = self._slugify(grand_prix)
        output_dir = (
            self.raw_base_dir
            / f"season_{season}"
            / f"race_{race_slug}"
        )
        output_dir.mkdir(parents=True, exist_ok=True)

        laps_df = self._prepare_laps_df(session, season, grand_prix, session_type, ingestion_ts)
        self._save_parquet(laps_df, output_dir / "laps.parquet")

        lap_weather_df = self._prepare_lap_weather_df(session, season, grand_prix, session_type, ingestion_ts)
        print(f"Lap weather data prepared with {len(lap_weather_df)} records.")
        print(lap_weather_df.head())
        self._save_parquet(lap_weather_df, output_dir / "lap_weather.parquet")

        results_df = self._prepare_results_df(session, season, grand_prix, session_type, ingestion_ts)
        self._save_parquet(results_df, output_dir / "results.parquet")

        print(f"Raw ingestion finished successfully: {output_dir}")

    def _prepare_laps_df(
        self,
        session,
        season: int,
        grand_prix: str,
        session_type: str,
        ingestion_ts: str,
    ) -> pd.DataFrame:
        laps_df = session.laps.copy()

        if laps_df is None or laps_df.empty:
            return self._empty_df_with_metadata(
                season, grand_prix, session_type, ingestion_ts
            )

        laps_df = laps_df.reset_index(drop=True)
        laps_df = self._add_metadata(laps_df, season, grand_prix, session_type, ingestion_ts)

        return laps_df

    def _prepare_lap_weather_df(
        self,
        session,
        season: int,
        grand_prix: str,
        session_type: str,
        ingestion_ts: str,
    ) -> pd.DataFrame:
        laps_df = session.laps.copy()

        if laps_df is None or laps_df.empty:
            return self._empty_df_with_metadata(
                season, grand_prix, session_type, ingestion_ts
            )

        lap_weather_df = session.laps.get_weather_data()

        if lap_weather_df is None or lap_weather_df.empty:
            return self._empty_df_with_metadata(
                season, grand_prix, session_type, ingestion_ts
            )

        laps_df = laps_df.reset_index(drop=True)
        lap_weather_df = lap_weather_df.reset_index(drop=True)

        if "Driver" in laps_df.columns:
            lap_weather_df["driver"] = laps_df["Driver"].values

        if "LapNumber" in laps_df.columns:
            lap_weather_df["lap_number"] = laps_df["LapNumber"].values

        lap_weather_df = self._add_metadata(
            lap_weather_df,
            season,
            grand_prix,
            session_type,
            ingestion_ts,
        )

        return lap_weather_df

    def _prepare_results_df(
        self,
        session,
        season: int,
        grand_prix: str,
        session_type: str,
        ingestion_ts: str,
    ) -> pd.DataFrame:
        results = getattr(session, "results", None)

        if results is None:
            return self._empty_df_with_metadata(
                season, grand_prix, session_type, ingestion_ts
            )

        results_df = results.copy()

        if isinstance(results_df, pd.Series):
            results_df = results_df.to_frame().T

        results_df = results_df.reset_index(drop=True)
        results_df = self._add_metadata(results_df, season, grand_prix, session_type, ingestion_ts)

        return results_df

    def _save_parquet(self, df: pd.DataFrame, output_path: Path) -> None:
        df.to_parquet(output_path, index=False)
        print(f"Saved: {output_path}")

    def _slugify(self, value: str) -> str:
        return (
            value.strip()
            .lower()
            .replace(" ", "_")
            .replace("-", "_")
            .replace("/", "_")
        )

    def _empty_df_with_metadata(
        self,
        season: int,
        grand_prix: str,
        session_type: str,
        ingestion_ts: str,
    ) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "season": [season],
                "grand_prix": [grand_prix],
                "session_type": [session_type],
                "source": ["fastf1"],
                "ingestion_timestamp": [ingestion_ts],
            }
        )

    def _add_metadata(self, df, season, grand_prix, session_type, ingestion_ts):
        df["season"] = season
        df["grand_prix"] = grand_prix
        df["session_type"] = session_type
        df["ingestion_timestamp"] = ingestion_ts
        return df


if __name__ == "__main__":
    ingestion = FastF1RawIngestion()

    season = 2021
    grand_prix = "Monaco"
    session_type = "R"

    ingestion.ingest_session(
        season=season,
        grand_prix=grand_prix,
        session_type=session_type,
    )