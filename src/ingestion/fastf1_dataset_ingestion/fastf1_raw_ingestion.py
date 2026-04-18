from pathlib import Path
from datetime import datetime, timezone
from typing import Optional

import pandas as pd
import fastf1


class FastF1RawIngestion:
    """
    Ingest raw FastF1 session data and store it in the raw layer.

    Saved raw tables:
    - laps
    - results
    - weather
    - track_status: green, yellow, red flags, vsc, safety car
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
        """
        Load one FastF1 session and persist raw tables
        Take only race session data
        """
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

        # get lap per lap performance data for all drivers in the session
        laps_df = self._prepare_laps_df(session, season, grand_prix, session_type, ingestion_ts)
        self._save_parquet(laps_df, output_dir / "laps.parquet")

        # get the end results of the session
        results_df = self._prepare_results_df(session, season, grand_prix, session_type, ingestion_ts)
        self._save_parquet(results_df, output_dir / "results.parquet")

        # get weather data for the session
        weather_df = self._prepare_weather_df(session, season, grand_prix, session_type, ingestion_ts)
        self._save_parquet(weather_df, output_dir / "weather.parquet")

        # get track status data for the session (green, yellow, red flags, vsc, safety car)
        track_status_df = self._prepare_track_status_df(session, season, grand_prix, session_type, ingestion_ts)
        self._save_parquet(track_status_df, output_dir / "track_status.parquet")

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

        laps_df["season"] = season
        laps_df["grand_prix"] = grand_prix
        laps_df["session_type"] = session_type
        laps_df["ingestion_timestamp"] = ingestion_ts

        return laps_df

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

        results_df["season"] = season
        results_df["grand_prix"] = grand_prix
        results_df["session_type"] = session_type
        results_df["ingestion_timestamp"] = ingestion_ts

        return results_df

    def _prepare_weather_df(
        self,
        session,
        season: int,
        grand_prix: str,
        session_type: str,
        ingestion_ts: str,
    ) -> pd.DataFrame:
        weather_data = None

        if hasattr(session, "weather_data"):
            weather_data = session.weather_data

        if weather_data is None:
            return self._empty_df_with_metadata(
                season, grand_prix, session_type, ingestion_ts
            )

        weather_df = weather_data.copy()

        if isinstance(weather_df, pd.Series):
            weather_df = weather_df.to_frame().T

        weather_df = weather_df.reset_index(drop=True)

        weather_df["season"] = season
        weather_df["grand_prix"] = grand_prix
        weather_df["session_type"] = session_type
        weather_df["ingestion_timestamp"] = ingestion_ts

        return weather_df

    def _prepare_track_status_df(
        self,
        session,
        season: int,
        grand_prix: str,
        session_type: str,
        ingestion_ts: str,
    ) -> pd.DataFrame:
        track_status = None

        if hasattr(session, "track_status"):
            track_status = session.track_status

        if track_status is None:
            return self._empty_df_with_metadata(
                season, grand_prix, session_type, ingestion_ts
            )

        track_status_df = track_status.copy()

        if isinstance(track_status_df, pd.Series):
            track_status_df = track_status_df.to_frame().T

        track_status_df = track_status_df.reset_index(drop=True)

        track_status_df["season"] = season
        track_status_df["grand_prix"] = grand_prix
        track_status_df["session_type"] = session_type
        track_status_df["source"] = "fastf1"
        track_status_df["ingestion_timestamp"] = ingestion_ts

        return track_status_df

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


if __name__ == "__main__":
    ingestion = FastF1RawIngestion()

    # Example: ingest one race
    ingestion.ingest_session(
        season=2021,
        grand_prix="Monaco",
        session_type="R",
    )