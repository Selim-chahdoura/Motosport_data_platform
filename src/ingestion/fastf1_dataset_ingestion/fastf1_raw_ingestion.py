from datetime import datetime, timezone
import pandas as pd
import fastf1
import tempfile
import os


class FastF1RawIngestion:

    def __init__(self, cache_dir: str = "/tmp/fastf1_cache"):
        self.cache_dir = cache_dir

        if self.cache_dir:
            print(f"[DEBUG] Initializing FastF1 cache at: {self.cache_dir}")
            os.makedirs(self.cache_dir, exist_ok=True)
            fastf1.Cache.enable_cache(self.cache_dir)
        else:
            print("[DEBUG] FastF1 cache disabled (no cache dir provided)")

    def ingest_session(self, season: int, grand_prix: str, session_type: str = "R"):

        print("\n==============================")
        print("[DEBUG] START INGEST SESSION")
        print(f"[DEBUG] season={season}, gp={grand_prix}, session={session_type}")

        session = fastf1.get_session(season, grand_prix, session_type)

        print("[DEBUG] Session object created")

        session.load()

        print("[DEBUG] Session loaded")

        print(f"[DEBUG] Has laps: {hasattr(session, '_laps')}")
        print(f"[DEBUG] Has results: {hasattr(session, '_results')}")
        print(f"[DEBUG] Has weather: {hasattr(session, '_weather_data')}")

        ingestion_ts = datetime.now(timezone.utc).isoformat()

        laps_df = self._prepare_laps_df(session, season, grand_prix, session_type, ingestion_ts)
        print(f"[DEBUG] Laps DF shape: {laps_df.shape}")

        lap_weather_df = self._prepare_lap_weather_df(session, season, grand_prix, session_type, ingestion_ts)
        print(f"[DEBUG] Lap weather DF shape: {lap_weather_df.shape}")

        results_df = self._prepare_results_df(session, season, grand_prix, session_type, ingestion_ts)
        print(f"[DEBUG] Results DF shape: {results_df.shape}")

        print("[DEBUG] END INGEST SESSION")
        print("==============================\n")

        return {
            "laps": laps_df,
            "lap_weather": lap_weather_df,
            "results": results_df,
        }

    def _prepare_laps_df(self, session, season, grand_prix, session_type, ingestion_ts):
        laps = session.laps

        if laps is None or laps.empty:
            print("[WARNING] Laps data is EMPTY")
            return self._empty_df_with_metadata(season, grand_prix, session_type, ingestion_ts)

        print("[DEBUG] Laps data FOUND")

        laps_df = laps.copy().reset_index(drop=True)
        return self._add_metadata(laps_df, season, grand_prix, session_type, ingestion_ts)

    def _prepare_lap_weather_df(self, session, season, grand_prix, session_type, ingestion_ts):
        laps = session.laps

        if laps is None or laps.empty:
            print("[WARNING] Laps missing → cannot build weather")
            return self._empty_df_with_metadata(season, grand_prix, session_type, ingestion_ts)

        lap_weather_df = session.laps.get_weather_data()

        if lap_weather_df is None or lap_weather_df.empty:
            print("[WARNING] Lap weather is EMPTY")
            return self._empty_df_with_metadata(season, grand_prix, session_type, ingestion_ts)

        print("[DEBUG] Lap weather FOUND")

        laps_df = laps.copy().reset_index(drop=True)
        lap_weather_df = lap_weather_df.reset_index(drop=True)

        if "Driver" in laps_df.columns:
            lap_weather_df["driver"] = laps_df["Driver"].values

        if "LapNumber" in laps_df.columns:
            lap_weather_df["lap_number"] = laps_df["LapNumber"].values

        return self._add_metadata(lap_weather_df, season, grand_prix, session_type, ingestion_ts)

    def _prepare_results_df(self, session, season, grand_prix, session_type, ingestion_ts):
        results = getattr(session, "results", None)

        if results is None:
            print("[WARNING] Results EMPTY")
            return self._empty_df_with_metadata(season, grand_prix, session_type, ingestion_ts)

        print("[DEBUG] Results FOUND")

        results_df = results.copy()

        if isinstance(results_df, pd.Series):
            results_df = results_df.to_frame().T

        results_df = results_df.reset_index(drop=True)
        return self._add_metadata(results_df, season, grand_prix, session_type, ingestion_ts)

    def _empty_df_with_metadata(self, season, grand_prix, session_type, ingestion_ts):
        return pd.DataFrame({
            "season": [season],
            "grand_prix": [grand_prix],
            "session_type": [session_type],
            "source": ["fastf1"],
            "ingestion_timestamp": [ingestion_ts],
        })

    def _add_metadata(self, df, season, grand_prix, session_type, ingestion_ts):
        df["season"] = season
        df["grand_prix"] = grand_prix
        df["session_type"] = session_type
        df["source"] = "fastf1"
        df["ingestion_timestamp"] = ingestion_ts
        return df
    

    def slugify(self, value: str) -> str:
        return (
            value.strip()
            .lower()
            .replace(" ", "_")
            .replace("-", "_")
            .replace("/", "_")
        )