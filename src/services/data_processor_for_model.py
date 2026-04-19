"""
 This class will be used to get the full features for the model to predict,
 based on the input from the API (season, round, grid, driver_id, constructor_id, circuit_id).
 It will query the database to get the necessary stats for the driver and constructor, 
 and then return a DataFrame that can be fed into the model for prediction.
"""
from pathlib import Path
from pyexpat import features
from types import SimpleNamespace
import pandas as pd


class DataProcessor:

    def __init__(self):
        # Load gold tables for driver and constructor stats
        BASE_DIR = Path(__file__).resolve().parents[2]
        self.driver_df = pd.read_parquet(BASE_DIR / "data_lake/gold/driver_stats")
        self.constructor_df = pd.read_parquet(BASE_DIR / "data_lake/gold/constructor_stats")
    
    def rename_columns(self, df, prefix, id_column):
        return df.rename(columns={
        col: f"{prefix}_{col}" for col in df.columns if col != id_column
    })

    def get_driver_features(self, driver_id: str):
        df = self.driver_df

        row = df[df["code"] == driver_id]

        if row.empty:
            raise ValueError("No driver data found")

        #row = self.rename_columns(row, "driver", "code") # To match the feature names expected by the model

        return row.drop(columns=["code"]).iloc[0].to_dict()

    def get_constructor_features(self, constructor_id: str):
        df = self.constructor_df

        row = df[df["constructor_id"] == constructor_id]

        if row.empty:
            raise ValueError("No constructor data found")

        #row = self.rename_columns(row, "constructor", "constructor_id") # To match the feature names expected by the model

        return row.drop(columns=["constructor_id"]).iloc[0].to_dict()

    def build_features(self, input_data):
        try:
            driver = self.get_driver_features(input_data.driver_id)
        except ValueError as e:
            raise ValueError(f"Error occurred while fetching driver features: {e}")

        try:
            constructor = self.get_constructor_features(input_data.constructor_id)
        except ValueError as e:
            raise ValueError(f"Error occurred while fetching constructor features: {e}")
        
        features = {
            "season": input_data.season,
            "round": input_data.round,
            "grid": input_data.grid,
            "circuit_id": input_data.circuit_id,
            "driver_id": input_data.driver_id,
            "constructor_id": input_data.constructor_id,
            **driver,
            **constructor
        }

        df = pd.DataFrame([features])

        expected_columns = [
            "season",
            "round",
            "grid",
            "circuit_id",
            "driver_id",
            "constructor_id",
            "driver_avg_points",
            "driver_avg_position",
            "driver_num_races",
            "driver_top10_finishes",
            "driver_podium_finishes",
            "driver_avg_position_gain",
            "constructor_avg_points",
            "constructor_avg_position",
            "constructor_num_races",
            "constructor_top10_finishes",
            "constructor_podium_finishes",
            "constructor_avg_position_gain"
        ]

        return df[expected_columns]

def main():
    processor = DataProcessor()

    test_input = SimpleNamespace(
        season=2024,
        round=5,
        grid=3,
        driver_id="VER",
        constructor_id="red_bull"
    )

    try:
        feature_df = processor.build_features(test_input)

        print("Built feature DataFrame:")
        print(feature_df)
        print("\nColumns:")
        print(feature_df.columns.tolist())

    except ValueError as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    main()