from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestClassifier
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder


NUMERICAL_FEATURES = [
    "season",
    "round",
    "grid",
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
    "constructor_avg_position_gain",
]

CATEGORICAL_FEATURES = [
    "circuit_id",
    "driver_id",
    "constructor_id",
]


def build_pipeline() -> Pipeline:
    """
    Build the full sklearn pipeline:
    - One-hot encode categorical features
    - pass numerical features unchanged
    - train a Random Forest classifier
    """
    preprocessor = ColumnTransformer(
        transformers=[
            (
                "categorical",
                OneHotEncoder(handle_unknown="ignore"),
                CATEGORICAL_FEATURES,
            ),
            (
                "numerical",
                "passthrough",
                NUMERICAL_FEATURES,
            ),
        ]
    )

    pipeline = Pipeline(
        steps=[
            ("preprocessor", preprocessor),
            ("model", RandomForestClassifier(n_estimators=100, random_state=42)),
        ]
    )

    return pipeline