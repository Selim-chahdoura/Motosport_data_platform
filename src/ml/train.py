from pathlib import Path
import joblib
import pandas as pd

from sklearn.metrics import (
    accuracy_score,
    classification_report,
    confusion_matrix,
    f1_score,
    precision_score,
    recall_score,
)

from src.ml.ml_pipeline import build_pipeline


BASE_DIR = Path(__file__).resolve().parents[2]
DATA_PATH = BASE_DIR / "data_lake/gold/prediction_dataset"
MODEL_DIR = BASE_DIR / "models"
MODEL_PATH = MODEL_DIR / "model_pipeline.pkl"

TARGET_COLUMN = "finished_top_10"

FEATURE_COLUMNS = [
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
    "constructor_avg_position_gain",
]


# -----------------------------
# Load Data
# -----------------------------
def load_dataset():
    if not DATA_PATH.exists():
        raise FileNotFoundError(f"Dataset not found: {DATA_PATH}")

    return pd.read_parquet(DATA_PATH)


# -----------------------------
# Prepare Data
# -----------------------------
def prepare_dataset(df):
    df = df.copy()

    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values(by="date").reset_index(drop=True)

    df = df[FEATURE_COLUMNS + [TARGET_COLUMN]]
    df = df.dropna(subset=[TARGET_COLUMN])

    return df


# -----------------------------
# Split Data
# -----------------------------
def split_data(df):
    split_index = int(len(df) * 0.8)

    train_df = df.iloc[:split_index]
    test_df = df.iloc[split_index:]

    X_train = train_df[FEATURE_COLUMNS]
    y_train = train_df[TARGET_COLUMN]

    X_test = test_df[FEATURE_COLUMNS]
    y_test = test_df[TARGET_COLUMN]

    return X_train, X_test, y_train, y_test


# -----------------------------
# Train Model
# -----------------------------
def train_model():
    df = load_dataset()
    df = prepare_dataset(df)

    print("Dataset shape:", df.shape)

    X_train, X_test, y_train, y_test = split_data(df)

    pipeline = build_pipeline()

    # Train
    pipeline.fit(X_train, y_train)

    # Predict
    y_pred = pipeline.predict(X_test)

    # Metrics
    accuracy = accuracy_score(y_test, y_pred)
    f1 = f1_score(y_test, y_pred)
    precision = precision_score(y_test, y_pred)
    recall = recall_score(y_test, y_pred)

    print("\nEvaluation:")
    print(f"Accuracy : {accuracy:.4f}")
    print(f"F1 Score : {f1:.4f}")
    print(f"Precision: {precision:.4f}")
    print(f"Recall   : {recall:.4f}")

    print("\nClassification Report:")
    print(classification_report(y_test, y_pred))

    print("\nConfusion Matrix:")
    print(confusion_matrix(y_test, y_pred))

    # Save model
    MODEL_DIR.mkdir(parents=True, exist_ok=True)
    joblib.dump(pipeline, MODEL_PATH)

    print(f"\nModel saved to: {MODEL_PATH}")


if __name__ == "__main__":
    train_model()