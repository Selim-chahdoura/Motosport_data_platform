from pathlib import Path
import joblib

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from src.services.data_processor_for_model import DataProcessor


app = FastAPI(title="Motorsport Prediction API")

# Paths
BASE_DIR = Path(__file__).resolve().parents[2]
MODEL_PATH = BASE_DIR / "models" / "model_pipeline.pkl"

# Load model + processor once when API starts
model = joblib.load(MODEL_PATH)
processor = DataProcessor()


class PredictionInput(BaseModel):
    season: int
    round: int
    grid: int
    circuit_id: str
    driver_id: str
    constructor_id: str


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/predict")
def predict(data: PredictionInput):
    try:
        input_df = processor.build_features(data)
        prediction = model.predict(input_df)[0]

        return {
            "prediction": int(prediction),
            "label": "Top 10" if int(prediction) == 1 else "Not Top 10"
        }

    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Unexpected error: {str(e)}")

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")