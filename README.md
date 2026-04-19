# 🏎️ Motorsport Data Platform

##  What this project is about

This project is an end-to-end **Data Engineering + Machine Learning pipeline** built on top of FastF1 data.

The main idea is simple:

 **Predict a driver’s lap time right when a new lap starts**, using only what is known up to that moment.

---

##  Data Architecture

The project follows a **Medallion Architecture (Bronze → Silver → Gold)** to keep things clean and scalable.

### 🥉 Bronze
- Raw FastF1 data as it comes from the API  
- Stored without transformation  
- Used for reproducibility  

---

### 🥈 Silver
- Cleaned and structured lap-level dataset  
- One row = **one driver lap**  
- Fixed data types, removed inconsistencies  

---

### 🥇 Gold

####  Analytical Gold
- Driver and constructor race summaries  
- Used for exploration and performance analysis  

####  ML Gold (main focus)
- Final dataset for training the model  

 One row = **driver at lap L**  
 Target = **lap time of that lap**

---

##  Machine Learning Problem

At the start of a lap, we want to answer:

> **"How fast will this lap be?"**

---

##  Features used

###  Previous laps (most important)
- `last_lap_time_ms`  
- `avg_lap_time_last_2`  
- `avg_lap_time_last_3`  

###  Tyre & race context
- `compound`  
- `tyre_life`  
- `stint`  
- `fresh_tyre`  

###  Track conditions
- `is_green`  
- `is_yellow`  
- `is_safety_car`  
- `is_vsc`  
- `is_red_flag`  

###  Weather
- `track_temp`  
- `air_temp`  
- `humidity`  
- `wind_speed`  
- `pressure`  

---

##  Data Leakage

Special care was taken to avoid data leakage:

- Only **past laps** or **lap-start information** are used  
- No information from the current lap outcome is used as input  

👉 This ensures the model reflects a realistic real-time scenario

---

##  Current State

- Full Medallion pipeline implemented  
- Lap-level dataset ready for ML  
- Time-series feature engineering completed  

---

##  Next Steps

- Train regression models:
  - Linear Regression (baseline)
  - Random Forest  

- Use **time-based split** (no random split)

- Evaluate performance:
  - MAE (Mean Absolute Error)
  - RMSE  

- Track experiments with MLflow  

- Deploy predictions using FastAPI  

---

## 🎯 Goal

Build a system that can estimate lap times in real time based on:

- Driver performance  
- Tyre condition  
- Race situation  
- Track and weather conditions  

---

## Why this project

This project demonstrates:

- Strong Data Engineering (Medallion architecture)
- Time-series feature engineering
- ML pipeline design with leakage awareness
- Real-world application in motorsport analytics
