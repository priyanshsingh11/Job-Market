from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from catboost import CatBoostRegressor
import joblib
import pandas as pd
import re
from pathlib import Path

# =========================
# Paths
# =========================

# this file: .../Job Market/backend/main.py
BASE_DIR = Path(__file__).resolve().parent
MODEL_DIR = BASE_DIR.parent / "model" / "models"

MODEL_PATH = MODEL_DIR / "salary_model.cbm"
META_PATH = MODEL_DIR / "model_meta.joblib"

# =========================
# Load model + metadata
# =========================

model = CatBoostRegressor()
model.load_model(str(MODEL_PATH))

meta = joblib.load(str(META_PATH))
features_better = meta["features"]          # same key you saved
seniority_map = meta["seniority_map"]

# =========================
# Paste your skill_map + assign_skills
# =========================
# ⚠️ VERY IMPORTANT:
# Copy the SAME skill_map and assign_skills() you used
# in your training notebook, and paste them here.

# Example placeholder (REPLACE with your full version):

skill_map = {
    "data analyst": ["python", "sql", "excel", "tableau", "power bi", "statistics", "data cleaning", "eda", "postgresql", "mysql"],
    "business analyst": ["sql", "excel", "power bi", "tableau", "requirements gathering", "business strategy", "documentation", "stakeholder communication"],
    "product analyst": ["sql", "python", "a/b testing", "tableau", "product metrics", "experimentation", "dashboards"],
    "operations analyst": ["excel", "sql", "process optimization", "kpi analysis", "tableau", "forecasting"],
    "marketing analyst": ["sql", "python", "google analytics", "marketing metrics", "tableau", "excel"],
    "finance analyst": ["excel", "sql", "forecasting", "financial modeling", "statistics", "power bi"],
    "risk analyst": ["sql", "python", "risk modeling", "statistics", "excel", "regression analysis"],
    "reporting analyst": ["power bi", "tableau", "excel", "sql", "dashboarding", "kpi reporting"],

    "bi analyst": ["power bi", "tableau", "sql", "dax", "data modeling", "ssrs", "ssas", "excel"],
    "analytics engineer": ["sql", "dbt", "python", "snowflake", "airflow", "bigquery", "data modeling"],

    "data scientist": ["python", "machine learning", "statistics", "data preprocessing", "tensorflow", "pytorch", "feature engineering", "sql", "scikit-learn"],
    "statistician": ["statistics", "r", "python", "probability", "regression", "anova", "hypothesis testing"],
    "mathematician": ["linear algebra", "probability", "optimization", "python", "statistics"],

    "ml engineer": ["python", "machine learning", "mlops", "docker", "kubernetes", "aws", "tensorflow", "pytorch", "airflow", "feature store"],
    "ai engineer": ["python", "deep learning", "transformers", "gen ai", "llms", "tensorflow", "pytorch", "huggingface"],
    "deep learning engineer": ["python", "pytorch", "tensorflow", "cnn", "rnn", "transformers", "opencv"],
    "computer vision engineer": ["opencv", "pytorch", "tensorflow", "image processing", "yolo", "segmentation"],
    "nlp engineer": ["nlp", "llms", "transformers", "bert", "huggingface", "python", "text preprocessing"],

    "data engineer": ["python", "sql", "spark", "airflow", "aws", "azure", "gcp", "kafka", "snowflake", "data pipelines"],
    "data architect": ["data modeling", "aws", "azure", "gcp", "snowflake", "db design", "data governance"],
    "cloud architect": ["aws", "azure", "gcp", "terraform", "docker", "kubernetes", "cloud security"],
    "solution architect": ["systems design", "cloud architecture", "api design", "scalability", "microservices"],
    "analytics architect": ["data modeling", "bi tools", "cloud", "sql", "data strategy"],

    "python developer": ["python", "django", "flask", "fastapi", "rest api", "git", "sql", "docker"],
    "software engineer": ["java", "python", "data structures", "system design", "git", "docker", "sql"],

    "data analyst trainee": ["excel", "sql", "python basics", "tableau", "statistics basics"],
    "intern": ["python", "sql", "excel"],

    "other": ["python", "sql", "excel"]
}


def assign_skills(role: str) -> str:
    r = str(role).lower()
    for key in skill_map:
        if key in r:
            return ", ".join(skill_map[key])
    return ", ".join(skill_map["other"])


# =========================
# Helper functions
# =========================

def has(pattern: str, text: str) -> int:
    return int(bool(re.search(pattern, text.lower())))

tier1 = ["bengaluru", "bangalore", "hyderabad", "mumbai",
         "pune", "gurugram", "delhi", "noida", "chennai"]
tier2 = ["jaipur", "indore", "ahmedabad", "lucknow", "bhopal"]

def get_city_tier(city: str) -> int:
    c = str(city).lower()
    if c in tier1:
        return 1
    if c in tier2:
        return 2
    return 3


# =========================
# FastAPI setup
# =========================

app = FastAPI(title="Job Market Salary & Skills API")

# Allow frontend access from localhost during development
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],   # for dev; later restrict to your frontend URL
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class PredictRequest(BaseModel):
    employment_type: str
    city: str
    seniority: str
    role: str

class PredictResponse(BaseModel):
    predicted_salary: float
    skills_required: str


# =========================
# Core prediction logic
# =========================

def predict_salary_and_skills_backend(req: PredictRequest):
    # get skills from your mapping
    skills_required = assign_skills(req.role)
    s = skills_required.lower()

    row = {
        "employment_type": req.employment_type,
        "city": req.city,
        "primary_role": req.role.lower(),
        "seniority_num": seniority_map.get(req.seniority.lower(), 2),
        "city_tier": get_city_tier(req.city),
        "has_programming": has(r"python|java|r\b", s),
        "has_bigdata": has(r"spark|kafka|hadoop", s),
        "has_devops": has(r"docker|kubernetes|airflow", s),
        "has_cloud": has(r"aws|azure|gcp", s),
        "has_ds": has(r"machine learning|deep learning|tensorflow|pytorch|sklearn", s),
        "has_db": has(r"sql|mysql|postgres|nosql", s),
        "has_bi": has(r"tableau|power bi|dax", s),
    }

    df_input = pd.DataFrame([row])[features_better]
    pred = float(model.predict(df_input)[0])

    return pred, skills_required


# =========================
# API endpoint
# =========================

@app.post("/predict", response_model=PredictResponse)
def predict(req: PredictRequest):
    salary, skills = predict_salary_and_skills_backend(req)
    return PredictResponse(predicted_salary=salary, skills_required=skills)
