# Job Market Intelligence

An end-to-end platform for job market data collection, analysis, and salary prediction using machine learning.

## Project Demo

<video src="./Recording%202025-12-14%20180127.mp4" controls width="100%"></video>

---

## System Architecture

The platform follows a modular architecture designed for scalable data ingestion, automated feature engineering, and high-performance model serving.

```mermaid
graph TB
    subgraph Data_Orchestration [Data Acquisition & Orchestration]
        S[RapidSearch API / JSearch] --> R[(Raw Data Lake)]
        R --> P[ETL / Cleaning Pipeline]
        P --> F[(Feature Store)]
    end

    subgraph Service_Layer [Inference & Service Layer]
        F --> B[FastAPI Engine]
        B --> M[CatBoost Inference Engine]
        B --> C[Career Guidance Logic]
    end

    subgraph Interface_Layer [Client Interface]
        B <--> UI[React Modern UI]
        UI --> D[Interactive Analytics]
    end

    subgraph Model_Lifecycle [Model Management]
        F --> T[Model Training / Validation]
        T --> M
    end
```

- **Data Acquisition**: Automated ingestion pipeline powered by **RapidSearch API** (JSearch), collecting thousands of job listings across multiple dimensions including role, seniority, and geography.
- **Service Layer**: Asynchronous FastAPI implementation for low-latency model inference and complex business logic execution.
- **ML Engine**: High-performance gradient boosting implementation (CatBoost) optimized for categorical feature handling and high precision salary estimation.
- **Client Interface**: Highly responsive React architecture utilizing Vite for optimized delivery and real-time state management.

---

## Technical Stack

- **Languages**: Python, JavaScript
- **ML**: CatBoost, Scikit-Learn, Pandas, NumPy
- **API/Data**: RapidSearch API (JSearch), FastAPI, Uvicorn
- **Frontend**: React, Vite
- **Analysis**: Jupyter Notebooks

---

## Setup Instructions

### Environment
1. Clone the repository: `git clone https://github.com/priyanshsingh11/Job-Market.git`
2. Initialize virtual environment:
   - Windows: `python -m venv .venv; .\.venv\Scripts\Activate.ps1`
   - Unix: `python3 -m venv .venv; source .venv/bin/activate`
3. Install dependencies: `pip install -r requirements.txt`

### Execution
- **Backend**: `uvicorn backend.main:app --reload`
- **Frontend**: `cd frontend; npm install; npm run dev`
- **Analysis**: `jupyter lab` (Navigate to `notebook/`)

---

## Repository Structure

- `backend/`: FastAPI implementation and model serving.
- `frontend/`: React application.
- `model/`: Trained models and serialization.
- `notebook/`: Exploratory Data Analysis and training experiments.
- `src/`: Core logic and data utilities.
- `data/`: Datasets (Raw and Processed).

---

## License
MIT License
Copyright (c) 2025 Priyansh Singh
