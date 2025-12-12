# Job Market Intelligence

> Work in progress — project to collect job listings, clean & analyze the data, and build ML models to predict salaries and recommend skills.

---

## Status

**Backend, frontend UI, and model pipeline are implemented. Only deeper data analysis/visualization work is pending.**

This repository now includes scraping, preprocessing, ML model inference API, and a working frontend connected to the backend. Advanced analytics dashboards are still in progress.

---

## Quick overview

**What this does**

* Scrapes job boards to collect job title, company, location, skills, posted date, salary (when available), and full job description.
* Cleans and normalizes the dataset: salary parsing, missing-value handling, skill extraction, date normalization.
* Runs exploratory analysis and notebooks to find trends in skills and compensation.
* Trains baseline ML models that predict salaries and highlight important skills.
* Exposes APIs to query processed data and model predictions (backend).

**Tech stack**

* Python (Pandas, NumPy)
* Scikit-Learn (baseline ML)
* Jupyter Notebooks (analysis & experiments)
* Backend (Python API) — development-ready
* Power BI (optional dashboards)

---

## Repository structure

```
.venv/              # optional - local virtual environment (ignored)
analysis/           # analysis scripts and reports
backend/            # backend API (development)
data/               # raw & processed datasets (not all included)
model/              # saved models, training code
notebook/           # jupyter notebooks for EDA and experiments
src/                # shared utilities and modules
requirements.txt
README.md
```

---

## Getting started (developer)

> These are generic instructions. If the backend has a README or an entrypoint file (e.g. `backend/app.py` or `backend/main.py`), prefer following that file.

1. Clone the repo

```bash
git clone https://github.com/priyanshsingh11/Job-Market.git
cd Job-Market
```

2. Create & activate a virtual environment

* macOS / Linux

```bash
python3 -m venv .venv
source .venv/bin/activate
```

* Windows (PowerShell)

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

3. Install dependencies

```bash
pip install -r requirements.txt
```

4. Running the backend (development)

> The exact entrypoint may vary. Common commands that should work if the backend exposes a FastAPI/ASGI app:

```bash
# if the app object is in backend.main:app
uvicorn backend.main:app --reload

# or if the entrypoint file is backend/app.py with `app` exposed
uvicorn backend.app:app --reload
```

If you encounter an `ImportError` or `ModuleNotFoundError`, open the `backend` folder and look for the file that creates the `app` or the CLI instructions.

5. Jupyter notebooks

```bash
jupyter lab  # or jupyter notebook
```

Open the notebooks in `notebook/` for EDA and experiments.

---

## Data

* `data/` currently holds RAW and processed CSVs used for training and analysis. Some datasets may be large and excluded from the repo — check `.gitignore` and the `data/` README (if available).
* When re-running experiments, ensure consistent preprocessing (documented inside notebooks and `src/` utilities).

---

## Models

* Trained models and pickles (if any) live under `model/`.
* Model training notebooks and scripts are in `notebook/` and `src/` respectively.
* Use the same preprocessing pipeline during inference as you used in training.

---

## API (currently development)

* The backend exposes endpoints to fetch processed data and to get model predictions.
* Example (replace with the real endpoints after checking `backend` code):

  * `GET /jobs/sample` — sample job listings
  * `POST /predict/salary` — salary prediction for a job description

Please check the backend code for exact routes and payload shapes.

---

## Roadmap / TODOs

* [x] Web scraping & dataset creation
* [x] Data cleaning & preprocessing utilities
* [x] ML model training (baseline salary prediction)
* [x] Backend API for job predictions + data access
* [x] Frontend UI (React) integrated with backend
* [ ] Advanced analytics + insights (Power BI / frontend visualizations)
* [ ] Improve model accuracy and feature engineering
* [ ] Deployment to production

---

## Contributing

1. Fork the repo
2. Create a branch for your feature: `git checkout -b feat/your-feature`
3. Commit changes and open a PR describing the work

Please include tests for important utilities and document any new endpoints or data fields you introduce.

---

## Notes from author

* Frontend is intentionally not present yet — this README clarifies the project's current state and the steps required to run backend and experiments locally.
* If you plan to add the frontend, I recommend using the existing backend endpoints for prototyping and adding a small README in `frontend/` describing start commands.

---

## License

Add your preferred license here (e.g. MIT). Example:

```
MIT License
Copyright (c) 2025 Priyansh Singh
```

---

## Contact

Priyansh Singh — link your email or GitHub profile here.

---

*This README was generated/updated to reflect the current project state and to clearly state that the frontend is pending.*
