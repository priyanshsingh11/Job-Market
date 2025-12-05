import os
import time
import re
import requests
import pandas as pd
from datetime import datetime
from typing import Optional

# ==========================
# CONFIG
# ==========================

API_KEY = "85f5497af7msh54af678d864ce2bp1d1ce2jsna380c053b1f9"
BASE_URL = "https://jsearch.p.rapidapi.com/search"

HEADERS = {
    "x-rapidapi-key": API_KEY,
    "x-rapidapi-host": "jsearch.p.rapidapi.com",
}

SAVE_DIR = "data/raw"
SAVE_PATH = os.path.join(SAVE_DIR, "jobs_big_dataset.csv")

# Try to import JOB_FIELDS from project config; fall back to default
try:
    from ..config import JOB_FIELDS
except Exception:
    JOB_FIELDS = [
        "job_id",
        "title",
        "company",
        "location",
        "source",
        "posted_date",
        "employment_type",
        "experience_level",
        "salary_min",
        "salary_max",
        "salary_currency",
        "skills_raw",
        "description",
        "url",
        "scraped_at",
        "role_query",
        "country_query",
    ]


# ==========================
# SAFE CSV SAVE (HANDLES EXCEL LOCK)
# ==========================

def safe_save_csv(df: pd.DataFrame, path: str):
    """
    Try to save CSV to `path`. If the file is locked (e.g. open in Excel),
    fall back to a timestamped backup file instead of crashing.
    """
    try:
        df.to_csv(path, index=False)
        print(f"[CHECKPOINT] Saved {len(df)} rows → {path}")
    except PermissionError:
        base, ext = os.path.splitext(path)
        backup_path = f"{base}_backup_{int(time.time())}{ext}"
        df.to_csv(backup_path, index=False)
        print(f"[WARN] Permission denied for {path} (is it open in Excel/VSCode?).")
        print(f"[WARN] Saved backup instead → {backup_path}")


# ==========================
# SALARY PARSER (HEURISTIC)
# ==========================

def parse_salary_text(s: Optional[str]):
    """Heuristic parser: extract min, max, currency from free text."""
    if not s:
        return None, None, None

    text = s.replace(",", " ")
    curr_match = re.search(r"(\$|£|€|₹)", text)
    currency = curr_match.group(1) if curr_match else None

    def _value_from_token(tok: str):
        multiplier = 1
        if re.search(r"[kK]$", tok):
            multiplier = 1000
            tok = re.sub(r"[kK]$", "", tok)
        if re.search(r"[mM]$", tok):
            multiplier = 1_000_000
            tok = re.sub(r"[mM]$", "", tok)
        try:
            return float(re.sub(r"[^0-9.]", "", tok)) * multiplier
        except Exception:
            return None

    # patterns like "50k - 70k", "10,00,000 - 15,00,000"
    range_match = re.search(r"(\d[\d,.kKmM]*)\s*[-–to]{1,3}\s*(\d[\d,.kKmM]*)", text)
    if range_match:
        v1 = _value_from_token(range_match.group(1))
        v2 = _value_from_token(range_match.group(2))
        if v1 is not None and v2 is not None:
            return v1, v2, currency

    # single number like "12 LPA", "80k"
    num_match = re.search(r"\d[\d,.kKmM]*", text)
    if num_match:
        v = _value_from_token(num_match.group(0))
        return v, v, currency

    return None, None, currency


# ==========================
# MAPPING FUNCTION
# ==========================

def map_rapid_item_to_fields(item: dict):
    """Map JSearch API item to unified JOB_FIELDS dict."""
    salary_min = item.get("job_min_salary") or item.get("min_salary")
    salary_max = item.get("job_max_salary") or item.get("max_salary")
    salary_currency = item.get("job_salary_currency") or item.get("salary_currency")
    salary_str = (
        item.get("job_salary")
        or item.get("salary")
        or item.get("salary_string")
        or ""
    )

    # If no structured salary, try to parse from text
    if not (salary_min or salary_max or salary_currency):
        salary_min, salary_max, salary_currency = parse_salary_text(
            salary_str
            or item.get("job_description")
            or item.get("job_title")
            or ""
        )

    exp = item.get("job_required_experience") or {}

    mapped = {
        "job_id": item.get("job_id") or item.get("id"),
        "title": item.get("job_title") or item.get("title"),
        "company": item.get("employer_name") or item.get("company_name"),
        "location": (
            item.get("job_city")
            or item.get("job_state")
            or item.get("job_country")
            or item.get("location")
        ),
        "source": item.get("job_publisher") or item.get("source") or "jsearch",
        "posted_date": item.get("job_posted_at_datetime_utc") or item.get("date_posted"),
        "employment_type": item.get("job_employment_type"),
        "experience_level": exp.get("experience_level") if isinstance(exp, dict) else item.get("experience_level"),
        "salary_min": salary_min,
        "salary_max": salary_max,
        "salary_currency": salary_currency,
        "skills_raw": item.get("job_required_skills") or item.get("skills"),
        "description": item.get("job_description") or item.get("description"),
        "url": item.get("job_apply_link") or item.get("job_url") or item.get("url"),
        "scraped_at": datetime.utcnow().isoformat(),
        "role_query": None,
        "country_query": None,
    }

    # Ensure we only keep keys present in JOB_FIELDS
    return {k: mapped.get(k) for k in JOB_FIELDS}


# ==========================
# FETCH ONE PAGE FROM JSEARCH
# ==========================

def fetch_rapidapi_page(query: str, location: str, country_code: str, page: int = 1):
    params = {
        "query": f"{query} in {location}",
        "page": str(page),
        "num_pages": "1",
        "date_posted": "all",
        "country": country_code,
        "language": "en",
    }

    print(f"[INFO] Fetching {query} | {location} | Page {page}")
    resp = requests.get(BASE_URL, headers=HEADERS, params=params, timeout=25)

    # Handle 429 rate limit: wait and retry once
    if resp.status_code == 429:
        print("[WARN] 429 Too Many Requests → sleeping 20s and retrying once...")
        time.sleep(20)
        resp = requests.get(BASE_URL, headers=HEADERS, params=params, timeout=25)

    # If still bad or other error
    if resp.status_code != 200:
        print(f"[WARN] Error {resp.status_code} for {query} | {location} | Page {page}")
        return []

    data = resp.json().get("data", [])
    print(f"[INFO] Got {len(data)} jobs")
    return data


# ==========================
# MAIN COLLECTION LOOP (WITH RESUME + CHECKPOINTS)
# ==========================

def collect_jobs_with_checkpoints(
    target_count: int = 1200,
    pages_per_combo: int = 8,
    pause: float = 1.0,
    save_path: str = SAVE_PATH,
    resume: bool = True,
):
    """
    Collect jobs until we hit roughly target_count (or exhaust combos),
    and save checkpoints to CSV after every page.
    If resume=True and CSV exists, load existing data and continue scraping.
    """
    roles = [
        "Data Analyst",
        "Business Analyst",
        "Data Scientist",
        "Machine Learning Engineer",
        "Data Engineer",
        "Data Architect",
        "Analytics Engineer",
        "BI Analyst",
        "Statistician",
        "AI Engineer",
        "Python Developer",
        "Backend Developer",
    ]

    # (location text, country code)
    locations = [
        ("India", "in"),
        ("Bengaluru", "in"),
        ("Hyderabad", "in"),
        ("United States", "us"),
    ]

    all_records = []
    seen_ids = set()

    # ===== Resume from existing CSV if enabled =====
    if resume and os.path.exists(save_path):
        try:
            existing_df = pd.read_csv(save_path)
            print(f"[INFO] Found existing CSV → loading {save_path}")
            print(f"[INFO] Existing rows: {len(existing_df)}")
            # Normalize columns
            for col in JOB_FIELDS:
                if col not in existing_df.columns:
                    existing_df[col] = None

            existing_df = existing_df[JOB_FIELDS]
            existing_records = existing_df.to_dict(orient="records")
            all_records.extend(existing_records)

            # Build seen_ids from existing data
            for r in existing_records:
                jid = r.get("job_id") or r.get("url")
                if jid:
                    seen_ids.add(jid)

            # If we already met target, just return
            if len(all_records) >= target_count:
                print("[INFO] Target already reached in existing CSV. No new scrape needed.")
                return existing_df

        except Exception as e:
            print(f"[WARN] Failed to load existing CSV, starting fresh: {e}")

    total_combos = len(roles) * len(locations)
    combo_idx = 0

    for role in roles:
        for (loc, ccode) in locations:
            combo_idx += 1
            print("\n" + "=" * 60)
            print(f"[INFO] Combo {combo_idx}/{total_combos} → Role: {role} | Location: {loc}")

            for page in range(1, pages_per_combo + 1):
                # Stop if we already hit target
                if len(all_records) >= target_count:
                    print("[INFO] Target reached, stopping collection loop.")
                    df_final = pd.DataFrame(all_records, columns=JOB_FIELDS)
                    safe_save_csv(df_final, save_path)
                    print(f"[CHECKPOINT] Final CSV saved (or backup created).")
                    return df_final

                items = []
                try:
                    items = fetch_rapidapi_page(role, loc, ccode, page=page)
                except Exception as e:
                    print(f"[ERROR] Request failed for {role} | {loc} | Page {page}: {e}")
                    break

                if not items:
                    print("[INFO] No items returned → moving to next combo")
                    break

                new_count_before = len(all_records)

                for it in items:
                    mapped = map_rapid_item_to_fields(it)

                    # Build a stable ID
                    jid = mapped.get("job_id") or mapped.get("url")
                    if not jid:
                        key_src = f"{mapped.get('title','')}_{mapped.get('company','')}_{mapped.get('location','')}"
                        jid = str(abs(hash(key_src)))
                        mapped["job_id"] = jid

                    if jid in seen_ids:
                        continue
                    seen_ids.add(jid)

                    # Add metadata for query
                    mapped["role_query"] = role
                    mapped["country_query"] = ccode

                    all_records.append(mapped)

                new_count_after = len(all_records)
                print(f"[INFO] Collected so far: {new_count_after} jobs (+{new_count_after - new_count_before} this page)")

                # 🔥 CHECKPOINT SAVE AFTER EACH PAGE
                df_checkpoint = pd.DataFrame(all_records, columns=JOB_FIELDS)
                safe_save_csv(df_checkpoint, save_path)

                time.sleep(pause)

    # If loop finishes naturally, save final
    df_final = pd.DataFrame(all_records, columns=JOB_FIELDS)
    safe_save_csv(df_final, save_path)
    print(f"[CHECKPOINT] Final CSV saved (or backup created).")
    return df_final


# ==========================
# ENTRY POINT
# ==========================

if __name__ == "__main__":
    os.makedirs(SAVE_DIR, exist_ok=True)

    df = collect_jobs_with_checkpoints(
        target_count=1200,   # Aim for ~1000–1200 rows
        pages_per_combo=8,   # Increase if not enough jobs
        pause=1.0,           # Be polite to the API
        save_path=SAVE_PATH,
        resume=True,         # Resume from existing CSV if present
    )

    print("\n" + "=" * 60)
    print(f"[INFO] Scraping completed. Total jobs: {len(df)}")
    print(f"[INFO] Final CSV at → {SAVE_PATH}")
