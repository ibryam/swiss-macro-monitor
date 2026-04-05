"""
ingest_oecd.py

Pulls Swiss macroeconomic indicators from the OECD KEI
(Key short-term Economic Indicators) API into BigQuery.

No authentication required — public SDMX REST API.
Runs monthly (1st of each month via GitHub Actions).

Dataset: OECD.SDD.STES, DSD_KEI@DF_KEI
Base URL: https://sdmx.oecd.org/public/rest/data/

Series pulled:
    GDP volume growth (quarterly, YoY)
    Manufacturing production volume growth (monthly, YoY)
    Retail trade volume growth (monthly, YoY)
    Merchandise exports growth (quarterly, YoY)
    Employment (quarterly)
    Immediate interest rate / call money rate (monthly)
"""

import os
import io
import json
import requests
import pandas as pd
from datetime import datetime, timezone
from dotenv import load_dotenv
from google.cloud import bigquery
from google.oauth2 import service_account

load_dotenv()

GCP_PROJECT = "swiss-macro-monitor"
DATASET     = "swiss_macro_raw"
TABLE       = "raw_oecd_indicators"
BASE_URL    = (
    "https://sdmx.oecd.org/public/rest/data/"
    "OECD.SDD.STES,DSD_KEI@DF_KEI/CHE..........?"
    "startPeriod={start}&format=csvfilewithlabels"
)

# Filters to apply after fetching all Swiss KEI data
# (measure_code, activity_code, adjustment, transformation, indicator_name, category, unit, frequency)
SERIES_FILTERS = [
    ("B1GQ_Q",  "_T", "Y", "GY", "GDP Volume Growth YoY",               "growth",   "percent",  "quarterly"),
    ("PRVM",    "C",  "Y", "GY", "Manufacturing Production Growth YoY",  "growth",   "percent",  "monthly"),
    ("TOVM",    "G47","Y", "GY", "Retail Trade Volume Growth YoY",       "growth",   "percent",  "monthly"),
    ("EX",      "_T", "Y", "G1", "Merchandise Exports Growth QoQ",       "external", "percent",  "quarterly"),
    ("EMP",     "_T", "Y", "_Z", "Employment Total",                     "labour",   "thousands","quarterly"),
    ("UNEMP",   "_T", "Y", "_Z", "Unemployment Rate",                    "labour",   "percent",  "quarterly"),
    ("IRSTCI",  "_Z", "_Z","_Z", "Call Money Interbank Rate",            "monetary", "percent",  "monthly"),
]


def fetch_all_swiss_kei(start: str = "2000-01") -> pd.DataFrame:
    """Fetch all Swiss KEI indicators in one API call."""
    url      = BASE_URL.format(start=start)
    response = requests.get(url, timeout=60)
    response.raise_for_status()

    if response.text.strip() in ("NoResultsFound", "NoRecordsFound"):
        raise ValueError("OECD returned no data for Switzerland KEI.")

    df = pd.read_csv(io.StringIO(response.text), dtype=str)
    return df


def extract_series(
    df: pd.DataFrame,
    measure: str,
    activity: str,
    adjustment: str,
    transformation: str,
    indicator_name: str,
    category: str,
    unit: str,
    frequency: str,
) -> list[dict]:
    """Filter the full KEI dataframe to one specific series and return rows."""
    mask = (
        (df["MEASURE"]        == measure)       &
        (df["ACTIVITY"]       == activity)      &
        (df["ADJUSTMENT"]     == adjustment)    &
        (df["TRANSFORMATION"] == transformation)
    )
    filtered = df[mask].copy()
    filtered["OBS_VALUE"] = pd.to_numeric(filtered["OBS_VALUE"], errors="coerce")
    filtered = filtered.dropna(subset=["OBS_VALUE"])

    rows = []
    for _, row in filtered.iterrows():
        series_id = f"KEI/CHE/{measure}/{activity}/{transformation}"
        rows.append({
            "date":                str(row["TIME_PERIOD"]),
            "source":              "OECD",
            "series_id":           series_id,
            "indicator_name":      indicator_name,
            "indicator_category":  category,
            "value":               float(row["OBS_VALUE"]),
            "unit":                unit,
            "frequency":           frequency,
            "ingested_at":         datetime.now(timezone.utc).isoformat(),
        })
    return rows


def ensure_table(client: bigquery.Client) -> None:
    """Create the raw table if it doesn't exist."""
    dataset_ref = bigquery.Dataset(f"{GCP_PROJECT}.{DATASET}")
    dataset_ref.location = "EU"
    client.create_dataset(dataset_ref, exists_ok=True)

    schema = [
        bigquery.SchemaField("date",               "STRING"),
        bigquery.SchemaField("source",             "STRING"),
        bigquery.SchemaField("series_id",          "STRING"),
        bigquery.SchemaField("indicator_name",     "STRING"),
        bigquery.SchemaField("indicator_category", "STRING"),
        bigquery.SchemaField("value",              "FLOAT64"),
        bigquery.SchemaField("unit",               "STRING"),
        bigquery.SchemaField("frequency",          "STRING"),
        bigquery.SchemaField("ingested_at",        "TIMESTAMP"),
    ]

    table_ref = f"{GCP_PROJECT}.{DATASET}.{TABLE}"
    table     = bigquery.Table(table_ref, schema=schema)
    client.create_table(table, exists_ok=True)
    print(f"Table ready: {table_ref}")


def get_last_loaded_date(client: bigquery.Client, series_id: str) -> str | None:
    """Return the latest date already loaded for this series."""
    query = f"""
        SELECT MAX(date) as max_date
        FROM `{GCP_PROJECT}.{DATASET}.{TABLE}`
        WHERE series_id = '{series_id}'
    """
    result = list(client.query(query).result())
    if result and result[0].max_date:
        return str(result[0].max_date)
    return None


def load_rows(client: bigquery.Client, rows: list[dict]) -> None:
    """Insert rows into BigQuery (sandbox compatible)."""
    if not rows:
        print("  No new rows to load.")
        return
    table_ref  = f"{GCP_PROJECT}.{DATASET}.{TABLE}"
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    )
    job = client.load_table_from_json(rows, table_ref, job_config=job_config)
    job.result()
    print(f"  Loaded {len(rows)} rows.")


def get_bigquery_client() -> bigquery.Client:
    key_json = os.getenv("GCP_SERVICE_ACCOUNT_KEY", "").strip()
    if key_json:
        info        = json.loads(key_json)
        credentials = service_account.Credentials.from_service_account_info(
            info, scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        return bigquery.Client(project=GCP_PROJECT, credentials=credentials)
    return bigquery.Client(project=GCP_PROJECT)


def main():
    client = get_bigquery_client()
    ensure_table(client)

    print("Fetching all Swiss KEI data from OECD...")
    df = fetch_all_swiss_kei(start="2000-01")
    print(f"  Raw rows fetched: {len(df)}")

    for measure, activity, adjustment, transformation, name, category, unit, frequency in SERIES_FILTERS:
        series_id = f"KEI/CHE/{measure}/{activity}/{transformation}"
        print(f"Extracting {name} ({series_id})...")

        rows = extract_series(df, measure, activity, adjustment, transformation, name, category, unit, frequency)

        last_date = get_last_loaded_date(client, series_id)
        if last_date:
            rows = [r for r in rows if r["date"] > last_date]
            print(f"  Incremental load from {last_date} — {len(rows)} new rows")
        else:
            print(f"  Full load — {len(rows)} rows")

        load_rows(client, rows)

    print("OECD ingestion complete.")


if __name__ == "__main__":
    main()
