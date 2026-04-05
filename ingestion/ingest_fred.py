"""
ingest_fred.py

Pulls Swiss macroeconomic indicators from the FRED API into BigQuery.
Runs monthly (1st of each month via GitHub Actions).

Series pulled:
    CHEPDNSNSAM586NRUG  - Switzerland unemployment rate
    CHECPIALLMINMEI     - Switzerland CPI (all items)
    CHEGDPNQDSMEI       - Switzerland GDP (nominal, quarterly)
    CHECAINTNBIS6USD    - Switzerland current account balance
    IRLTLT01CHM156N     - Switzerland long-term interest rate (10yr govt bond)
"""

import os
import json
from datetime import datetime, timezone
from dotenv import load_dotenv
from fredapi import Fred
from google.cloud import bigquery
from google.oauth2 import service_account

load_dotenv()

FRED_API_KEY = os.getenv("FRED_API_KEY")
GCP_PROJECT  = "swiss-macro-monitor"
DATASET      = "swiss_macro_raw"
TABLE        = "raw_fred_indicators"

# Series to pull: {series_id: (indicator_name, category, unit)}
# NOTE: Unemployment (LMUNRRTTCHM156N) removed — replaced by BFS ingest_bfs.py
#       BFS publishes 3-4 days after month end vs 3-6 months lag for FRED/OECD.
#       Historical FRED unemployment rows remain in BigQuery but are no longer updated.
FRED_SERIES = {
    "CPALTT01CHM657N":   ("CPI All Items",               "prices",   "index"),
    "CLVMNACSAB1GQCH":   ("Real GDP",                   "growth",   "chf_millions"),
    "IRLTLT01CHM156N":   ("10Y Government Bond Yield",   "monetary", "percent"),
    "FPCPITOTLZGCHE":    ("CPI Annual Inflation",        "prices",   "percent"),
}


def fetch_series(fred: Fred, series_id: str, name: str, category: str, unit: str) -> list[dict]:
    """Fetch one FRED series and return as list of BigQuery rows."""
    data = fred.get_series(series_id)
    rows = []
    for date, value in data.items():
        if value != value:  # skip NaN
            continue
        rows.append({
            "date":                str(date.date()),
            "source":              "FRED",
            "series_id":           series_id,
            "indicator_name":      name,
            "indicator_category":  category,
            "value":               float(value),
            "unit":                unit,
            "frequency":           "monthly",
            "ingested_at":         datetime.now(timezone.utc).isoformat(),
        })
    return rows


def ensure_table(client: bigquery.Client) -> None:
    """Create the raw table if it doesn't exist."""
    dataset_ref = bigquery.Dataset(f"{GCP_PROJECT}.{DATASET}")
    dataset_ref.location = "EU"
    client.create_dataset(dataset_ref, exists_ok=True)

    schema = [
        bigquery.SchemaField("date",               "DATE"),
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
    """Insert rows into BigQuery using load_table_from_json (sandbox compatible)."""
    if not rows:
        print("  No new rows to load.")
        return
    table_ref = f"{GCP_PROJECT}.{DATASET}.{TABLE}"
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    )
    job = client.load_table_from_json(rows, table_ref, job_config=job_config)
    job.result()
    print(f"  Loaded {len(rows)} rows.")


def get_bigquery_client() -> bigquery.Client:
    """Create BigQuery client. Uses GCP_SERVICE_ACCOUNT_KEY env var in CI,
    falls back to GOOGLE_APPLICATION_CREDENTIALS locally."""
    key_json = os.getenv("GCP_SERVICE_ACCOUNT_KEY", "").strip()
    if key_json:
        info        = json.loads(key_json)
        credentials = service_account.Credentials.from_service_account_info(
            info, scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        return bigquery.Client(project=GCP_PROJECT, credentials=credentials)
    return bigquery.Client(project=GCP_PROJECT)


def main():
    fred   = Fred(api_key=FRED_API_KEY)
    client = get_bigquery_client()

    ensure_table(client)

    for series_id, (name, category, unit) in FRED_SERIES.items():
        print(f"Fetching {name} ({series_id})...")
        rows = fetch_series(fred, series_id, name, category, unit)

        last_date = get_last_loaded_date(client, series_id)
        if last_date:
            rows = [r for r in rows if r["date"] > last_date]
            print(f"  Incremental load from {last_date} — {len(rows)} new rows")
        else:
            print(f"  Full load — {len(rows)} rows")

        load_rows(client, rows)

    print("FRED ingestion complete.")


if __name__ == "__main__":
    main()
