"""
ingest_snb.py

Pulls Swiss macroeconomic indicators from the SNB (Swiss National Bank)
data portal API into BigQuery.

No authentication required — public REST API.
Runs monthly (1st of each month via GitHub Actions).

Series pulled:
    snbgwdzid / SARON   - Swiss Average Rate Overnight
    snbgwdzid / ZIGBL   - SNB interest rate upper bound
    snbgwdzid / ZIG     - SNB policy rate (SNB rate)
    devkum    / EUR1    - CHF per EUR (monthly average)
    devkum    / USD1    - CHF per USD (monthly average)
"""

import os
import io
import requests
import pandas as pd
from datetime import datetime, timezone
from dotenv import load_dotenv
from google.cloud import bigquery

load_dotenv()

GCP_PROJECT = "swiss-macro-monitor"
DATASET     = "swiss_macro_raw"
TABLE       = "raw_snb_indicators"
BASE_URL    = "https://data.snb.ch/api/cube/{cube}/data/csv/en"

# SNB series to pull:
# (cube_id, dim_selection, series_code, indicator_name, category, unit, frequency)
SNB_SERIES = [
    ("snbgwdzid", "D0(SARON)",  "SARON",  "SARON Overnight Rate",    "monetary", "percent", "daily"),
    ("snbgwdzid", "D0(ZIGBL)",  "ZIGBL",  "SNB Rate Upper Bound",    "monetary", "percent", "daily"),
    ("snbgwdzid", "D0(ZIG)",    "ZIG",    "SNB Policy Rate",         "monetary", "percent", "daily"),
    ("devkum",    "D0(M0),D1(EUR1)", "EUR1", "CHF per EUR",          "currency", "chf",     "monthly"),
    ("devkum",    "D0(M0),D1(USD1)", "USD1", "CHF per USD",          "currency", "chf",     "monthly"),
]


def fetch_snb_series(
    cube: str,
    dim_sel: str,
    series_code: str,
    indicator_name: str,
    category: str,
    unit: str,
    frequency: str,
) -> list[dict]:
    """Fetch one SNB series and return as list of BigQuery rows."""
    url    = BASE_URL.format(cube=cube)
    params = {"dimSel": dim_sel, "fromDate": "2000-01"}

    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()

    # SNB CSV has 2 header lines — skip them
    lines = response.text.splitlines()
    data_start = next(i for i, l in enumerate(lines) if l.startswith('"Date"'))
    csv_content = "\n".join(lines[data_start:])

    df = pd.read_csv(
        io.StringIO(csv_content),
        sep=";",
        quotechar='"',
        dtype=str,
    )

    # Filter to the specific series code (last dimension column = Value)
    # For multi-dimension cubes, filter by series_code in any column
    value_col = df.columns[-1]  # "Value" is always last
    df[value_col] = pd.to_numeric(df[value_col], errors="coerce")
    df = df.dropna(subset=[value_col])

    # Filter rows matching our series code if there are multiple in the response
    for col in df.columns[1:-1]:  # dimension columns between Date and Value
        if series_code in df[col].values:
            df = df[df[col] == series_code]
            break

    rows = []
    for _, row in df.iterrows():
        rows.append({
            "date":                str(row["Date"]),
            "source":              "SNB",
            "series_id":           f"{cube}/{series_code}",
            "indicator_name":      indicator_name,
            "indicator_category":  category,
            "value":               float(row[value_col]),
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


def main():
    client = bigquery.Client(project=GCP_PROJECT)
    ensure_table(client)

    for cube, dim_sel, series_code, name, category, unit, frequency in SNB_SERIES:
        series_id = f"{cube}/{series_code}"
        print(f"Fetching {name} ({series_id})...")

        rows = fetch_snb_series(cube, dim_sel, series_code, name, category, unit, frequency)

        last_date = get_last_loaded_date(client, series_id)
        if last_date:
            rows = [r for r in rows if r["date"] > last_date]
            print(f"  Incremental load from {last_date} — {len(rows)} new rows")
        else:
            print(f"  Full load — {len(rows)} rows")

        load_rows(client, rows)

    print("SNB ingestion complete.")


if __name__ == "__main__":
    main()
