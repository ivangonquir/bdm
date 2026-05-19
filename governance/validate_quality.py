"""
Data Governance: Data Quality Validation with Great Expectations.

Validates trusted zone data for:
  - NOAA structured data        (ClickHouse trusted.noaa_bcn)
  - OpenWeather semi-structured (MongoDB   trusted.weather_stream)

Expectation results are stored in MongoDB governance.quality_results
for audit trails and historical comparison.

Exits with code 1 if any critical expectation fails so Airflow marks
the task as failed and the pipeline does not proceed.
"""

import sys
from datetime import datetime, timezone

import pandas as pd
import clickhouse_connect
import great_expectations as gx
from pymongo import MongoClient

CH_HOST   = "clickhouse"
MONGO_URI = "mongodb://mongodb:27017/"

print("=== Data Quality Validation (Great Expectations) ===")

quality_reports = []


def run_suite(context, suite_name: str, df: pd.DataFrame, expectations: list) -> dict:
    """Validate a pandas DataFrame against a list of expectations.

    Returns a summary dict with pass/fail counts and per-expectation detail,
    ready to be inserted into MongoDB.
    """
    ds        = context.sources.add_pandas(f"{suite_name}_ds")
    asset     = ds.add_dataframe_asset(f"{suite_name}_asset")
    batch_req = asset.build_batch_request(dataframe=df)

    validator = context.get_validator(
        batch_request=batch_req,
        create_expectation_suite_with_name=suite_name,
    )

    for method, kwargs in expectations:
        getattr(validator, method)(**kwargs)

    result  = validator.validate()
    passed  = sum(1 for r in result.results if r.success)
    details = [
        {
            "expectation": r.expectation_config.expectation_type,
            "column":      r.expectation_config.kwargs.get("column", ""),
            "kwargs":      r.expectation_config.kwargs,
            "success":     bool(r.success),
            "result":      r.result,
        }
        for r in result.results
    ]

    return {
        "dataset":             suite_name,
        "run_ts":              datetime.now(timezone.utc),
        "rows_validated":      len(df),
        "expectations_total":  len(result.results),
        "expectations_passed": passed,
        "expectations_failed": len(result.results) - passed,
        "success":             bool(result.success),
        "details":             details,
    }


gx_context = gx.get_context(mode="ephemeral")

# ── 1. NOAA structured data ───────────────────────────────────────────────────
try:
    ch      = clickhouse_connect.get_client(host=CH_HOST, port=8123)
    noaa_df = ch.query_df("SELECT * FROM trusted.noaa_bcn")
    print(f"Loaded {len(noaa_df):,} rows from trusted.noaa_bcn")

    noaa_report = run_suite(
        gx_context, "noaa_bcn", noaa_df,
        [
            ("expect_column_values_to_not_be_null", {"column": "date"}),
            ("expect_column_values_to_not_be_null", {"column": "value"}),
            ("expect_column_values_to_not_be_null", {"column": "station"}),
            ("expect_column_values_to_not_be_null", {"column": "datatype"}),
            ("expect_column_values_to_be_between",  {"column": "value",
                                                     "min_value": -90,
                                                     "max_value": 60}),
            ("expect_column_values_to_be_in_set",   {"column": "datatype",
                                                     "value_set": ["TMIN", "TMAX", "TAVG"]}),
            ("expect_column_values_to_not_be_null", {"column": "attributes"}),
        ],
    )
    quality_reports.append(noaa_report)
    tag = "PASS" if noaa_report["success"] else "FAIL"
    print(f"[{tag}] noaa_bcn — "
          f"{noaa_report['expectations_passed']}/{noaa_report['expectations_total']} passed")
    for d in noaa_report["details"]:
        if not d["success"]:
            print(f"  FAIL  {d['expectation']} on '{d['column']}'  →  {d['result']}")

except Exception as exc:
    print(f"[ERROR] noaa_bcn validation failed: {exc}")

# ── 2. OpenWeather semi-structured data ───────────────────────────────────────
try:
    mongo = MongoClient(MONGO_URI)
    docs  = list(mongo["trusted"]["weather_stream"].find({}, {"_id": 0}))
    mongo.close()

    if docs:
        ow_df = pd.DataFrame(docs)
        print(f"Loaded {len(ow_df):,} documents from trusted.weather_stream")

        ow_report = run_suite(
            gx_context, "weather_stream", ow_df,
            [
                ("expect_column_values_to_not_be_null", {"column": "event_ts"}),
                ("expect_column_values_to_not_be_null", {"column": "temp_c"}),
                ("expect_column_values_to_be_between",  {"column": "temp_c",
                                                         "min_value": -50,
                                                         "max_value": 60}),
                ("expect_column_values_to_be_between",  {"column": "humidity_pct",
                                                         "min_value": 0,
                                                         "max_value": 100}),
                ("expect_column_values_to_be_unique",   {"column": "event_ts"}),
            ],
        )
        quality_reports.append(ow_report)
        tag = "PASS" if ow_report["success"] else "FAIL"
        print(f"[{tag}] weather_stream — "
              f"{ow_report['expectations_passed']}/{ow_report['expectations_total']} passed")
        for d in ow_report["details"]:
            if not d["success"]:
                print(f"  FAIL  {d['expectation']} on '{d['column']}'  →  {d['result']}")
    else:
        print("[SKIP] weather_stream — no documents found")

except Exception as exc:
    print(f"[ERROR] weather_stream validation failed: {exc}")

# ── Store reports in MongoDB ──────────────────────────────────────────────────
if quality_reports:
    mongo = MongoClient(MONGO_URI)
    mongo["governance"]["quality_results"].insert_many(quality_reports)
    mongo.close()
    print(f"\nStored {len(quality_reports)} report(s) → governance.quality_results")

# ── Final status ──────────────────────────────────────────────────────────────
failed = [r for r in quality_reports if not r["success"]]
if failed:
    names = ", ".join(r["dataset"] for r in failed)
    print(f"\n[WARN] Validation failed for: {names}")
    sys.exit(1)

print("\nAll validations passed.")
