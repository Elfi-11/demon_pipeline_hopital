"""
Validation Great Expectations de la colonne patient.age (PostgreSQL).

Produit un rapport JSON dans gx/reports/ (exploitable par Grafana côté collègue).
"""
from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path

import great_expectations as ge
import pandas as pd
import psycopg2
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException

GX_REPORT_DIR = Path("/opt/airflow/gx/reports")

# Tranches d'âge + distribution de référence (démo hôpital, jeux CSV initiaux)
AGE_BINS = [0, 18, 40, 65, 131]
AGE_BIN_LABELS = ["0-17", "18-39", "40-64", "65+"]
REFERENCE_BIN_WEIGHTS = [0.28, 0.17, 0.33, 0.22]  # doit sommer à 1.0 (requis par GX)
KL_DIVERGENCE_THRESHOLD = 0.75


def pg_conn():
    return psycopg2.connect(
        host=os.environ["HOSPITAL_PG_HOST"],
        port=os.environ["HOSPITAL_PG_PORT"],
        dbname=os.environ["HOSPITAL_PG_DB"],
        user=os.environ["HOSPITAL_PG_USER"],
        password=os.environ["HOSPITAL_PG_PASSWORD"],
    )


def age_histogram(ages: pd.Series) -> dict[str, float]:
    binned = pd.cut(ages, bins=AGE_BINS, labels=AGE_BIN_LABELS, right=False)
    counts = binned.value_counts().reindex(AGE_BIN_LABELS, fill_value=0)
    total = int(counts.sum())
    if total == 0:
        return {label: 0.0 for label in AGE_BIN_LABELS}
    return {label: round(count / total, 4) for label, count in counts.items()}


def run_gx_validations(df: pd.DataFrame) -> tuple[dict, bool]:
    ge_df = ge.from_pandas(df[["age"]])

    results = [
        ge_df.expect_table_row_count_to_be_between(min_value=1),
        ge_df.expect_column_values_to_not_be_null("age"),
        ge_df.expect_column_values_to_be_between("age", min_value=0, max_value=130),
        ge_df.expect_column_mean_to_be_between("age", min_value=5, max_value=100),
        ge_df.expect_column_kl_divergence_to_be_less_than(
            "age",
            partition_object={
                "bins": AGE_BINS,
                "weights": REFERENCE_BIN_WEIGHTS,
            },
            threshold=KL_DIVERGENCE_THRESHOLD,
        ),
    ]

    validation = ge_df.validate(result_format="SUMMARY")
    all_success = validation["success"] and all(r["success"] for r in results)

    details = {
        "expectations": [
            {
                "type": r.expectation_config.expectation_type,
                "success": r.success,
                "result": r.result,
            }
            for r in results
        ],
        "summary": validation.to_json_dict(),
    }
    return details, all_success


@dag(
    dag_id="validate_patient_age_distribution_gx",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["demo", "great-expectations", "data-quality", "hopital"],
    doc_md="""
    ## Qualité des données — distribution d'âge (Great Expectations)

    1. Lire `patient.age` dans PostgreSQL (après le DAG d'ingestion).
    2. Vérifier bornes, moyenne et écart de distribution vs référence (KL).
    3. Écrire `gx/reports/age_distribution_latest.json` pour suivi / Grafana.

    **Ordre démo :** `ingest_patients_from_minio` puis ce DAG.
    """,
)
def validate_patient_age_distribution_gx():

    @task
    def validate_age_distribution() -> dict:
        with pg_conn() as conn:
            df = pd.read_sql("SELECT age FROM patient ORDER BY id", conn)

        if df.empty:
            raise AirflowException(
                "Aucun patient en base. Lance d'abord le DAG ingest_patients_from_minio."
            )

        histogram = age_histogram(df["age"])
        gx_details, success = run_gx_validations(df)

        report = {
            "validated_at": datetime.utcnow().isoformat() + "Z",
            "row_count": len(df),
            "age_min": int(df["age"].min()),
            "age_max": int(df["age"].max()),
            "age_mean": round(float(df["age"].mean()), 2),
            "histogram": histogram,
            "reference_weights": dict(zip(AGE_BIN_LABELS, REFERENCE_BIN_WEIGHTS)),
            "kl_threshold": KL_DIVERGENCE_THRESHOLD,
            "great_expectations": gx_details,
            "success": success,
        }

        GX_REPORT_DIR.mkdir(parents=True, exist_ok=True)
        report_path = GX_REPORT_DIR / "age_distribution_latest.json"
        report_path.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")

        if not success:
            raise AirflowException(
                f"Échec Great Expectations — voir {report_path} dans le conteneur Airflow."
            )

        return {
            "success": success,
            "row_count": report["row_count"],
            "report_path": str(report_path),
        }

    validate_age_distribution()


validate_patient_age_distribution_gx()
