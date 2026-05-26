from __future__ import annotations

import io
import os
import re
from datetime import datetime

import boto3
import pandas as pd
import psycopg2
from airflow.decorators import dag, task
from unidecode import unidecode


BUCKET = os.environ["MINIO_BUCKET"]


def normalize_text(value: object) -> str:
    if value is None or pd.isna(value):
        return ""
    return str(value).strip()


def normalize_key(value: object) -> str:
    text = normalize_text(value).lower()
    text = unidecode(text)
    text = re.sub(r"[^a-z0-9]+", "_", text)
    return text.strip("_")


COLUMN_MAPPING = {
    "nom": "nom",
    "nom_patient": "nom",
    "last_name": "nom",

    "prenom": "prenom",
    "prenom_patient": "prenom",
    "first_name": "prenom",

    "age": "age",
    "age_patient": "age",
    "years_old": "age",

    "pathologie": "pathologie",
    "diagnostic_pathologie": "pathologie",
    "disease": "pathologie",

    "service": "service",
    "service_destination": "service",
    "service_demande": "service",
    "department": "service",
}

SERVICE_MAPPING = {
    "cardio": "Cardiologie",
    "cardiologie": "Cardiologie",
    "chir_cardio": "Chirurgie cardiovasculaire",
    "chirurgie_cardiovasculaire": "Chirurgie cardiovasculaire",

    "ortho": "Orthopédie",
    "orthopedie": "Orthopédie",

    "pediatrie": "Pédiatrie",

    "gynecologie": "Gynécologie",

    "neuro": "Neurologie",
    "neurologie": "Neurologie",

    "endocrinologie": "Endocrinologie",
    "urgences": "Urgences",
}


def parse_age(value: object) -> int | None:
    text = normalize_text(value).lower()

    if not text:
        return None

    if "mois" in text:
        return 0

    match = re.search(r"-?\d+", text)
    if not match:
        return None

    age = int(match.group())
    if age < 0 or age > 130:
        return None

    return age


def read_csv_smart(raw: bytes) -> pd.DataFrame:
    text = raw.decode("utf-8-sig")
    first_line = text.splitlines()[0]
    sep = ";" if first_line.count(";") > first_line.count(",") else ","
    return pd.read_csv(io.StringIO(text), sep=sep)


def pg_conn():
    return psycopg2.connect(
        host=os.environ["HOSPITAL_PG_HOST"],
        port=os.environ["HOSPITAL_PG_PORT"],
        dbname=os.environ["HOSPITAL_PG_DB"],
        user=os.environ["HOSPITAL_PG_USER"],
        password=os.environ["HOSPITAL_PG_PASSWORD"],
    )


def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.environ["MINIO_ENDPOINT"],
        aws_access_key_id=os.environ["MINIO_ACCESS_KEY"],
        aws_secret_access_key=os.environ["MINIO_SECRET_KEY"],
    )


@dag(
    dag_id="ingest_patients_from_minio",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["demo", "pipeline-as-code", "hopital"],
)
def ingest_patients_from_minio():

    @task
    def list_csv_files() -> list[str]:
        s3 = s3_client()
        response = s3.list_objects_v2(Bucket=BUCKET)
        return [
            obj["Key"]
            for obj in response.get("Contents", [])
            if obj["Key"].lower().endswith(".csv")
        ]

    @task
    def ingest_one_file(key: str) -> dict:
        s3 = s3_client()
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        raw = obj["Body"].read()

        df = read_csv_smart(raw)
        df.columns = [COLUMN_MAPPING.get(normalize_key(col), normalize_key(col)) for col in df.columns]

        required_cols = {"nom", "prenom", "age", "pathologie", "service"}
        missing = required_cols - set(df.columns)
        if missing:
            return {"file": key, "inserted": 0, "rejected": len(df), "reason": f"colonnes manquantes: {missing}"}

        clean_rows = []
        rejected = 0

        for _, row in df.iterrows():
            nom = normalize_text(row.get("nom")).title()
            prenom = normalize_text(row.get("prenom")).title()
            pathologie = normalize_text(row.get("pathologie"))
            service_raw = normalize_key(row.get("service"))
            service = SERVICE_MAPPING.get(service_raw)
            age = parse_age(row.get("age"))

            if nom.lower() == "test" or prenom.lower() == "patient":
                rejected += 1
                continue

            if not nom or not prenom or not pathologie or not service or age is None:
                rejected += 1
                continue

            clean_rows.append((nom, prenom, age, pathologie, service, key))

        inserted = 0

        with pg_conn() as conn:
            with conn.cursor() as cur:
                for nom, prenom, age, pathologie, service, source_file in clean_rows:
                    cur.execute("SELECT id FROM service WHERE nom = %s", (service,))
                    service_id = cur.fetchone()[0]

                    cur.execute(
                        """
                        INSERT INTO patient (nom, prenom, age, pathologie, service_id, source_file)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (nom, prenom, age, pathologie, service_id) DO NOTHING
                        """,
                        (nom, prenom, age, pathologie, service_id, source_file),
                    )
                    inserted += cur.rowcount

        return {"file": key, "inserted": inserted, "rejected": rejected}

    keys = list_csv_files()
    ingest_one_file.expand(key=keys)


ingest_patients_from_minio()
