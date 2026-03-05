"""DuckDB utilities for the HomeCare cohort MCP."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Iterable, Mapping

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parent
DATA_DIR = PROJECT_ROOT / "data"
DB_PATH = DATA_DIR / "homecare.duckdb"
DEFAULT_CSV_DIR = PROJECT_ROOT.parent / "synthetic_data"

CSV_TABLES: Mapping[str, str] = {
    "patients": "patients.csv",
    "encounters": "encounters.csv",
    "diagnoses": "diagnoses.csv",
    "medications": "medications.csv",
    "labs": "labs.csv",
    "vital_signs": "vital_signs.csv",
    "procedures": "procedures.csv",
}


def _resolve_csv_dir() -> Path:
    """Return the directory that holds the raw synthetic CSV files."""

    override = os.getenv("HOMECARE_DATA_DIR")
    if override:
        candidate = Path(override).expanduser().resolve()
        if candidate.exists():
            return candidate
        raise FileNotFoundError(
            f"HOMECARE_DATA_DIR={override!r} does not exist."
        )

    if DEFAULT_CSV_DIR.exists():
        return DEFAULT_CSV_DIR

    raise FileNotFoundError(
        "Synthetic data directory not found. Set HOMECARE_DATA_DIR to the folder "
        "containing the CSV extracts (patients.csv, encounters.csv, etc.)."
    )


def ensure_database(force_rebuild: bool = False) -> None:
    """Create the DuckDB file from CSV inputs if it does not already exist."""

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    if DB_PATH.exists() and not force_rebuild:
        return

    if DB_PATH.exists() and force_rebuild:
        DB_PATH.unlink()

    csv_dir = _resolve_csv_dir()

    conn = duckdb.connect(str(DB_PATH))
    try:
        for table, filename in CSV_TABLES.items():
            source = csv_dir / filename
            if not source.exists():
                raise FileNotFoundError(f"Missing CSV file for table '{table}': {source}")

            conn.execute(
                f"""
                CREATE OR REPLACE TABLE {table} AS
                SELECT *
                FROM read_csv_auto('{source.as_posix()}', HEADER=TRUE);
                """
            )

        date_columns = {
            "encounters": ["encounter_date"],
            "diagnoses": ["diagnosis_date"],
            "labs": ["lab_date"],
            "medications": ["med_start", "med_end"],
            "procedures": ["procedure_date"],
            "vital_signs": ["measure_date"],
        }
        for table, columns in date_columns.items():
            for column in columns:
                conn.execute(
                    f"""
                    ALTER TABLE {table}
                    ALTER COLUMN {column}
                    TYPE DATE
                    USING TRY_CAST({column} AS DATE);
                    """
                )

        conn.execute(
            """
            CREATE OR REPLACE VIEW chronic_condition_flags AS
            WITH flagged AS (
                SELECT
                    patient_id,
                    CASE
                        WHEN diagnosis_code LIKE 'E11%' THEN 'Type 2 Diabetes'
                        WHEN diagnosis_code LIKE 'I10%' THEN 'Hypertension'
                        WHEN diagnosis_code LIKE 'I50%' THEN 'Heart Failure'
                        WHEN diagnosis_code LIKE 'J44%' THEN 'COPD'
                        WHEN diagnosis_code LIKE 'N18%' THEN 'Chronic Kidney Disease'
                        WHEN diagnosis_code LIKE 'C%' THEN 'Cancer'
                        ELSE NULL
                    END AS condition_label
                FROM diagnoses
            )
            SELECT DISTINCT patient_id, condition_label
            FROM flagged
            WHERE condition_label IS NOT NULL;
            """
        )

        conn.execute(
            """
            CREATE OR REPLACE VIEW latest_hba1c AS
            SELECT patient_id, lab_result AS value, lab_date
            FROM (
                SELECT *,
                       ROW_NUMBER() OVER (PARTITION BY patient_id ORDER BY lab_date DESC) AS rn
                FROM labs
                WHERE lower(lab_name) LIKE '%hba1c%'
            )
            WHERE rn = 1;
            """
        )

        conn.execute(
            """
            CREATE OR REPLACE VIEW latest_sbp AS
            SELECT patient_id, measure_value AS value, measure_date
            FROM (
                SELECT *,
                       ROW_NUMBER() OVER (PARTITION BY patient_id ORDER BY measure_date DESC) AS rn
                FROM vital_signs
                WHERE upper(vital_type) = 'SBP'
            )
            WHERE rn = 1;
            """
        )

        conn.execute(
            """
            CREATE OR REPLACE VIEW latest_dbp AS
            SELECT patient_id, measure_value AS value, measure_date
            FROM (
                SELECT *,
                       ROW_NUMBER() OVER (PARTITION BY patient_id ORDER BY measure_date DESC) AS rn
                FROM vital_signs
                WHERE upper(vital_type) = 'DBP'
            )
            WHERE rn = 1;
            """
        )

        conn.execute(
            """
            CREATE OR REPLACE VIEW risk_scores AS
            SELECT
                p.patient_id,
                CASE
                    WHEN hf.patient_id IS NOT NULL
                        OR ckd.patient_id IS NOT NULL
                        OR (hba1c.value IS NOT NULL AND hba1c.value >= 9)
                        OR (sbp.value IS NOT NULL AND sbp.value >= 160)
                        THEN 'HIGH'
                    WHEN dm.patient_id IS NOT NULL
                        OR (sbp.value IS NOT NULL AND sbp.value BETWEEN 140 AND 159)
                        OR (hba1c.value IS NOT NULL AND hba1c.value BETWEEN 8 AND 8.99)
                        THEN 'MEDIUM'
                    ELSE 'LOW'
                END AS risk_tier
            FROM patients p
            LEFT JOIN (
                SELECT DISTINCT patient_id FROM chronic_condition_flags WHERE condition_label = 'Heart Failure'
            ) AS hf USING (patient_id)
            LEFT JOIN (
                SELECT DISTINCT patient_id FROM chronic_condition_flags WHERE condition_label = 'Chronic Kidney Disease'
            ) AS ckd USING (patient_id)
            LEFT JOIN (
                SELECT DISTINCT patient_id FROM chronic_condition_flags WHERE condition_label = 'Type 2 Diabetes'
            ) AS dm USING (patient_id)
            LEFT JOIN latest_hba1c AS hba1c USING (patient_id)
            LEFT JOIN latest_sbp AS sbp USING (patient_id);
            """
        )

        conn.execute(
            """
            CREATE OR REPLACE VIEW last_encounter_summary AS
            WITH latest AS (
                SELECT *,
                       ROW_NUMBER() OVER (PARTITION BY patient_id ORDER BY encounter_date DESC) AS rn
                FROM encounters
            )
            SELECT
                latest.patient_id,
                latest.encounter_id,
                latest.encounter_date AS last_encounter_date,
                latest.encounter_type AS last_encounter_type,
                latest.facility_type AS last_encounter_facility,
                latest.primary_dx_code,
                dx.diagnosis_desc AS primary_dx_desc
            FROM latest
            LEFT JOIN diagnoses dx
                ON latest.encounter_id = dx.encounter_id
            WHERE latest.rn = 1;
            """
        )

        conn.execute(
            """
            CREATE OR REPLACE VIEW latest_eye_exam AS
            SELECT patient_id, MAX(procedure_date) AS eye_exam_date
            FROM procedures
            WHERE LOWER(procedure_desc) LIKE '%eye%'
               OR LOWER(procedure_desc) LIKE '%retina%'
               OR CAST(procedure_code AS TEXT) IN ('2023F', '2022F', '92250', '92227', '92228', '3072F')
            GROUP BY patient_id;
            """
        )

        conn.execute(
            """
            CREATE OR REPLACE VIEW latest_nephropathy_screen AS
            SELECT patient_id, MAX(lab_date) AS nephropathy_date
            FROM labs
            WHERE LOWER(lab_name) LIKE '%microalbumin%'
               OR LOWER(lab_name) LIKE '%micro albumin%'
               OR LOWER(lab_name) LIKE '%urine albumin%'
            GROUP BY patient_id;
            """
        )
    finally:
        conn.close()


def get_connection(read_only: bool = True) -> duckdb.DuckDBPyConnection:
    ensure_database()
    return duckdb.connect(str(DB_PATH), read_only=read_only)


def query_df(sql: str, params: Iterable | None = None) -> list[dict]:
    conn = get_connection()
    try:
        result = conn.execute(sql, params or []).fetchdf()
        result = result.where(result.notna(), None)
        return result.to_dict("records")
    finally:
        conn.close()


def query_one(sql: str, params: Iterable | None = None) -> dict | None:
    records = query_df(sql, params)
    return records[0] if records else None


def query_value(sql: str, params: Iterable | None = None):
    conn = get_connection()
    try:
        row = conn.execute(sql, params or []).fetchone()
        return row[0] if row else None
    finally:
        conn.close()
