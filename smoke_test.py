"""Lightweight regression test for the HomeCare Step 1 MCP."""

from __future__ import annotations

from dataclasses import dataclass

from db import ensure_database
from server import get_highrisk_cohort, care_gap_closure_plan


@dataclass
class SmokeResult:
    cohort_patients: int
    plan_patients: int


def run_smoke(limit: int = 6) -> SmokeResult:
    ensure_database()

    cohort = get_highrisk_cohort(None, limit=limit)
    cohort_patients = len(cohort.structuredContent.get("patients", []))
    if cohort_patients == 0:
        raise AssertionError("get_highrisk_cohort returned no patients")

    patient_ids = [row["patient_id"] for row in cohort.structuredContent["patients"]]
    plan = care_gap_closure_plan(None, patient_ids=patient_ids)
    plan_patients = len(plan.structuredContent.get("patients", []))
    if plan_patients == 0:
        raise AssertionError("care_gap_closure_plan returned no patients")

    return SmokeResult(cohort_patients=cohort_patients, plan_patients=plan_patients)


if __name__ == "__main__":
    result = run_smoke(limit=4)
    print(f"Smoke test passed: cohort={result.cohort_patients}, plan={result.plan_patients}")
