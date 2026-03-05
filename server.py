"""FastMCP server exposing cohort insights for the HomeCare demo."""

import contextlib
import datetime as dt
from typing import Iterable, List, Optional

from mcp.server.fastmcp import Context, FastMCP
from mcp.server.transport_security import TransportSecuritySettings
from starlette.applications import Starlette
from starlette.middleware.cors import CORSMiddleware
from starlette.routing import Mount
from mcp.types import CallToolResult, TextContent

from db import ensure_database, query_df, query_one, query_value
import ui

ensure_database()


def _transport_security_settings() -> TransportSecuritySettings:
    """Configure transport security defaults for local + Vercel hosting."""

    return TransportSecuritySettings(
        enable_dns_rebinding_protection=False,
    )


def md_table(headers: Iterable[str], rows: Iterable[Iterable[object]]) -> str:
    header_list = [str(h) for h in headers]
    header_row = " | ".join(header_list)
    divider = " | ".join(["---"] * len(header_list))
    body_rows = [
        " | ".join("" if v is None else str(v) for v in row)
        for row in rows
    ]
    return "\n".join([header_row, divider, *body_rows]) if body_rows else "(no data)"


mcp = FastMCP(
    "homecare-cohort",
    stateless_http=True,
    streamable_http_path="/",
    transport_security=_transport_security_settings(),
)
ui.register(mcp)


def _to_float(value) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _parse_date(raw) -> Optional[dt.date]:
    if raw is None:
        return None
    if isinstance(raw, dt.datetime):
        return raw.date()
    if isinstance(raw, dt.date):
        return raw
    if hasattr(raw, "to_pydatetime"):
        value = raw.to_pydatetime()
        if isinstance(value, dt.datetime):
            return value.date()
        if isinstance(value, dt.date):
            return value
    raw_str = str(raw)
    if raw_str.upper() in {"NAT", "NAN", "NONE", ""}:
        return None
    try:
        return dt.date.fromisoformat(raw_str.split("T")[0])
    except (TypeError, ValueError):
        return None


def _days_since(date_value: Optional[dt.date]) -> Optional[int]:
    if not date_value:
        return None
    if hasattr(date_value, "to_pydatetime"):
        value = date_value.to_pydatetime()
        if isinstance(value, dt.datetime):
            date_value = value.date()
        elif isinstance(value, dt.date):
            date_value = value
        else:
            return None
    if str(date_value).upper() in {"NAT", "NAN"}:
        return None
    return (dt.date.today() - date_value).days


def _load_patient_feature_rows() -> List[dict]:
    return query_df(
        """
        SELECT
            p.patient_id,
            p.sex,
            p.year_of_birth,
            p.insurance_type,
            rs.risk_tier,
            lh.value AS hba1c_value,
            lh.lab_date AS hba1c_date,
            sbp.value AS sbp_value,
            sbp.measure_date AS sbp_date,
            dbp.value AS dbp_value,
            dbp.measure_date AS dbp_date,
            le.last_encounter_date,
            le.last_encounter_type,
            le.last_encounter_facility,
            le.primary_dx_code,
            le.primary_dx_desc,
            eye.eye_exam_date,
            neph.nephropathy_date
        FROM patients p
        LEFT JOIN risk_scores rs USING (patient_id)
        LEFT JOIN latest_hba1c lh USING (patient_id)
        LEFT JOIN latest_sbp sbp USING (patient_id)
        LEFT JOIN latest_dbp dbp USING (patient_id)
        LEFT JOIN last_encounter_summary le USING (patient_id)
        LEFT JOIN latest_eye_exam eye USING (patient_id)
        LEFT JOIN latest_nephropathy_screen neph USING (patient_id);
        """
    )


def _summarize_patient_row(row: dict) -> dict:
    today = dt.date.today()
    year_of_birth = row.get("year_of_birth")
    age = today.year - int(year_of_birth) if year_of_birth else None

    hba1c_value = _to_float(row.get("hba1c_value"))
    hba1c_date = _parse_date(row.get("hba1c_date"))
    hba1c_days_overdue = _days_since(hba1c_date) if hba1c_date else None

    sbp_value = _to_float(row.get("sbp_value"))
    sbp_date = _parse_date(row.get("sbp_date"))
    sbp_days_overdue = _days_since(sbp_date) if sbp_date else None

    dbp_value = _to_float(row.get("dbp_value"))

    eye_exam_date = _parse_date(row.get("eye_exam_date"))
    eye_exam_days = _days_since(eye_exam_date) if eye_exam_date else None

    neph_date = _parse_date(row.get("nephropathy_date"))
    neph_days = _days_since(neph_date) if neph_date else None

    last_encounter_date = _parse_date(row.get("last_encounter_date"))
    last_encounter_days = _days_since(last_encounter_date) if last_encounter_date else None

    severity = 0
    reasons: List[str] = []

    if hba1c_value is not None and hba1c_value >= 9:
        severity += 4
        reasons.append(f"HbA1c {hba1c_value:.1f}% ({hba1c_date})")
    elif hba1c_days_overdue is not None and hba1c_days_overdue > 180:
        severity += 2
        reasons.append(f"HbA1c overdue {hba1c_days_overdue} days")

    if sbp_value is not None and sbp_value >= 160:
        severity += 3
        reasons.append(f"SBP {int(round(sbp_value))} mmHg ({sbp_date})")
    elif sbp_days_overdue is not None and sbp_days_overdue > 45:
        severity += 1
        reasons.append(f"BP not captured in {sbp_days_overdue} days")

    if eye_exam_days is None or eye_exam_days > 365:
        severity += 1
        reasons.append("Retinal exam overdue")

    if neph_days is None or neph_days > 365:
        severity += 1
        reasons.append("Kidney screening overdue")

    if last_encounter_days is not None and last_encounter_days <= 30:
        if row.get("last_encounter_type") and row["last_encounter_type"].upper() in {"INPATIENT", "ED", "EMERGENCY", "HOSPITAL"}:
            severity += 2
            reasons.append(f"Recent {row['last_encounter_type']} encounter ({last_encounter_date})")

    if (row.get("risk_tier") or "").upper() == "HIGH":
        severity += 1

    composite_risk = max(45, min(95, 55 + severity * 5))
    missing_screenings: List[str] = []
    if eye_exam_days is None or eye_exam_days > 365:
        overdue = "never" if eye_exam_days is None else f"{eye_exam_days} days ago"
        missing_screenings.append(f"Diabetic retinal exam ({overdue})")
    if neph_days is None or neph_days > 365:
        overdue = "never" if neph_days is None else f"{neph_days} days ago"
        missing_screenings.append(f"Nephropathy screening ({overdue})")

    alternate_flag = (hba1c_value is not None and hba1c_value >= 9.5) or (sbp_value is not None and sbp_value >= 170)

    return {
        "patient_id": row["patient_id"],
        "sex": row.get("sex"),
        "year_of_birth": year_of_birth,
        "age": age,
        "insurance_type": row.get("insurance_type"),
        "risk_tier": row.get("risk_tier") or "LOW",
        "primary_dx_code": row.get("primary_dx_code"),
        "primary_dx_desc": row.get("primary_dx_desc"),
        "last_encounter_date": last_encounter_date.isoformat() if last_encounter_date else None,
        "last_encounter_type": row.get("last_encounter_type"),
        "hba1c_value": hba1c_value,
        "hba1c_date": hba1c_date.isoformat() if hba1c_date else None,
        "hba1c_days_overdue": hba1c_days_overdue,
        "sbp_value": sbp_value,
        "dbp_value": dbp_value,
        "bp_date": sbp_date.isoformat() if sbp_date else None,
        "bp_days_overdue": sbp_days_overdue,
        "composite_risk_score": composite_risk,
        "severity": severity,
        "severity_reasons": reasons,
        "missing_screenings": missing_screenings,
        "alternate_therapy_flag": alternate_flag,
    }


def _select_highrisk_patients(patient_ids: Optional[List[str]] = None, limit: int = 6) -> List[dict]:
    rows = [_summarize_patient_row(row) for row in _load_patient_feature_rows()]
    if patient_ids:
        id_set = set(patient_ids)
        selected = [row for row in rows if row["patient_id"] in id_set]
    else:
        selected = [row for row in rows if row["severity"] > 0]
        selected.sort(key=lambda r: (r["severity"], r.get("hba1c_value") or 0, r.get("sbp_value") or 0), reverse=True)
        selected = selected[:limit]
    return selected


def _build_gap_plan(summary: dict) -> List[dict]:
    gaps: List[dict] = []

    def make_days_overdue(days: Optional[int], target: int) -> Optional[int]:
        if days is None:
            return None
        overdue = days - target
        return overdue if overdue > 0 else 0

    hba1c_gap_days = make_days_overdue(summary.get("hba1c_days_overdue"), 90)
    hba1c_value = summary.get("hba1c_value")
    if hba1c_value is None or hba1c_value >= 8 or (hba1c_gap_days is not None and hba1c_gap_days > 0):
        gap_notes = (
            "No HbA1c result recorded in the last 90 days."
            if hba1c_value is None
            else f"Latest HbA1c {hba1c_value:.1f}% (overdue {hba1c_gap_days} days)." if hba1c_gap_days else f"Latest HbA1c {hba1c_value:.1f}%."
        )
        gaps.append(
            {
                "gap_id": "CDC-HbA1c",
                "gap_name": "Comprehensive Diabetes Care: HbA1c Control <8%",
                "days_overdue": hba1c_gap_days,
                "closure_opportunity": "Draw labs at next RN home visit; escalate to PCP if >9%.",
                "recommended_channel": "Home care RN + PCP follow-up",
                "quality_score_impact": 580,
                "notes": gap_notes,
                "alternate_therapy_flag": bool(summary.get("alternate_therapy_flag")),
            }
        )

    bp_days_overdue = make_days_overdue(summary.get("bp_days_overdue"), 30)
    sbp_value = summary.get("sbp_value")
    dbp_value = summary.get("dbp_value")
    if sbp_value is None or sbp_value >= 140 or (bp_days_overdue is not None and bp_days_overdue > 0):
        bp_note = (
            "No recent blood pressure capture."
            if sbp_value is None
            else f"Latest blood pressure {int(round(sbp_value))}/{int(round(dbp_value or 0))} mmHg."
        )
        gaps.append(
            {
                "gap_id": "CBP",
                "gap_name": "Controlling High Blood Pressure",
                "days_overdue": bp_days_overdue,
                "closure_opportunity": "Manual BP + medication reconciliation next visit; escalate if still uncontrolled.",
                "recommended_channel": "RN home visit",
                "quality_score_impact": 420,
                "notes": bp_note,
                "alternate_therapy_flag": bool(summary.get("alternate_therapy_flag") and (sbp_value or 0) >= 170),
            }
        )

    if summary.get("missing_screenings"):
        for item in summary["missing_screenings"]:
            if "retinal" in item.lower():
                hed_id = "CDC-Eye"
                impact = 210
                channel = "Schedule ophthalmology referral"
            else:
                hed_id = "CDC-Neph"
                impact = 180
                channel = "Order urine microalbumin via lab partner"
            gaps.append(
                {
                    "gap_id": hed_id,
                    "gap_name": item,
                    "days_overdue": None,
                    "closure_opportunity": channel,
                    "recommended_channel": channel,
                    "quality_score_impact": impact,
                    "notes": item,
                    "alternate_therapy_flag": False,
                }
            )

    if summary.get("severity") >= 7:
        gaps.append(
            {
                "gap_id": "ALTERNATE-THERAPY",
                "gap_name": "Alternate Therapy Evaluation Required",
                "days_overdue": None,
                "closure_opportunity": "Escalate to quality manager for alternate therapy review.",
                "recommended_channel": "Care gap escalation desk",
                "quality_score_impact": 0,
                "notes": "Multiple uncontrolled indicators in spite of standard therapy.",
                "alternate_therapy_flag": True,
            }
        )

    return gaps


@mcp.tool(
    name="population_snapshot",
    description=(
        "Summarize the synthetic homecare cohort: risk distribution, chronic conditions, "
        "payer mix, and recent acute encounters."
    ),
    meta={
        "ui": {"resourceUri": "ui://homecare-cohort/population-snapshot.html"},
        "ui/resourceUri": "ui://homecare-cohort/population-snapshot.html",
        "openai/widgetAccessible": True,
        "openai/outputTemplate": "ui://homecare-cohort/population-snapshot.html",
    },
)
def population_snapshot(ctx: Context, window_days: int = 90) -> CallToolResult:  # noqa: ARG001
    cutoff = (dt.date.today() - dt.timedelta(days=max(window_days, 1))).isoformat()

    total_patients = query_value("SELECT COUNT(*) FROM patients") or 0
    risk_rows = query_df(
        """
        SELECT coalesce(risk_tier, 'UNKNOWN') AS risk_tier, COUNT(*) AS patients
        FROM risk_scores
        GROUP BY risk_tier
        ORDER BY CASE risk_tier WHEN 'HIGH' THEN 1 WHEN 'MEDIUM' THEN 2 WHEN 'LOW' THEN 3 ELSE 4 END;
        """
    )

    condition_rows = query_df(
        """
        SELECT condition_label AS condition, COUNT(DISTINCT patient_id) AS patients
        FROM chronic_condition_flags
        GROUP BY condition_label
        ORDER BY patients DESC;
        """
    )

    payer_rows = query_df(
        """
        SELECT insurance_type AS payer, COUNT(*) AS patients
        FROM patients
        GROUP BY insurance_type
        ORDER BY patients DESC;
        """
    )

    recent_rows = query_df(
        """
        SELECT
            encounter_type,
            COUNT(*) AS encounters,
            COUNT(DISTINCT patient_id) AS unique_patients
        FROM encounters
        WHERE encounter_date >= ?
          AND upper(encounter_type) IN ('INPATIENT', 'EMERGENCY', 'ED', 'HOSPITAL')
        GROUP BY encounter_type
        ORDER BY encounters DESC;
        """,
        [cutoff],
    )

    markdown_sections = [
        "# Cohort Snapshot",
        f"**Total patients:** {total_patients}",
        "\n## Risk Distribution",
        md_table(["Risk tier", "Patients"], ((row["risk_tier"], row["patients"]) for row in risk_rows)),
        "\n## Chronic Condition Coverage",
        md_table(["Condition", "Patients"], ((row["condition"], row["patients"]) for row in condition_rows)),
        "\n## Payer Mix",
        md_table(["Payer", "Patients"], ((row["payer"], row["patients"]) for row in payer_rows)),
        "\n## Recent Acute Encounters",
        md_table(
            ["Encounter type", "Encounters", "Unique patients"],
            ((row["encounter_type"], row["encounters"], row["unique_patients"]) for row in recent_rows),
        ),
    ]

    structured = {
        "generated_at": dt.datetime.utcnow().isoformat() + "Z",
        "window_days": window_days,
        "totals": {"patients": total_patients},
        "risk_breakdown": risk_rows,
        "condition_counts": condition_rows,
        "payer_mix": payer_rows,
        "recent_acute_encounters": recent_rows,
    }

    return CallToolResult(
        content=[TextContent(type="text", text="\n".join(markdown_sections))],
        structuredContent=structured,
        _meta={
            "openai/outputTemplate": "ui://homecare-cohort/population-snapshot.html",
            "openai/widgetAccessible": True,
            **structured,
        },
    )


@mcp.tool(
    name="get_highrisk_cohort",
    description=(
        "Identify the Step 1a high-risk cohort: patients with uncontrolled HbA1c/BP, overdue screenings, "
        "or recent acute events. Returns a limited list with key metrics for quality managers."
    ),
    meta={
        "ui": {"resourceUri": "ui://homecare-cohort/highrisk-cohort.html"},
        "ui/resourceUri": "ui://homecare-cohort/highrisk-cohort.html",
        "openai/widgetAccessible": True,
        "openai/outputTemplate": "ui://homecare-cohort/highrisk-cohort.html",
    },
)
def get_highrisk_cohort(ctx: Context, limit: int = 6) -> CallToolResult:  # noqa: ARG001
    limit = max(1, min(limit, 10))
    selected = _select_highrisk_patients(limit=limit)
    if not selected:
        message = "No patients met the outlier thresholds right now."
        return CallToolResult(content=[TextContent(type="text", text=message)], structuredContent={"patients": []})

    headers = ["Patient", "Age", "Risk (score)", "Primary DX", "Last encounter", "HbA1c", "BP", "Key flags"]
    rows = []
    for item in selected:
        hba1c_display = (
            "n/a"
            if item.get("hba1c_value") is None
            else f"{item['hba1c_value']:.1f}% ({item.get('hba1c_date') or 'no date'})"
        )
        bp_display = "n/a"
        if item.get("sbp_value") is not None:
            systolic = int(round(item["sbp_value"]))
            diastolic = int(round(item.get("dbp_value") or 0))
            bp_display = f"{systolic}/{diastolic} ({item.get('bp_date') or 'no date'})"

        last_encounter = item.get("last_encounter_date")
        if last_encounter and item.get("last_encounter_type"):
            last_encounter = f"{last_encounter} ({item['last_encounter_type']})"

        rows.append(
            [
                item["patient_id"],
                item.get("age") or "—",
                f"{item.get('risk_tier')} ({item.get('composite_risk_score')})",
                item.get("primary_dx_code") or item.get("primary_dx_desc") or "—",
                last_encounter or "—",
                hba1c_display,
                bp_display,
                "; ".join(item.get("severity_reasons") or []) or "Escalation recommended",
            ]
        )

    markdown_sections = [
        "# High-Risk Cohort (Step 1a)",
        f"Flagged **{len(selected)}** patients meeting outlier criteria (HbA1c ≥ 9%, SBP ≥ 160, overdue screenings, or recent acute utilization).",
        md_table(headers, rows),
    ]

    structured = {
        "generated_at": dt.datetime.utcnow().isoformat() + "Z",
        "patients": selected,
        "criteria": {
            "hba1c_threshold": 9,
            "bp_threshold_sbp": 160,
            "overdue_hba1c_days": 180,
            "overdue_bp_days": 45,
            "overdue_screening_days": 365,
        },
    }

    return CallToolResult(
        content=[TextContent(type="text", text="\n\n".join(markdown_sections))],
        structuredContent=structured,
        _meta={
            "openai/outputTemplate": "ui://homecare-cohort/highrisk-cohort.html",
            "openai/widgetAccessible": True,
            **structured,
        },
    )


def _latest_values(patient_id: str, vital: str) -> dict | None:
    row = query_one(
        """
        SELECT measure_value, measure_date
        FROM (
            SELECT *, ROW_NUMBER() OVER (ORDER BY measure_date DESC) AS rn
            FROM vital_signs
            WHERE patient_id = ? AND upper(vital_type) = ?
        )
        WHERE rn = 1;
        """,
        [patient_id, vital.upper()],
    )
    return row


def _lab_series(patient_id: str, keyword: str) -> list[dict]:
    return query_df(
        """
        SELECT lab_date, lab_name, lab_result, units
        FROM labs
        WHERE patient_id = ? AND lower(lab_name) LIKE ?
        ORDER BY lab_date DESC
        LIMIT 6;
        """,
        [patient_id, f"%{keyword.lower()}%"],
    )


def _format_flags(flags: list[str]) -> str:
    if not flags:
        return "- No urgent clinical flags detected."
    return "\n".join(f"- {flag}" for flag in flags)


@mcp.tool(
    name="patient_profile",
    description="Return a risk-aware summary for a specific patient, including trends and outstanding gaps.",
    meta={
        "ui": {"resourceUri": "ui://homecare-cohort/patient-profile.html"},
        "ui/resourceUri": "ui://homecare-cohort/patient-profile.html",
        "openai/widgetAccessible": True,
        "openai/outputTemplate": "ui://homecare-cohort/patient-profile.html",
    },
)
def patient_profile(ctx: Context, patient_id: str) -> CallToolResult:  # noqa: ARG001
    patient = query_one("SELECT * FROM patients WHERE patient_id = ?", [patient_id])
    if not patient:
        message = f"Patient {patient_id} not found in synthetic dataset."
        return CallToolResult(content=[TextContent(type="text", text=message)], structuredContent={"error": message})

    risk = query_one("SELECT risk_tier FROM risk_scores WHERE patient_id = ?", [patient_id]) or {}
    chronic_conditions = query_df(
        "SELECT condition_label FROM chronic_condition_flags WHERE patient_id = ? ORDER BY condition_label",
        [patient_id],
    )

    latest_hba1c_series = _lab_series(patient_id, "hba1c")
    latest_egfr = _lab_series(patient_id, "egfr")

    labs = query_df(
        """
        SELECT lab_date, lab_name, lab_result, units
        FROM labs
        WHERE patient_id = ?
        ORDER BY lab_date DESC
        LIMIT 12;
        """,
        [patient_id],
    )

    medications = query_df(
        """
        SELECT medication_name, med_start, med_end, dose, route
        FROM medications
        WHERE patient_id = ?
        ORDER BY med_start DESC NULLS LAST
        LIMIT 8;
        """,
        [patient_id],
    )

    encounters = query_df(
        """
        SELECT encounter_date, encounter_type, facility_type, primary_dx_code, discharge_status
        FROM encounters
        WHERE patient_id = ?
        ORDER BY encounter_date DESC
        LIMIT 6;
        """,
        [patient_id],
    )

    latest_sbp = _latest_values(patient_id, "SBP")
    latest_dbp = _latest_values(patient_id, "DBP")

    flags: list[str] = []

    if latest_hba1c_series:
        current = latest_hba1c_series[0]
        value = current.get("lab_result")
        if isinstance(value, (int, float)) and value >= 8:
            flags.append(
                f"HbA1c {value:.1f}% on {current['lab_date']} (above <8% target)."
            )
        if len(latest_hba1c_series) >= 2:
            previous = latest_hba1c_series[1]
            prev_value = previous.get("lab_result")
            if isinstance(value, (int, float)) and isinstance(prev_value, (int, float)):
                delta = value - prev_value
                if abs(delta) >= 0.3:
                    direction = "higher" if delta > 0 else "lower"
                    flags.append(
                        f"HbA1c is {abs(delta):.1f} points {direction} than prior reading ({previous['lab_date']})."
                    )

    if latest_sbp and latest_dbp:
        sbp = latest_sbp.get("measure_value")
        dbp = latest_dbp.get("measure_value")
        if isinstance(sbp, (int, float)) and sbp >= 140:
            flags.append(f"Blood pressure elevated at {int(round(sbp))}/{int(round(dbp or 0))} mmHg (latest {latest_sbp['measure_date']}).")
        elif isinstance(sbp, (int, float)) and isinstance(dbp, (int, float)):
            flags.append(
                f"Blood pressure currently {int(round(sbp))}/{int(round(dbp))} mmHg (latest {latest_sbp['measure_date']})."
            )

    if latest_egfr:
        current = latest_egfr[0]
        value = current.get("lab_result")
        if isinstance(value, (int, float)) and value < 60:
            flags.append(f"eGFR {value:.0f} mL/min/1.73m2 (possible CKD stage 3).")

    today = dt.date.today()
    age = today.year - int(patient.get("year_of_birth", today.year))

    chronic_list = ", ".join(row["condition_label"] for row in chronic_conditions) or "None documented"
    risk_label = risk.get("risk_tier", "UNASSESSED")

    summary_lines = [
        f"# Patient Profile — {patient_id}",
        f"**Name:** {patient_id} | **Sex:** {patient['sex']} | **Year of birth:** {patient['year_of_birth']} (≈{age} yr)",
        f"**Primary payer:** {patient['insurance_type']} | **Risk tier:** {risk_label}",
        f"**Chronic conditions:** {chronic_list}",
        "\n## Key Clinical Flags",
        _format_flags(flags),
        "\n## Recent Labs",
        md_table(["Date", "Lab", "Result", "Units"], ((lab["lab_date"], lab["lab_name"], lab["lab_result"], lab["units"]) for lab in labs)),
        "\n## Recent Medications",
        md_table(["Medication", "Start", "End", "Dose", "Route"], ((med["medication_name"], med["med_start"], med["med_end"], med["dose"], med["route"]) for med in medications)),
        "\n## Recent Encounters",
        md_table(
            ["Date", "Type", "Facility", "Primary DX", "Discharge"],
            (
                (
                    enc["encounter_date"],
                    enc["encounter_type"],
                    enc["facility_type"],
                    enc["primary_dx_code"],
                    enc["discharge_status"],
                )
                for enc in encounters
            ),
        ),
    ]

    structured = {
        "patient": patient,
        "risk": risk_label,
        "chronic_conditions": [row["condition_label"] for row in chronic_conditions],
        "flags": flags,
        "labs": labs,
        "medications": medications,
        "encounters": encounters,
        "latest_vitals": {
            "sbp": latest_sbp,
            "dbp": latest_dbp,
        },
        "latest_hba1c_series": latest_hba1c_series,
        "latest_egfr_series": latest_egfr,
    }

    return CallToolResult(
        content=[TextContent(type="text", text="\n".join(summary_lines))],
        structuredContent=structured,
        _meta={
            "openai/outputTemplate": "ui://homecare-cohort/patient-profile.html",
            "openai/widgetAccessible": True,
            **structured,
        },
    )


@mcp.tool(
    name="care_gap_closure_plan",
    description=(
        "Generate the Step 1b care gap closure table for flagged patients. "
        "Returns HEDIS gaps, days overdue, recommended channel, and alternate therapy tags."
    ),
    meta={
        "ui": {"resourceUri": "ui://homecare-cohort/care-gap-plan.html"},
        "ui/resourceUri": "ui://homecare-cohort/care-gap-plan.html",
        "openai/widgetAccessible": True,
        "openai/outputTemplate": "ui://homecare-cohort/care-gap-plan.html",
    },
)
def care_gap_closure_plan(
    ctx: Context,
    patient_ids: Optional[List[str]] = None,
    limit: int = 6,
) -> CallToolResult:  # noqa: ARG001
    selected = _select_highrisk_patients(patient_ids=patient_ids, limit=limit)
    if not selected:
        message = "No patients available to build a care gap closure plan."
        return CallToolResult(content=[TextContent(type="text", text=message)], structuredContent={"patients": []})

    headers = ["Gap", "Measure ID", "Days overdue", "Next action", "Quality impact", "Alternate therapy?"]
    patient_sections: List[str] = []
    structured_patients: List[dict] = []

    for item in selected:
        gaps = _build_gap_plan(item)
        structured_patients.append({"patient": item, "gaps": gaps})

        rows = [
            [
                gap["gap_name"],
                gap["gap_id"],
                gap["days_overdue"] if gap["days_overdue"] is not None else "—",
                gap["closure_opportunity"],
                gap["quality_score_impact"],
                "Yes" if gap["alternate_therapy_flag"] else "No",
            ]
            for gap in gaps
        ]
        table = md_table(headers, rows) if rows else "_No actionable gaps identified._"
        patient_sections.append(
            "\n".join(
                [
                    f"## {item['patient_id']} — {item.get('risk_tier')} risk",
                    f"*Focus areas:* {'; '.join(item.get('severity_reasons') or [])}",
                    table,
                ]
            )
        )

    markdown = "\n\n".join(
        [
            "# Care Gap Closure Plan (Step 1b)",
            "Prioritized gaps for each flagged patient with suggested closure channels and quality score impact.",
            *patient_sections,
        ]
    )

    structured = {
        "generated_at": dt.datetime.utcnow().isoformat() + "Z",
        "patients": structured_patients,
        "assumptions": {
            "hedis_ids": {
                "CDC-HbA1c": "Comprehensive Diabetes Care: HbA1c Control <8%",
                "CBP": "Controlling High Blood Pressure",
                "CDC-Eye": "Comprehensive Diabetes Care: Eye Exam",
                "CDC-Neph": "Comprehensive Diabetes Care: Nephropathy Screening",
            },
            "alternate_therapy_flag_logic": "Triggered when severity ≥ 7 or key metrics remain uncontrolled (HbA1c ≥ 9.5 or SBP ≥ 170).",
        },
        "source_tools": {
            "cohort_derivation": "get_highrisk_cohort",
            "optional_hdi_tools": [
                "hdi_search_concepts",
                "hdi_get_codings",
                "hdi_resolve_code",
            ],
        },
    }

    return CallToolResult(
        content=[TextContent(type="text", text=markdown)],
        structuredContent=structured,
        _meta={
            "openai/outputTemplate": "ui://homecare-cohort/care-gap-plan.html",
            "openai/widgetAccessible": True,
            **structured,
        },
    )


_http_app = mcp.streamable_http_app()


@contextlib.asynccontextmanager
async def lifespan(app: Starlette):
    async with mcp.session_manager.run():
        yield


def build_app() -> Starlette:
    app = Starlette(routes=[Mount("/mcp", app=_http_app)], lifespan=lifespan)

    app = CORSMiddleware(
        app,
        allow_origins=["*"],
        allow_methods=["GET", "POST", "OPTIONS"],
        allow_headers=["Content-Type", "Authorization", "Mcp-Session-Id"],
        expose_headers=["Mcp-Session-Id"],
    )
    return app


app = build_app()


if __name__ == "__main__":
    from mcp.server.stdio import StdioServerTransport

    transport = StdioServerTransport(mcp)
    mcp.run(transport)
