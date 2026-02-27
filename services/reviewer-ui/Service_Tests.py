from __future__ import annotations

import datetime as dt
import json
from typing import Any

import streamlit as st

from utils import (
    CALCULATION_URL,
    DB_URL,
    DEFAULT_BOUNDARY_PATH,
    DEFAULT_CONFIG_VERSION,
    DEFAULT_END_DATE,
    DEFAULT_HERD_PATH,
    DEFAULT_MODEL_VERSION,
    DEFAULT_SOURCE_DB,
    DEFAULT_START_DATE,
    get_default_boundary_id,
    get_default_calc_date,
    parse_date_or_default,
    run_command,
    run_http_get,
    run_http_json,
)


st.set_page_config(page_title="Service Tests", layout="wide")


def _parse_json_text(value: str) -> dict[str, Any] | None:
    text = value.strip()
    if not text:
        return None
    try:
        parsed = json.loads(text)
    except Exception:
        return None
    return parsed if isinstance(parsed, dict) else None


def _render_failure(result: dict[str, Any]) -> None:
    stderr = str(result.get("stderr", "")).strip()
    stdout = str(result.get("stdout", "")).strip()
    message = stderr or stdout or "No error message available."
    st.code(message)


def _render_ingestion_summary(summary: dict[str, Any]) -> None:
    metric_1, metric_2, metric_3 = st.columns(3)
    metric_1.metric("Records Ingested", int(summary.get("records_ingested", 0)))
    metric_2.metric("Backfilled Weather Days", int(summary.get("weather_backfilled_days", 0)))
    metric_3.metric("RAP Staleness (Days)", int(summary.get("rap_staleness_days", 0)))

    left_col, right_col = st.columns(2)
    with left_col:
        st.write(f"**Run ID:** `{summary.get('run_id', '')}`")
        st.write(f"**Snapshot ID:** `{summary.get('snapshot_id', '')}`")
        st.write(f"**Boundary ID:** `{summary.get('boundary_id', '')}`")
    with right_col:
        st.write(f"**Start Date:** `{summary.get('timeframe_start', '')}`")
        st.write(f"**End Date:** `{summary.get('timeframe_end', '')}`")
        st.write(f"**Weather Source:** `{summary.get('weather_source', '')}`")

    quality = summary.get("quality")
    if isinstance(quality, dict) and quality:
        st.subheader("Daily Join Coverage")
        rows = [{"field": key, "value": value} for key, value in quality.items()]
        st.dataframe(rows, use_container_width=True, hide_index=True)


def _render_calculation_summary(payload: dict[str, Any]) -> None:
    summary = payload.get("result")
    if not isinstance(summary, dict):
        st.warning("No calculation result returned.")
        return

    metric_1, metric_2, metric_3 = st.columns(3)
    metric_1.metric("Days Remaining", f"{float(summary.get('days_of_grazing_remaining', 0.0)):.1f}")
    metric_2.metric("Move Date", str(summary.get("recommended_move_date", "")))
    metric_3.metric("Model Version", str(summary.get("model_version", "")))

    left_col, right_col = st.columns(2)
    with left_col:
        st.write(f"**Recommendation ID:** `{summary.get('recommendation_id', '')}`")
        st.write(f"**Run ID:** `{summary.get('run_id', '')}`")
        st.write(f"**Boundary ID:** `{summary.get('boundary_id', '')}`")
    with right_col:
        st.write(f"**Calculation Date:** `{summary.get('calculation_date', '')}`")
        st.write(f"**Snapshot ID:** `{summary.get('snapshot_id', '')}`")
        st.write(f"**Decision Snapshot ID:** `{summary.get('decision_snapshot_id', '')}`")


def _render_explain_summary(payload: dict[str, Any]) -> None:
    recommendation = payload.get("recommendation")
    lineage = payload.get("lineage")

    if isinstance(recommendation, dict):
        metric_1, metric_2, metric_3 = st.columns(3)
        metric_1.metric(
            "Days Remaining",
            f"{float(recommendation.get('days_of_grazing_remaining', 0.0)):.1f}",
        )
        metric_2.metric("Move Date", str(recommendation.get("recommended_move_date", "")))
        metric_3.metric("Model Version", str(recommendation.get("model_version", "")))

    if not isinstance(lineage, dict):
        st.warning("Lineage payload missing.")
        return

    calc_run = lineage.get("calculation_run") or {}
    ingestion_run = lineage.get("ingestion_run") or {}
    model = lineage.get("model") or {}
    checks = lineage.get("quality_checks") or []

    st.subheader("Lineage Summary")
    left_col, right_col = st.columns(2)
    with left_col:
        st.write(f"**Calculation Run:** `{calc_run.get('run_id', '')}`")
        st.write(f"**Calculation Status:** `{calc_run.get('status', '')}`")
        st.write(f"**Model Version:** `{model.get('version_id', '')}`")
        st.write(f"**Config Version:** `{recommendation.get('config_version', '') if isinstance(recommendation, dict) else ''}`")
    with right_col:
        st.write(f"**Ingestion Run:** `{ingestion_run.get('ingestion_run_id', '')}`")
        st.write(f"**Ingestion Snapshot:** `{ingestion_run.get('snapshot_id', '')}`")
        st.write(f"**Ingestion Status:** `{ingestion_run.get('status', '')}`")

    if isinstance(checks, list) and checks:
        rows: list[dict[str, Any]] = []
        passed_count = 0
        for check in checks:
            if not isinstance(check, dict):
                continue
            passed = bool(check.get("passed"))
            passed_count += int(passed)
            rows.append(
                {
                    "check_name": check.get("check_name"),
                    "check_type": check.get("check_type"),
                    "passed": passed,
                    "checked_at": check.get("checked_at"),
                }
            )
        st.subheader("Data Quality Checks")
        st.caption(f"Passed {passed_count}/{len(rows)} checks")
        st.dataframe(rows, use_container_width=True, hide_index=True)


def _render_result(kind: str, result: dict[str, Any]) -> None:
    exit_code = int(result.get("returncode", 1))
    if exit_code == 0:
        st.success("Success")
    elif bool(result.get("timed_out")):
        st.error("Timed out")
    else:
        st.error(f"Failed (exit {exit_code})")
        _render_failure(result)
        return

    parsed = _parse_json_text(str(result.get("stdout", "")))
    if parsed is None:
        st.warning("Response was not JSON.")
        _render_failure(result)
        return

    if kind == "ingestion":
        _render_ingestion_summary(parsed)
    elif kind == "calculation":
        _render_calculation_summary(parsed)
    elif kind == "explain":
        _render_explain_summary(parsed)

    raw_stderr = str(result.get("stderr", "")).strip()
    if raw_stderr:
        with st.expander("stderr"):
            st.code(raw_stderr)
    with st.expander("Raw JSON"):
        st.json(parsed)


if "boundary_id" not in st.session_state:
    st.session_state["boundary_id"] = get_default_boundary_id()
if "start_date" not in st.session_state:
    st.session_state["start_date"] = parse_date_or_default(DEFAULT_START_DATE, dt.date(2024, 1, 1))
if "end_date" not in st.session_state:
    st.session_state["end_date"] = parse_date_or_default(DEFAULT_END_DATE, dt.date(2024, 12, 31))
if "calculation_date" not in st.session_state:
    st.session_state["calculation_date"] = get_default_calc_date(st.session_state["boundary_id"])
if "service_test_result" not in st.session_state:
    st.session_state["service_test_result"] = None
if "service_test_kind" not in st.session_state:
    st.session_state["service_test_kind"] = ""


st.title("Service Tests")
st.caption("Run ingestion, calculation, and explain checks with prefilled inputs.")

tab_ingest, tab_calculate, tab_explain = st.tabs(["Ingestion", "Calculation", "Explain"])

with tab_ingest:
    settings_col, payload_col = st.columns(2)

    with settings_col:
        with st.form("ingestion_form"):
            boundary_id = st.text_input("Boundary ID", st.session_state["boundary_id"])
            date_col_1, date_col_2 = st.columns(2)
            with date_col_1:
                start_date = st.date_input("Start date", st.session_state["start_date"])
            with date_col_2:
                end_date = st.date_input("End date", st.session_state["end_date"])
            backfill_weather = st.checkbox("Backfill weather", value=True)
            run_ingest = st.form_submit_button("Run ingestion")

    ingestion_payload = {
        "db_url": DB_URL,
        "source_db": DEFAULT_SOURCE_DB,
        "boundary_path": DEFAULT_BOUNDARY_PATH,
        "herd_path": DEFAULT_HERD_PATH,
        "boundary_id": boundary_id,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "backfill_weather": backfill_weather,
    }
    with payload_col:
        st.json(ingestion_payload)

    if run_ingest:
        st.session_state["boundary_id"] = boundary_id
        st.session_state["start_date"] = start_date
        st.session_state["end_date"] = end_date

        cmd = [
            "python3",
            "services/ingestion-worker/main.py",
            "--source-db",
            DEFAULT_SOURCE_DB,
            "--boundary-path",
            DEFAULT_BOUNDARY_PATH,
            "--herd-path",
            DEFAULT_HERD_PATH,
            "--boundary-id",
            boundary_id,
            "--start-date",
            start_date.isoformat(),
            "--end-date",
            end_date.isoformat(),
        ]
        if DB_URL:
            cmd.extend(["--db-url", DB_URL])
        if backfill_weather:
            cmd.append("--backfill-weather")

        st.session_state["service_test_kind"] = "ingestion"
        st.session_state["service_test_result"] = run_command(cmd, timeout_seconds=420)

with tab_calculate:
    settings_col, payload_col = st.columns(2)

    with settings_col:
        with st.form("calculation_form"):
            calculation_url = st.text_input("Calculation URL", CALCULATION_URL)
            calc_boundary = st.text_input("Boundary ID", st.session_state["boundary_id"], key="calc_boundary")
            calculation_date = st.date_input("Calculation date", st.session_state["calculation_date"])
            model_version = st.selectbox(
                "Model version",
                ["v1", "v2"],
                index=1 if DEFAULT_MODEL_VERSION == "v2" else 0,
            )
            config_version = st.text_input("Config version", DEFAULT_CONFIG_VERSION)
            run_calculation = st.form_submit_button("Run calculation")

    calculation_payload = {
        "boundary_id": calc_boundary,
        "calculation_date": calculation_date.isoformat(),
        "model_version": model_version,
        "config_version": config_version,
    }
    with payload_col:
        st.json(calculation_payload)

    if run_calculation:
        st.session_state["boundary_id"] = calc_boundary
        st.session_state["calculation_date"] = calculation_date
        endpoint = calculation_url.rstrip("/") + "/calculate"
        st.session_state["service_test_kind"] = "calculation"
        st.session_state["service_test_result"] = run_http_json(
            endpoint, calculation_payload, timeout_seconds=180
        )

with tab_explain:
    settings_col, payload_col = st.columns(2)

    with settings_col:
        with st.form("explain_form"):
            explain_url = st.text_input("Calculation URL", CALCULATION_URL, key="explain_url")
            explain_boundary = st.text_input(
                "Boundary ID",
                st.session_state["boundary_id"],
                key="explain_boundary",
            )
            explain_date = st.date_input(
                "Calculation date",
                st.session_state["calculation_date"],
                key="explain_date",
            )
            recommendation_id = st.text_input("Recommendation ID (optional)", "")
            run_explain = st.form_submit_button("Run explain")

    explain_payload: dict[str, str] = {"calculation_date": explain_date.isoformat()}
    if explain_boundary.strip():
        explain_payload["boundary_id"] = explain_boundary.strip()
    if recommendation_id.strip():
        explain_payload["recommendation_id"] = recommendation_id.strip()
    with payload_col:
        st.json(explain_payload)

    if run_explain:
        if explain_boundary.strip():
            st.session_state["boundary_id"] = explain_boundary.strip()
        st.session_state["calculation_date"] = explain_date
        endpoint = explain_url.rstrip("/") + "/recommendations/explain"
        st.session_state["service_test_kind"] = "explain"
        st.session_state["service_test_result"] = run_http_get(
            endpoint, explain_payload, timeout_seconds=120
        )


if st.session_state["service_test_result"] is not None:
    st.divider()
    _render_result(st.session_state["service_test_kind"], st.session_state["service_test_result"])
