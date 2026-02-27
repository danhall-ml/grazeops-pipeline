from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st

from utils import DEFAULT_BOUNDARY_PATH, get_default_boundary_id, get_default_calc_date, run_http_get


CALCULATION_URL = "http://calculation-service:8081"


st.set_page_config(page_title="Grazing Guidance", layout="wide")


def _parse_json_result(result: dict[str, Any]) -> dict[str, Any] | None:
    if int(result.get("returncode", 1)) != 0:
        return None
    try:
        payload = json.loads(str(result.get("stdout", "")).strip())
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def _as_int(value: Any, default: int | None = None) -> int | None:
    try:
        return int(value)
    except Exception:
        return default


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _load_geojson_points(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=["lat", "lon"])

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return pd.DataFrame(columns=["lat", "lon"])

    geometry: dict[str, Any] | None = None
    payload_type = str(payload.get("type") or "") if isinstance(payload, dict) else ""
    if payload_type == "FeatureCollection":
        features = payload.get("features")
        if isinstance(features, list) and features and isinstance(features[0], dict):
            geom = features[0].get("geometry")
            if isinstance(geom, dict):
                geometry = geom
    elif payload_type == "Feature":
        geom = payload.get("geometry")
        if isinstance(geom, dict):
            geometry = geom
    elif payload_type in {"Polygon", "MultiPolygon"}:
        geometry = payload

    if not isinstance(geometry, dict):
        return pd.DataFrame(columns=["lat", "lon"])

    geometry_type = str(geometry.get("type") or "")
    coords = geometry.get("coordinates")
    points: list[dict[str, float]] = []

    def append_points(raw_points: Any) -> None:
        if not isinstance(raw_points, list):
            return
        for point in raw_points:
            if isinstance(point, list) and len(point) >= 2:
                lon = _as_float(point[0], None)
                lat = _as_float(point[1], None)
                if lon is not None and lat is not None:
                    points.append({"lat": lat, "lon": lon})

    if geometry_type == "Polygon" and isinstance(coords, list) and coords:
        append_points(coords[0])
    elif geometry_type == "MultiPolygon" and isinstance(coords, list) and coords:
        first_poly = coords[0]
        if isinstance(first_poly, list) and first_poly:
            append_points(first_poly[0])

    return pd.DataFrame(points, columns=["lat", "lon"])


def _confidence(checks: list[dict[str, Any]]) -> tuple[str, str]:
    if not checks:
        return ("Unknown", "Data confidence is not available for this run.")

    missing_weather_days = None
    missing_rap_days = None
    rap_staleness_days = None
    rap_threshold_days = None

    for check in checks:
        details = check.get("details")
        if not isinstance(details, dict):
            continue
        name = str(check.get("check_name") or "")
        if name == "daily_join_coverage":
            missing_weather_days = _as_int(details.get("missing_weather_days"))
            missing_rap_days = _as_int(details.get("missing_rap_days"))
        elif name == "rap_not_stale":
            rap_staleness_days = _as_int(details.get("staleness_days"))
            rap_threshold_days = _as_int(details.get("threshold_days"))

    low = (
        (missing_weather_days is not None and missing_weather_days > 0)
        or (missing_rap_days is not None and missing_rap_days > 0)
        or (
            rap_staleness_days is not None
            and rap_threshold_days is not None
            and rap_staleness_days > rap_threshold_days
        )
    )
    if low:
        return ("Low", "Some key inputs are missing or out of date. Confirm field conditions before moving.")

    failed_checks = sum(1 for c in checks if isinstance(c, dict) and not bool(c.get("passed")))
    if failed_checks > 0:
        return ("Medium", "Most checks passed, but not all. Use this plan together with recent pasture observations.")

    return ("High", "Data checks are healthy for this pasture and date.")


def _friendly_pasture_name(boundary_meta: dict[str, Any], recommendation: dict[str, Any]) -> str:
    display_name = str(boundary_meta.get("name") or "").strip()
    if display_name:
        return display_name

    raw = str(recommendation.get("boundary_id") or "").strip()
    if not raw:
        return "Selected Pasture"
    cleaned = raw.replace("_", " ").replace("-", " ").strip()
    if cleaned.lower().startswith("boundary "):
        cleaned = cleaned[9:].strip()
    return cleaned.title() if cleaned else "Selected Pasture"


if "viz_boundary_id" not in st.session_state:
    st.session_state["viz_boundary_id"] = get_default_boundary_id()
if "viz_calc_date" not in st.session_state:
    st.session_state["viz_calc_date"] = get_default_calc_date(st.session_state["viz_boundary_id"])
if "viz_latest_result" not in st.session_state:
    st.session_state["viz_latest_result"] = None
if "viz_explain_result" not in st.session_state:
    st.session_state["viz_explain_result"] = None


st.title("Grazing Guidance")
st.caption("Use this page to decide when to move the herd from the selected pasture.")

with st.form("guidance_form"):
    c1, c2, c3 = st.columns([1.5, 1.2, 0.7])
    with c1:
        boundary_id = st.text_input("Pasture ID", st.session_state["viz_boundary_id"])
    with c2:
        calculation_date = st.date_input("Date", st.session_state["viz_calc_date"])
    with c3:
        st.markdown("##### ")
        load_plan = st.form_submit_button("Load Plan", use_container_width=True)

if load_plan:
    st.session_state["viz_boundary_id"] = boundary_id
    st.session_state["viz_calc_date"] = calculation_date
    query = {"boundary_id": boundary_id, "calculation_date": calculation_date.isoformat()}
    latest_url = CALCULATION_URL.rstrip("/") + "/recommendations/latest"
    explain_url = CALCULATION_URL.rstrip("/") + "/recommendations/explain"
    st.session_state["viz_latest_result"] = run_http_get(latest_url, query, timeout_seconds=60)
    st.session_state["viz_explain_result"] = run_http_get(explain_url, query, timeout_seconds=60)

latest_result = st.session_state["viz_latest_result"]
if latest_result is None:
    st.info("Select a pasture and date, then click Load Plan.")
    st.stop()

latest_payload = _parse_json_result(latest_result)
if latest_payload is None:
    st.error("Recommendation is unavailable right now.")
    st.stop()

recommendation = latest_payload.get("recommendation")
if not isinstance(recommendation, dict):
    st.warning("No recommendation found for this pasture and date.")
    st.stop()

explain_payload = _parse_json_result(st.session_state["viz_explain_result"] or {})
lineage = explain_payload.get("lineage") if isinstance(explain_payload, dict) else {}
lineage = lineage if isinstance(lineage, dict) else {}
boundary_meta = lineage.get("boundary") if isinstance(lineage.get("boundary"), dict) else {}
herd_meta = lineage.get("herd_configuration") if isinstance(lineage.get("herd_configuration"), dict) else {}
checks = lineage.get("quality_checks") if isinstance(lineage.get("quality_checks"), list) else []
checks = [item for item in checks if isinstance(item, dict)]

days_remaining = _as_float(recommendation.get("days_of_grazing_remaining"))
move_date = str(recommendation.get("recommended_move_date") or "")
available_forage = _as_float(recommendation.get("available_forage_kg"))
daily_demand = _as_float(recommendation.get("daily_consumption_kg"))
confidence_label, confidence_message = _confidence(checks)

ranch_name = "Your Ranch"
pasture_name = _friendly_pasture_name(boundary_meta, recommendation)

st.subheader(f"{ranch_name} - {pasture_name}")

m1, m2, m3, m4 = st.columns(4)
m1.metric("Move Herd By", move_date)
m2.metric("Days Left", f"{days_remaining:.1f}")
m3.metric("Daily Feed Need", f"{daily_demand:,.0f} kg")
m4.metric("Available Forage", f"{available_forage:,.0f} kg")

if days_remaining < 3:
    st.error("Move soon. This pasture is close to running out.")
elif days_remaining < 7:
    st.warning("Plan your move this week.")
else:
    st.success("Based on current forage estimates, the herd can remain in this pasture for the near term.")

left, right = st.columns([1.5, 1.0])

with left:
    st.subheader("Pasture Map")
    source_file = str(boundary_meta.get("source_file") or DEFAULT_BOUNDARY_PATH)
    points = _load_geojson_points(Path(source_file))
    if points.empty and source_file != DEFAULT_BOUNDARY_PATH:
        points = _load_geojson_points(Path(DEFAULT_BOUNDARY_PATH))
    if points.empty:
        st.info("The pasture map is unavailable for this selection.")
    else:
        st.map(points, use_container_width=True)

with right:
    st.subheader("Confidence")
    if confidence_label == "High":
        st.success("Confidence is high.")
    elif confidence_label == "Medium":
        st.warning("Confidence is medium.")
    elif confidence_label == "Low":
        st.error("Confidence is low.")
    else:
        st.info("Confidence is unknown.")
    st.write(confidence_message)

    st.subheader("Recommended Next Step")
    if days_remaining < 3:
        st.write("Move preparation should start now.")
        st.write("Confirm water access and fencing in the next pasture.")
    elif days_remaining < 7:
        st.write("Schedule the move for this week.")
        st.write("Re-check this plan in the next 24 to 48 hours.")
    else:
        st.write("Keep the herd in the current pasture for now.")
        st.write("Review this plan again in a few days.")
