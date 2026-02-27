from __future__ import annotations

import json
import sqlite3
import urllib.parse
import urllib.request
from datetime import date
from pathlib import Path
from typing import Any

from .util import date_iter, parse_date, slugify


def fetch_source_nrcs(source_conn: sqlite3.Connection, boundary_id: str) -> list[dict[str, Any]]:
    """Load NRCS soil rows for a boundary from the reference database.

    Parameters
    ----------
    source_conn : sqlite3.Connection
        Connection to the source reference database.
    boundary_id : str
        Boundary identifier.

    Returns
    -------
    list[dict[str, Any]]
        Soil rows normalized for ingestion writes.
    """
    rows = source_conn.execute(
        """
        SELECT mukey, component_name, productivity_index, land_capability_class,
               hydrologic_group, available_water_capacity, source_version
        FROM nrcs_soil_data
        WHERE boundary_id = ?
        ORDER BY id
        """,
        (boundary_id,),
    ).fetchall()
    return [
        {
            "mukey": r[0],
            "component_name": r[1],
            "productivity_index": r[2],
            "land_capability_class": r[3],
            "hydrologic_group": r[4],
            "available_water_capacity": r[5],
            "source_version": r[6] or "reference_db",
        }
        for r in rows
    ]


def fetch_source_rap(
    source_conn: sqlite3.Connection, boundary_id: str, start_date: date, end_date: date
) -> list[dict[str, Any]]:
    """Load RAP biomass rows for a boundary date window.

    Parameters
    ----------
    source_conn : sqlite3.Connection
        Connection to the source reference database.
    boundary_id : str
        Boundary identifier.
    start_date : date
        Inclusive start date.
    end_date : date
        Inclusive end date.

    Returns
    -------
    list[dict[str, Any]]
        RAP rows normalized for ingestion writes.
    """
    rows = source_conn.execute(
        """
        SELECT composite_date, biomass_kg_per_ha, annual_herbaceous_cover_pct, ndvi, source_version
        FROM rap_biomass
        WHERE boundary_id = ? AND composite_date >= ? AND composite_date <= ?
        ORDER BY composite_date
        """,
        (boundary_id, start_date.isoformat(), end_date.isoformat()),
    ).fetchall()
    return [
        {
            "composite_date": r[0],
            "biomass_kg_per_ha": r[1],
            "annual_herbaceous_cover_pct": r[2],
            "ndvi": r[3],
            "source_version": r[4] or "reference_db",
        }
        for r in rows
    ]


def fetch_source_weather(
    source_conn: sqlite3.Connection, boundary_id: str, start_date: date, end_date: date
) -> list[dict[str, Any]]:
    """Load weather rows for a boundary date window from source DB.

    Parameters
    ----------
    source_conn : sqlite3.Connection
        Connection to the source reference database.
    boundary_id : str
        Boundary identifier.
    start_date : date
        Inclusive start date.
    end_date : date
        Inclusive end date.

    Returns
    -------
    list[dict[str, Any]]
        Weather rows normalized for ingestion writes.
    """
    rows = source_conn.execute(
        """
        SELECT forecast_date, latitude, longitude, precipitation_mm, temp_max_c,
               temp_min_c, wind_speed_kmh, source_version
        FROM weather_forecasts
        WHERE boundary_id = ? AND forecast_date >= ? AND forecast_date <= ?
        ORDER BY forecast_date
        """,
        (boundary_id, start_date.isoformat(), end_date.isoformat()),
    ).fetchall()
    return [
        {
            "forecast_date": r[0],
            "latitude": r[1],
            "longitude": r[2],
            "precipitation_mm": r[3],
            "temp_max_c": r[4],
            "temp_min_c": r[5],
            "wind_speed_kmh": r[6],
            "source_version": r[7] or "reference_db",
        }
        for r in rows
    ]


def fetch_openmeteo_weather(
    lat: float, lon: float, start_date: date, end_date: date
) -> list[dict[str, Any]]:
    """Fetch daily weather data from Open-Meteo.

    Parameters
    ----------
    lat : float
        Boundary centroid latitude.
    lon : float
        Boundary centroid longitude.
    start_date : date
        Inclusive start date.
    end_date : date
        Inclusive end date.

    Returns
    -------
    list[dict[str, Any]]
        Daily weather rows formatted for ingestion writes.
    """
    params = {
        "latitude": f"{lat:.6f}",
        "longitude": f"{lon:.6f}",
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "timezone": "UTC",
        "daily": "precipitation_sum,temperature_2m_max,temperature_2m_min,windspeed_10m_max",
    }
    url = "https://api.open-meteo.com/v1/forecast?" + urllib.parse.urlencode(params)
    with urllib.request.urlopen(url, timeout=20) as response:
        payload = json.loads(response.read().decode("utf-8"))

    daily = payload.get("daily", {})
    dates = daily.get("time", [])
    precip = daily.get("precipitation_sum", [])
    tmax = daily.get("temperature_2m_max", [])
    tmin = daily.get("temperature_2m_min", [])
    wind = daily.get("windspeed_10m_max", [])

    rows: list[dict[str, Any]] = []
    for idx, day_text in enumerate(dates):
        rows.append(
            {
                "forecast_date": day_text,
                "latitude": lat,
                "longitude": lon,
                "precipitation_mm": float(precip[idx]) if idx < len(precip) else None,
                "temp_max_c": float(tmax[idx]) if idx < len(tmax) else None,
                "temp_min_c": float(tmin[idx]) if idx < len(tmin) else None,
                "wind_speed_kmh": float(wind[idx]) if idx < len(wind) else None,
                "source_version": "openmeteo_live",
            }
        )
    return rows


def backfill_weather(
    weather_rows: list[dict[str, Any]],
    start_date: date,
    end_date: date,
    boundary: dict[str, Any],
    enabled: bool,
) -> tuple[list[dict[str, Any]], int, int]:
    """Fill missing weather days in a requested window.

    Parameters
    ----------
    weather_rows : list[dict[str, Any]]
        Existing weather rows.
    start_date : date
        Inclusive start date.
    end_date : date
        Inclusive end date.
    boundary : dict[str, Any]
        Boundary payload with centroid fields for fallback rows.
    enabled : bool
        Whether missing days should be backfilled.

    Returns
    -------
    tuple[list[dict[str, Any]], int, int]
        Backfill result as ``(rows, backfilled_count, missing_count)``.
    """
    by_day = {parse_date(str(row["forecast_date"])): row for row in weather_rows}
    result: list[dict[str, Any]] = []
    backfilled = 0
    missing = 0

    available = sorted(by_day.keys())
    first_row = by_day[available[0]] if available else None
    last_row: dict[str, Any] | None = None

    for day in date_iter(start_date, end_date):
        existing = by_day.get(day)
        if existing:
            result.append(existing)
            last_row = existing
            continue

        if not enabled:
            missing += 1
            continue

        if last_row is not None:
            template = last_row
        elif first_row is not None:
            template = first_row
        else:
            template = {
                "latitude": boundary["centroid_lat"],
                "longitude": boundary["centroid_lon"],
                "precipitation_mm": 0.0,
                "temp_max_c": None,
                "temp_min_c": None,
                "wind_speed_kmh": None,
                "source_version": "weather_backfill_empty",
            }

        result.append(
            {
                "forecast_date": day.isoformat(),
                "latitude": template.get("latitude", boundary["centroid_lat"]),
                "longitude": template.get("longitude", boundary["centroid_lon"]),
                "precipitation_mm": template.get("precipitation_mm"),
                "temp_max_c": template.get("temp_max_c"),
                "temp_min_c": template.get("temp_min_c"),
                "wind_speed_kmh": template.get("wind_speed_kmh"),
                "source_version": "weather_backfill",
            }
        )
        backfilled += 1

    result.sort(key=lambda r: r["forecast_date"])
    return result, backfilled, missing


def select_herd_config(
    herd_path: Path,
    ranch_id: str,
    pasture_id: str,
    boundary_id: str,
    end_date: date,
) -> dict[str, Any]:
    """Select and normalize the active herd configuration for a boundary.

    Parameters
    ----------
    herd_path : Path
        Path to herd JSON input.
    ranch_id : str
        Ranch identifier for filtering.
    pasture_id : str
        Pasture identifier for filtering.
    boundary_id : str
        Boundary identifier attached to the selected record.
    end_date : date
        Effective-date cutoff used for selecting the latest valid herd.

    Returns
    -------
    dict[str, Any]
        Normalized herd configuration record.
    """
    payload = json.loads(herd_path.read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        raise ValueError("Herd sample JSON must be a list")

    candidates = [
        row
        for row in payload
        if row.get("operation_id") == ranch_id and row.get("pasture_id") == pasture_id
    ]
    if not candidates:
        raise ValueError("No herd config found for boundary ranch/pasture")

    dated: list[tuple[date, dict[str, Any]]] = []
    for row in candidates:
        effective = parse_date(str(row.get("effective_date")))
        if effective <= end_date:
            dated.append((effective, row))
    if not dated:
        dated = [(parse_date(str(candidates[0]["effective_date"])), candidates[0])]

    effective_date, selected = max(dated, key=lambda item: item[0])
    herd = selected.get("herd") or {}
    if int(herd.get("animal_count", 0)) <= 0:
        raise ValueError("Selected herd has invalid animal_count")
    if float(herd.get("daily_intake_kg_per_head", 0)) <= 0:
        raise ValueError("Selected herd has invalid daily_intake_kg_per_head")

    config_id = f"herd_{slugify(ranch_id)}_{slugify(pasture_id)}_{effective_date.isoformat()}"
    return {
        "id": config_id,
        "ranch_id": ranch_id,
        "pasture_id": pasture_id,
        "boundary_id": boundary_id,
        "animal_count": int(herd["animal_count"]),
        "animal_type": str(herd.get("animal_type") or "unknown"),
        "daily_intake_kg_per_head": float(herd["daily_intake_kg_per_head"]),
        "avg_daily_gain_kg": float(herd.get("average_daily_gain_kg") or 0.0),
        "config_snapshot_json": json.dumps(selected),
        "valid_from": effective_date.isoformat(),
        "valid_to": None,
    }


def daily_join_coverage(
    start_date: date, end_date: date, rap_rows: list[dict[str, Any]], weather_rows: list[dict[str, Any]]
) -> dict[str, int]:
    """Compute daily RAP and weather coverage over a date window.

    Parameters
    ----------
    start_date : date
        Inclusive start date.
    end_date : date
        Inclusive end date.
    rap_rows : list[dict[str, Any]]
        RAP rows containing ``composite_date``.
    weather_rows : list[dict[str, Any]]
        Weather rows containing ``forecast_date``.

    Returns
    -------
    dict[str, int]
        Coverage summary containing total days and missing-day counts.
    """
    rap_dates = sorted(parse_date(str(r["composite_date"])) for r in rap_rows)
    weather_map = {parse_date(str(w["forecast_date"])): w for w in weather_rows}

    rap_idx = 0
    missing_rap = 0
    missing_weather = 0
    total_days = 0

    for day in date_iter(start_date, end_date):
        total_days += 1
        while rap_idx + 1 < len(rap_dates) and rap_dates[rap_idx + 1] <= day:
            rap_idx += 1
        has_rap = bool(rap_dates) and rap_dates[rap_idx] <= day
        if not has_rap:
            missing_rap += 1
        if day not in weather_map:
            missing_weather += 1

    return {
        "total_days": total_days,
        "missing_rap_days": missing_rap,
        "missing_weather_days": missing_weather,
    }
