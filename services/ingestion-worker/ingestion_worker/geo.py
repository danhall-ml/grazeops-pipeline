from __future__ import annotations

import json
import math
import sqlite3
from pathlib import Path
from typing import Any

from .util import slugify


def epsg3857_to_4326(x: float, y: float) -> tuple[float, float]:
    """Convert EPSG:3857 coordinates to EPSG:4326.

    Parameters
    ----------
    x : float
        Web Mercator x coordinate.
    y : float
        Web Mercator y coordinate.

    Returns
    -------
    tuple[float, float]
        Converted ``(longitude, latitude)`` pair.
    """
    lon = (x / 20037508.34) * 180.0
    lat = (y / 20037508.34) * 180.0
    lat = 180.0 / math.pi * (2.0 * math.atan(math.exp(lat * math.pi / 180.0)) - math.pi / 2.0)
    return lon, lat


def transform_3857_coords(obj: Any) -> Any:
    """Recursively convert nested coordinate arrays from EPSG:3857 to EPSG:4326.

    Parameters
    ----------
    obj : Any
        Coordinate list or nested coordinate structure.

    Returns
    -------
    Any
        Converted coordinate structure.
    """
    if isinstance(obj, list) and obj and isinstance(obj[0], (int, float)):
        lon, lat = epsg3857_to_4326(float(obj[0]), float(obj[1]))
        return [lon, lat]
    if isinstance(obj, list):
        return [transform_3857_coords(v) for v in obj]
    return obj


def detect_crs(raw_geojson: dict[str, Any], feature: dict[str, Any] | None) -> str:
    """Detect CRS name from GeoJSON-level or feature-level ``crs`` fields.

    Parameters
    ----------
    raw_geojson : dict[str, Any]
        Raw GeoJSON payload.
    feature : dict[str, Any] or None
        Feature object when payload is a collection.

    Returns
    -------
    str
        Uppercased CRS name, defaults to ``EPSG:4326``.
    """
    for candidate in (raw_geojson.get("crs"), (feature or {}).get("crs")):
        if not isinstance(candidate, dict):
            continue
        props = candidate.get("properties")
        if isinstance(props, dict) and isinstance(props.get("name"), str):
            return props["name"].upper()
    return "EPSG:4326"


def polygon_centroid(ring: list[list[float]]) -> tuple[float, float]:
    """Compute centroid for a polygon ring.

    Parameters
    ----------
    ring : list[list[float]]
        Polygon exterior ring coordinates as ``[lon, lat]`` points.

    Returns
    -------
    tuple[float, float]
        Centroid ``(longitude, latitude)``.
    """
    pts = ring[:]
    if pts and pts[0] != pts[-1]:
        pts.append(pts[0])
    area2 = 0.0
    cx = 0.0
    cy = 0.0
    for i in range(len(pts) - 1):
        x1, y1 = pts[i]
        x2, y2 = pts[i + 1]
        cross = (x1 * y2) - (x2 * y1)
        area2 += cross
        cx += (x1 + x2) * cross
        cy += (y1 + y2) * cross
    if abs(area2) < 1e-12:
        xs = [p[0] for p in pts[:-1]]
        ys = [p[1] for p in pts[:-1]]
        return sum(xs) / len(xs), sum(ys) / len(ys)
    area = area2 / 2.0
    return cx / (6.0 * area), cy / (6.0 * area)


def load_boundary(boundary_path: Path) -> dict[str, Any]:
    """Load, validate, and normalize boundary GeoJSON.

    Parameters
    ----------
    boundary_path : Path
        Path to boundary GeoJSON file.

    Returns
    -------
    dict[str, Any]
        Normalized boundary payload for ingestion.

    Raises
    ------
    ValueError
        Raised when geometry or CRS is invalid/unsupported.
    """
    raw = json.loads(boundary_path.read_text(encoding="utf-8"))
    feature: dict[str, Any] | None = None
    if raw.get("type") == "FeatureCollection":
        features = raw.get("features")
        if not isinstance(features, list) or not features:
            raise ValueError("Boundary FeatureCollection has no features")
        feature = features[0]
        geometry = feature.get("geometry")
        props = feature.get("properties", {}) or {}
    elif raw.get("type") == "Feature":
        feature = raw
        geometry = raw.get("geometry")
        props = raw.get("properties", {}) or {}
    else:
        geometry = raw
        props = {}

    if not isinstance(geometry, dict):
        raise ValueError("Boundary GeoJSON missing geometry")
    if geometry.get("type") != "Polygon":
        raise ValueError("Only Polygon boundaries are supported in this MVP")

    coords = geometry.get("coordinates")
    if not isinstance(coords, list) or not coords or not isinstance(coords[0], list):
        raise ValueError("Boundary polygon coordinates are invalid")

    crs = detect_crs(raw, feature)
    if "3857" in crs:
        coords = transform_3857_coords(coords)
        crs = "EPSG:4326"
    elif "4326" not in crs and "CRS84" not in crs:
        raise ValueError(f"Unsupported CRS: {crs}")

    ring = coords[0]
    lon, lat = polygon_centroid(ring)

    name = str(props.get("name") or "Unnamed Boundary")
    ranch_id = str(props.get("ranch_id") or "")
    pasture_id = str(props.get("pasture_id") or "")
    area_ha = props.get("area_ha")
    area_ha_value = float(area_ha) if isinstance(area_ha, (int, float)) else None

    return {
        "name": name,
        "ranch_id": ranch_id,
        "pasture_id": pasture_id,
        "area_ha": area_ha_value,
        "geometry_geojson": json.dumps({"type": "Polygon", "coordinates": coords}),
        "crs": "EPSG:4326",
        "centroid_lat": lat,
        "centroid_lon": lon,
        "source_file": boundary_path.name,
    }


def resolve_boundary_id(
    source_conn: sqlite3.Connection,
    boundary: dict[str, Any],
    explicit_boundary_id: str | None,
) -> str:
    """Resolve boundary identifier from explicit input or source metadata.

    Parameters
    ----------
    source_conn : sqlite3.Connection
        Source reference database connection.
    boundary : dict[str, Any]
        Normalized boundary payload.
    explicit_boundary_id : str or None
        Explicit boundary id override.

    Returns
    -------
    str
        Resolved boundary identifier.
    """
    if explicit_boundary_id:
        return explicit_boundary_id
    if boundary["ranch_id"] and boundary["pasture_id"]:
        row = source_conn.execute(
            """
            SELECT boundary_id FROM geographic_boundaries
            WHERE ranch_id = ? AND pasture_id = ?
            LIMIT 1
            """,
            (boundary["ranch_id"], boundary["pasture_id"]),
        ).fetchone()
        if row:
            return str(row[0])
    if boundary["pasture_id"]:
        return f"boundary_{slugify(boundary['pasture_id'])}"
    return f"boundary_{slugify(boundary['name'])}"
