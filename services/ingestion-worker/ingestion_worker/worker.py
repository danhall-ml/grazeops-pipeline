from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .db import (
    acquire_lock,
    add_quality_check,
    ensure_run_lock_table,
    ensure_run_metadata_table,
    release_lock,
)
from .geo import load_boundary, resolve_boundary_id
from .operational_db import connect_operational_db
from .sources import (
    backfill_weather,
    daily_join_coverage,
    fetch_openmeteo_weather,
    fetch_source_nrcs,
    fetch_source_rap,
    fetch_source_weather,
    select_herd_config,
)
from .util import parse_date, slugify, stable_hash, utc_now, wait_for_file


def run_ingestion(args: Any) -> dict[str, Any]:
    """Execute one ingestion run end-to-end.

    Parameters
    ----------
    args : Any
        Parsed ingestion arguments from CLI.

    Returns
    -------
    dict[str, Any]
        Run summary containing run/snapshot identifiers and key counts.
    """
    run_started = utc_now()

    source_conn = sqlite3.connect(args.source_db)
    source_conn.row_factory = sqlite3.Row
    target_conn: Any = None
    boundary_id = ""
    run_id = ""
    lock_acquired = False

    try:
        if not args.db_url:
            wait_for_file(args.db, args.wait_for_db_seconds)

        target_conn = connect_operational_db(db_url=args.db_url, db_path=args.db)
        ensure_run_metadata_table(target_conn)
        ensure_run_lock_table(target_conn)

        boundary = load_boundary(args.boundary_path)
        boundary_id = resolve_boundary_id(source_conn, boundary, args.boundary_id)

        run_id = args.run_id or f"run_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')}_{slugify(boundary_id)}"
        if not acquire_lock(target_conn, boundary_id, run_id, args.lock_ttl_minutes):
            raise RuntimeError(f"Another ingestion lock is active for {boundary_id}")
        lock_acquired = True

        target_conn.execute(
            """
            INSERT INTO ingestion_runs (
                run_id, boundary_id, timeframe_start, timeframe_end, sources_included, status, started_at
            ) VALUES (?, ?, ?, ?, ?, 'running', ?)
            """,
            (
                run_id,
                boundary_id,
                args.start_date_obj.isoformat(),
                args.end_date_obj.isoformat(),
                "nrcs,rap,weather,herd",
                run_started,
            ),
        )
        target_conn.execute(
            """
            INSERT INTO ingestion_run_metadata (
                ingestion_run_id, scheduled_for, started_at, status, snapshot_id
            ) VALUES (?, ?, ?, 'running', ?)
            """,
            (run_id, args.scheduled_for, run_started, "pending"),
        )
        target_conn.commit()
    except Exception:
        if target_conn is not None and lock_acquired and boundary_id and run_id:
            try:
                release_lock(target_conn, boundary_id, run_id)
            except Exception:
                pass
        if target_conn is not None:
            target_conn.close()
        source_conn.close()
        raise

    snapshot_id = ""

    try:
        nrcs_rows = fetch_source_nrcs(source_conn, boundary_id)
        rap_rows = fetch_source_rap(source_conn, boundary_id, args.start_date_obj, args.end_date_obj)
        weather_rows = fetch_source_weather(source_conn, boundary_id, args.start_date_obj, args.end_date_obj)
        weather_source = "reference_db"

        if args.prefer_openmeteo:
            try:
                weather_rows = fetch_openmeteo_weather(
                    boundary["centroid_lat"],
                    boundary["centroid_lon"],
                    args.start_date_obj,
                    args.end_date_obj,
                )
                weather_source = "openmeteo_live"
            except Exception:
                weather_rows = fetch_source_weather(
                    source_conn, boundary_id, args.start_date_obj, args.end_date_obj
                )
                weather_source = "reference_db_fallback"

        filled_weather_rows, backfilled_count, missing_weather_after_fill = backfill_weather(
            weather_rows=weather_rows,
            start_date=args.start_date_obj,
            end_date=args.end_date_obj,
            boundary=boundary,
            enabled=args.backfill_weather,
        )

        herd_config = select_herd_config(
            herd_path=args.herd_path,
            ranch_id=boundary["ranch_id"],
            pasture_id=boundary["pasture_id"],
            boundary_id=boundary_id,
            end_date=args.end_date_obj,
        )

        coverage = daily_join_coverage(
            args.start_date_obj,
            args.end_date_obj,
            rap_rows=rap_rows,
            weather_rows=filled_weather_rows,
        )
        if rap_rows:
            latest_rap_date = max(parse_date(str(r["composite_date"])) for r in rap_rows)
            rap_staleness_days = (args.end_date_obj - latest_rap_date).days
        else:
            latest_rap_date = None
            rap_staleness_days = 10_000

        snapshot_payload = {
            "boundary_id": boundary_id,
            "timeframe_start": args.start_date_obj.isoformat(),
            "timeframe_end": args.end_date_obj.isoformat(),
            "nrcs_records": len(nrcs_rows),
            "rap_records": len(rap_rows),
            "weather_records": len(filled_weather_rows),
            "herd_config_id": herd_config["id"],
        }
        snapshot_id = stable_hash(snapshot_payload)[:16]

        now = utc_now()
        target_conn.execute("BEGIN")

        target_conn.execute(
            """
            INSERT INTO geographic_boundaries (
                boundary_id, name, ranch_id, pasture_id, geometry_geojson, area_ha, crs, created_at, source_file
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(boundary_id) DO UPDATE SET
                name=excluded.name,
                ranch_id=excluded.ranch_id,
                pasture_id=excluded.pasture_id,
                geometry_geojson=excluded.geometry_geojson,
                area_ha=excluded.area_ha,
                crs=excluded.crs,
                source_file=excluded.source_file
            """,
            (
                boundary_id,
                boundary["name"],
                boundary["ranch_id"],
                boundary["pasture_id"],
                boundary["geometry_geojson"],
                boundary["area_ha"],
                boundary["crs"],
                now,
                boundary["source_file"],
            ),
        )

        target_conn.execute("DELETE FROM nrcs_soil_data WHERE boundary_id = ?", (boundary_id,))
        for row in nrcs_rows:
            target_conn.execute(
                """
                INSERT INTO nrcs_soil_data (
                    boundary_id, mukey, component_name, productivity_index, land_capability_class,
                    hydrologic_group, available_water_capacity, source_version, ingested_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    boundary_id,
                    row["mukey"],
                    row["component_name"],
                    row["productivity_index"],
                    row["land_capability_class"],
                    row["hydrologic_group"],
                    row["available_water_capacity"],
                    row["source_version"],
                    now,
                ),
            )

        target_conn.execute(
            """
            DELETE FROM rap_biomass
            WHERE boundary_id = ? AND composite_date >= ? AND composite_date <= ?
            """,
            (boundary_id, args.start_date_obj.isoformat(), args.end_date_obj.isoformat()),
        )
        for row in rap_rows:
            target_conn.execute(
                """
                INSERT INTO rap_biomass (
                    boundary_id, composite_date, biomass_kg_per_ha, annual_herbaceous_cover_pct,
                    ndvi, source_version, ingested_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(boundary_id, composite_date) DO UPDATE SET
                    biomass_kg_per_ha=excluded.biomass_kg_per_ha,
                    annual_herbaceous_cover_pct=excluded.annual_herbaceous_cover_pct,
                    ndvi=excluded.ndvi,
                    source_version=excluded.source_version,
                    ingested_at=excluded.ingested_at
                """,
                (
                    boundary_id,
                    row["composite_date"],
                    row["biomass_kg_per_ha"],
                    row["annual_herbaceous_cover_pct"],
                    row["ndvi"],
                    row["source_version"],
                    now,
                ),
            )

        target_conn.execute(
            """
            DELETE FROM weather_forecasts
            WHERE boundary_id = ? AND forecast_date >= ? AND forecast_date <= ?
            """,
            (boundary_id, args.start_date_obj.isoformat(), args.end_date_obj.isoformat()),
        )
        for row in filled_weather_rows:
            target_conn.execute(
                """
                INSERT INTO weather_forecasts (
                    boundary_id, forecast_date, latitude, longitude, precipitation_mm,
                    temp_max_c, temp_min_c, wind_speed_kmh, source_version, ingested_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    boundary_id,
                    row["forecast_date"],
                    row.get("latitude", boundary["centroid_lat"]),
                    row.get("longitude", boundary["centroid_lon"]),
                    row.get("precipitation_mm"),
                    row.get("temp_max_c"),
                    row.get("temp_min_c"),
                    row.get("wind_speed_kmh"),
                    row.get("source_version", weather_source),
                    now,
                ),
            )

        target_conn.execute(
            """
            INSERT INTO herd_configurations (
                id, ranch_id, pasture_id, boundary_id, animal_count, animal_type,
                daily_intake_kg_per_head, avg_daily_gain_kg, config_snapshot_json,
                valid_from, valid_to, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                ranch_id=excluded.ranch_id,
                pasture_id=excluded.pasture_id,
                boundary_id=excluded.boundary_id,
                animal_count=excluded.animal_count,
                animal_type=excluded.animal_type,
                daily_intake_kg_per_head=excluded.daily_intake_kg_per_head,
                avg_daily_gain_kg=excluded.avg_daily_gain_kg,
                config_snapshot_json=excluded.config_snapshot_json,
                valid_from=excluded.valid_from,
                valid_to=excluded.valid_to
            """,
            (
                herd_config["id"],
                herd_config["ranch_id"],
                herd_config["pasture_id"],
                herd_config["boundary_id"],
                herd_config["animal_count"],
                herd_config["animal_type"],
                herd_config["daily_intake_kg_per_head"],
                herd_config["avg_daily_gain_kg"],
                herd_config["config_snapshot_json"],
                herd_config["valid_from"],
                herd_config["valid_to"],
                now,
            ),
        )

        add_quality_check(
            target_conn,
            run_id,
            "nrcs_records_present",
            "ingestion",
            len(nrcs_rows) > 0,
            {"count": len(nrcs_rows)},
        )
        add_quality_check(
            target_conn,
            run_id,
            "rap_records_present",
            "ingestion",
            len(rap_rows) > 0,
            {"count": len(rap_rows)},
        )
        add_quality_check(
            target_conn,
            run_id,
            "herd_config_valid",
            "validation",
            herd_config["animal_count"] > 0 and herd_config["daily_intake_kg_per_head"] > 0,
            {
                "herd_config_id": herd_config["id"],
                "animal_count": herd_config["animal_count"],
                "daily_intake_kg_per_head": herd_config["daily_intake_kg_per_head"],
            },
        )
        add_quality_check(
            target_conn,
            run_id,
            "weather_backfill",
            "monitoring",
            missing_weather_after_fill == 0,
            {
                "backfilled_days": backfilled_count,
                "missing_days_after_fill": missing_weather_after_fill,
                "weather_source": weather_source,
            },
        )
        add_quality_check(
            target_conn,
            run_id,
            "weather_source_available",
            "monitoring",
            weather_source != "reference_db_fallback",
            {"weather_source": weather_source},
        )
        add_quality_check(
            target_conn,
            run_id,
            "rap_not_stale",
            "freshness",
            rap_staleness_days <= args.rap_stale_days,
            {
                "latest_rap_date": None if latest_rap_date is None else latest_rap_date.isoformat(),
                "staleness_days": rap_staleness_days,
                "threshold_days": args.rap_stale_days,
            },
        )
        add_quality_check(
            target_conn,
            run_id,
            "daily_join_coverage",
            "monitoring",
            coverage["missing_rap_days"] == 0 and coverage["missing_weather_days"] == 0,
            coverage,
        )

        records_ingested = len(nrcs_rows) + len(rap_rows) + len(filled_weather_rows) + 1
        run_ended = utc_now()

        target_conn.execute(
            """
            UPDATE ingestion_runs
            SET status='completed', completed_at=?, records_ingested=?, error_message=NULL
            WHERE run_id=?
            """,
            (run_ended, records_ingested, run_id),
        )
        target_conn.execute(
            """
            UPDATE ingestion_run_metadata
            SET status='success', ended_at=?, error=NULL, snapshot_id=?
            WHERE ingestion_run_id=?
            """,
            (run_ended, snapshot_id, run_id),
        )
        target_conn.commit()

        return {
            "run_id": run_id,
            "snapshot_id": snapshot_id,
            "boundary_id": boundary_id,
            "timeframe_start": args.start_date_obj.isoformat(),
            "timeframe_end": args.end_date_obj.isoformat(),
            "records_ingested": records_ingested,
            "weather_source": weather_source,
            "weather_backfilled_days": backfilled_count,
            "rap_staleness_days": rap_staleness_days,
            "quality": coverage,
        }
    except Exception as exc:
        target_conn.rollback()
        failed_at = utc_now()
        target_conn.execute(
            """
            UPDATE ingestion_runs
            SET status='failed', completed_at=?, error_message=?
            WHERE run_id=?
            """,
            (failed_at, str(exc), run_id),
        )
        target_conn.execute(
            """
            UPDATE ingestion_run_metadata
            SET status='failed', ended_at=?, error=?
            WHERE ingestion_run_id=?
            """,
            (failed_at, str(exc), run_id),
        )
        target_conn.commit()
        raise
    finally:
        try:
            release_lock(target_conn, boundary_id, run_id)
        except Exception:
            pass
        source_conn.close()
        if target_conn is not None:
            target_conn.close()


def maybe_write_manifest(summary: dict[str, Any], manifest_dir: Path | None) -> None:
    """Optionally write ingestion summary manifest to disk.

    Parameters
    ----------
    summary : dict[str, Any]
        Ingestion summary payload.
    manifest_dir : Path or None
        Target directory. When ``None``, no file is written.
    """
    if manifest_dir is None:
        return
    manifest_dir.mkdir(parents=True, exist_ok=True)
    path = manifest_dir / f"{summary['run_id']}.json"
    path.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
