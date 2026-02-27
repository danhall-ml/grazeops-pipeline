from __future__ import annotations

from datetime import date

from ingestion_worker.sources import backfill_weather, daily_join_coverage


def test_backfill_weather_disabled_tracks_missing_days() -> None:
    rows = [
        {
            "forecast_date": "2024-01-01",
            "latitude": 40.0,
            "longitude": -100.0,
            "precipitation_mm": 0.0,
            "temp_max_c": 12.0,
            "temp_min_c": 2.0,
            "wind_speed_kmh": 10.0,
            "source_version": "reference_db",
        }
    ]
    boundary = {"centroid_lat": 40.0, "centroid_lon": -100.0}
    out, backfilled, missing = backfill_weather(
        weather_rows=rows,
        start_date=date(2024, 1, 1),
        end_date=date(2024, 1, 3),
        boundary=boundary,
        enabled=False,
    )
    assert len(out) == 1
    assert backfilled == 0
    assert missing == 2


def test_backfill_weather_enabled_fills_gap_days() -> None:
    rows = [
        {
            "forecast_date": "2024-01-01",
            "latitude": 40.0,
            "longitude": -100.0,
            "precipitation_mm": 1.5,
            "temp_max_c": 12.0,
            "temp_min_c": 2.0,
            "wind_speed_kmh": 10.0,
            "source_version": "reference_db",
        }
    ]
    boundary = {"centroid_lat": 40.0, "centroid_lon": -100.0}
    out, backfilled, missing = backfill_weather(
        weather_rows=rows,
        start_date=date(2024, 1, 1),
        end_date=date(2024, 1, 3),
        boundary=boundary,
        enabled=True,
    )
    assert len(out) == 3
    assert backfilled == 2
    assert missing == 0
    assert out[1]["forecast_date"] == "2024-01-02"
    assert out[1]["source_version"] == "weather_backfill"
    assert out[2]["forecast_date"] == "2024-01-03"
    assert out[2]["source_version"] == "weather_backfill"


def test_daily_join_coverage_counts_missing_sources() -> None:
    rap_rows = [{"composite_date": "2024-01-02"}]
    weather_rows = [{"forecast_date": "2024-01-01"}, {"forecast_date": "2024-01-03"}]
    coverage = daily_join_coverage(
        start_date=date(2024, 1, 1),
        end_date=date(2024, 1, 3),
        rap_rows=rap_rows,
        weather_rows=weather_rows,
    )
    assert coverage["total_days"] == 3
    assert coverage["missing_rap_days"] == 1
    assert coverage["missing_weather_days"] == 1
