from __future__ import annotations

from datetime import date

from calculation_service.models import ModelInputs, calculate_v1, calculate_v2, model_parameters


def test_calculate_v1_expected_values() -> None:
    inp = ModelInputs(
        biomass_kg_per_ha=100.0,
        area_ha=10.0,
        animal_count=10,
        daily_intake_kg_per_head=2.0,
        utilization_target_pct=50.0,
        avg_temp_max_7d=None,
        total_precip_7d=None,
    )
    out = calculate_v1(date(2024, 3, 15), inp)
    assert out.available_forage_kg == 500.0
    assert out.daily_consumption_kg == 20.0
    assert out.days_of_grazing_remaining == 25.0
    assert out.recommended_move_date == date(2024, 4, 9)


def test_calculate_v2_applies_hot_dry_adjustments() -> None:
    inp = ModelInputs(
        biomass_kg_per_ha=100.0,
        area_ha=10.0,
        animal_count=10,
        daily_intake_kg_per_head=2.0,
        utilization_target_pct=50.0,
        avg_temp_max_7d=31.0,
        total_precip_7d=2.0,
    )
    out = calculate_v2(date(2024, 3, 15), inp)
    assert out.available_forage_kg == 382.5
    assert out.daily_consumption_kg == 20.0
    assert out.days_of_grazing_remaining == 19.125
    assert out.recommended_move_date == date(2024, 4, 3)


def test_model_parameters_shape() -> None:
    p1 = model_parameters("v1", 50.0)
    p2 = model_parameters("v2", 50.0)
    assert p1["weather_adjustments"] is False
    assert p2["weather_adjustments"] is True
