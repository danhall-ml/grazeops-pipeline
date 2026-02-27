from __future__ import annotations

from datetime import date

from calculation_service.models import ModelInputs, calculate_v1, calculate_v2, model_parameters


def run() -> None:
    """Execute lightweight smoke assertions for calculation model code."""
    as_of = date(2024, 3, 15)
    hot_dry_inputs = ModelInputs(
        biomass_kg_per_ha=800.0,
        area_ha=45.2,
        animal_count=120,
        daily_intake_kg_per_head=11.5,
        utilization_target_pct=50.0,
        avg_temp_max_7d=31.0,
        total_precip_7d=2.0,
    )
    cool_wet_inputs = ModelInputs(
        biomass_kg_per_ha=800.0,
        area_ha=45.2,
        animal_count=120,
        daily_intake_kg_per_head=11.5,
        utilization_target_pct=50.0,
        avg_temp_max_7d=22.0,
        total_precip_7d=30.0,
    )

    out_v1_hot_dry = calculate_v1(as_of, hot_dry_inputs)
    out_v2_hot_dry = calculate_v2(as_of, hot_dry_inputs)
    out_v1_cool_wet = calculate_v1(as_of, cool_wet_inputs)
    out_v2_cool_wet = calculate_v2(as_of, cool_wet_inputs)

    assert out_v1_hot_dry.available_forage_kg > 0
    assert out_v1_hot_dry.daily_consumption_kg > 0
    assert out_v1_hot_dry.days_of_grazing_remaining > 0
    assert out_v1_hot_dry.recommended_move_date >= as_of

    assert out_v2_hot_dry.available_forage_kg > 0
    assert out_v2_hot_dry.daily_consumption_kg == out_v1_hot_dry.daily_consumption_kg
    assert out_v2_hot_dry.recommended_move_date >= as_of

    # v2 should apply stress adjustments for hot/dry conditions and yield less forage than v1.
    assert out_v2_hot_dry.available_forage_kg < out_v1_hot_dry.available_forage_kg

    # v2 should increase available forage under cool/wet conditions.
    assert out_v2_cool_wet.available_forage_kg > out_v1_cool_wet.available_forage_kg

    p1 = model_parameters("v1", 50.0)
    p2 = model_parameters("v2", 50.0)
    assert p1["weather_adjustments"] is False
    assert p2["weather_adjustments"] is True

    print("calculation_service smoke tests passed")


if __name__ == "__main__":
    run()
