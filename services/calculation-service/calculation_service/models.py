from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
import math


@dataclass
class ModelInputs:
    """Inputs for grazing recommendation calculations."""

    biomass_kg_per_ha: float
    area_ha: float
    animal_count: int
    daily_intake_kg_per_head: float
    utilization_target_pct: float
    avg_temp_max_7d: float | None
    total_precip_7d: float | None


@dataclass
class ModelOutput:
    """Outputs produced by grazing recommendation calculations."""

    available_forage_kg: float
    daily_consumption_kg: float
    days_of_grazing_remaining: float
    recommended_move_date: date


def calculate_v1(as_of_date: date, inp: ModelInputs) -> ModelOutput:
    """Calculate recommendation using baseline formula.

    Parameters
    ----------
    as_of_date : date
        Calculation anchor date.
    inp : ModelInputs
        Model input values.

    Returns
    -------
    ModelOutput
        Recommendation output for model ``v1``.
    """
    available = inp.biomass_kg_per_ha * inp.area_ha * (inp.utilization_target_pct / 100.0)
    daily = inp.animal_count * inp.daily_intake_kg_per_head
    days = max(0.0, available / daily) if daily > 0 else 0.0
    move_date = as_of_date + timedelta(days=math.floor(days))
    return ModelOutput(
        available_forage_kg=round(available, 3),
        daily_consumption_kg=round(daily, 3),
        days_of_grazing_remaining=round(days, 3),
        recommended_move_date=move_date,
    )


def calculate_v2(as_of_date: date, inp: ModelInputs) -> ModelOutput:
    """Calculate recommendation with weather-adjusted availability.

    Parameters
    ----------
    as_of_date : date
        Calculation anchor date.
    inp : ModelInputs
        Model input values.

    Returns
    -------
    ModelOutput
        Recommendation output for model ``v2``.
    """
    base_available = inp.biomass_kg_per_ha * inp.area_ha * (inp.utilization_target_pct / 100.0)

    precip_adj = 1.0
    if inp.total_precip_7d is not None:
        if inp.total_precip_7d < 5:
            precip_adj = 0.85
        elif inp.total_precip_7d > 25:
            precip_adj = 1.05

    temp_adj = 1.0
    if inp.avg_temp_max_7d is not None and inp.avg_temp_max_7d >= 30:
        temp_adj = 0.9

    available = base_available * precip_adj * temp_adj
    daily = inp.animal_count * inp.daily_intake_kg_per_head
    days = max(0.0, available / daily) if daily > 0 else 0.0
    move_date = as_of_date + timedelta(days=math.floor(days))
    return ModelOutput(
        available_forage_kg=round(available, 3),
        daily_consumption_kg=round(daily, 3),
        days_of_grazing_remaining=round(days, 3),
        recommended_move_date=move_date,
    )


def model_parameters(model_version: str, utilization_target_pct: float) -> dict:
    """Return parameter metadata for a model version.

    Parameters
    ----------
    model_version : str
        Model version identifier.
    utilization_target_pct : float
        Utilization target applied by the model.

    Returns
    -------
    dict
        Parameter/logic description used for registry metadata.
    """
    if model_version == "v1":
        return {
            "formula": "biomass_kg_per_ha * area_ha * utilization_pct / daily_consumption",
            "utilization_target_pct": utilization_target_pct,
            "weather_adjustments": False,
        }
    return {
        "formula": "v1_core * precip_adjustment * temperature_adjustment",
        "utilization_target_pct": utilization_target_pct,
        "weather_adjustments": True,
        "precip_adjustment": "<5mm=>0.85, >25mm=>1.05, else=>1.0",
        "temp_adjustment": "avg_temp_max_7d>=30C => 0.9",
    }
