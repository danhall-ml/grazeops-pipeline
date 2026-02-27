CREATE TABLE IF NOT EXISTS geographic_boundaries (
    boundary_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    ranch_id TEXT,
    pasture_id TEXT,
    geometry_geojson TEXT NOT NULL,
    area_ha DOUBLE PRECISION,
    crs TEXT DEFAULT 'EPSG:4326',
    created_at TEXT NOT NULL,
    source_file TEXT
);

CREATE TABLE IF NOT EXISTS nrcs_soil_data (
    id BIGSERIAL PRIMARY KEY,
    boundary_id TEXT NOT NULL REFERENCES geographic_boundaries(boundary_id),
    mukey TEXT,
    component_name TEXT,
    productivity_index DOUBLE PRECISION,
    land_capability_class TEXT,
    hydrologic_group TEXT,
    available_water_capacity DOUBLE PRECISION,
    source_version TEXT,
    ingested_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS rap_biomass (
    id BIGSERIAL PRIMARY KEY,
    boundary_id TEXT NOT NULL REFERENCES geographic_boundaries(boundary_id),
    composite_date TEXT NOT NULL,
    biomass_kg_per_ha DOUBLE PRECISION,
    annual_herbaceous_cover_pct DOUBLE PRECISION,
    ndvi DOUBLE PRECISION,
    source_version TEXT,
    ingested_at TEXT NOT NULL,
    UNIQUE(boundary_id, composite_date)
);

CREATE TABLE IF NOT EXISTS weather_forecasts (
    id BIGSERIAL PRIMARY KEY,
    boundary_id TEXT NOT NULL REFERENCES geographic_boundaries(boundary_id),
    forecast_date TEXT NOT NULL,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    precipitation_mm DOUBLE PRECISION,
    temp_max_c DOUBLE PRECISION,
    temp_min_c DOUBLE PRECISION,
    wind_speed_kmh DOUBLE PRECISION,
    source_version TEXT,
    ingested_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS herd_configurations (
    id TEXT PRIMARY KEY,
    ranch_id TEXT NOT NULL,
    pasture_id TEXT,
    boundary_id TEXT,
    animal_count INTEGER NOT NULL,
    animal_type TEXT,
    daily_intake_kg_per_head DOUBLE PRECISION NOT NULL,
    avg_daily_gain_kg DOUBLE PRECISION,
    config_snapshot_json TEXT,
    valid_from TEXT NOT NULL,
    valid_to TEXT,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS ingestion_runs (
    run_id TEXT PRIMARY KEY,
    boundary_id TEXT,
    timeframe_start TEXT,
    timeframe_end TEXT,
    sources_included TEXT,
    status TEXT NOT NULL,
    started_at TEXT NOT NULL,
    completed_at TEXT,
    records_ingested INTEGER,
    error_message TEXT
);

CREATE TABLE IF NOT EXISTS grazing_recommendations (
    id BIGSERIAL PRIMARY KEY,
    boundary_id TEXT NOT NULL REFERENCES geographic_boundaries(boundary_id),
    herd_config_id TEXT NOT NULL REFERENCES herd_configurations(id),
    calculation_date TEXT NOT NULL,
    available_forage_kg DOUBLE PRECISION,
    daily_consumption_kg DOUBLE PRECISION,
    days_of_grazing_remaining DOUBLE PRECISION,
    recommended_move_date TEXT,
    model_version TEXT NOT NULL,
    config_version TEXT,
    input_data_versions_json TEXT,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS data_quality_checks (
    id BIGSERIAL PRIMARY KEY,
    run_id TEXT REFERENCES ingestion_runs(run_id),
    check_name TEXT NOT NULL,
    check_type TEXT,
    passed INTEGER NOT NULL,
    details_json TEXT,
    checked_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS model_versions (
    version_id TEXT PRIMARY KEY,
    description TEXT,
    parameters_json TEXT,
    deployed_at TEXT NOT NULL,
    deprecated_at TEXT,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS ingestion_run_metadata (
    ingestion_run_id TEXT PRIMARY KEY,
    scheduled_for TEXT,
    started_at TEXT NOT NULL,
    ended_at TEXT,
    status TEXT NOT NULL,
    error TEXT,
    snapshot_id TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS ingestion_run_lock (
    boundary_id TEXT PRIMARY KEY,
    lock_owner TEXT NOT NULL,
    lock_until TEXT NOT NULL,
    acquired_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS calculation_runs (
    run_id TEXT PRIMARY KEY,
    scheduled_for TEXT,
    boundary_id TEXT NOT NULL,
    calculation_date TEXT NOT NULL,
    model_version TEXT NOT NULL,
    config_version TEXT,
    status TEXT NOT NULL,
    started_at TEXT NOT NULL,
    ended_at TEXT,
    recommendation_id BIGINT,
    error TEXT
);

CREATE TABLE IF NOT EXISTS calculation_manifests (
    decision_snapshot_id TEXT PRIMARY KEY,
    recommendation_id BIGINT NOT NULL,
    boundary_id TEXT NOT NULL,
    calculation_date TEXT NOT NULL,
    model_version TEXT NOT NULL,
    config_version TEXT,
    manifest_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_rap_boundary_date ON rap_biomass(boundary_id, composite_date);
CREATE INDEX IF NOT EXISTS idx_weather_boundary_date ON weather_forecasts(boundary_id, forecast_date);
CREATE INDEX IF NOT EXISTS idx_recommendations_lookup ON grazing_recommendations(boundary_id, calculation_date);
CREATE INDEX IF NOT EXISTS idx_herd_ranch_pasture ON herd_configurations(ranch_id, pasture_id);
CREATE INDEX IF NOT EXISTS idx_ingestion_timeframe ON ingestion_runs(timeframe_start, timeframe_end);
CREATE INDEX IF NOT EXISTS idx_calculation_manifests_recommendation_id ON calculation_manifests(recommendation_id);
