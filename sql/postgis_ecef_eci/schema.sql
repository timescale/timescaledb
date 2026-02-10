-- PostGIS ECEF/ECI Integration for TimescaleDB
-- Schema setup and reference table definitions

-- =============================================================================
-- Schema
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS ecef_eci;

COMMENT ON SCHEMA ecef_eci
IS 'PostGIS ECEF/ECI integration for TimescaleDB — spatial partitioning, frame conversion, and trajectory management';

-- =============================================================================
-- Reference: Object type enum
-- =============================================================================

CREATE TABLE IF NOT EXISTS ecef_eci.object_types (
    id      SMALLINT PRIMARY KEY,
    label   TEXT NOT NULL UNIQUE
);

INSERT INTO ecef_eci.object_types (id, label) VALUES
    (0, 'satellite'),
    (1, 'debris'),
    (2, 'rocket_body'),
    (3, 'aircraft'),
    (4, 'ground_station'),
    (5, 'unknown')
ON CONFLICT (id) DO NOTHING;

-- =============================================================================
-- Reference: Coordinate frame enum
-- =============================================================================

CREATE TABLE IF NOT EXISTS ecef_eci.coord_frames (
    id      SMALLINT PRIMARY KEY,
    label   TEXT NOT NULL UNIQUE,
    srid    INT,
    description TEXT
);

INSERT INTO ecef_eci.coord_frames (id, label, srid, description) VALUES
    (0, 'ECEF',  4978,   'Earth-Centered Earth-Fixed (WGS84 geocentric)'),
    (1, 'J2000', 900001, 'Earth-Centered Inertial — J2000/EME2000 epoch'),
    (2, 'GCRF',  900002, 'Geocentric Celestial Reference Frame'),
    (3, 'TEME',  900003, 'True Equator Mean Equinox (SGP4 native)')
ON CONFLICT (id) DO NOTHING;

-- =============================================================================
-- Core: Trajectory hypertable
-- =============================================================================
-- This is the reference schema. Users may adjust column selection based on
-- their compression approach (see spec-02-compression for Approach A/B/C).

CREATE TABLE IF NOT EXISTS ecef_eci.trajectories (
    time            TIMESTAMPTZ     NOT NULL,
    object_id       INT             NOT NULL,
    object_type     SMALLINT        NOT NULL DEFAULT 5,
    frame           SMALLINT        NOT NULL DEFAULT 0,

    -- Decomposed ECEF coordinates (meters from Earth center)
    -- These compress well with Gorilla algorithm
    x               FLOAT8          NOT NULL,
    y               FLOAT8          NOT NULL,
    z               FLOAT8          NOT NULL,

    -- Velocity components (m/s) — optional
    vx              FLOAT8,
    vy              FLOAT8,
    vz              FLOAT8,

    -- Derived fields for partitioning and querying
    altitude_km     FLOAT8          NOT NULL,
    spatial_bucket  SMALLINT        NOT NULL
);

-- =============================================================================
-- Hypertable setup
-- =============================================================================

SELECT create_hypertable(
    'ecef_eci.trajectories',
    'time',
    chunk_time_interval => INTERVAL '1 hour',
    if_not_exists => TRUE
);

-- Add spatial closed dimension using the bucket column directly.
-- The altitude_band_bucket() function computes the bucket at insert time;
-- TimescaleDB uses the integer value for chunk routing.
SELECT add_dimension(
    'ecef_eci.trajectories',
    'spatial_bucket',
    number_partitions => 16,
    if_not_exists => TRUE
);

-- =============================================================================
-- Insert trigger: compute derived fields
-- =============================================================================
-- For moderate ingest rates. High-throughput users should compute these
-- application-side and skip the trigger.

CREATE OR REPLACE FUNCTION ecef_eci.trajectories_compute_derived()
RETURNS TRIGGER
LANGUAGE plpgsql
SET search_path = ecef_eci, pg_catalog, public
AS $$
BEGIN
    NEW.altitude_km := ecef_eci.ecef_altitude_km(NEW.x, NEW.y, NEW.z);
    NEW.spatial_bucket := ecef_eci.altitude_band_bucket(NEW.x, NEW.y, NEW.z);
    RETURN NEW;
END;
$$;

CREATE TRIGGER trg_trajectories_compute_derived
    BEFORE INSERT ON ecef_eci.trajectories
    FOR EACH ROW
    WHEN (NEW.altitude_km IS NULL OR NEW.spatial_bucket IS NULL)
    EXECUTE FUNCTION ecef_eci.trajectories_compute_derived();

-- =============================================================================
-- Indexes
-- =============================================================================
-- See spec-05-index-strategy for rationale.

-- Index A: Primary object+time lookup
CREATE INDEX IF NOT EXISTS idx_traj_object_time
    ON ecef_eci.trajectories (object_id, time DESC);

-- Index D: BRIN on altitude for altitude-band queries
CREATE INDEX IF NOT EXISTS idx_traj_altitude_brin
    ON ecef_eci.trajectories USING brin (altitude_km)
    WITH (pages_per_range = 32);

-- Index E: Spatial bucket + time for conjunction screening
CREATE INDEX IF NOT EXISTS idx_traj_bucket_time
    ON ecef_eci.trajectories (spatial_bucket, time DESC);

-- =============================================================================
-- Compression settings
-- =============================================================================

ALTER TABLE ecef_eci.trajectories SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'object_id',
    timescaledb.compress_orderby = 'time ASC'
);
