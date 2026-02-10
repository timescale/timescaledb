-- PostGIS ECEF/ECI Integration for TimescaleDB
-- Synthetic trajectory data generator for testing and benchmarks
--
-- Generates realistic orbital trajectories using simplified Keplerian mechanics.
-- No external dependencies — pure SQL.

-- =============================================================================
-- Generate a single circular orbit trajectory
-- =============================================================================
-- Returns one row per time step for an object in a circular orbit.
--
-- Parameters:
--   p_object_id:    unique object identifier
--   p_object_type:  0=satellite, 1=debris, 2=rocket_body (see ecef_eci.object_types)
--   p_altitude_km:  orbital altitude above Earth surface (km)
--   p_inclination:  orbital inclination (degrees)
--   p_raan:         right ascension of ascending node (degrees)
--   p_start_time:   trajectory start time
--   p_duration:     total duration to generate
--   p_interval:     time between observations

CREATE OR REPLACE FUNCTION ecef_eci.generate_circular_orbit(
    p_object_id    INT,
    p_object_type  SMALLINT DEFAULT 0,
    p_altitude_km  FLOAT8 DEFAULT 400.0,
    p_inclination  FLOAT8 DEFAULT 51.6,
    p_raan         FLOAT8 DEFAULT 0.0,
    p_start_time   TIMESTAMPTZ DEFAULT '2025-01-01 00:00:00+00',
    p_duration     INTERVAL DEFAULT '2 hours',
    p_interval     INTERVAL DEFAULT '10 seconds'
) RETURNS TABLE (
    time           TIMESTAMPTZ,
    object_id      INT,
    object_type    SMALLINT,
    frame          SMALLINT,
    x              FLOAT8,
    y              FLOAT8,
    z              FLOAT8,
    vx             FLOAT8,
    vy             FLOAT8,
    vz             FLOAT8,
    altitude_km    FLOAT8,
    spatial_bucket SMALLINT
)
LANGUAGE SQL
IMMUTABLE
PARALLEL SAFE
AS $$
    WITH params AS (
        SELECT
            6371000.0 + (p_altitude_km * 1000.0)       AS radius_m,
            radians(p_inclination)                       AS inc_rad,
            radians(p_raan)                              AS raan_rad,
            -- Orbital period: T = 2*pi*sqrt(r^3 / mu)
            -- mu (Earth) = 3.986004418e14 m^3/s^2
            2.0 * pi() * sqrt(
                power(6371000.0 + p_altitude_km * 1000.0, 3) / 3.986004418e14
            )                                            AS period_s,
            -- Mean motion (rad/s)
            sqrt(3.986004418e14 / power(6371000.0 + p_altitude_km * 1000.0, 3))
                                                         AS n_rad_s,
            -- Orbital velocity (m/s)
            sqrt(3.986004418e14 / (6371000.0 + p_altitude_km * 1000.0))
                                                         AS v_ms
    ),
    time_series AS (
        SELECT
            p_start_time + (s * p_interval) AS t,
            EXTRACT(EPOCH FROM s * p_interval) AS t_sec
        FROM generate_series(0, (EXTRACT(EPOCH FROM p_duration) / EXTRACT(EPOCH FROM p_interval))::INT) AS s
    ),
    orbital_pos AS (
        SELECT
            ts.t,
            ts.t_sec,
            -- True anomaly (circular orbit: mean anomaly = true anomaly)
            p.n_rad_s * ts.t_sec AS nu,
            p.radius_m,
            p.v_ms,
            p.inc_rad,
            p.raan_rad
        FROM time_series ts
        CROSS JOIN params p
    )
    SELECT
        op.t                                           AS time,
        p_object_id                                    AS object_id,
        p_object_type                                  AS object_type,
        0::SMALLINT                                    AS frame,
        -- ECEF position (simplified: perifocal -> ECEF via RAAN + inclination)
        -- X = r*(cos(RAAN)*cos(nu) - sin(RAAN)*sin(nu)*cos(i))
        op.radius_m * (
            cos(op.raan_rad) * cos(op.nu) -
            sin(op.raan_rad) * sin(op.nu) * cos(op.inc_rad)
        )                                              AS x,
        -- Y = r*(sin(RAAN)*cos(nu) + cos(RAAN)*sin(nu)*cos(i))
        op.radius_m * (
            sin(op.raan_rad) * cos(op.nu) +
            cos(op.raan_rad) * sin(op.nu) * cos(op.inc_rad)
        )                                              AS y,
        -- Z = r*(sin(nu)*sin(i))
        op.radius_m * (
            sin(op.nu) * sin(op.inc_rad)
        )                                              AS z,
        -- Velocity components (tangential to orbit)
        op.v_ms * (
            -cos(op.raan_rad) * sin(op.nu) -
            sin(op.raan_rad) * cos(op.nu) * cos(op.inc_rad)
        )                                              AS vx,
        op.v_ms * (
            -sin(op.raan_rad) * sin(op.nu) +
            cos(op.raan_rad) * cos(op.nu) * cos(op.inc_rad)
        )                                              AS vy,
        op.v_ms * (
            cos(op.nu) * sin(op.inc_rad)
        )                                              AS vz,
        p_altitude_km                                  AS altitude_km,
        ecef_eci.altitude_band_bucket(
            op.radius_m * (cos(op.raan_rad)*cos(op.nu) - sin(op.raan_rad)*sin(op.nu)*cos(op.inc_rad)),
            op.radius_m * (sin(op.raan_rad)*cos(op.nu) + cos(op.raan_rad)*sin(op.nu)*cos(op.inc_rad)),
            op.radius_m * (sin(op.nu)*sin(op.inc_rad))
        )::SMALLINT                                    AS spatial_bucket
    FROM orbital_pos op
$$;

COMMENT ON FUNCTION ecef_eci.generate_circular_orbit(INT, SMALLINT, FLOAT8, FLOAT8, FLOAT8, TIMESTAMPTZ, INTERVAL, INTERVAL)
IS 'Generates synthetic circular orbit trajectory data in ECEF coordinates for testing and benchmarks';

-- =============================================================================
-- Generate a mixed-regime test dataset
-- =============================================================================
-- Creates a batch of objects across LEO, MEO, and GEO for testing
-- partitioning, compression, and continuous aggregate behavior.

CREATE OR REPLACE FUNCTION ecef_eci.generate_test_dataset(
    p_start_time   TIMESTAMPTZ DEFAULT '2025-01-01 00:00:00+00',
    p_duration     INTERVAL DEFAULT '2 hours',
    p_interval     INTERVAL DEFAULT '10 seconds'
) RETURNS SETOF ecef_eci.trajectories
LANGUAGE SQL
STABLE
PARALLEL SAFE
AS $$
    -- LEO objects (ISS-like): 400 km, various inclinations
    SELECT * FROM ecef_eci.generate_circular_orbit(25544, 0::SMALLINT, 400.0,  51.6,   0.0, p_start_time, p_duration, p_interval)
    UNION ALL
    SELECT * FROM ecef_eci.generate_circular_orbit(25545, 0::SMALLINT, 550.0,  97.4,  45.0, p_start_time, p_duration, p_interval)
    UNION ALL
    SELECT * FROM ecef_eci.generate_circular_orbit(25546, 1::SMALLINT, 800.0,  98.7,  90.0, p_start_time, p_duration, p_interval)
    UNION ALL
    SELECT * FROM ecef_eci.generate_circular_orbit(25547, 1::SMALLINT, 1200.0, 65.0, 135.0, p_start_time, p_duration, p_interval)
    UNION ALL
    -- MEO objects (GPS-like): ~20200 km
    SELECT * FROM ecef_eci.generate_circular_orbit(28000, 0::SMALLINT, 20200.0, 55.0,   0.0, p_start_time, p_duration, p_interval)
    UNION ALL
    SELECT * FROM ecef_eci.generate_circular_orbit(28001, 0::SMALLINT, 20200.0, 55.0, 120.0, p_start_time, p_duration, p_interval)
    UNION ALL
    -- GEO objects: ~35786 km, near-zero inclination
    SELECT * FROM ecef_eci.generate_circular_orbit(30000, 0::SMALLINT, 35786.0,  0.1,   0.0, p_start_time, p_duration, p_interval)
    UNION ALL
    SELECT * FROM ecef_eci.generate_circular_orbit(30001, 0::SMALLINT, 35786.0,  0.1,  90.0, p_start_time, p_duration, p_interval)
    UNION ALL
    SELECT * FROM ecef_eci.generate_circular_orbit(30002, 0::SMALLINT, 35786.0,  0.1, 180.0, p_start_time, p_duration, p_interval)
    UNION ALL
    SELECT * FROM ecef_eci.generate_circular_orbit(30003, 0::SMALLINT, 35786.0,  0.1, 270.0, p_start_time, p_duration, p_interval)
$$;

COMMENT ON FUNCTION ecef_eci.generate_test_dataset(TIMESTAMPTZ, INTERVAL, INTERVAL)
IS 'Generates a mixed LEO/MEO/GEO test dataset with 10 objects across orbital regimes';
