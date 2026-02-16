-- PostGIS ECEF/ECI Integration for TimescaleDB
-- Earth Orientation Parameters (EOP) data management
--
-- EOP data (published by IERS) is required for precise ECEF<->ECI frame conversion.
-- This module provides:
--   1. EOP storage table
--   2. Data loading functions (from IERS finals2000A format)
--   3. TimescaleDB background job for automatic refresh
--   4. Interpolation function for arbitrary epochs

-- =============================================================================
-- EOP Storage
-- =============================================================================

CREATE TABLE IF NOT EXISTS ecef_eci.eop_data (
    -- Modified Julian Date (MJD = JD - 2400000.5)
    mjd             FLOAT8      PRIMARY KEY,
    -- Calendar date for convenience
    date            DATE        NOT NULL,
    -- Polar motion (arcseconds)
    xp              FLOAT8      NOT NULL,
    yp              FLOAT8      NOT NULL,
    -- UT1 - UTC (seconds)
    -- UT1 tracks Earth's actual rotation; UTC is atomic time + leap seconds
    dut1            FLOAT8      NOT NULL,
    -- Length of day excess (milliseconds) — LOD = -(d(UT1-UTC)/dt) in ms
    lod             FLOAT8,
    -- Celestial pole offsets (milliarcseconds) — corrections to IAU 2006 precession-nutation
    dx              FLOAT8,
    dy              FLOAT8,
    -- Data type: 'I' = IERS (observed), 'P' = predicted, 'B' = Bulletin B (final)
    data_type       CHAR(1)     DEFAULT 'I',
    -- When this row was loaded
    loaded_at       TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_eop_date ON ecef_eci.eop_data (date);

COMMENT ON TABLE ecef_eci.eop_data
IS 'IERS Earth Orientation Parameters for ECEF<->ECI frame conversion. Updated daily from IERS finals2000A.';

-- =============================================================================
-- MJD <-> Timestamp conversion
-- =============================================================================

CREATE OR REPLACE FUNCTION ecef_eci.mjd_to_timestamp(mjd FLOAT8)
RETURNS TIMESTAMPTZ
LANGUAGE SQL
IMMUTABLE
PARALLEL SAFE
SET search_path = ecef_eci, pg_catalog, public
AS $$
    -- MJD 51544.0 = 2000-01-01 00:00:00 UTC
    SELECT '2000-01-01 00:00:00+00'::timestamptz + ((mjd - 51544.0) * INTERVAL '1 day')
$$;

CREATE OR REPLACE FUNCTION ecef_eci.timestamp_to_mjd(ts TIMESTAMPTZ)
RETURNS FLOAT8
LANGUAGE SQL
IMMUTABLE
PARALLEL SAFE
SET search_path = ecef_eci, pg_catalog, public
AS $$
    -- MJD 51544.0 = 2000-01-01 00:00:00 UTC
    SELECT 51544.0 + EXTRACT(EPOCH FROM ts - '2000-01-01 00:00:00+00'::timestamptz) / 86400.0
$$;

-- =============================================================================
-- EOP Interpolation
-- =============================================================================
-- Linear interpolation of EOP values at an arbitrary epoch.
-- Uses the two nearest MJD entries bracketing the requested time.

CREATE OR REPLACE FUNCTION ecef_eci.eop_at_epoch(
    epoch TIMESTAMPTZ
) RETURNS TABLE (
    xp   FLOAT8,
    yp   FLOAT8,
    dut1 FLOAT8,
    lod  FLOAT8,
    dx   FLOAT8,
    dy   FLOAT8
)
LANGUAGE SQL
STABLE
PARALLEL SAFE
SET search_path = ecef_eci, pg_catalog, public
AS $$
    WITH target AS (
        SELECT ecef_eci.timestamp_to_mjd(epoch) AS mjd
    ),
    bracket AS (
        -- Find the two EOP entries bracketing the target MJD
        SELECT
            before.mjd AS mjd0,
            after.mjd  AS mjd1,
            before.xp  AS xp0,  after.xp  AS xp1,
            before.yp  AS yp0,  after.yp  AS yp1,
            before.dut1 AS dut10, after.dut1 AS dut11,
            before.lod AS lod0, after.lod AS lod1,
            before.dx  AS dx0,  after.dx  AS dx1,
            before.dy  AS dy0,  after.dy  AS dy1,
            t.mjd AS target_mjd
        FROM target t
        CROSS JOIN LATERAL (
            SELECT * FROM ecef_eci.eop_data
            WHERE mjd <= t.mjd
            ORDER BY mjd DESC LIMIT 1
        ) before
        CROSS JOIN LATERAL (
            SELECT * FROM ecef_eci.eop_data
            WHERE mjd > t.mjd
            ORDER BY mjd ASC LIMIT 1
        ) after
    )
    SELECT
        -- Linear interpolation: v = v0 + (v1-v0) * (t-t0) / (t1-t0)
        b.xp0  + (b.xp1  - b.xp0)  * (b.target_mjd - b.mjd0) / NULLIF(b.mjd1 - b.mjd0, 0) AS xp,
        b.yp0  + (b.yp1  - b.yp0)  * (b.target_mjd - b.mjd0) / NULLIF(b.mjd1 - b.mjd0, 0) AS yp,
        b.dut10 + (b.dut11 - b.dut10) * (b.target_mjd - b.mjd0) / NULLIF(b.mjd1 - b.mjd0, 0) AS dut1,
        b.lod0 + (b.lod1 - b.lod0) * (b.target_mjd - b.mjd0) / NULLIF(b.mjd1 - b.mjd0, 0) AS lod,
        b.dx0  + (b.dx1  - b.dx0)  * (b.target_mjd - b.mjd0) / NULLIF(b.mjd1 - b.mjd0, 0) AS dx,
        b.dy0  + (b.dy1  - b.dy0)  * (b.target_mjd - b.mjd0) / NULLIF(b.mjd1 - b.mjd0, 0) AS dy
    FROM bracket b
$$;

COMMENT ON FUNCTION ecef_eci.eop_at_epoch(TIMESTAMPTZ)
IS 'Returns linearly interpolated Earth Orientation Parameters for a given epoch. Requires eop_data table to be populated.';

-- =============================================================================
-- EOP Data Loading — Parse IERS finals2000A fixed-width format
-- =============================================================================
-- IERS finals2000A format (see https://maia.usno.navy.mil/ser7/readme.finals2000A):
--   Columns 8-15:   MJD
--   Columns 19-27:  PM-x (arcsec)
--   Columns 38-46:  PM-y (arcsec)
--   Columns 59-68:  UT1-UTC (sec)
--   Columns 80-86:  LOD (ms)
--   Columns 98-106: dX (mas)
--   Columns 117-125: dY (mas)
--   Column 17:      I/P flag

CREATE OR REPLACE FUNCTION ecef_eci.load_eop_finals2000a(
    raw_text TEXT
) RETURNS INT
LANGUAGE plpgsql
SET search_path = ecef_eci, pg_catalog, public
AS $$
DECLARE
    line TEXT;
    n_loaded INT := 0;
    v_mjd FLOAT8;
    v_xp FLOAT8;
    v_yp FLOAT8;
    v_dut1 FLOAT8;
    v_lod FLOAT8;
    v_dx FLOAT8;
    v_dy FLOAT8;
    v_flag CHAR(1);
BEGIN
    FOR line IN SELECT unnest(string_to_array(raw_text, E'\n'))
    LOOP
        -- Skip short lines and header lines
        IF length(line) < 68 THEN
            CONTINUE;
        END IF;

        -- Parse fixed-width fields
        BEGIN
            v_mjd  := trim(substring(line FROM 8 FOR 8))::FLOAT8;
            v_xp   := trim(substring(line FROM 19 FOR 9))::FLOAT8;
            v_yp   := trim(substring(line FROM 38 FOR 9))::FLOAT8;
            v_dut1 := trim(substring(line FROM 59 FOR 10))::FLOAT8;
            v_flag := substring(line FROM 17 FOR 1);

            -- LOD and celestial pole offsets may be absent
            IF length(line) >= 86 AND trim(substring(line FROM 80 FOR 7)) != '' THEN
                v_lod := trim(substring(line FROM 80 FOR 7))::FLOAT8;
            ELSE
                v_lod := NULL;
            END IF;

            IF length(line) >= 106 AND trim(substring(line FROM 98 FOR 9)) != '' THEN
                v_dx := trim(substring(line FROM 98 FOR 9))::FLOAT8;
            ELSE
                v_dx := NULL;
            END IF;

            IF length(line) >= 125 AND trim(substring(line FROM 117 FOR 9)) != '' THEN
                v_dy := trim(substring(line FROM 117 FOR 9))::FLOAT8;
            ELSE
                v_dy := NULL;
            END IF;
        EXCEPTION WHEN OTHERS THEN
            CONTINUE;  -- Skip malformed lines
        END;

        -- Insert or update
        INSERT INTO ecef_eci.eop_data (mjd, date, xp, yp, dut1, lod, dx, dy, data_type)
        VALUES (
            v_mjd,
            ecef_eci.mjd_to_timestamp(v_mjd)::date,
            v_xp, v_yp, v_dut1, v_lod, v_dx, v_dy,
            COALESCE(v_flag, 'I')
        )
        ON CONFLICT (mjd) DO UPDATE SET
            xp = EXCLUDED.xp,
            yp = EXCLUDED.yp,
            dut1 = EXCLUDED.dut1,
            lod = EXCLUDED.lod,
            dx = EXCLUDED.dx,
            dy = EXCLUDED.dy,
            data_type = EXCLUDED.data_type,
            loaded_at = now();

        n_loaded := n_loaded + 1;
    END LOOP;

    RETURN n_loaded;
END;
$$;

COMMENT ON FUNCTION ecef_eci.load_eop_finals2000a(TEXT)
IS 'Parses IERS finals2000A fixed-width text and upserts into eop_data table. Returns number of rows loaded.';

-- =============================================================================
-- EOP Refresh Job (TimescaleDB Background Worker)
-- =============================================================================
-- This procedure can be scheduled via TimescaleDB add_job().
-- It uses pg_http or dblink or an external script to fetch the latest
-- finals2000A data. The actual HTTP fetch depends on available extensions.
--
-- For environments without pg_http, load via:
--   \copy or COPY FROM PROGRAM 'curl -s https://maia.usno.navy.mil/ser7/finals2000A.all'
--   SELECT ecef_eci.load_eop_finals2000a(pg_read_file('/tmp/finals2000A.all'));

CREATE OR REPLACE PROCEDURE ecef_eci.refresh_eop(job_id INT, config JSONB)
LANGUAGE plpgsql
SET search_path = ecef_eci, pg_catalog, public
AS $$
DECLARE
    source_path TEXT;
    eop_file TEXT;
    n_loaded INT;
    n_synced INT;
    staleness RECORD;
    stale_threshold INT;
BEGIN
    -- Extract config
    source_path := config->>'eop_file_path';
    stale_threshold := COALESCE((config->>'staleness_threshold_days')::INT, 7);

    -- Validate config
    IF source_path IS NULL THEN
        RAISE WARNING 'EOP refresh (job %): no eop_file_path in config. '
            'Schedule with: SELECT add_job(''ecef_eci.refresh_eop'', ''1 day'', '
            'config => ''{"eop_file_path": "/path/to/finals2000A.all"}''::jsonb);',
            job_id;
        RETURN;
    END IF;

    -- Read file with error handling
    BEGIN
        eop_file := pg_read_file(source_path);
    EXCEPTION
        WHEN undefined_file THEN
            RAISE WARNING 'EOP refresh (job %): file not found: %', job_id, source_path;
            RETURN;
        WHEN insufficient_privilege THEN
            RAISE WARNING 'EOP refresh (job %): permission denied reading: %', job_id, source_path;
            RETURN;
        WHEN OTHERS THEN
            RAISE WARNING 'EOP refresh (job %): error reading %: %', job_id, source_path, SQLERRM;
            RETURN;
    END;

    -- Validate file content
    IF eop_file IS NULL OR length(eop_file) < 1000 THEN
        RAISE WARNING 'EOP refresh (job %): file too small or empty (% bytes). '
            'Expected finals2000A format (~500KB+).',
            job_id, COALESCE(length(eop_file), 0);
        RETURN;
    END IF;

    -- Load into eop_data
    n_loaded := ecef_eci.load_eop_finals2000a(eop_file);

    IF n_loaded = 0 THEN
        RAISE WARNING 'EOP refresh (job %): 0 rows parsed from %. File may be corrupt.',
            job_id, source_path;
        RETURN;
    END IF;

    RAISE NOTICE 'EOP refresh (job %): loaded % entries from %', job_id, n_loaded, source_path;

    -- Sync to PostGIS table
    n_synced := ecef_eci.sync_eop_to_postgis();
    IF n_synced >= 0 THEN
        RAISE NOTICE 'EOP refresh (job %): synced % entries to postgis_eop', job_id, n_synced;
    END IF;

    -- Post-load staleness check
    SELECT * INTO staleness FROM ecef_eci.eop_staleness();
    IF staleness.is_stale THEN
        RAISE WARNING 'EOP refresh (job %): data is stale — latest EOP is % days old (threshold: % days). '
            'Check that the external download script is running.',
            job_id, staleness.gap_days, stale_threshold;
    END IF;
END;
$$;

COMMENT ON PROCEDURE ecef_eci.refresh_eop(INT, JSONB)
IS 'Background job procedure for refreshing EOP data from a local file. '
   'An external scheduler (cron/systemd) must download the IERS file first. '
   'Schedule with: SELECT ecef_eci.setup_eop_refresh(''/path/to/finals2000A.all'');';

-- =============================================================================
-- Convenience: EOP status summary
-- =============================================================================

CREATE OR REPLACE FUNCTION ecef_eci.eop_status()
RETURNS TABLE (
    total_entries  BIGINT,
    earliest_date  DATE,
    latest_date    DATE,
    latest_mjd     FLOAT8,
    observed_count BIGINT,
    predicted_count BIGINT,
    last_loaded    TIMESTAMPTZ
)
LANGUAGE SQL
STABLE
PARALLEL SAFE
SET search_path = ecef_eci, pg_catalog, public
AS $$
    SELECT
        count(*),
        min(date),
        max(date),
        max(mjd),
        count(*) FILTER (WHERE data_type = 'I' OR data_type = 'B'),
        count(*) FILTER (WHERE data_type = 'P'),
        max(loaded_at)
    FROM ecef_eci.eop_data
$$;

COMMENT ON FUNCTION ecef_eci.eop_status()
IS 'Returns summary of EOP data coverage: total entries, date range, observed vs predicted count, last load time.';

-- =============================================================================
-- EOP Sync to PostGIS
-- =============================================================================
-- Copies eop_data into the PostGIS-managed postgis_eop table.
-- Data flows through the TimescaleDB parser only (avoids PostGIS parser
-- column offset bugs documented in §8.3).

CREATE OR REPLACE FUNCTION ecef_eci.sync_eop_to_postgis()
RETURNS INT
LANGUAGE plpgsql
SET search_path = ecef_eci, pg_catalog, public
AS $$
DECLARE
    n_synced INT;
    postgis_table_exists BOOLEAN;
BEGIN
    -- Check if postgis_eop table exists (PostGIS extension may not be installed)
    SELECT EXISTS (
        SELECT 1 FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'public' AND c.relname = 'postgis_eop'
    ) INTO postgis_table_exists;

    IF NOT postgis_table_exists THEN
        RAISE NOTICE 'sync_eop_to_postgis: postgis_eop table not found (PostGIS ECEF/ECI extension not installed). Skipping sync.';
        RETURN -1;
    END IF;

    -- Sync: INSERT from eop_data into postgis_eop, upsert on conflict
    INSERT INTO public.postgis_eop (mjd, xp, yp, dut1, dx, dy)
    SELECT mjd, xp, yp, dut1, dx, dy
    FROM ecef_eci.eop_data
    ON CONFLICT (mjd) DO UPDATE SET
        xp   = EXCLUDED.xp,
        yp   = EXCLUDED.yp,
        dut1 = EXCLUDED.dut1,
        dx   = EXCLUDED.dx,
        dy   = EXCLUDED.dy;

    GET DIAGNOSTICS n_synced = ROW_COUNT;
    RETURN n_synced;
END;
$$;

COMMENT ON FUNCTION ecef_eci.sync_eop_to_postgis()
IS 'Syncs eop_data table to PostGIS postgis_eop table. Returns row count, or -1 if postgis_eop does not exist.';

-- =============================================================================
-- EOP Staleness Detection
-- =============================================================================
-- Reports how current the EOP data is relative to today.

CREATE OR REPLACE FUNCTION ecef_eci.eop_staleness(
    threshold_days INT DEFAULT 7
)
RETURNS TABLE (
    latest_date    DATE,
    gap_days       INT,
    is_stale       BOOLEAN,
    load_age_hours FLOAT8
)
LANGUAGE SQL
STABLE
PARALLEL SAFE
SET search_path = ecef_eci, pg_catalog, public
AS $$
    SELECT
        max(e.date)                                              AS latest_date,
        COALESCE((CURRENT_DATE - max(e.date))::INT, -1)          AS gap_days,
        COALESCE((CURRENT_DATE - max(e.date))::INT, -1) > threshold_days AS is_stale,
        COALESCE(
            EXTRACT(EPOCH FROM (now() - max(e.loaded_at))) / 3600.0,
            -1
        )                                                        AS load_age_hours
    FROM ecef_eci.eop_data e
$$;

COMMENT ON FUNCTION ecef_eci.eop_staleness(INT)
IS 'Returns EOP data freshness: latest date, gap in days from today, staleness flag, hours since last load.';

-- =============================================================================
-- EOP Refresh Setup Convenience Wrapper
-- =============================================================================
-- One-call setup for the background job.

CREATE OR REPLACE FUNCTION ecef_eci.setup_eop_refresh(
    file_path   TEXT,
    refresh_interval INTERVAL DEFAULT INTERVAL '1 day',
    staleness_threshold_days INT DEFAULT 7
)
RETURNS INT
LANGUAGE plpgsql
SET search_path = ecef_eci, pg_catalog, public
AS $$
DECLARE
    job_config JSONB;
    new_job_id INT;
BEGIN
    job_config := jsonb_build_object(
        'eop_file_path', file_path,
        'staleness_threshold_days', staleness_threshold_days
    );

    SELECT add_job(
        'ecef_eci.refresh_eop'::regproc,
        refresh_interval,
        config => job_config
    ) INTO new_job_id;

    RAISE NOTICE 'EOP refresh job created: id=%, interval=%, file=%',
        new_job_id, refresh_interval, file_path;

    RETURN new_job_id;
END;
$$;

COMMENT ON FUNCTION ecef_eci.setup_eop_refresh(TEXT, INTERVAL, INT)
IS 'Convenience wrapper to create a TimescaleDB background job for EOP refresh. '
   'Returns the job ID. Example: SELECT ecef_eci.setup_eop_refresh(''/var/lib/postgresql/eop/finals2000A.all'')';