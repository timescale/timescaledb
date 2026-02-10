# Spec 03: Frame Conversion Integration

## Status: Draft

## Overview

Define how time-dependent ECEF <-> ECI coordinate frame conversions integrate with
TimescaleDB's timestamp handling, continuous aggregates, and query planning.

## Context

### Coordinate Frames

- **ECEF (Earth-Centered, Earth-Fixed)**: Rotates with the Earth. X points to the
  prime meridian/equator intersection, Z to the north pole. Used for ground-based
  and near-surface applications.

- **ECI (Earth-Centered Inertial)**: Does not rotate with the Earth. Typically J2000
  or GCRF frame. Used for orbital mechanics and satellite tracking.

The conversion between them is a **time-dependent rotation** based on:
- Earth Rotation Angle (ERA) — dominant term
- Precession and nutation — arcsecond-level corrections
- Polar motion — milliarcsecond-level corrections

### TimescaleDB Timestamp Handling
- Stored as INT64 microseconds since 2000-01-01 UTC (`src/dimension.c`)
- PostgreSQL `TIMESTAMPTZ` — no leap second encoding
- Continuous aggregates bucket by `time_bucket()` which operates on this representation

### Key Concern
Frame conversion is **non-commutative with aggregation**:
```
avg(ECEF_to_ECI(pos, time)) != ECEF_to_ECI(avg(pos), avg(time))
```
Because the rotation matrix is time-dependent, you cannot aggregate first and
convert after (or vice versa) without introducing error.

## Design

### 1. Conversion Function Signatures

Expected from the PostGIS fork:

```sql
-- ECEF -> ECI at a specific epoch
ST_ECEF_To_ECI(
    geom geometry(PointZ, 4978),    -- ECEF position
    epoch TIMESTAMPTZ,               -- conversion epoch
    frame TEXT DEFAULT 'J2000'       -- target ECI frame
) RETURNS geometry(PointZ, <ECI_SRID>)

-- ECI -> ECEF at a specific epoch
ST_ECI_To_ECEF(
    geom geometry(PointZ, <ECI_SRID>),
    epoch TIMESTAMPTZ,
    frame TEXT DEFAULT 'J2000'
) RETURNS geometry(PointZ, 4978)

-- Batch conversion (set-returning for efficiency)
ST_ECEF_To_ECI_Batch(
    geoms geometry[],
    epochs TIMESTAMPTZ[]
) RETURNS geometry[]
```

### 2. Timestamp Precision Considerations

| Source | Precision | Notes |
|--------|-----------|-------|
| PostgreSQL TIMESTAMPTZ | 1 microsecond | No leap seconds |
| UTC (IERS) | Continuous + leap seconds | Leap second table needed |
| GPS Time | Continuous, no leap seconds | Offset from UTC known |
| TDB/TT | Relativistic time scales | For high-precision ephemerides |

**For LEO tracking (7+ km/s)**: 1 microsecond = ~7 mm position error from Earth
rotation alone. PostgreSQL microsecond precision is adequate for most applications.

**For high-precision orbit determination**: The lack of leap second encoding in
PostgreSQL timestamps means the PostGIS fork must either:
- Accept a separate `leap_seconds_offset` parameter
- Maintain an internal leap second table
- Document the precision limitation

### 3. Frame Metadata in Schema

```sql
-- Add to the trajectories table
ALTER TABLE trajectories ADD COLUMN epoch_utc TIMESTAMPTZ;  -- may differ from `time`
ALTER TABLE trajectories ADD COLUMN time_system SMALLINT DEFAULT 0;
-- 0=UTC, 1=GPS, 2=TAI, 3=TDB
```

The `time` column is the TimescaleDB partitioning key (always UTC for chunk
boundaries). The `epoch_utc` can carry the precise observation epoch if it differs
from the database timestamp.

### 4. Conversion in Queries

**Point query — convert on the fly**:
```sql
SELECT time, object_id,
    ST_ECEF_To_ECI(pos_ecef, time) AS pos_eci
FROM trajectories
WHERE object_id = 25544
    AND time >= '2025-01-01' AND time < '2025-01-02';
```

**Materialized ECI view — convert at ingest**:
If queries are predominantly in ECI, consider storing ECI directly and converting
at insert time. The PostGIS fork would need to register an ECI SRID.

### 5. Continuous Aggregate Implications

**Safe patterns**:
```sql
-- Aggregate in the SAME frame as storage (no conversion in aggregate)
CREATE MATERIALIZED VIEW traj_hourly
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', time) AS bucket,
    object_id,
    avg(x) AS avg_x, avg(y) AS avg_y, avg(z) AS avg_z,
    count(*) AS n_obs
FROM trajectories
GROUP BY bucket, object_id;
```

**Unsafe patterns** (document as anti-pattern):
```sql
-- DO NOT: aggregate after frame conversion
-- The rotation varies within the bucket, so avg(converted) is wrong
CREATE MATERIALIZED VIEW traj_eci_hourly_WRONG
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', time) AS bucket,
    object_id,
    avg(ST_X(ST_ECEF_To_ECI(pos_ecef, time))) AS avg_eci_x  -- WRONG
FROM trajectories
GROUP BY bucket, object_id;
```

**Correct approach for ECI aggregates**:
Convert the aggregated ECEF position at the bucket midpoint epoch:
```sql
SELECT bucket, object_id,
    ST_ECEF_To_ECI(
        ST_MakePoint(avg_x, avg_y, avg_z)::geometry(PointZ, 4978),
        bucket + INTERVAL '30 minutes'  -- midpoint epoch
    ) AS avg_pos_eci
FROM traj_hourly;
```
This introduces a small error proportional to bucket width * orbital velocity but
is well-defined and documented.

### 6. Earth Orientation Parameters (EOP)

The ECEF<->ECI conversion depends on Earth orientation data published by IERS:
- **Finals2000A**: Daily EOP updates
- **Bulletin A**: Predictions for near-future

The PostGIS fork needs a mechanism to load/update EOP data:
- Internal table `postgis_eop` with UTC date, xp, yp, dUT1, dX, dY
- Background job to refresh from IERS (or manual load)
- Fallback to IAU 2006 precession-only for dates beyond EOP coverage

TimescaleDB could provide the scheduling mechanism:
```sql
SELECT add_job('refresh_eop_data', schedule_interval => INTERVAL '1 day');
```

## Tasks

- [ ] Confirm PostGIS fork conversion function signatures and SRIDs
- [ ] Validate PostgreSQL timestamp precision is adequate for target use cases
- [ ] Document safe vs unsafe aggregation patterns for frame conversion
- [ ] Design EOP data loading mechanism (table schema, refresh job)
- [ ] Benchmark frame conversion cost per row (to inform query planning decisions)
- [ ] Test frame conversion within TimescaleDB continuous aggregate refresh
- [ ] Quantify error bounds for midpoint-epoch aggregation approach

## Open Questions

1. Which ECI frames will the PostGIS fork support? (J2000, GCRF, TEME, TOD, MOD)
2. Will the fork include nutation/precession models (IAU 2006/2000A) or delegate
   to an external library (SOFA, ERFA)?
3. Should we store pre-converted ECI alongside ECEF, or always convert at query time?
4. How will the PostGIS fork handle dates beyond EOP coverage (prediction vs error)?
5. Is there a need for relativistic corrections (TDB vs TT) for deep-space objects?

## Coordination Points (PostGIS Fork)

- **Need from PostGIS**: Conversion function signatures, supported ECI frames
- **Need from PostGIS**: EOP data storage approach (internal table vs extension parameter)
- **Need from PostGIS**: Precision guarantees (position accuracy vs timestamp precision)
- **Provide to PostGIS**: TimescaleDB `add_job()` API for scheduling EOP refresh
- **Provide to PostGIS**: Continuous aggregate semantics (what SQL constructs are supported
  in cagg definitions — immutable functions only, no volatile/stable)
