# Spec 04: Continuous Aggregate Templates

## Status: Draft

## Overview

Define reusable continuous aggregate patterns for spatiotemporal trajectory data,
accounting for the unique requirements of ECEF/ECI coordinate systems including
frame-dependent aggregation semantics.

## Context

TimescaleDB continuous aggregates (`tsl/src/continuous_aggs/`) provide:
- Automatic incremental materialization of time-bucketed aggregates
- Invalidation tracking on the source hypertable's time dimension
- Background refresh policies
- Real-time aggregation (combining materialized + recent data)

### Limitations for Spatial Data
- `time_bucket()` operates on scalar time values only — no spatial bucketing
- Aggregation functions must be IMMUTABLE or STABLE for cagg definitions
- No built-in spatial aggregate functions (these come from PostGIS)
- Invalidation is time-dimension only — spatial changes within a time range
  trigger full-bucket re-materialization (acceptable, not fine-grained)

### Key Source References (TimescaleDB)
- `tsl/src/continuous_aggs/create.c` — Cagg creation and validation
- `tsl/src/continuous_aggs/materialize.c` — Materialization logic
- `tsl/src/continuous_aggs/invalidation.c` — Change tracking
- `tsl/src/continuous_aggs/refresh.c` — Refresh policies

## Design

### Template 1: Per-Object Trajectory Summary (Hourly)

**Use case**: Dashboard showing object positions at reduced resolution.

```sql
CREATE MATERIALIZED VIEW traj_hourly
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', time) AS bucket,
    object_id,
    object_type,
    -- Position: average of decomposed ECEF coordinates
    avg(x) AS avg_x,
    avg(y) AS avg_y,
    avg(z) AS avg_z,
    -- Velocity: average
    avg(vx) AS avg_vx,
    avg(vy) AS avg_vy,
    avg(vz) AS avg_vz,
    -- Altitude stats
    min(altitude_km) AS min_alt_km,
    max(altitude_km) AS max_alt_km,
    avg(altitude_km) AS avg_alt_km,
    -- Observation count
    count(*) AS n_obs,
    -- First and last observation times (for interpolation)
    min(time) AS first_obs,
    max(time) AS last_obs
FROM trajectories
GROUP BY bucket, object_id, object_type;
```

**Refresh policy**:
```sql
SELECT add_continuous_aggregate_policy('traj_hourly',
    start_offset => INTERVAL '3 hours',
    end_offset   => INTERVAL '1 hour',
    schedule_interval => INTERVAL '30 minutes');
```

### Template 2: Spatial Density (Per Altitude Band, Hourly)

**Use case**: How many objects are in each orbital regime over time?

```sql
CREATE MATERIALIZED VIEW spatial_density_hourly
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', time) AS bucket,
    spatial_bucket,
    object_type,
    count(DISTINCT object_id) AS n_objects,
    count(*) AS n_observations,
    avg(altitude_km) AS mean_alt_km,
    stddev(altitude_km) AS stddev_alt_km
FROM trajectories
GROUP BY bucket, spatial_bucket, object_type;
```

### Template 3: Conjunction Screening Pre-Filter (15-Minute)

**Use case**: Narrow down potential close approaches before expensive pairwise computation.

```sql
CREATE MATERIALIZED VIEW conjunction_prefilter
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('15 minutes', time) AS bucket,
    spatial_bucket,
    -- Bounding box of all objects in this bucket/region
    min(x) AS min_x, max(x) AS max_x,
    min(y) AS min_y, max(y) AS max_y,
    min(z) AS min_z, max(z) AS max_z,
    -- Object list (for downstream pairwise check)
    count(DISTINCT object_id) AS n_objects,
    array_agg(DISTINCT object_id) AS object_ids
FROM trajectories
GROUP BY bucket, spatial_bucket;
```

**Note**: `array_agg(DISTINCT ...)` may not be supported in all cagg versions.
Test and potentially move to a post-aggregation query.

### Template 4: Daily Orbital Element Summary

**Use case**: Long-term trending of orbital parameters.

```sql
CREATE MATERIALIZED VIEW orbital_daily
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 day', time) AS bucket,
    object_id,
    -- Position statistics
    avg(x) AS avg_x, avg(y) AS avg_y, avg(z) AS avg_z,
    -- Radius (proxy for semi-major axis)
    avg(sqrt(x*x + y*y + z*z)) AS avg_radius_m,
    min(sqrt(x*x + y*y + z*z)) AS min_radius_m,  -- proxy for perigee
    max(sqrt(x*x + y*y + z*z)) AS max_radius_m,  -- proxy for apogee
    -- Observation statistics
    count(*) AS n_obs,
    min(time) AS first_obs,
    max(time) AS last_obs
FROM trajectories
GROUP BY bucket, object_id;
```

### Template 5: Hierarchical Aggregation (Cagg-on-Cagg)

Build coarser rollups from finer ones:

```sql
-- Daily from hourly
CREATE MATERIALIZED VIEW traj_daily
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 day', bucket) AS bucket,
    object_id,
    object_type,
    avg(avg_x) AS avg_x,
    avg(avg_y) AS avg_y,
    avg(avg_z) AS avg_z,
    sum(n_obs) AS n_obs,
    min(first_obs) AS first_obs,
    max(last_obs) AS last_obs
FROM traj_hourly
GROUP BY time_bucket('1 day', bucket), object_id, object_type;
```

### Frame Conversion in Aggregates

See [spec-03-frame-conversion.md](spec-03-frame-conversion.md) for detailed
analysis. Summary of rules:

| Pattern | Safe? | Notes |
|---------|-------|-------|
| Aggregate ECEF, query in ECEF | Yes | No conversion needed |
| Aggregate ECEF, convert result to ECI at midpoint | Approximate | Error ~ bucket_width * angular_velocity |
| Convert to ECI per-row then aggregate | Wrong | Non-commutative with time-varying rotation |
| Aggregate ECI (stored natively) | Yes | If data ingested in ECI frame |

### Gapfill for Trajectory Interpolation

TimescaleDB's gapfill (`tsl/src/nodes/gapfill/`) can fill missing time buckets:

```sql
SELECT
    time_bucket_gapfill('1 minute', time) AS bucket,
    object_id,
    interpolate(avg(x)) AS x,
    interpolate(avg(y)) AS y,
    interpolate(avg(z)) AS z
FROM trajectories
WHERE object_id = 25544
    AND time >= '2025-01-01' AND time < '2025-01-01 02:00:00'
GROUP BY bucket, object_id;
```

**Caveat**: Linear interpolation of ECEF coordinates produces straight-line
paths through the Earth for objects on the far side. For orbital objects,
interpolation should happen in orbital element space or use a propagator.
Document this limitation clearly.

## Tasks

- [ ] Implement Template 1-5 against test schema
- [ ] Validate cagg creation with PostGIS fork functions (IMMUTABLE requirement)
- [ ] Benchmark refresh performance with representative data volumes
- [ ] Test hierarchical cagg (cagg-on-cagg) with trajectory data
- [ ] Measure invalidation overhead for high-frequency ingest
- [ ] Document gapfill limitations for orbital trajectories
- [ ] Test real-time aggregation (materialized + recent unmaterialized data)

## Open Questions

1. Can `array_agg(DISTINCT ...)` be used in continuous aggregates? (needed for Template 3)
2. What is the maximum practical number of cagg-on-cagg levels before refresh
   latency becomes problematic?
3. Should we provide a custom `orbital_interpolate()` function for gapfill that
   uses Keplerian propagation instead of linear interpolation?
4. Are PostGIS aggregate functions (e.g., `ST_Union`, `ST_Extent`) usable in caggs?

## Coordination Points (PostGIS Fork)

- **Need from PostGIS**: Confirm which spatial aggregate functions are IMMUTABLE
  (required for cagg definitions)
- **Need from PostGIS**: Whether `ST_Extent3D` or equivalent is available for
  3D bounding box aggregation in ECEF space
- **Provide to PostGIS**: Cagg SQL constraints (what constructs are allowed in
  the defining query)
