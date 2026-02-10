# Spec 01: Schema & Partitioning Design

## Status: Draft

## Overview

Define the hypertable schema for ECEF/ECI trajectory data and implement a custom
spatial partitioning function that maps 3D coordinates to TimescaleDB closed-dimension
buckets for effective chunk exclusion.

## Context

TimescaleDB partitions data using an N-dimensional hyperspace model:
- **Open dimensions** (time): auto-expanding chunk boundaries
- **Closed dimensions** (space): fixed-range hash/custom partitions

The default hash partitioning (`get_partition_hash()` in `src/partitioning.h`) treats
the column value as opaque — it has no spatial awareness. For ECEF/ECI data, we need
chunk boundaries that correlate with spatial locality so that spatial queries can
eliminate chunks efficiently.

### Key Source References (TimescaleDB)
- `src/dimension.h` — Dimension definitions (open vs closed)
- `src/dimension.c` — Dimension creation, custom partitioning function support
- `src/hypercube.h` — N-dimensional chunk representation
- `src/partitioning.h` — Partitioning function interface
- `src/chunk.c` — Chunk creation and lookup
- `src/subspace_store.c` — In-memory chunk index

## Design

### 1. Reference Schema

```sql
-- Core trajectory table
CREATE TABLE trajectories (
    time          TIMESTAMPTZ    NOT NULL,
    object_id     INT            NOT NULL,
    object_type   SMALLINT       NOT NULL DEFAULT 0,  -- 0=satellite, 1=debris, 2=aircraft, ...
    frame         SMALLINT       NOT NULL DEFAULT 0,  -- 0=ECEF, 1=ECI (J2000)

    -- Native PostGIS ECEF geometry (EPSG:4978 = WGS84 geocentric)
    pos_ecef      geometry(PointZ, 4978),

    -- Decomposed coordinates for compression (meters from Earth center)
    x             FLOAT8         NOT NULL,
    y             FLOAT8         NOT NULL,
    z             FLOAT8         NOT NULL,

    -- Velocity (m/s) — optional, for orbit determination consumers
    vx            FLOAT8,
    vy            FLOAT8,
    vz            FLOAT8,

    -- Derived scalar for spatial partitioning (km above WGS84 ellipsoid)
    altitude_km   FLOAT8         NOT NULL,

    -- Derived spatial bucket (computed by trigger or application)
    spatial_bucket INT           NOT NULL
);
```

### 2. Hypertable Creation

```sql
SELECT create_hypertable('trajectories', 'time',
    chunk_time_interval => INTERVAL '1 hour');

-- Spatial closed dimension using custom partitioning
SELECT add_dimension('trajectories', 'spatial_bucket',
    number_partitions => 16);
```

### 3. Spatial Partitioning Function Options

**Option A: Altitude-Band Bucketing**
Partition by orbital shell / altitude regime:
```
Bucket 0-3:   LEO   (0–2000 km)    — 4 sub-bands by 500 km
Bucket 4-7:   MEO   (2000–35786 km) — 4 sub-bands
Bucket 8-11:  GEO   (35786 ± 200 km) — 4 sub-bands by longitude quadrant
Bucket 12-14: HEO   (>35986 km)     — 3 sub-bands
Bucket 15:    Sub-orbital / terrestrial
```

Advantages: Simple, fast, matches common query patterns ("show me all LEO objects").
Disadvantages: Uneven data distribution (LEO is crowded, HEO is sparse).

**Option B: 3D Octree Cell ID**
Recursively divide the ECEF bounding volume into octants, assign a cell ID at a
fixed depth, and hash to N buckets.

Advantages: Spatially balanced, works for any altitude.
Disadvantages: More complex, octree depth affects granularity.

**Option C: Geohash of Sub-Satellite Point + Altitude Band**
Project ECEF to lat/lon, compute geohash prefix, combine with altitude band.

Advantages: Familiar, good for ground-track queries.
Disadvantages: Degenerate at poles; requires ECEF->geodetic conversion on insert.

### 4. Recommended Approach

Start with **Option A** (altitude-band) for simplicity. It aligns with the most
common query patterns in satellite/debris tracking and is trivially computed from
the ECEF position vector magnitude:

```sql
CREATE OR REPLACE FUNCTION compute_spatial_bucket(
    x FLOAT8, y FLOAT8, z FLOAT8
) RETURNS INT AS $$
DECLARE
    radius_km FLOAT8;
    alt_km FLOAT8;
    earth_radius_km CONSTANT FLOAT8 := 6371.0;
BEGIN
    radius_km := sqrt(x*x + y*y + z*z) / 1000.0;
    alt_km := radius_km - earth_radius_km;

    IF alt_km < 0 THEN RETURN 15;          -- sub-orbital/terrestrial
    ELSIF alt_km < 500 THEN RETURN 0;
    ELSIF alt_km < 1000 THEN RETURN 1;
    ELSIF alt_km < 1500 THEN RETURN 2;
    ELSIF alt_km < 2000 THEN RETURN 3;
    ELSIF alt_km < 10000 THEN RETURN 4;
    ELSIF alt_km < 20000 THEN RETURN 5;
    ELSIF alt_km < 30000 THEN RETURN 6;
    ELSIF alt_km < 35786 THEN RETURN 7;
    ELSIF alt_km < 35986 THEN RETURN 8 + (atan2(y, x)::int % 4 + 4) % 4;  -- GEO quadrants
    ELSIF alt_km < 50000 THEN RETURN 12;
    ELSIF alt_km < 100000 THEN RETURN 13;
    ELSE RETURN 14;
    END IF;
END;
$$ LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE;
```

### 5. Insert-Time Computation

Use a BEFORE INSERT trigger or require the application to compute `spatial_bucket`
and `altitude_km`:

```sql
CREATE OR REPLACE FUNCTION trajectories_before_insert()
RETURNS TRIGGER AS $$
BEGIN
    NEW.altitude_km := (sqrt(NEW.x * NEW.x + NEW.y * NEW.y + NEW.z * NEW.z) / 1000.0) - 6371.0;
    NEW.spatial_bucket := compute_spatial_bucket(NEW.x, NEW.y, NEW.z);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_trajectories_spatial
    BEFORE INSERT ON trajectories
    FOR EACH ROW EXECUTE FUNCTION trajectories_before_insert();
```

**Performance note**: Triggers add per-row overhead. For high-throughput ingest,
compute `spatial_bucket` application-side and skip the trigger.

## Tasks

- [ ] Finalize schema with PostGIS fork maintainers (SRID numbers, type names)
- [ ] Implement and benchmark partitioning function options A, B, C
- [ ] Choose partition count (8, 16, 32) based on data distribution analysis
- [ ] Decide trigger vs application-side bucket computation based on ingest benchmarks
- [ ] Test chunk exclusion effectiveness with representative spatial queries
- [ ] Document schema for downstream consumers

## Open Questions

1. Should `pos_ecef` geometry column be stored alongside decomposed floats, or is it
   redundant? (Depends on whether PostGIS spatial functions are needed at query time
   vs reconstruction from x/y/z.)
2. What SRID will the PostGIS fork register for ECI J2000? (EPSG:4978 is ECEF only.)
3. Should we support multiple ECI frames (J2000, TEME, GCRF) or normalize to one?
4. Is `object_id` sufficient for segmentation, or do we need composite keys?

## Coordination Points (PostGIS Fork)

- **Need from PostGIS**: Confirmed SRID assignments for ECEF and ECI frames
- **Need from PostGIS**: `ST_ECEF_To_ECI(geometry, timestamptz)` function signature
- **Need from PostGIS**: Whether `geometry(PointZ, 4978)` will be the canonical ECEF type
  or if a new subtype is introduced
- **Provide to PostGIS**: TimescaleDB chunk constraint format so PostGIS planner hooks
  don't conflict
