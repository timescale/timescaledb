# Interface Contract: TimescaleDB <-> PostGIS ECEF/ECI

## Purpose

This file defines the formal interface between the TimescaleDB integration work
(this repo) and the PostGIS ECEF/ECI fork (`montge/postgis`). It is the single
source of truth for what each side needs from and provides to the other.

**Both repos should contain a copy of this contract.** When the contract changes,
update both copies and note the change in a commit message prefixed with
`[contract]`.

## Version

Contract version: **0.1.0-draft**
Last updated: 2025-01-01
Status: **DRAFT — awaiting PostGIS fork confirmation**

---

## 1. Type Definitions (PostGIS -> TimescaleDB)

### 1.1 ECEF Point Type
```
Type:       geometry(PointZ, 4978)
SRID:       4978 (EPSG — already in stock PostGIS)
Storage:    Standard GSERIALIZED / WKB
Axes:       X (meters, prime meridian), Y (meters, 90E), Z (meters, north pole)
Datum:      WGS84
```

**Status**: [ ] Confirmed by PostGIS fork

### 1.2 ECI Point Type (J2000)
```
Type:       geometry(PointZ, 900001)  -- PROPOSED
SRID:       900001 (user-defined range)
Storage:    Standard GSERIALIZED / WKB (no frame metadata in binary)
Axes:       X (meters, vernal equinox), Y (meters, 90 degrees), Z (meters, north pole)
Frame:      J2000 / EME2000
Epoch:      Separate from geometry — passed as function parameter
```

**Status**: [ ] Confirmed by PostGIS fork

### 1.3 Additional ECI Frames (Optional)
```
SRID 900002:  GCRF (Geocentric Celestial Reference Frame)
SRID 900003:  TEME (True Equator Mean Equinox — SGP4 native)
```

**Status**: [ ] Confirmed by PostGIS fork

### 1.4 SRID Registration
PostGIS fork registers SRIDs in its install script via `INSERT INTO spatial_ref_sys`.
These must be present before any TimescaleDB hypertable uses the types.

**Status**: [ ] Confirmed by PostGIS fork

---

## 2. Conversion Functions (PostGIS -> TimescaleDB)

### 2.1 ECEF to ECI
```sql
ST_ECEF_To_ECI(
    geom    geometry(PointZ, 4978),   -- ECEF input
    epoch   TIMESTAMPTZ,               -- conversion epoch (UTC)
    frame   TEXT DEFAULT 'J2000'       -- target: 'J2000', 'GCRF', 'TEME'
) RETURNS geometry(PointZ, 900001)     -- ECI output (SRID matches frame)
```
- **Volatility**: STABLE (depends on EOP data, not on transaction state)
- **Parallel safety**: PARALLEL SAFE
- **Precision**: Sub-meter for LEO with current EOP; degrade spec TBD

**Status**: [ ] Signature confirmed | [ ] Implemented | [ ] Tested

### 2.2 ECI to ECEF
```sql
ST_ECI_To_ECEF(
    geom    geometry(PointZ, 900001),  -- ECI input
    epoch   TIMESTAMPTZ,
    frame   TEXT DEFAULT 'J2000'
) RETURNS geometry(PointZ, 4978)       -- ECEF output
```
- **Volatility**: STABLE
- **Parallel safety**: PARALLEL SAFE

**Status**: [ ] Signature confirmed | [ ] Implemented | [ ] Tested

### 2.3 Coordinate Accessors (for decomposed storage)
```sql
ST_ECEF_X(geom geometry) RETURNS FLOAT8   -- IMMUTABLE, PARALLEL SAFE
ST_ECEF_Y(geom geometry) RETURNS FLOAT8   -- IMMUTABLE, PARALLEL SAFE
ST_ECEF_Z(geom geometry) RETURNS FLOAT8   -- IMMUTABLE, PARALLEL SAFE
```
These are equivalent to `ST_X`, `ST_Y`, `ST_Z` but named explicitly for clarity.
They must be **IMMUTABLE** (not just STABLE) for use in continuous aggregate
definitions and index expressions.

**Status**: [ ] Confirmed | [ ] Implemented

### 2.4 Distance Functions
```sql
ST_3DDistance(a geometry, b geometry) RETURNS FLOAT8
-- Already in stock PostGIS. Confirm it works with ECEF SRID 4978.
-- Returns Euclidean distance in meters (not geodesic — correct for ECEF).

ST_3DDWithin(a geometry, b geometry, distance FLOAT8) RETURNS BOOLEAN
-- Already in stock PostGIS. Confirm 3D support with SRID 4978.
```

**Status**: [ ] Confirmed working with ECEF types

---

## 3. Operator Classes (PostGIS -> TimescaleDB)

### 3.1 GiST 3D Index Support
```sql
CREATE INDEX ... USING gist (pos_ecef gist_geometry_ops_nd);
```
- Must support 3D bounding box operations on SRID 4978 and 900001+
- Used for within-chunk spatial indexing in TimescaleDB

**Status**: [ ] Confirmed working with ECEF/ECI types

### 3.2 Selectivity Estimators
PostGIS selectivity estimators (`gserialized_gist_sel`, etc.) should work
with ECEF/ECI types for accurate query planning across TimescaleDB chunks.

**Status**: [ ] Confirmed

---

## 4. Earth Orientation Parameters (PostGIS -> TimescaleDB)

### 4.1 EOP Storage
```sql
-- Table managed by PostGIS fork
CREATE TABLE IF NOT EXISTS postgis_eop (
    mjd         FLOAT8 PRIMARY KEY,    -- Modified Julian Date
    xp          FLOAT8,                -- Polar motion X (arcseconds)
    yp          FLOAT8,                -- Polar motion Y (arcseconds)
    dut1        FLOAT8,                -- UT1 - UTC (seconds)
    dx          FLOAT8,                -- Celestial pole offset X (milliarcseconds)
    dy          FLOAT8                 -- Celestial pole offset Y (milliarcseconds)
);
```

**Status**: [ ] Schema confirmed | [ ] Load mechanism defined

### 4.2 EOP Refresh Job
TimescaleDB can schedule periodic EOP updates:
```sql
SELECT add_job('postgis_refresh_eop', schedule_interval => INTERVAL '1 day');
```
PostGIS fork must provide the `postgis_refresh_eop` procedure.

**Status**: [ ] Procedure defined | [ ] Tested with TimescaleDB scheduler

---

## 5. Extension Metadata (PostGIS -> TimescaleDB)

### 5.1 Extension Identity
```
Extension name:     postgis_ecef_eci  (PROPOSED — separate from stock postgis)
Depends on:         postgis >= 3.4
Min PostgreSQL:     15
Control file:       postgis_ecef_eci.control
```

**Status**: [ ] Packaging approach confirmed

### 5.2 Planner/Executor Hooks
List any hooks the PostGIS fork registers (beyond stock PostGIS):
```
Hook name:          (NONE KNOWN — confirm)
Chaining:           Must save and call previous hook value
```

**Status**: [ ] Confirmed

---

## 6. What TimescaleDB Provides to PostGIS

### 6.1 Chunk Constraint Format
TimescaleDB CHECK constraints on chunk tables:
```sql
-- Time dimension
CHECK (time >= '2025-01-01 00:00:00+00' AND time < '2025-01-01 01:00:00+00')

-- Closed dimension (spatial bucket)
CHECK (spatial_bucket >= 0 AND spatial_bucket < 4)
```
PostGIS planner hooks should not interfere with these constraints.

### 6.2 Compression Behavior
- Geometry columns compressed with Array algorithm (TOAST/LZ fallback)
- Compressed chunks have no GiST indexes
- Decompression is segment-at-a-time, not row-at-a-time
- `SEGMENT BY object_id, ORDER BY time ASC`

### 6.3 Continuous Aggregate Constraints
Functions used in continuous aggregate definitions must be:
- **IMMUTABLE** (preferred) or **STABLE** for the defining query
- **PARALLEL SAFE** for optimal execution
- Must not reference mutable state (no VOLATILE functions)

Frame conversion functions (STABLE, depends on EOP) can be used in
regular queries but **NOT** in continuous aggregate definitions.

### 6.4 Background Job Scheduling
```sql
-- TimescaleDB provides:
SELECT add_job(proc REGPROC, schedule_interval INTERVAL, ...) RETURNS INTEGER;
SELECT delete_job(job_id INTEGER);
SELECT alter_job(job_id INTEGER, schedule_interval INTERVAL, ...);
```
Available for PostGIS fork to schedule EOP refresh, SRID maintenance, etc.

---

## 7. Integration Test Checklist

Both sides must pass these tests:

- [ ] `CREATE EXTENSION postgis; CREATE EXTENSION postgis_ecef_eci; CREATE EXTENSION timescaledb;` — succeeds
- [ ] Reverse order: `CREATE EXTENSION timescaledb; CREATE EXTENSION postgis; CREATE EXTENSION postgis_ecef_eci;` — succeeds
- [ ] `CREATE TABLE ... (pos geometry(PointZ, 4978)); SELECT create_hypertable(...)` — succeeds
- [ ] INSERT ECEF point, SELECT back — data integrity
- [ ] `ST_3DDistance` query with GiST index on hypertable — correct results
- [ ] Compress chunk with geometry column — succeeds
- [ ] SELECT from compressed chunk — geometry data intact
- [ ] `ST_ECEF_To_ECI(pos, time)` on hypertable — correct conversion
- [ ] Continuous aggregate with `avg(x), avg(y), avg(z)` — refreshes correctly
- [ ] `ALTER EXTENSION postgis_ecef_eci UPDATE` — no data loss
- [ ] `ALTER EXTENSION timescaledb UPDATE` — no data loss
- [ ] Both extensions in same `pg_dump` / `pg_restore` cycle — succeeds

---

## Change Log

| Date | Version | Change | Author |
|------|---------|--------|--------|
| 2025-01-01 | 0.1.0-draft | Initial contract | TimescaleDB integration (claude branch) |
