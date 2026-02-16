# Interface Contract: TimescaleDB <-> PostGIS ECEF/ECI

## Purpose

This file defines the formal interface between the TimescaleDB integration work
(this repo) and the PostGIS ECEF/ECI fork (`montge/postgis`). It is the single
source of truth for what each side needs from and provides to the other.

**Both repos should contain a copy of this contract.** When the contract changes,
update both copies and note the change in a commit message prefixed with
`[contract]`.

## Version

Contract version: **0.4.0**
Last updated: 2026-02-15
Status: **All §8 gaps resolved — §8.3 closed with file-based EOP auto-refresh**

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

**Status**: [x] Confirmed — `SRID_ECEF=4978` in `postgis/lwgeom_ecef_eci.c:48`;
test tables use `geometry(PointZ, 4978)`; SRID 4978 in stock `spatial_ref_sys.sql`

### 1.2 ECI Point Type (ICRF — primary inertial frame)
```
Type:       geometry(PointZ, 900001)
SRID:       900001 (user-defined range — matches SRID_ECI_ICRF in liblwgeom.h)
Storage:    Standard GSERIALIZED / WKB (no frame metadata in binary)
Axes:       X (meters, vernal equinox), Y (meters, 90 degrees), Z (meters, north pole)
Frame:      ICRF (International Celestial Reference Frame)
Epoch:      Separate from geometry — passed as function parameter
```

**Status**: [x] Confirmed by PostGIS fork (SRID_ECI_ICRF=900001 in liblwgeom.h.in)

### 1.3 Additional ECI Frames
```
SRID 900002:  J2000 / EME2000 (Earth Mean Equator and Equinox of J2000.0)
SRID 900003:  TEME (True Equator Mean Equinox — SGP4 native)
```

> **SRID Reconciliation Note (2026-02-10):** The original TimescaleDB contract
> proposed SRID 900001=J2000. The PostGIS fork's C code defines
> `SRID_ECI_ICRF=900001`, `SRID_ECI_J2000=900002`, `SRID_ECI_TEME=900003`.
> PostGIS numbering is now canonical. ICRF is the IAU-recommended primary
> inertial frame; J2000 is a near-equivalent for most practical purposes.

**Status**: [x] Confirmed by PostGIS fork (SRID_ECI_J2000=900002, SRID_ECI_TEME=900003)

### 1.4 SRID Registration
PostGIS fork registers SRIDs in its install script via `INSERT INTO spatial_ref_sys`.
These must be present before any TimescaleDB hypertable uses the types.

**Status**: [x] Confirmed — `ecef_eci.sql.in:24-51` registers SRIDs 900001-900003
with `ON CONFLICT (srid) DO NOTHING` for idempotent re-install

---

## 2. Conversion Functions (PostGIS -> TimescaleDB)

### 2.1 ECEF to ECI
```sql
ST_ECEF_To_ECI(
    geom    geometry(PointZ, 4978),   -- ECEF input
    epoch   TIMESTAMPTZ,               -- conversion epoch (UTC)
    frame   TEXT DEFAULT 'ICRF'        -- target: 'ICRF', 'J2000', 'TEME'
) RETURNS geometry(PointZ, 900001)     -- ECI output (SRID matches frame)
```
- **Volatility**: STABLE (depends on EOP data, not on transaction state)
- **Parallel safety**: PARALLEL SAFE
- **Precision**: Sub-meter for LEO with current EOP; degrade spec TBD

**Status**: [x] Signature confirmed | [x] Implemented | [x] Tested
- Defined in `ecef_eci.sql.in:59-63` as `STABLE STRICT PARALLEL SAFE`
- C impl: `lwgeom_ecef_eci.c:103-154` — validates SRID 4978 input, sets output
  SRID dynamically (900001/900002/900003 based on frame)
- SQL return type is unqualified `geometry` (standard PostGIS convention)
- Regression tested in `regress/core/ecef_eci.sql`

### 2.2 ECI to ECEF
```sql
ST_ECI_To_ECEF(
    geom    geometry(PointZ, 900001),  -- ECI input
    epoch   TIMESTAMPTZ,
    frame   TEXT DEFAULT 'ICRF'
) RETURNS geometry(PointZ, 4978)       -- ECEF output
```
- **Volatility**: STABLE
- **Parallel safety**: PARALLEL SAFE

**Status**: [x] Signature confirmed | [x] Implemented | [x] Tested
- Defined in `ecef_eci.sql.in:67-71` as `STABLE STRICT PARALLEL SAFE`
- C impl: `lwgeom_ecef_eci.c:163-217` — validates input via `SRID_IS_ECI()`,
  cross-checks SRID/frame consistency (stricter than contract — rejects mismatches)
- Sets output SRID to 4978

### 2.3 Coordinate Accessors (for decomposed storage)
```sql
ST_ECEF_X(geom geometry) RETURNS FLOAT8   -- IMMUTABLE, PARALLEL SAFE
ST_ECEF_Y(geom geometry) RETURNS FLOAT8   -- IMMUTABLE, PARALLEL SAFE
ST_ECEF_Z(geom geometry) RETURNS FLOAT8   -- IMMUTABLE, PARALLEL SAFE
```
These are equivalent to `ST_X`, `ST_Y`, `ST_Z` but named explicitly for clarity.
They must be **IMMUTABLE** (not just STABLE) for use in continuous aggregate
definitions and index expressions.

**Status**: [x] Confirmed | [x] Implemented
- Defined in `ecef_eci.sql.in:142-160` as `IMMUTABLE STRICT PARALLEL SAFE`
- C impl: `lwgeom_ecef_eci.c:354-409` — validates POINT type + SRID 4978
- Uses `gserialized_peek_first_point()` for fast extraction

### 2.4 Distance Functions
```sql
ST_3DDistance(a geometry, b geometry) RETURNS FLOAT8
-- Already in stock PostGIS. Confirm it works with ECEF SRID 4978.
-- Returns Euclidean distance in meters (not geodesic — correct for ECEF).

ST_3DDWithin(a geometry, b geometry, distance FLOAT8) RETURNS BOOLEAN
-- Already in stock PostGIS. Confirm 3D support with SRID 4978.
```

**Status**: [x] Confirmed working with ECEF and ECI types
- `ST_3DDWithin` regression-tested with SRID 4978 (`ecef_eci.sql:447`)
  and SRID 900001 (`ecef_eci.sql:503`)
- Both use Cartesian 3D Euclidean — correct for ECEF/ECI (meters)
- SRID mismatch enforced: cross-SRID queries correctly raise errors

### 2.5 EOP-Enhanced Conversion Functions (not in original contract)
```sql
ST_ECEF_To_ECI_EOP(geom geometry, epoch TIMESTAMPTZ, frame TEXT DEFAULT 'ICRF')
    RETURNS geometry  -- STABLE STRICT PARALLEL SAFE
ST_ECI_To_ECEF_EOP(geom geometry, epoch TIMESTAMPTZ, frame TEXT DEFAULT 'ICRF')
    RETURNS geometry  -- STABLE STRICT PARALLEL SAFE
```
- SQL wrappers (`ecef_eci.sql.in:101-135`) that auto-lookup EOP parameters
  via `postgis_eop_interpolate()` and fall back to non-EOP functions when
  no EOP data is available
- Implements IERS 2003 convention: dut1 correction to ERA + Rx(yp)/Ry(xp)
  polar motion rotations

### 2.6 ST_Transform Epoch Overload (alternative path)
```sql
ST_Transform(geom geometry, to_srid INTEGER, epoch TIMESTAMPTZ)
    RETURNS geometry  -- STABLE STRICT PARALLEL SAFE
```
- Provides an alternative: `ST_Transform(ecef_geom, 900001, epoch)` is
  equivalent to `ST_ECEF_To_ECI(ecef_geom, epoch, 'ICRF')`
- Also supports M-coordinate epoch path: 2-arg `ST_Transform(geomM, srid)`
  uses per-point M values as decimal-year epochs
- Volatility now consistent with `ST_ECEF_To_ECI` (both STABLE)

---

## 3. Operator Classes (PostGIS -> TimescaleDB)

### 3.1 GiST 3D Index Support
```sql
CREATE INDEX ... USING gist (pos_ecef gist_geometry_ops_nd);
```
- Must support 3D bounding box operations on SRID 4978 and 900001+
- Used for within-chunk spatial indexing in TimescaleDB

**Status**: [x] Confirmed working with ECEF/ECI types
- Regression tests create GiST ND indexes on both SRID 4978 (`ecef_eci.sql:403`)
  and SRID 900001 (`ecef_eci.sql:477`)
- Index scan verified (not seq scan) for `&&&` and `ST_3DDWithin` queries
- No GiST C code modifications needed — operator class is SRID-agnostic
- ECI GBOX computed via `lwgeom_eci_compute_gbox()` using Cartesian (not geodetic)
  bounding boxes (`lwgeom_eci.c:494-510`)
- Benchmark script at `regress/core/ecef_gist_benchmark.sql` (10K-100K points)

### 3.2 Selectivity Estimators
PostGIS selectivity estimators (`gserialized_gist_sel`, etc.) should work
with ECEF/ECI types for accurate query planning across TimescaleDB chunks.

**Status**: [x] Confirmed — estimators are SRID-agnostic, operate on N-D
bounding box overlap ratios. No ECEF/ECI-specific code needed.

### 3.3 Geocentric Guards (not in original contract)
The PostGIS fork adds guards on 11+ spatial functions to prevent misuse
with geocentric (ECEF) types. Functions like `ST_Area`, `ST_Buffer`,
`ST_Centroid`, `ST_Perimeter`, `ST_Azimuth`, `ST_Project`, `ST_Segmentize`
raise `ERRCODE_FEATURE_NOT_SUPPORTED` for SRID 4978 input.

`ST_Distance`, `ST_Length`, `ST_DWithin` dispatch to 3D Euclidean
instead of erroring.

> **Resolved (§8.1)**: Guards now check both `LW_CRS_GEOCENTRIC` and
> `LW_CRS_INERTIAL`, blocking ECI input from geodetic functions as well.

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

**Status**: [x] Schema confirmed | [x] Load mechanism defined
- Defined in `ecef_eci.sql.in:166-173` — matches contract with additive
  CHECK constraints: `xp/yp BETWEEN -0.000278 AND 0.000278`, `dut1 BETWEEN -1.0 AND 1.0`
- `postgis_eop_load(data TEXT) RETURNS integer` — parses IERS Bulletin A
  fixed-width format, upserts via `ON CONFLICT (mjd) DO UPDATE`
- `postgis_eop_interpolate(epoch TIMESTAMPTZ) RETURNS TABLE(xp, yp, dut1, dx, dy)`
  — converts epoch to MJD, linear interpolation between bracketing rows
- CUnit tests in `cu_eci.c:827-957` (5 EOP-specific tests)
- Regression tests in `ecef_eci.sql:163-256`

### 4.2 EOP Refresh Job
TimescaleDB can schedule periodic EOP updates:
```sql
SELECT add_job('postgis_refresh_eop', schedule_interval => INTERVAL '1 day');
```
PostGIS fork must provide the `postgis_refresh_eop` procedure.

**Status**: [x] Procedure defined | [x] TimescaleDB-side refresh implemented
- PostGIS: `ecef_eci.sql.in:299-304` — placeholder (emits RAISE NOTICE);
  overload `postgis_refresh_eop(job_id INT, config JSONB)` added for `add_job()` compat
- TimescaleDB: `ecef_eci.refresh_eop(INT, JSONB)` — production-ready file-based
  refresh with error handling, staleness detection, and PostGIS sync
- External download: `scripts/fetch_iers_eop.sh` (cron/systemd timer)
- Setup convenience: `ecef_eci.setup_eop_refresh(file_path, interval, threshold)`

---

## 5. Extension Metadata (PostGIS -> TimescaleDB)

### 5.1 Extension Identity
```
Extension name:     postgis_ecef_eci
Depends on:         postgis  (no version floor — PG .control limitation)
Min PostgreSQL:     Not enforced in extension (PG12+ via PostGIS configure.ac)
Control file:       postgis_ecef_eci.control
Version:            3.7.0dev
```

**Status**: [x] Packaging approach confirmed
- Control file: `extensions/postgis_ecef_eci/postgis_ecef_eci.control.in`
- Makefile: `extensions/postgis_ecef_eci/Makefile.in` (PGXS, follows `postgis_sfcgal` pattern)
- Unconditionally built (no `ifeq` guard in `extensions/Makefile.in:34`)
- Install SQL: `postgis_ecef_eci--3.7.0dev.sql`
- Upgrade paths: `--ANY--3.7.0dev.sql`, `--unpackaged--3.7.0dev.sql`
- C functions compiled into main `postgis-3.so` (not a separate `.so`)

### 5.2 Planner/Executor Hooks
List any hooks the PostGIS fork registers (beyond stock PostGIS):
```
Hook name:          NONE
Chaining:           N/A
```

**Status**: [x] Confirmed — zero planner/executor/parse-analysis hooks
in entire fork. All `_PG_init` functions audited. Fork adds two GUCs
(Valkey batch workers, GPU dispatch threshold) but no hooks.

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

## 8. Remaining Gaps & Open Questions

These items surfaced during the 2026-02-15 audit and need decisions before
the integration can proceed to live testing.

### 8.1 ECI Geocentric Guards Gap
**Problem**: Spatial function guards (§3.3) block `LW_CRS_GEOCENTRIC` (SRID 4978)
from functions like `ST_Area`, `ST_Buffer`, `ST_Centroid`. But they do NOT block
`LW_CRS_INERTIAL` (SRID 900001+). These functions would produce equally
meaningless results on ECI input — `ST_Area` of an ECI polygon has no physical
meaning.

**Options**:
- (a) Broaden guards to `_not_geocentric_or_inertial` — safest
- (b) Leave as-is, document that ECI users must avoid geodetic functions — least work
- (c) Add ECI-specific guards only for the most dangerous functions

**Decision**: [x] Option (a) — `srid_check_crs_family_not_geocentric()` broadened
to check both `LW_CRS_GEOCENTRIC` and `LW_CRS_INERTIAL`. All 12 call sites
automatically get ECI blocking. No function-level changes needed.

### 8.2 EOP Refresh Procedure Signature
**Problem**: `postgis_refresh_eop()` takes zero arguments. TimescaleDB's `add_job()`
passes `(job_id INT, config JSONB)` to scheduled procedures.

**Options**:
- (a) PostGIS adds overload: `postgis_refresh_eop(job_id INT, config JSONB)`
- (b) TimescaleDB wraps it: `CREATE PROCEDURE _ts_eop_refresh(INT, JSONB) AS $$ CALL postgis_refresh_eop(); $$`
- (c) Both — PostGIS adds the overload, TimescaleDB uses it directly

**Decision**: [x] Option (a) — PostGIS added overload
`postgis_refresh_eop(job_id INT, config JSONB)`. Compatible with
TimescaleDB `add_job()` signature directly.

### 8.3 EOP Automatic Fetching
**Problem**: `postgis_refresh_eop()` is a placeholder (RAISE NOTICE only). It does
not actually fetch IERS Bulletin A data. Users must manually obtain the data and
call `postgis_eop_load(text)`.

**Options**:
- (a) PostGIS implements HTTP fetch via `pg_curl` or `http` extension dependency
- (b) PostGIS provides a shell script, TimescaleDB job calls it via `pg_cron`
- (c) Leave as manual process, document the workflow
- (d) TimescaleDB side implements the fetch in its own wrapper job

**Decision**: [x] Option (d) — File-based refresh architecture. External scheduler
(cron/systemd) downloads IERS Bulletin A via `scripts/fetch_iers_eop.sh`. TimescaleDB
background job (`ecef_eci.refresh_eop`) reads the local file via `pg_read_file()`,
loads into `ecef_eci.eop_data`, and syncs to PostGIS's `postgis_eop` table via
`ecef_eci.sync_eop_to_postgis()`. No HTTP extension dependency required.

Functions added:
- `ecef_eci.sync_eop_to_postgis() RETURNS INT` — upserts eop_data into postgis_eop
- `ecef_eci.eop_staleness(threshold_days INT) RETURNS TABLE` — gap_days, is_stale, load_age_hours
- `ecef_eci.setup_eop_refresh(file_path, interval, threshold) RETURNS INT` — convenience wrapper

**Note**: PostGIS's `postgis_eop_load()` parser has incorrect column offsets for
MJD, PM-x, and PM-y fields. Data flows exclusively through the TimescaleDB parser
(`load_eop_finals2000a`), avoiding this bug. The PostGIS parser should be fixed
separately.

### 8.4 ST_Transform Epoch Volatility Inconsistency
**Problem**: `ST_Transform(geom, srid, epoch)` is declared `IMMUTABLE` but
`ST_ECEF_To_ECI(geom, epoch, frame)` is `STABLE`. Both call the same underlying
`lwgeom_transform_ecef_to_eci()`. For the non-EOP ERA-only path, IMMUTABLE is
technically correct (pure math). But this inconsistency could cause confusion
for continuous aggregate usage.

**Impact**: If users discover that `ST_Transform` epoch overload works in cagg
definitions (because IMMUTABLE) while `ST_ECEF_To_ECI` doesn't (because STABLE),
they may use the wrong function to bypass the safety check.

**Options**:
- (a) Downgrade `ST_Transform` epoch overload to STABLE — consistent but limits cagg use
- (b) Upgrade `ST_ECEF_To_ECI` to IMMUTABLE for the non-EOP path — risky if EOP is added later
- (c) Document the difference and accept it

**Decision**: [x] Option (a) — `ST_Transform(geom, srid, epoch)` downgraded
from IMMUTABLE to STABLE. Consistent with `ST_ECEF_To_ECI` volatility.
Neither function can be used in continuous aggregate definitions; users
should use decomposed float columns for caggs instead.

### 8.5 Precession-Nutation Model
**Current state**: PostGIS implements IERS 2003 simplified model for ECI transforms.
Full IERS 2006/2000A precession-nutation is documented as deferred.

**Impact on TimescaleDB**: For LEO conjunction screening at sub-kilometer accuracy,
the simplified model is sufficient. For high-precision GEO operations, the full
model may be needed.

**Decision**: [x] Acceptable for Phase 1 — revisit if precision requirements tighten

---

## Change Log

| Date | Version | Change | Author |
|------|---------|--------|--------|
| 2025-01-01 | 0.1.0-draft | Initial contract | TimescaleDB integration (claude branch) |
| 2026-02-10 | 0.2.0-draft | Align SRID numbering with PostGIS fork: ICRF=900001, J2000=900002, TEME=900003; default frame 'ICRF' | PostGIS openspec creation |
| 2026-02-15 | 0.3.0 | Audit PostGIS `develop` branch: confirm all §1-5 deliverables; add §2.5 EOP-enhanced functions, §2.6 ST_Transform epoch overload, §3.3 geocentric guards; document 5 open gaps in §8 | Audit against commit 00bc1009d |
| 2026-02-15 | 0.3.1 | Resolve §8.1 (ECI guards broadened), §8.2 (EOP refresh overload added), §8.4 (ST_Transform epoch downgraded to STABLE), §8.5 (precession-nutation accepted for Phase 1); update §2.6 volatility to STABLE; §8.3 remains open | Contract alignment fixes |
| 2026-02-15 | 0.4.0 | Resolve §8.3: file-based EOP auto-refresh — add sync_eop_to_postgis(), eop_staleness(), setup_eop_refresh(); external download script; all §8 gaps now closed | EOP auto-refresh implementation |
