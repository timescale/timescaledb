# Spec 06: Extension Compatibility Layer

## Status: Draft (Phase 0 — Gate for All Other Work)

## Overview

Ensure clean co-loading and interoperation of TimescaleDB and the PostGIS ECEF/ECI
fork within the same PostgreSQL instance. This is the foundational work that must
be validated before any other spec proceeds.

## Context

Both TimescaleDB and PostGIS are PostgreSQL extensions that hook into the server's
planner, executor, and type system. They must coexist without:
- Symbol conflicts in shared libraries
- Planner hook chain breakage
- Type OID collisions
- Catalog corruption
- Extension dependency ordering issues

### Key Source References (TimescaleDB)
- `src/loader/` — Multi-version loader (loads `timescaledb-<version>.so`)
- `src/init.c` — `_PG_init()` hook registration
- `src/extension.h` — Extension state tracking
- `src/planner/planner.h` — Planner hook chain
- `src/process_utility.h` — Process utility hook
- `src/cross_module_fn.h` — Cross-module function dispatch
- `timescaledb.control.in` — Extension metadata

### PostGIS Loading
PostGIS registers:
- Custom types (geometry, geography, raster, etc.)
- GiST/SP-GiST operator classes
- Planner support functions (selectivity estimators)
- Type input/output functions
- Potentially: executor hooks for parallel query support

The ECEF/ECI fork likely adds:
- New SRIDs in `spatial_ref_sys`
- New or modified transformation functions
- Possibly new type modifiers or subtypes

## Design

### 1. Extension Dependency Declaration

PostgreSQL extensions can declare dependencies:

```sql
-- In the PostGIS fork's control file or install script:
-- No dependency on TimescaleDB (they're independent extensions)

-- In application setup SQL:
CREATE EXTENSION postgis;           -- Must be first (types needed)
CREATE EXTENSION timescaledb;       -- Second (uses PostGIS types)
-- Or:
CREATE EXTENSION timescaledb CASCADE;  -- Auto-creates dependencies
```

**Critical**: PostGIS types must be registered before any hypertable uses them.
TimescaleDB does not depend on PostGIS at the extension level, but the user's
schema does.

### 2. Planner Hook Chain

TimescaleDB hooks into the planner at multiple points (`src/init.c`):

```c
// Hooks registered in _PG_init():
planner_hook
post_parse_analyze_hook
set_rel_pathlist_hook
create_upper_paths_hook
process_utility_hook
executor_start_hook
executor_end_hook
```

PostGIS registers:
```c
// Selectivity estimators (not hooks, but registered functions)
// No known planner_hook or executor_hook in stock PostGIS
```

**Risk**: If the ECEF/ECI fork adds planner hooks (e.g., for spatial query
optimization), it must chain properly with TimescaleDB's hooks. Both extensions
should save and call the previous hook value:

```c
// Correct hook chaining:
static planner_hook_type prev_planner_hook = NULL;

void _PG_init(void) {
    prev_planner_hook = planner_hook;
    planner_hook = my_planner_hook;
}

PlannedStmt *my_planner_hook(Query *parse, ...) {
    // My processing...
    if (prev_planner_hook)
        return prev_planner_hook(parse, ...);
    else
        return standard_planner(parse, ...);
}
```

### 3. SRID Registration

The PostGIS fork needs to register custom SRIDs for:

| Frame | SRID | Authority | Notes |
|-------|------|-----------|-------|
| WGS84 ECEF | 4978 | EPSG | Already in stock PostGIS |
| ECI J2000 | TBD | Custom | No EPSG code exists |
| ECI GCRF | TBD | Custom | No EPSG code exists |
| ECI TEME | TBD | Custom | No EPSG code exists |

**Recommendations**:
- Use SRID range 900000-999999 (PostGIS convention for user-defined SRIDs)
- Register SRIDs in PostGIS fork's install script (`postgis_ecef_eci--1.0.sql`)
- Ensure SRIDs are present before `CREATE TABLE` with geometry columns

```sql
-- Example SRID registration (in PostGIS fork install script)
INSERT INTO spatial_ref_sys (srid, auth_name, auth_srid, srtext, proj4text)
VALUES (
    900001,
    'CUSTOM',
    900001,
    'GEOCCS["ECI_J2000", ...]',
    '+proj=geocent +datum=WGS84 +units=m +no_defs'  -- approximate
);
```

### 4. Shared Library Symbol Conflicts

TimescaleDB's loader (`src/loader/`) uses versioned library names
(`timescaledb-2.x.y.so`) to avoid conflicts. PostGIS uses `postgis-3.so`.

**Check for**:
- No duplicate symbol names between libraries
- No conflicting PostgreSQL catalog entries
- Both libraries compatible with the same PostgreSQL major version

**Test procedure**:
```bash
# Check for symbol conflicts
nm -D postgis-3.so | awk '{print $3}' | sort > postgis_symbols.txt
nm -D timescaledb-2.x.y.so | awk '{print $3}' | sort > tsdb_symbols.txt
comm -12 postgis_symbols.txt tsdb_symbols.txt
# Should be empty (no shared symbols except standard libc/PG ones)
```

### 5. Type System Interaction

TimescaleDB needs to handle PostGIS types in:

| Component | Interaction | Risk |
|-----------|------------|------|
| Dimension partitioning | Hash PostGIS types for closed dimensions | Low (use scalar spatial_bucket instead) |
| Compression | Compress geometry columns | Medium (falls back to Array — OK) |
| Chunk constraint | Range exclusion on geometry | N/A (geometry not a dimension column) |
| Continuous aggregates | Aggregate geometry values | Medium (need IMMUTABLE PostGIS functions) |
| Vectorized execution | Process geometry in SIMD paths | Low (falls back to scalar — OK) |

### 6. Extension Upgrade Path

Both extensions have their own upgrade mechanisms:
- TimescaleDB: `ALTER EXTENSION timescaledb UPDATE TO '2.x.y'`
- PostGIS: `ALTER EXTENSION postgis UPDATE TO '3.x.y'`

**Concern**: If the PostGIS fork modifies type definitions or SRIDs, upgrades must
not break existing hypertable chunks that contain those types.

**Recommendation**: The PostGIS fork should version its ECEF/ECI additions as a
separate extension (`postgis_ecef_eci`) that depends on `postgis`:

```
postgis (stock)
    └── postgis_ecef_eci (fork additions)
            └── (user schema uses both + timescaledb)
```

This allows independent upgrades and clear separation of concerns.

### 7. Testing Matrix

| PostgreSQL | TimescaleDB | PostGIS | PostGIS ECEF/ECI | Status |
|-----------|-------------|---------|------------------|--------|
| 15 | 2.14.x | 3.4.x | 1.0.0 | TBD |
| 16 | 2.14.x | 3.4.x | 1.0.0 | TBD |
| 17 | 2.14.x | 3.5.x | 1.0.0 | TBD |

### 8. Installation Order Validation

```sql
-- Test script: validate clean installation
\c postgres
CREATE DATABASE test_compat;
\c test_compat

-- Step 1: PostGIS base
CREATE EXTENSION postgis;
SELECT postgis_full_version();

-- Step 2: ECEF/ECI additions
CREATE EXTENSION postgis_ecef_eci;  -- or however the fork packages it
SELECT srid, auth_name FROM spatial_ref_sys WHERE srid >= 900000;

-- Step 3: TimescaleDB
CREATE EXTENSION timescaledb;
SELECT extversion FROM pg_extension WHERE extname = 'timescaledb';

-- Step 4: Create hypertable with ECEF geometry
CREATE TABLE test_traj (
    time TIMESTAMPTZ NOT NULL,
    pos geometry(PointZ, 4978)
);
SELECT create_hypertable('test_traj', 'time');

-- Step 5: Insert and query
INSERT INTO test_traj VALUES (now(), ST_MakePoint(6378137, 0, 0)::geometry(PointZ, 4978));
SELECT * FROM test_traj;

-- Step 6: Compress
ALTER TABLE test_traj SET (timescaledb.compress, timescaledb.compress_orderby = 'time');
SELECT compress_chunk(c) FROM show_chunks('test_traj') c;
SELECT * FROM test_traj;  -- verify data survives compression round-trip

-- Cleanup
DROP TABLE test_traj;
DROP EXTENSION timescaledb;
DROP EXTENSION postgis_ecef_eci;
DROP EXTENSION postgis CASCADE;
```

## Tasks

- [ ] Verify PostGIS fork loads alongside TimescaleDB without symbol conflicts
- [ ] Test planner hook chain with both extensions active
- [ ] Confirm SRID registration approach and reserve SRID range
- [ ] Validate geometry type in hypertable creation, insertion, and querying
- [ ] Test compression round-trip with PostGIS geometry columns
- [ ] Test extension upgrade paths (both directions)
- [ ] Establish CI test matrix (PG versions x extension versions)
- [ ] Document installation order and prerequisites
- [ ] Decide on packaging: separate `postgis_ecef_eci` extension vs fork of `postgis`

## Open Questions

1. Will the PostGIS ECEF/ECI additions be packaged as a separate extension or
   as a modified `postgis` extension? (Separate is strongly preferred.)
2. Does the fork add any planner or executor hooks? (If so, hook chaining is critical.)
3. What is the minimum PostGIS version the fork requires?
4. Will the fork need to modify any PostGIS internal types (e.g., GSERIALIZED format)?
5. How will EOP (Earth Orientation Parameter) data be distributed with the extension?

## Coordination Points (PostGIS Fork)

- **Need from PostGIS**: Extension packaging decision (separate vs modified postgis)
- **Need from PostGIS**: List of hooks registered (if any)
- **Need from PostGIS**: SRID range and registration mechanism
- **Need from PostGIS**: GSERIALIZED format changes (if any)
- **Need from PostGIS**: Minimum PostgreSQL version requirement
- **Provide to PostGIS**: TimescaleDB hook list and chaining requirements
- **Provide to PostGIS**: Compression behavior with geometry types
- **Provide to PostGIS**: Chunk constraint format and planner interaction points
