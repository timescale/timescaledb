# Spec 07: Testing Coverage & Security

## Status: Draft

## Overview

Establish comprehensive test coverage and security scanning for the PostGIS ECEF/ECI
integration, targeting 90% overall coverage and 80%+ per test category. Leverage
TimescaleDB's existing CI infrastructure (46 workflows) rather than building parallel
tooling.

## Context

### Existing TimescaleDB QA Infrastructure

| Tool | What It Does | Applies to Our SQL? |
|------|-------------|---------------------|
| **pg_regress** | SQL regression tests | Yes — our primary test harness |
| **pg_isolation_regress** | Concurrent transaction tests | Yes — trajectory inserts + compression |
| **pgspot** | SQL security best practices (search_path, etc.) | Yes — must pass for all our functions |
| **Coverity** | C static analysis | No — we have no C code |
| **Coccinelle** | C defect patterns | No — we have no C code |
| **clang-format** | C/SQL formatting | Yes — SQL files |
| **codespell** | Spelling | Yes — all files |
| **SQLsmith** | SQL fuzzing | Indirectly — our tables/functions get fuzzed if loaded |
| **lcov/gcov** | C code coverage | No — we need SQL-level coverage tracking |
| **pgspot** | SQL security scanning | Yes — critical |

### What's Missing for Our Integration

1. **No Dependabot** — TimescaleDB doesn't use it (C project, no package.json/requirements.txt)
2. **No CodeQL** — TimescaleDB uses Coverity instead; CodeQL could add value for SQL injection patterns
3. **No SQL-specific coverage tool** — pg_regress doesn't report line coverage; we track by test-to-feature mapping
4. **No isolation tests** for spatial partitioning concurrent access
5. **No pgspot run** on our `sql/postgis_ecef_eci/` files specifically

## Design

### Test Categories & Coverage Targets

```
Category          | Target | Current | Gap
------------------|--------|---------|----
Unit tests        |  80%+  |  ~60%   | EOP interpolation edge cases, octree boundary cases
Branch coverage   |  80%+  |  ~50%   | Partitioning function CASE branches, NULL handling
Integration tests |  80%+  |  ~40%   | Hypertable + compression + cagg pipeline
End-to-end tests  |  80%+  |  ~30%   | Full ingest->compress->query->cagg workflow
Isolation tests   |  80%+  |   0%    | Concurrent inserts, compress during query
Security scanning |  pass  |  none   | pgspot, search_path, SQL injection patterns
Overall           |  90%   |  ~45%   | Need all categories above
```

### 1. Unit Tests (Function-Level)

Test each SQL function in isolation with edge cases.

| Function | Tests Needed |
|----------|-------------|
| `altitude_band_bucket()` | All 16 bucket boundaries, negative altitude, zero, exact boundaries, NaN/Inf |
| `octree_bucket()` | All 8 octants at L1, L2 subdivision, boundary points, origin, extreme values |
| `ecef_altitude_km()` | Zero vector, single-axis, off-axis, very large values |
| `altitude_band_label()` | All 16 labels, out-of-range input |
| `stub_ecef_to_eci()` | J2000 epoch, known rotation angles, Z-axis invariance |
| `stub_eci_to_ecef()` | Inverse of above |
| `test_roundtrip()` | Various epochs, tolerance edge cases |
| `mjd_to_timestamp()` | Known dates, fractional MJD, epoch boundary |
| `timestamp_to_mjd()` | Inverse, round-trip |
| `eop_at_epoch()` | Exact entry, midpoint, outside range, single entry, empty table |
| `load_eop_finals2000a()` | Valid data, malformed lines, empty input, partial lines |
| `generate_circular_orbit()` | Various altitudes/inclinations, zero duration, single point |
| `make_ecef_point()` | PostGIS dependency — only with PostGIS loaded |

### 2. Branch Coverage

Every CASE/WHEN path in partitioning functions must be exercised:

```sql
-- altitude_band_bucket: 17 branches (16 buckets + ELSE)
-- Each needs at least one test point INSIDE the band
-- and one test point AT the exact boundary (boundary goes to the next bucket)

-- octree_bucket: 8 octants * 2 levels = 16 sub-octants
-- Need test points in each octant, plus boundary points on axes
```

### 3. Integration Tests

Test component interactions across TimescaleDB features:

| Test | Components |
|------|-----------|
| Insert + auto-partition | Trigger → bucket computation → chunk routing |
| Insert + compression | Data in → compress → query → decompress → verify |
| Insert + cagg refresh | Data in → cagg definition → refresh → query materialized |
| Compression + decompression + DML | Compress → update → recompress → verify |
| EOP load + frame conversion | Load EOP → convert ECEF→ECI → verify against known values |
| Schema variant comparison | Load same data into A/B/C → compress → compare sizes |
| Multi-object trajectory | 100+ objects across regimes → partition distribution |

### 4. End-to-End Tests

Full pipeline scenarios that mirror real-world usage:

| Scenario | Steps |
|----------|-------|
| SSA ingest pipeline | Generate 1000 objects × 2h data → insert → compress → create caggs → query dashboard views |
| Conjunction screening | Generate crossing orbits → insert → query conjunction_prefilter cagg → identify close approaches |
| EOP-aware conversion | Load real-format EOP data → insert trajectory → convert to ECI → verify epoch dependence |
| Retention + recompression | Insert → compress → add retention policy → drop old chunks → verify data integrity |
| Upgrade simulation | Create schema v1 → insert data → "upgrade" schema → verify backward compatibility |

### 5. Isolation Tests (Concurrent Access)

Using `pg_isolation_regress` format:

| Test | Sessions | Scenario |
|------|----------|----------|
| Concurrent trajectory insert | 2 inserters | Both insert to same chunk simultaneously |
| Insert during compression | 1 inserter + 1 compressor | Insert while chunk being compressed |
| Query during compression | 1 querier + 1 compressor | Read while chunk transitions compressed→uncompressed |
| Concurrent cagg refresh + insert | 1 inserter + 1 refresher | Insert while cagg refresh runs |
| Concurrent spatial bucket inserts | 4 inserters | Each inserts to different spatial bucket |

### 6. Security Scanning

#### pgspot
Run pgspot on all `sql/postgis_ecef_eci/*.sql` files:
- Verify explicit `search_path` on all functions (or document intentional omissions)
- Check for SQL injection vectors in dynamic SQL (e.g., `load_eop_finals2000a`)
- Validate transaction control in procedures

#### SQL Injection Audit
Functions that accept text input need review:
- `load_eop_finals2000a(raw_text TEXT)` — parses user-provided text; uses `substring()` not `EXECUTE`, so safe
- `altitude_band_label(bucket INT)` — integer input only, safe
- No `EXECUTE format()` patterns — no injection risk

#### search_path Security
All functions should use `SET search_path` or be schema-qualified:
```sql
CREATE FUNCTION ecef_eci.my_func(...)
RETURNS ...
SET search_path = ecef_eci, pg_catalog  -- explicit search_path
AS $$ ... $$;
```

### 7. Lint & Formatting

| Tool | Scope | Config |
|------|-------|--------|
| `clang-format-14` | `.sql` files in `sql/postgis_ecef_eci/` | Existing `.clang-format` |
| `codespell` | All files | Existing `.github/codespell-ignore-words` |
| `yamllint` | `config.yaml` | Existing `.yamllint.yaml` |
| `pgspot` | All SQL files | New: specific opts for ecef_eci schema |

### 8. Dependabot & CodeQL

**Dependabot**: Not applicable — our integration is pure SQL with no external
package dependencies. If we add a Python benchmark harness or Node.js tooling later,
add `dependabot.yml` at that point.

**CodeQL**: Consider adding for the PostGIS fork (which has C code), not needed
for this repo's SQL-only integration. If the integration later adds C helper
functions, add CodeQL then.

### 9. Coverage Tracking Approach

Since we have no C code, `lcov`/`gcov` don't apply. Track coverage via a
**test-to-feature matrix** maintained in this spec:

```
Feature                        | Unit | Branch | Integration | E2E | Isolation
-------------------------------|------|--------|-------------|-----|----------
altitude_band_bucket()         | [x]  | [ ]    | [x]         | [ ] | n/a
octree_bucket()                | [ ]  | [ ]    | [ ]         | [ ] | n/a
ecef_altitude_km()             | [x]  | [x]    | [x]         | [ ] | n/a
schema creation                | n/a  | n/a    | [x]         | [ ] | n/a
hypertable partitioning        | n/a  | n/a    | [x]         | [ ] | [ ]
compression round-trip         | n/a  | n/a    | [x]         | [ ] | [ ]
continuous aggregates          | n/a  | n/a    | [x]         | [ ] | [ ]
frame conversion stubs         | [x]  | [ ]    | [x]         | [ ] | n/a
EOP loading                    | [ ]  | [ ]    | [ ]         | [ ] | n/a
EOP interpolation              | [ ]  | [ ]    | [ ]         | [ ] | n/a
data generator                 | [x]  | n/a    | [x]         | [ ] | n/a
schema variants (A/B/C)        | n/a  | n/a    | [ ]         | [ ] | n/a
```

## Tasks

- [ ] Add `SET search_path` to all ecef_eci functions (pgspot compliance)
- [ ] Write comprehensive unit tests for all partition boundary conditions
- [ ] Write octree bucket unit tests
- [ ] Write EOP edge case tests (empty table, single entry, out of range)
- [ ] Write isolation tests (concurrent insert, insert+compress, query+compress)
- [ ] Write end-to-end SSA pipeline test
- [ ] Write end-to-end conjunction screening test
- [ ] Run pgspot on `sql/postgis_ecef_eci/*.sql` and fix findings
- [ ] Add codespell check to CI for our files
- [ ] Create CI workflow or extend existing for postgis_ecef_eci tests
- [ ] Update test-to-feature coverage matrix as tests are added
- [ ] Document security audit findings

## Open Questions

1. Should we add a `pgspot` step to the existing workflow or create a separate one?
   (Recommend: extend existing, since pgspot already runs on the main SQL.)
2. Do we need fuzz testing for the partitioning functions? (SQLsmith would exercise
   them if the schema is loaded, but targeted fuzzing with random ECEF coordinates
   could find edge cases.)
3. Should we track coverage percentage numerically or is the feature matrix sufficient?
