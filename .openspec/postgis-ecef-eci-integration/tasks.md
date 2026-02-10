# Master Task Checklist: PostGIS ECEF/ECI + TimescaleDB Integration

## Dependency Key

- Tasks with no tag: can proceed immediately with stock PostGIS
- `[BLOCKED:postgis]`: requires PostGIS fork deliverable
- `[BLOCKED:phase-N]`: requires earlier phase completion

---

## Phase 0: Foundation (Can Start Now)

### 0a: Stock PostGIS + TimescaleDB Validation (No fork needed)
> Use stock PostGIS `geometry(PointZ, 4978)` as stand-in for ECEF

- [x] Validate `geometry(PointZ, 4978)` in hypertable creation, insertion, querying — `test/sql/postgis_ecef_eci/schema_hypertable.sql`
- [x] Test compression round-trip with PostGIS geometry columns — `test/sql/postgis_ecef_eci/schema_hypertable.sql` Test 5
- [ ] Test GiST 3D index (`gist_geometry_ops_nd`) on hypertable chunks — needs running PG instance
- [x] Document TimescaleDB planner hook list and chaining requirements — `interface-contract.md` §6
- [x] Document compression behavior with geometry types — `interface-contract.md` §6.2
- [x] Document continuous aggregate SQL constraints (IMMUTABLE requirement) — `interface-contract.md` §6.3, `AGGREGATION_RULES.md`

**Exit criteria**: Stock PostGIS geometry works in hypertables end-to-end.
This validates the TimescaleDB side independently.

### 0b: Fork Extension Compatibility `[BLOCKED:postgis]`
> Requires PostGIS fork to exist with SRID registration

- [ ] `[BLOCKED:postgis]` Verify PostGIS fork loads alongside TimescaleDB without symbol conflicts
- [ ] `[BLOCKED:postgis]` Test planner hook chain with both extensions active
- [ ] `[BLOCKED:postgis]` Confirm SRID registration (900001-900003) works
- [ ] `[BLOCKED:postgis]` Test extension upgrade paths
- [ ] `[BLOCKED:postgis]` Decide on packaging: separate `postgis_ecef_eci` extension vs fork
- [ ] Establish CI test matrix (PG 15/16/17 x extension versions)

**Exit criteria**: Both extensions load, ECI SRIDs registered, no conflicts.

---

## Phase 1a: Schema & Partitioning (Can Start Now)
> Spec: [specs/schema-partitioning/spec.md](specs/schema-partitioning/spec.md)

- [x] Implement altitude-band partitioning function (Option A) — `sql/postgis_ecef_eci/partitioning.sql`
- [x] Prototype octree partitioning function (Option B) for comparison — `sql/postgis_ecef_eci/partitioning.sql` (2-level octree)
- [x] Create reference schema with stock PostGIS geometry(PointZ, 4978) — `sql/postgis_ecef_eci/schema.sql`
- [ ] Benchmark partitioning options with synthetic orbit data distribution — needs running PG instance
- [ ] Choose partition count (8, 16, 32) based on analysis
- [x] Decide trigger vs application-side bucket computation — trigger with WHEN NULL guard, app-side for high throughput
- [ ] Test chunk exclusion effectiveness with spatial queries — needs running PG instance
- [x] Document final schema for downstream consumers — `sql/postgis_ecef_eci/schema.sql`
- [ ] `[BLOCKED:postgis]` Finalize schema with PostGIS fork maintainers (SRID numbers, type names)

**Exit criteria**: Hypertable created with time + spatial_bucket dimensions,
chunk exclusion demonstrated for altitude-band queries.

## Phase 1b: Frame Conversion Design (Partial — Design Now, Test Later)
> Spec: [specs/frame-conversion/spec.md](specs/frame-conversion/spec.md)

Can do now:
- [x] Validate PostgreSQL timestamp precision for target use cases — `sql/postgis_ecef_eci/TIMESTAMP_PRECISION.md`
- [x] Document safe vs unsafe aggregation patterns for frame conversion — `sql/postgis_ecef_eci/AGGREGATION_RULES.md`
- [x] Design EOP data loading mechanism (table schema, refresh job) — `sql/postgis_ecef_eci/eop.sql`
- [x] Quantify error bounds for midpoint-epoch aggregation approach — `sql/postgis_ecef_eci/AGGREGATION_RULES.md`
- [x] Write stub `ST_ECEF_To_ECI()` PL/pgSQL function for testing (simplified rotation) — `sql/postgis_ecef_eci/frame_conversion_stubs.sql`

Blocked:
- [ ] `[BLOCKED:postgis]` Confirm PostGIS fork conversion function signatures and SRIDs
- [ ] `[BLOCKED:postgis]` Benchmark actual fork frame conversion cost per row
- [ ] `[BLOCKED:postgis]` Test frame conversion within continuous aggregate refresh

**Exit criteria**: Aggregation rules documented, EOP mechanism designed,
stub function available for integration testing.

---

## Phase 2a: Compression (Can Start After 1a)
> Spec: [specs/compression/spec.md](specs/compression/spec.md)

- [x] ~~`[BLOCKED:phase-1a]`~~ Implement reference schema with compression settings — `sql/postgis_ecef_eci/schema.sql`
- [x] ~~`[BLOCKED:phase-1a]`~~ Generate synthetic trajectory data (LEO, MEO, GEO) — `sql/postgis_ecef_eci/test_data_generator.sql`
- [ ] Benchmark Approach A (geometry + floats) compression ratio — needs running PG instance; test ready at `test/sql/postgis_ecef_eci/compression_benchmark.sql`
- [ ] Benchmark Approach B (floats only, reconstruct geometry) compression ratio — schema at `sql/postgis_ecef_eci/schema_variants.sql`
- [ ] Benchmark Approach C (geometry only) compression ratio — schema at `sql/postgis_ecef_eci/schema_variants.sql`
- [ ] `[BLOCKED:phase-1a]` Measure query performance across approaches
- [ ] `[BLOCKED:phase-1a]` Measure ingest throughput with/without compression policy
- [ ] `[BLOCKED:phase-1a]` Test DML on compressed chunks
- [ ] `[BLOCKED:phase-1a]` Document recommended compression settings
- [ ] `[BLOCKED:postgis]` Test compression with fork-specific ECEF/ECI types (if WKB differs)

**Exit criteria**: Compression approach chosen, documented with benchmarks.

## Phase 2b: Index Strategy (Can Start After 1a)
> Spec: [specs/index-strategy/spec.md](specs/index-strategy/spec.md)

- [x] ~~`[BLOCKED:phase-1a]`~~ Implement index set A (object_id, time), C (GiST 3D), D (BRIN altitude), E (spatial_bucket) — `sql/postgis_ecef_eci/schema.sql` (A, D, E created; C deferred pending PostGIS geometry column decision)
- [ ] `[BLOCKED:phase-1a]` Benchmark Q1-Q5 query patterns with/without each index
- [ ] `[BLOCKED:phase-1a]` Measure index build time impact on ingest throughput
- [ ] `[BLOCKED:phase-1a]` Test GiST 3D index effectiveness for ECEF distance queries
- [ ] `[BLOCKED:phase-1a]` Evaluate SP-GiST vs GiST for point workloads
- [ ] `[BLOCKED:phase-1a]` Evaluate index size vs query speedup tradeoff
- [ ] `[BLOCKED:phase-1a]` Test `reorder_chunk()` impact on compression ratio
- [ ] `[BLOCKED:phase-1a]` Benchmark spatial queries on compressed vs uncompressed chunks
- [ ] `[BLOCKED:postgis]` Confirm `gist_geometry_ops_nd` with fork ECEF/ECI types

**Exit criteria**: Index set finalized, per-chunk overhead acceptable.

---

## Phase 3: Continuous Aggregates (After Phase 1 + 2)
> Spec: [specs/continuous-aggregates/spec.md](specs/continuous-aggregates/spec.md)

- [ ] `[BLOCKED:phase-1a,2a]` Implement Template 1: Per-object trajectory summary (hourly)
- [ ] `[BLOCKED:phase-1a,2a]` Implement Template 2: Spatial density (per altitude band, hourly)
- [ ] `[BLOCKED:phase-1a,2a]` Implement Template 3: Conjunction screening pre-filter (15-min)
- [ ] `[BLOCKED:phase-1a,2a]` Implement Template 4: Daily orbital element summary
- [ ] `[BLOCKED:phase-1a,2a]` Test Template 5: Hierarchical cagg (cagg-on-cagg)
- [ ] `[BLOCKED:phase-1a,2a]` Benchmark refresh performance with representative data volumes
- [ ] `[BLOCKED:phase-1a,2a]` Measure invalidation overhead for high-frequency ingest
- [ ] `[BLOCKED:phase-1a,2a]` Document gapfill limitations for orbital trajectories
- [ ] `[BLOCKED:phase-1a,2a]` Test real-time aggregation (materialized + recent)
- [ ] `[BLOCKED:postgis]` Validate cagg creation with PostGIS fork functions (IMMUTABLE check)

**Exit criteria**: All templates working, refresh policies configured.

---

## Cross-Cutting Coordination

### Items Needed from PostGIS Fork
- [ ] SRID assignments for ECI frames (J2000, GCRF, TEME)
- [ ] `ST_ECEF_To_ECI()` / `ST_ECI_To_ECEF()` function signatures
- [ ] Canonical ECEF type confirmation (geometry(PointZ, 4978) or new subtype?)
- [ ] WKB byte layout for ECEF/ECI (standard PointZ WKB or frame metadata?)
- [ ] GSERIALIZED format changes (if any)
- [ ] Planner/executor hooks list (if any)
- [ ] Decomposed PointZ storage feasibility (for compression optimization)
- [ ] IMMUTABLE spatial aggregate functions list
- [ ] `gist_geometry_ops_nd` compatibility with ECEF PointZ
- [ ] EOP data storage approach
- [ ] Minimum PostgreSQL version requirement

### Items to Provide to PostGIS Fork (Can Produce Now)
- [x] TimescaleDB chunk constraint format — documented in interface-contract.md
- [x] Planner hook list and chaining requirements — documented in interface-contract.md
- [ ] Compression behavior with geometry types (benchmarks) — Phase 2a output
- [x] Continuous aggregate SQL constraints — documented in interface-contract.md
- [x] `add_job()` API documentation for EOP scheduling — documented in interface-contract.md
- [ ] Per-chunk index overhead data — Phase 2b output
