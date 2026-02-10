# Master Task Checklist: PostGIS ECEF/ECI + TimescaleDB Integration

## Phase 0: Extension Compatibility (Gate)
> Spec: [specs/extension-compatibility/spec.md](specs/extension-compatibility/spec.md)

- [ ] Verify PostGIS fork loads alongside TimescaleDB without symbol conflicts
- [ ] Test planner hook chain with both extensions active
- [ ] Confirm SRID registration approach and reserve SRID range (900001–900003)
- [ ] Validate geometry type in hypertable creation, insertion, and querying
- [ ] Test compression round-trip with PostGIS geometry columns
- [ ] Test extension upgrade paths
- [ ] Establish CI test matrix (PG 15/16/17 x extension versions)
- [ ] Document installation order and prerequisites
- [ ] Decide on packaging: separate `postgis_ecef_eci` extension vs fork

**Exit criteria**: Both extensions load, a hypertable with geometry(PointZ, 4978)
can be created, inserted into, compressed, and queried successfully.

---

## Phase 1a: Schema & Partitioning
> Spec: [specs/schema-partitioning/spec.md](specs/schema-partitioning/spec.md)

- [ ] Finalize schema with PostGIS fork maintainers (SRID numbers, type names)
- [ ] Implement altitude-band partitioning function (Option A)
- [ ] Prototype octree partitioning function (Option B) for comparison
- [ ] Benchmark partitioning options with representative data distribution
- [ ] Choose partition count (8, 16, 32) based on analysis
- [ ] Decide trigger vs application-side bucket computation
- [ ] Test chunk exclusion effectiveness with spatial queries
- [ ] Document final schema for downstream consumers

**Exit criteria**: Hypertable created with time + spatial_bucket dimensions,
chunk exclusion demonstrated for altitude-band queries.

## Phase 1b: Frame Conversion (Parallel with 1a)
> Spec: [specs/frame-conversion/spec.md](specs/frame-conversion/spec.md)

- [ ] Confirm PostGIS fork conversion function signatures and SRIDs
- [ ] Validate PostgreSQL timestamp precision for target use cases
- [ ] Document safe vs unsafe aggregation patterns
- [ ] Design EOP data loading mechanism (table schema, refresh job)
- [ ] Benchmark frame conversion cost per row
- [ ] Test frame conversion within continuous aggregate refresh
- [ ] Quantify error bounds for midpoint-epoch aggregation approach

**Exit criteria**: ECEF<->ECI conversion works in SQL, aggregation rules documented,
EOP loading mechanism designed.

---

## Phase 2a: Compression (After Phase 1a)
> Spec: [specs/compression/spec.md](specs/compression/spec.md)

- [ ] Implement reference schema with compression settings
- [ ] Generate synthetic trajectory data (LEO, MEO, GEO)
- [ ] Benchmark Approach A (geometry + floats) compression ratio
- [ ] Benchmark Approach B (floats only, reconstruct geometry) compression ratio
- [ ] Benchmark Approach C (geometry only) compression ratio
- [ ] Measure query performance across approaches
- [ ] Measure ingest throughput with/without compression policy
- [ ] Test DML on compressed chunks
- [ ] Document recommended compression settings

**Exit criteria**: Compression approach chosen, documented with benchmarks,
compression policy configured.

## Phase 2b: Index Strategy (Parallel with 2a)
> Spec: [specs/index-strategy/spec.md](specs/index-strategy/spec.md)

- [ ] Implement index set A (object_id, time), C (GiST 3D), D (BRIN altitude), E (spatial_bucket)
- [ ] Benchmark Q1-Q5 query patterns with/without each index
- [ ] Measure index build time impact on ingest throughput
- [ ] Test GiST 3D index effectiveness for ECEF distance queries
- [ ] Evaluate SP-GiST vs GiST for point workloads
- [ ] Evaluate index size vs query speedup tradeoff
- [ ] Test `reorder_chunk()` impact on compression ratio
- [ ] Benchmark spatial queries on compressed vs uncompressed chunks

**Exit criteria**: Index set finalized, per-chunk overhead acceptable,
query patterns benchmarked.

---

## Phase 3: Continuous Aggregates (After Phase 1 + 2)
> Spec: [specs/continuous-aggregates/spec.md](specs/continuous-aggregates/spec.md)

- [ ] Implement Template 1: Per-object trajectory summary (hourly)
- [ ] Implement Template 2: Spatial density (per altitude band, hourly)
- [ ] Implement Template 3: Conjunction screening pre-filter (15-min)
- [ ] Implement Template 4: Daily orbital element summary
- [ ] Test Template 5: Hierarchical cagg (cagg-on-cagg)
- [ ] Validate cagg creation with PostGIS fork functions
- [ ] Benchmark refresh performance with representative data volumes
- [ ] Measure invalidation overhead for high-frequency ingest
- [ ] Document gapfill limitations for orbital trajectories
- [ ] Test real-time aggregation (materialized + recent)

**Exit criteria**: All templates working, refresh policies configured,
frame conversion rules documented for aggregate consumers.

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

### Items to Provide to PostGIS Fork
- [ ] TimescaleDB chunk constraint format
- [ ] Planner hook list and chaining requirements
- [ ] Compression behavior with geometry types (benchmarks)
- [ ] Continuous aggregate SQL constraints
- [ ] `add_job()` API documentation for EOP scheduling
- [ ] Per-chunk index overhead data
