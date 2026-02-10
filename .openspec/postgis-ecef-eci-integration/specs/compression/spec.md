# Spec 02: Compression Profile

## Status: Draft

## Overview

Determine optimal TimescaleDB compression settings for ECEF/ECI trajectory data,
balancing compression ratio, ingest speed, and query performance for spatial time-series
workloads.

## Context

TimescaleDB's columnar compression (`tsl/src/compression/`) applies per-column algorithms:

| Algorithm | Type | Best For |
|-----------|------|----------|
| DeltaDelta | Integer/Timestamp | Monotonic sequences, fixed intervals |
| Gorilla | Float | Smooth, slowly-changing values |
| Dictionary | Any | Low cardinality (< ~1000 distinct values) |
| Array | Any | Fallback — relies on TOAST/LZ |

### Key Source References (TimescaleDB)
- `tsl/src/compression/compression.h` — Compression API
- `tsl/src/compression/algorithms/gorilla.h` — Gorilla float compression
- `tsl/src/compression/algorithms/deltadelta.h` — DeltaDelta integer compression
- `tsl/src/compression/create.c` — Compression configuration
- `tsl/src/compression/compression_dml.c` — DML on compressed chunks

## Design

### 1. Column-Level Compression Strategy

```
Column          | Type       | Algorithm    | Rationale
----------------|------------|--------------|------------------------------------------
time            | TIMESTAMPTZ| DeltaDelta   | Regular sampling intervals
object_id       | INT        | Dictionary   | Repeated within segment (SEGMENT BY)
object_type     | SMALLINT   | Dictionary   | Low cardinality enum
frame           | SMALLINT   | Dictionary   | Typically constant per segment
x               | FLOAT8     | Gorilla      | Smooth orbital/trajectory curves
y               | FLOAT8     | Gorilla      | Smooth orbital/trajectory curves
z               | FLOAT8     | Gorilla      | Smooth orbital/trajectory curves
vx              | FLOAT8     | Gorilla      | Smooth velocity changes
vy              | FLOAT8     | Gorilla      | Smooth velocity changes
vz              | FLOAT8     | Gorilla      | Smooth velocity changes
altitude_km     | FLOAT8     | Gorilla      | Derived from x,y,z — smooth
spatial_bucket  | INT        | Dictionary   | Low cardinality (16 buckets)
pos_ecef        | geometry   | Array        | WKB blob — opaque to typed compressors
```

### 2. Compression Configuration

```sql
ALTER TABLE trajectories SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'object_id',
    timescaledb.compress_orderby = 'time ASC',
    timescaledb.compress_chunk_time_interval = '1 hour'
);
```

**Segmentation by `object_id`**:
- Groups all positions for one object together
- Gorilla sees smooth coordinate sequences (one trajectory)
- Enables efficient "give me object X's trajectory" queries

**Ordering by `time ASC`**:
- Natural ordering for trajectory data
- DeltaDelta on timestamps achieves near-zero storage for regular intervals
- Gorilla on coordinates benefits from temporal smoothness

### 3. The Geometry Column Problem

PostGIS `geometry` is stored as WKB (Well-Known Binary) — a variable-length bytea.
TimescaleDB's typed compressors (Gorilla, DeltaDelta) cannot decompose it. It falls
back to Array compression (TOAST/LZ), which is significantly worse.

**Benchmark needed**: Compare these approaches:

| Approach | Storage | Query Convenience | Compression |
|----------|---------|-------------------|-------------|
| A: Store `pos_ecef` + x/y/z | Redundant | Full PostGIS functions on geometry | geometry=poor, floats=good |
| B: Store x/y/z only, reconstruct geometry | No redundancy | Must call `ST_MakePoint(x,y,z)` | All columns compressed well |
| C: Store `pos_ecef` only | Compact source | Full PostGIS functions | Poor (WKB blob) |
| D: Custom PostGIS accessor compression | Requires fork changes | Native | Best if feasible |

**Recommended starting point**: Approach A (redundant storage). The x/y/z columns
compress 10-20x with Gorilla; the geometry column bloat is acceptable if spatial
index queries need it. Benchmark Approach B to see if reconstruction cost is tolerable.

### 4. Approach D: Custom Compression (Aspirational)

If the PostGIS fork can expose ECEF coordinates as individual typed columns at the
storage layer (decomposed storage), TimescaleDB's Gorilla compressor could operate
directly. This would require:

- PostGIS fork stores PointZ as 3x FLOAT8 columns internally
- TimescaleDB compression recognizes the decomposed form
- Reconstruction into geometry happens at query time

This is a deeper integration and should be a future work stream, not Phase 1.

### 5. Compression Policy

```sql
-- Auto-compress chunks older than 2 hours
SELECT add_compression_policy('trajectories', INTERVAL '2 hours');
```

Rationale:
- 1-hour chunk interval + 2-hour compression delay = 1 hour of warm data
- Recent data stays uncompressed for fast DML and index updates
- Adjust based on ingest latency requirements

### 6. Expected Compression Ratios

Based on typical trajectory data patterns:

| Column | Uncompressed | Gorilla/DD | Ratio |
|--------|-------------|------------|-------|
| time (8B) | 8 B/row | ~0.1 B/row | 80:1 (regular intervals) |
| x,y,z (24B) | 24 B/row | ~3 B/row | 8:1 (smooth trajectory) |
| vx,vy,vz (24B) | 24 B/row | ~4 B/row | 6:1 (smooth velocity) |
| object_id (4B) | 4 B/row | ~0 B/row | ~inf (segment key) |
| pos_ecef (~56B) | 56 B/row | ~40 B/row | 1.4:1 (WKB blob) |

**Without geometry column**: ~7 B/row compressed from ~68 B/row = **~10:1**
**With geometry column**: ~47 B/row compressed from ~124 B/row = **~2.6:1**

This strongly favors Approach B (no geometry column) if PostGIS spatial functions
aren't needed on compressed data.

## Tasks

- [ ] Implement reference schema with compression settings
- [ ] Generate synthetic trajectory data (LEO, MEO, GEO orbits)
- [ ] Benchmark Approach A vs B vs C compression ratios
- [ ] Benchmark query performance: trajectory retrieval, spatial range, time range
- [ ] Measure ingest throughput with/without compression policy
- [ ] Test DML on compressed chunks (UPDATE/DELETE on recent trajectory corrections)
- [ ] Document recommended compression settings per use case

## Open Questions

1. Is direct-to-compressed-column ingest (`enable_direct_compress_insert`) compatible
   with PostGIS geometry types?
2. Can the PostGIS fork provide a "decomposed storage" mode for PointZ that lets
   TimescaleDB compress individual coordinate components?
3. What is the acceptable query latency for reconstructing geometry from x/y/z
   at SELECT time? (Matters for Approach B viability.)

## Coordination Points (PostGIS Fork)

- **Need from PostGIS**: Whether a decomposed storage option for PointZ is feasible
- **Need from PostGIS**: Byte layout of ECEF/ECI WKB (is it identical to standard
  PointZ WKB or does it include frame metadata?)
- **Provide to PostGIS**: Compression ratio benchmarks to motivate decomposed storage
