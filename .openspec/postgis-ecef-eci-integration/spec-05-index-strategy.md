# Spec 05: Index Strategy

## Status: Draft

## Overview

Define the indexing strategy for ECEF/ECI trajectory hypertables, optimizing for
the primary query patterns while managing per-chunk index overhead.

## Context

Each TimescaleDB chunk is a standard PostgreSQL table with its own indexes. For a
hypertable with 1-hour chunks and 16 spatial partitions, one day of data produces
384 chunks, each with its own set of indexes.

### Key Source References (TimescaleDB)
- `src/chunk.c` — Chunk table creation (indexes are inherited or created per-chunk)
- `src/indexing.c` — Hypertable-level index management
- `src/chunk_index.c` — Per-chunk index creation
- `src/nodes/chunk_append/` — Query planning uses indexes within each chunk

### PostGIS Index Types
- **GiST**: General search tree — supports spatial predicates (`&&`, `ST_DWithin`, etc.)
- **SP-GiST**: Space-partitioned GiST — better for point data, lower overlap
- **BRIN**: Block Range INdex — very compact, good for physically ordered data

## Design

### 1. Primary Query Patterns

| # | Query Pattern | Dimensions | Frequency |
|---|--------------|------------|-----------|
| Q1 | Object trajectory: all positions for object X in time range | object_id, time | Very High |
| Q2 | Spatial region: all objects within distance of a point at time T | pos_ecef (3D), time | High |
| Q3 | Altitude band: all objects in LEO/MEO/GEO at time T | altitude_km, time | Medium |
| Q4 | Conjunction screening: objects in same spatial bucket at time T | spatial_bucket, time | Medium |
| Q5 | Latest position: most recent observation per object | object_id, time DESC | High |

### 2. Recommended Index Set

#### Index A: Primary Lookup (Object + Time)
```sql
CREATE INDEX ON trajectories (object_id, time DESC);
```
- **Serves**: Q1, Q5
- **Per chunk**: B-tree, ~2% of data size
- **TimescaleDB optimization**: Chunk append with ordered scan avoids sorting

#### Index B: Time (Chunk Exclusion Support)
```sql
-- Created automatically by create_hypertable on the time column
-- TimescaleDB uses chunk constraints for time-range exclusion
-- No explicit index needed beyond the default
```

#### Index C: 3D Spatial (GiST on ECEF Geometry)
```sql
CREATE INDEX ON trajectories USING gist (pos_ecef gist_geometry_ops_nd);
```
- **Serves**: Q2
- **Per chunk**: GiST tree, ~5-15% of data size
- **Note**: `gist_geometry_ops_nd` enables N-dimensional indexing (important for 3D ECEF
  where Z is not "up" but a coordinate axis)
- **Only if**: `pos_ecef` column is present (Approach A from compression spec)

#### Index D: Altitude BRIN (For Altitude-Band Queries)
```sql
CREATE INDEX ON trajectories USING brin (altitude_km)
    WITH (pages_per_range = 32);
```
- **Serves**: Q3
- **Per chunk**: Extremely compact (~0.1% of data size)
- **Effective because**: Within a chunk (1 hour, 1 spatial bucket), altitude values
  are physically clustered (objects don't change altitude bands quickly)

#### Index E: Spatial Bucket (For Conjunction Screening)
```sql
CREATE INDEX ON trajectories (spatial_bucket, time DESC);
```
- **Serves**: Q4
- **Per chunk**: B-tree, small (only 16 distinct values)
- **Note**: If spatial_bucket is the closed dimension, TimescaleDB already uses
  chunk constraints for bucket exclusion. This index helps within-chunk filtering.

### 3. Indexes on Compressed Chunks

TimescaleDB compressed chunks store data columnar with per-segment metadata:
- **Min/max per column per segment**: Used for segment-level filtering
- **Bloom filters**: Optional, for equality predicates
- **Orderby index**: Implicit from `compress_orderby` setting

For compressed data:
- B-tree and GiST indexes **do not exist** on compressed chunks
- Segment-level min/max on `object_id` enables Q1 (since `object_id` is SEGMENT BY)
- Spatial queries (Q2) on compressed chunks require decompressing candidate segments
  — no spatial index acceleration

**Implication**: Keep recent data uncompressed (2+ hours) for spatial query workloads.
Historical spatial queries will be slower unless decompressed first.

### 4. Indexes on Continuous Aggregate Hypertables

Continuous aggregates are themselves hypertables and can be indexed:

```sql
-- On traj_hourly (Template 1 from spec-04)
CREATE INDEX ON traj_hourly (object_id, bucket DESC);

-- On spatial_density_hourly (Template 2 from spec-04)
CREATE INDEX ON spatial_density_hourly (spatial_bucket, bucket DESC);

-- On conjunction_prefilter (Template 3 from spec-04)
CREATE INDEX ON conjunction_prefilter (spatial_bucket, bucket DESC);
```

### 5. Index Size Budget

For a trajectory table with:
- 100,000 objects
- 1 observation per object per 10 seconds
- 1-hour chunks, 16 spatial partitions

Per chunk: ~625 objects * ~360 rows/object = ~225,000 rows

| Index | Type | Estimated Size/Chunk | Total (384 chunks/day) |
|-------|------|---------------------|----------------------|
| A: (object_id, time) | B-tree | ~5 MB | 1.9 GB/day |
| C: (pos_ecef) GiST 3D | GiST | ~15 MB | 5.8 GB/day |
| D: (altitude_km) BRIN | BRIN | ~0.05 MB | 19 MB/day |
| E: (spatial_bucket, time) | B-tree | ~5 MB | 1.9 GB/day |
| **Total** | | **~25 MB** | **~9.6 GB/day** |

Data size per chunk (uncompressed): ~225,000 * 120 B = ~27 MB
**Index-to-data ratio**: ~0.9:1 (high, but expected for spatial workloads)

### 6. Index Maintenance Considerations

- **Compression drops indexes**: When a chunk is compressed, its per-chunk indexes
  are dropped. Re-created on decompression. This is normal and expected.
- **Reorder for locality**: `reorder_chunk()` can physically reorder chunk data by
  an index to improve locality:
  ```sql
  SELECT reorder_chunk('_timescaledb_internal._hyper_1_42_chunk',
      'trajectories_object_id_time_idx');
  ```
  Consider reordering by (object_id, time) before compression for best Gorilla
  compression on per-object trajectories.

## Tasks

- [ ] Implement index set A, C, D, E on test hypertable
- [ ] Benchmark Q1-Q5 query patterns with and without each index
- [ ] Measure index build time per chunk (impacts ingest throughput)
- [ ] Measure GiST 3D index effectiveness for ECEF distance queries
- [ ] Test SP-GiST vs GiST for point-only workloads
- [ ] Evaluate index size vs query speedup tradeoff — drop Index C if not worth it
- [ ] Test `reorder_chunk()` impact on subsequent compression ratio
- [ ] Benchmark spatial queries on compressed vs uncompressed chunks

## Open Questions

1. Does the PostGIS fork's ECEF point type work with `gist_geometry_ops_nd`?
   Or does it need a custom operator class?
2. Is SP-GiST viable for 3D ECEF points? (Standard SP-GiST supports 2D only
   in stock PostGIS.)
3. Should we consider a custom BRIN operator class for ECEF radius/altitude?
4. What is the break-even point where GiST index overhead isn't worth it vs
   sequential scan + filter on small chunks?

## Coordination Points (PostGIS Fork)

- **Need from PostGIS**: Confirm `gist_geometry_ops_nd` compatibility with ECEF PointZ
- **Need from PostGIS**: Whether custom operator classes are needed for ECEF/ECI types
- **Need from PostGIS**: SP-GiST support for 3D ECEF (if planned)
- **Provide to PostGIS**: Per-chunk index overhead data to inform type design decisions
