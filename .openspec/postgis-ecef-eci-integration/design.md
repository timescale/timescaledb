# Design Overview: PostGIS ECEF/ECI + TimescaleDB

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                        PostgreSQL 15+                           │
│                                                                 │
│  ┌──────────────┐   ┌──────────────────┐   ┌────────────────┐  │
│  │   PostGIS    │   │ PostGIS ECEF/ECI │   │  TimescaleDB   │  │
│  │   (stock)    │   │     (fork)       │   │    2.x         │  │
│  │              │   │                  │   │                │  │
│  │ - geometry   │   │ - ECEF types     │   │ - Hypertables  │  │
│  │ - geography  │   │ - ECI types      │   │ - Chunks       │  │
│  │ - GiST ops   │   │ - Frame convert  │   │ - Compression  │  │
│  │ - SRID 4978  │   │ - EOP data       │   │ - Cont. Aggs   │  │
│  │              │   │ - SRIDs 900001+  │   │ - Bgw Jobs     │  │
│  └──────┬───────┘   └────────┬─────────┘   └───────┬────────┘  │
│         │                    │                      │           │
│         └────────────────────┼──────────────────────┘           │
│                              │                                  │
│                    ┌─────────▼──────────┐                       │
│                    │   User Schema      │                       │
│                    │                    │                       │
│                    │  trajectories      │                       │
│                    │  (hypertable)      │                       │
│                    │                    │                       │
│                    │  Dimensions:       │                       │
│                    │  - time (open)     │                       │
│                    │  - spatial_bucket  │                       │
│                    │    (closed, 16)    │                       │
│                    └─────────┬──────────┘                       │
│                              │                                  │
│              ┌───────────────┼───────────────┐                  │
│              │               │               │                  │
│     ┌────────▼───┐  ┌───────▼────┐  ┌───────▼────┐             │
│     │ Chunk 1    │  │ Chunk 2    │  │ Chunk N    │             │
│     │ t:[0h,1h)  │  │ t:[0h,1h)  │  │ t:[1h,2h)  │             │
│     │ sb:[0,1)   │  │ sb:[1,2)   │  │ sb:[0,1)   │             │
│     │            │  │            │  │            │             │
│     │ Indexes:   │  │ Indexes:   │  │ Indexes:   │             │
│     │ - B-tree   │  │ - B-tree   │  │ - B-tree   │             │
│     │ - GiST 3D  │  │ - GiST 3D  │  │ - GiST 3D  │             │
│     │ - BRIN     │  │ - BRIN     │  │ - BRIN     │             │
│     └────────────┘  └────────────┘  └────────────┘             │
│                                                                 │
│     ┌────────────────────────────────────────────────────┐      │
│     │          Continuous Aggregates                     │      │
│     │                                                    │      │
│     │  traj_hourly ──► traj_daily (cagg-on-cagg)        │      │
│     │  spatial_density_hourly                            │      │
│     │  conjunction_prefilter                             │      │
│     │  orbital_daily                                     │      │
│     └────────────────────────────────────────────────────┘      │
│                                                                 │
│     ┌────────────────────────────────────────────────────┐      │
│     │          Background Jobs                           │      │
│     │                                                    │      │
│     │  - Compression policy (compress chunks > 2h old)   │      │
│     │  - Cagg refresh policies                           │      │
│     │  - EOP data refresh (daily from IERS)              │      │
│     │  - Retention policy (configurable per object_type) │      │
│     └────────────────────────────────────────────────────┘      │
└─────────────────────────────────────────────────────────────────┘
```

## Data Flow

```
Ingest (Application / TLE Processor / Radar Feed)
    │
    ▼
┌─────────────────────────────┐
│ INSERT INTO trajectories    │
│                             │
│ - time (TIMESTAMPTZ)        │
│ - object_id (INT)           │
│ - x, y, z (FLOAT8)         │
│ - vx, vy, vz (FLOAT8)      │
│ - pos_ecef (geometry)       │  ◄── Optional, see compression spec
│ - spatial_bucket (INT)      │  ◄── Computed by trigger or app
│ - altitude_km (FLOAT8)     │  ◄── Computed by trigger or app
└──────────────┬──────────────┘
               │
               ▼
┌─────────────────────────────┐
│ TimescaleDB Chunk Router    │
│                             │
│ 1. Extract time → find      │
│    open dimension slice     │
│ 2. Extract spatial_bucket → │
│    find closed dimension    │
│    slice                    │
│ 3. Route to chunk           │
└──────────────┬──────────────┘
               │
               ▼
┌─────────────────────────────┐
│ Chunk (PostgreSQL table)    │
│                             │
│ - CHECK constraints define  │
│   time range + bucket range │
│ - PostGIS GiST index on    │
│   pos_ecef (within chunk)   │
│ - B-tree on (object_id,    │
│   time DESC)                │
└──────────────┬──────────────┘
               │
               ▼ (background, after 2h)
┌─────────────────────────────┐
│ Compression                 │
│                             │
│ - SEGMENT BY object_id      │
│ - ORDER BY time ASC         │
│ - Gorilla on x,y,z,vx,vy,vz│
│ - DeltaDelta on time        │
│ - Dictionary on object_type │
│ - Array on pos_ecef (WKB)   │
│                             │
│ ~10:1 ratio (float columns) │
│ ~2.6:1 ratio (with geometry)│
└─────────────────────────────┘
```

## Query Flow (Spatial + Temporal)

```
SELECT * FROM trajectories
WHERE time >= '2025-01-01' AND time < '2025-01-02'
  AND ST_DWithin(pos_ecef, ST_MakePoint(6378137,0,0)::geometry, 1000000)

    │
    ▼
┌─────────────────────────────────────────────┐
│ TimescaleDB Planner                         │
│                                             │
│ 1. Time constraint → eliminate chunks       │
│    outside [Jan 1, Jan 2)                   │
│ 2. No spatial_bucket constraint in WHERE →  │
│    all spatial partitions scanned           │
│ 3. Expand to matching chunks (24h * 16     │
│    partitions = 384 chunks)                 │
└──────────────┬──────────────────────────────┘
               │
               ▼
┌─────────────────────────────────────────────┐
│ Per-Chunk Execution                         │
│                                             │
│ Uncompressed chunks:                        │
│   → GiST 3D index scan on pos_ecef         │
│   → Fast spatial filtering                  │
│                                             │
│ Compressed chunks:                          │
│   → Decompress candidate segments           │
│   → Apply spatial filter post-decompression │
│   → Slower (no GiST on compressed data)     │
└─────────────────────────────────────────────┘
```

## Key Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Partitioning function | Altitude-band (Option A) | Simple, matches common queries, fast to compute |
| Geometry storage | Approach A: redundant (geometry + floats) initially | Full PostGIS function support + good float compression |
| Frame conversion | At query time, not storage time | Avoid duplicating storage; frame choice is query-dependent |
| Continuous agg frame | Aggregate in storage frame (ECEF), convert result at midpoint | Well-defined error bounds, compatible with cagg constraints |
| ECI SRIDs | 900001+ range | No EPSG codes exist; user-defined range avoids collisions |
| PostGIS fork packaging | Separate extension preferred | Independent upgrades, clean dependency graph |
| Compression segment | object_id | One trajectory per segment = smooth Gorilla sequences |
| Chunk interval | 1 hour | Balance between chunk count and per-chunk query cost |
