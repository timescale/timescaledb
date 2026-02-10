# Proposal: PostGIS ECEF/ECI Integration with TimescaleDB

## Summary

Integrate a custom PostGIS fork with native ECEF (Earth-Centered, Earth-Fixed) and ECI
(Earth-Centered Inertial) coordinate system support into TimescaleDB's time-series
infrastructure. This enables first-class spatiotemporal tracking of objects in orbital,
aerial, and terrestrial reference frames with time-series-native partitioning, compression,
and continuous aggregation.

## Motivation

Time-series geospatial workloads — satellite tracking, space debris catalogs, aircraft
surveillance, ballistic trajectory analysis, GPS signal processing — require both:

1. **High-ingest-rate time-series storage** with automatic partitioning and compression
2. **Native 3D coordinate frame awareness** (ECEF for ground-fixed, ECI for inertial/orbital)

Today these are handled by separate systems or by storing raw floats without coordinate
frame semantics. This integration brings them together in PostgreSQL, leveraging TimescaleDB
for temporal management and a PostGIS fork for spatial semantics.

## Goals

- Enable hypertables with ECEF/ECI geometry columns that compress efficiently
- Provide spatial partitioning strategies aware of 3D coordinate structure
- Support time-dependent ECEF <-> ECI frame conversions within SQL
- Define continuous aggregate patterns for spatiotemporal rollups
- Ensure clean extension co-loading between TimescaleDB and the PostGIS fork
- Maintain compatibility with standard PostGIS workflows (WGS84, geographic types)

## Non-Goals

- Replacing PostGIS — this extends it with additional coordinate reference frames
- Modifying TimescaleDB core — all integration via documented extension points
- Real-time orbit propagation — this is a storage/query layer, not a dynamics engine

## Stakeholders

- **PostGIS fork maintainers**: Providing ECEF/ECI type definitions, SRIDs, and
  transformation functions
- **TimescaleDB consumers**: Schema design, partitioning functions, compression tuning
- **Application layer**: Query patterns, API contracts for spatiotemporal access

## Dependencies

- PostgreSQL 15+ (required by both TimescaleDB and PostGIS fork)
- TimescaleDB 2.x (current codebase)
- PostGIS fork with ECEF/ECI support (provider branch)
- IERS Earth Orientation Parameters (for precise frame conversions)

## Risk Assessment

| Risk | Impact | Mitigation |
|------|--------|------------|
| PostGIS fork type registration conflicts with TimescaleDB loader | High | Extension dependency ordering; test co-loading early |
| WKB blob storage defeats columnar compression | Medium | Decompose into float columns; benchmark both approaches |
| ECI<->ECEF conversion precision loss in aggregates | Medium | Document aggregation semantics; convert before aggregating |
| Chunk exclusion ineffective for 3D spatial queries | Medium | Custom partitioning function mapping 3D to 1D bucket |
| GiST index bloat across many chunks | Low | Monitor; consider BRIN for ordered dimensions |

## Work Streams

1. [Schema & Partitioning Design](specs/schema-partitioning/spec.md)
2. [Compression Profile](specs/compression/spec.md)
3. [Frame Conversion Integration](specs/frame-conversion/spec.md)
4. [Continuous Aggregate Templates](specs/continuous-aggregates/spec.md)
5. [Index Strategy](specs/index-strategy/spec.md)
6. [Extension Compatibility Layer](specs/extension-compatibility/spec.md)
7. [Interface Contract](interface-contract.md) — cross-repo coordination

## Timeline

This proposal follows iterative development. Each spec can proceed independently once
the Extension Compatibility Layer (spec-06) confirms co-loading works. Schema &
Partitioning (spec-01) should be finalized before Compression (spec-02) and Continuous
Aggregates (spec-04) since they depend on the table structure.

```
Phase 0: Extension Compatibility (spec-06) — gate for all other work
Phase 1: Schema & Partitioning (spec-01) + Frame Conversion (spec-03) — parallel
Phase 2: Compression (spec-02) + Index Strategy (spec-05) — parallel, after Phase 1
Phase 3: Continuous Aggregates (spec-04) — after Phase 1 + Phase 2
```
