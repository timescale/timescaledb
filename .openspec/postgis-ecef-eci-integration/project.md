# PostGIS ECEF/ECI Integration with TimescaleDB

## Project Description

Integrate a custom PostGIS fork with native ECEF (Earth-Centered, Earth-Fixed) and ECI
(Earth-Centered Inertial) coordinate system support into TimescaleDB's time-series
infrastructure for spatiotemporal tracking of objects in orbital, aerial, and terrestrial
reference frames.

## Repository Map

This project spans multiple repositories, each with its own Claude development branch:

### montge/timescaledb (THIS REPO)
- **Role**: Consumer of PostGIS spatial types; provides time-series storage, chunking,
  compression, continuous aggregates, and background job scheduling
- **Branch**: `claude/openspec-postgis-integration-*`
- **Specs**: Schema design, compression tuning, continuous aggregate templates,
  index strategy, extension compatibility

### montge/postgis (EXTERNAL)
- **Role**: Provider of ECEF/ECI types, SRIDs, frame conversion functions, spatial
  operators, Earth Orientation Parameter management
- **Branch**: TBD — `claude/ecef-eci-*` (expected)
- **Specs**: Type definitions, SRID assignments, conversion algorithms, EOP loading

## Coordination Model

Since OpenSpec is single-repo, cross-repo coordination uses:

1. **Interface Contract** (`interface-contract.md`): Versioned document defining the
   exact types, functions, and behaviors each repo expects from the other. Copied
   to both repos. Changes require version bump and coordinated commits.

2. **CLAUDE.md**: Each repo's `CLAUDE.md` references the other repo and the contract,
   so any Claude session working on either repo has full context.

3. **GitHub Cross-References**: Commits and PRs reference the other repo's branch/PR
   using `montge/postgis#<PR>` or `montge/timescaledb#<PR>` syntax.

4. **Coordination Tags**: Tasks blocked on the other repo are tagged `[BLOCKED:postgis]`
   or `[BLOCKED:timescaledb]` in `tasks.md`.

## Phasing

```
Phase 0: Extension Compatibility — both extensions load cleanly
         (requires PostGIS fork to exist with basic SRID registration)

Phase 1: Schema + Frame Conversion — in parallel
         TimescaleDB: hypertable schema, partitioning function
         PostGIS: type definitions, conversion functions

Phase 2: Compression + Indexing — after Phase 1
         TimescaleDB: compression benchmarks, index strategy
         PostGIS: confirm WKB format, operator class compatibility

Phase 3: Continuous Aggregates — after Phase 2
         TimescaleDB: cagg templates, gapfill integration
         PostGIS: confirm function volatility/immutability
```
