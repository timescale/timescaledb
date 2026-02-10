# TimescaleDB — PostGIS ECEF/ECI Integration

## Project Context

This branch (`claude/openspec-postgis-integration-cIgmI`) coordinates TimescaleDB-side
changes for integrating a PostGIS fork with native ECEF and ECI coordinate system support.

## Related Repositories

This is a multi-repo effort. Each repo has its own Claude branch with `.openspec/` specs:

| Repo | Role | Branch Pattern |
|------|------|---------------|
| `montge/timescaledb` (this repo) | Time-series storage, partitioning, compression, continuous aggregates | `claude/openspec-postgis-integration-*` |
| `montge/postgis` | ECEF/ECI types, SRIDs, frame conversions, spatial operators | `claude/ecef-eci-*` (expected) |

## Cross-Repo Interface Contract

The integration boundary between repos is defined in:
`.openspec/postgis-ecef-eci-integration/interface-contract.md`

When working on this repo, consult that file for what we **need from** and **provide to**
the PostGIS fork. Any changes to the contract must be reflected in both repos.

## OpenSpec Planning

All planning docs live in `.openspec/postgis-ecef-eci-integration/`:
- `proposal.md` — top-level proposal and phasing
- `design.md` — architecture diagrams and data flow
- `spec-01` through `spec-06` — detailed specs per work stream
- `tasks.md` — master checklist with cross-repo coordination items
- `interface-contract.md` — the formal interface between repos

## Development Rules

- **Do not modify** TimescaleDB core (`src/`, `tsl/`) without explicit approval
- Integration work uses TimescaleDB's documented extension points only
- All SQL artifacts (schemas, functions, policies) go in `sql/postgis_ecef_eci/`
- Test fixtures go in `test/expected/` and `test/sql/` following existing conventions
- Compression and partitioning benchmarks go in `test/benchmarks/`

## Key Decisions

- Spatial partitioning: altitude-band bucketing (16 partitions)
- Compression: Gorilla on decomposed float columns, SEGMENT BY object_id
- Frame conversion: at query time, not storage time
- Geometry storage: Approach A (redundant geometry + floats) initially
- ECI SRIDs: 900001+ range (user-defined)
- PostGIS fork: packaged as separate `postgis_ecef_eci` extension (preferred)
