# Hypertable limitations (developer notes)

Product documentation lives on the Tiger Data docs site. This file tracks
limitation notes that belong next to the extension source when the public docs
repo is not the contribution path.

## Partitioning dimensions

- Time/space partitioning columns cannot be NULL (a `NOT NULL` constraint is
  required or added by `create_hypertable`).
- **Generated columns cannot be used as partitioning dimensions.**  
  `create_hypertable` / `add_dimension` reject columns defined with
  `GENERATED ALWAYS AS ... STORED`. Use a concrete column instead. If you need
  a derived partition key, persist it in a normal column (for example with a
  trigger) rather than a generated column.
- Unique indexes must include all partitioning dimension columns.
- `UPDATE` that would move a row across chunks is not supported.
- Foreign keys from one hypertable to another hypertable are not supported.

Enforced in code via `ts_dimension_info_create_open_interval` /
dimension validation (`src/dimension.c`), which raises:

> Generated columns cannot be used as partitioning dimensions.