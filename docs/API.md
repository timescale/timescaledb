## TimescaleDB API Reference


### `create_hypertable()`

Creates a TimescaleDB hypertable from a Postgres table (replacing the
latter), partitioned on time and optionally another column.
Target table must be empty. All actions, such as `ALTER TABLE`, `SELECT`,
etc., still work on the resulting hypertable.

**Required arguments**

|Name|Description|
|---|---|
| `main_table` | Identifier of table to convert to hypertable|
| `time_column_name` | Name of the column containing time values|

**Optional arguments**

|Name|Description|
|---|---|
| `partitioning_column` | Name of an additional column to partition by. If provided, `number_partitions` must be set.
| `number_partitions` | Number of partitions to use when `partitioning_column` is set. Must be > 0.

**Sample usage**

Convert table `foo` to hypertable with just time partitioning on column `ts`:
```sql
SELECT create_hypertable('foo', 'ts');
```

Convert table `foo` to hypertable with time partitioning on `ts` and
space partitioning (2 partitions) on `bar`:
```sql
SELECT create_hypertable('foo', 'ts', 'bar', 2);
```

---

### `drop_chunks()`
_**NOTE: Currently only supported on single-partition deployments**_

Removes data chunks that are older than a given time interval across all
hypertables or a specific one. Chunks are removed only if all their data is
beyond the cut-off point, so the remaining data may contain timestamps that
are before the cut-off point, but only one chunk worth.



**Required arguments**

|Name|Description|
|---|---|
| `older_than` | Timestamp of cut-off point for data to be dropped, i.e., anything older than this should be removed. |

**Optional arguments**

|Name|Description|
|---|---|
| `table_name` | Hypertable name from which to drop chunks. If not supplied, all hypertables are affected.
| `schema_name` | Schema name of the hypertable from which to drop chunks. Defaults to `public`.

**Sample usage**

Drop all chunks older than 3 months:
```sql
SELECT drop_chunks(interval '3 months');
```

Drop all chunks from hypertable `foo` older than 3 months:
```sql
SELECT drop_chunks(interval '3 months', 'foo');
```

---

### `setup_timescaledb()`

Initializes a Postgres database to fully use TimescaleDB.

**Sample usage**

```sql
SELECT setup_timescaledb();
```
