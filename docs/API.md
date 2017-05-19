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
| `chunk_time_interval` | Interval in event time (micro seconds for TIMESTAMP and TIMESTAMPTZ) that each chunk covers. Must be > 0. Default is 1 month.
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

### `time_bucket()`

This is a more powerful version of the standard postgres `date_trunc` function.
It allows for arbitrary time intervals instead of the second, minute, hour, etc.
provided by `date_trunc`. The return value is the bucket's start time.

#### For TIMESTAMP/TIMESTAMPTZ time inputs

**Notes about TIMESTAMPTZ inputs:**
TIMESTAMPTZ arguments are bucketed by the time at UTC. So the alignment of
buckets is on UTC time. One consequence of this is that daily buckets are
aligned to midnight UTC, not local time.

If the user wants buckets aligned by local time, the TIMESTAMPTZ input should be
cast to TIMESTAMP (such a cast converts the value to local time) before being
passed to time_bucket (see example below).  Note that along daylight savings
time boundaries the amount of data aggregated into a bucket after such a cast is
irregular: for example if the bucket_width is 2 hours, the number of UTC hours
bucketed by local time on daylight savings time boundaries can be either 3 hours
or 1 hour.

**Required arguments**

|Name|Description|
|---|---|
| `bucket_width` | A postgres time interval for how long each bucket is (interval) |
| `time` | The timestamp to bucket (timestamp/timestamptz)|

**Optional arguments**

|Name|Description|
|---|---|
| `offset` | The time interval to offset all buckets by (interval) |

#### For integer time inputs

**Required arguments**

|Name|Description|
|---|---|
| `bucket_width` | The bucket width (integer) |
| `time` | The timestamp to bucket (integer) |

**Optional arguments**

|Name|Description|
|---|---|
| `offset` | The amount to offset all buckets by (integer) |


**Sample usage**

Simple 5-minute averaging

```sql
SELECT time_bucket('5 minutes', time) five_min, avg(cpu)
FROM metrics
GROUP BY five_min
ORDER BY five_min
LIMIT 10;
```

To report the middle of the bucket, instead of the left edge:
```sql
SELECT time_bucket('5 minutes', time)+'2.5 minutes' five_min, avg(cpu)
FROM metrics
GROUP BY five_min
ORDER BY five_min
LIMIT 10;
```

For rounding, move the alignment so that the middle of the bucket is at the 5 minute mark (and report the middle of the bucket):
```sql
SELECT time_bucket('5 minutes', time, '-2.5 minutes')+'2.5 minutes' five_min,
       avg(cpu)
FROM metrics
GROUP BY five_min
ORDER BY five_min
LIMIT 10;
```

Bucketing a TIMESTAMPTZ at local time instead of UTC(see note above):
```sql
SELECT time_bucket('2 hours', timetz::TIMESTAMP) five_min,
       avg(cpu)
FROM metrics
GROUP BY five_min
ORDER BY five_min
```
Note that the above cast to TIMESTAMP converts the time to local time according
to the server's timezone setting.

---

### `last()` and `first()`

The `last()` and `first()` aggregates allow you to get the value of one column as ordered by another. For example, `last(temperature, time)` will return the latest temperature value based on time within an aggregate group.

**Required arguments**

|Name|Description|
|---|---|
| `value` | The value to return (anyelement) |
| `time` | The timestamp to use for comparison (TIMESTAMP/TIMESTAMPTZ or integer type)  |

**Examples**

Get the latest temperature by device_id
```sql
SELECT device_id, last(temp, time)
FROM metrics
GROUP BY device_id;
```
