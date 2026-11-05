# Storage policies

> [Description](#storage-policy-description)<br>
> [Details](#storage-policy-details)<br>
> [Usage Example](#storage-policy-example)<br>
> [API](#storage-policy-api)

## Description <a id="storage-policy-description"></a>

A storage policy declares, once per database, how tables with a given name are stored. When a plain `CREATE TABLE` creates a table whose name matches a policy, TimescaleDB makes it a hypertable with the policy's settings (partition column, chunk interval, columnstore settings, compression and retention) in the same statement and transaction. This is for clients that can only issue plain DDL, such as ORMs, Kafka and Flink connectors or migration tools, and for the window between `CREATE TABLE` and a separate setup script in which rows can arrive in a table that isn't a hypertable yet.

A policy is declarative. It states how a table is to be stored, not the calls that get it there, so the same `CREATE TABLE` gives the same result in every client. [`explain_storage_policy`](#explain_storage_policy) shows what a statement would do, [`check_storage_policy_drift`](#check_storage_policy_drift) shows where a table differs from its policy, and [`apply_storage_policy`](#apply_storage_policy) brings it into line. A policy decides only the settings it names.

## Details <a id="storage-policy-details"></a>

A policy matches a schema and table name either exactly or by regular expression. Regular expressions are anchored, so `ev_.*` is matched as `^(?:ev_.*)$` and does not match `my_ev_1`. Anchors of your own do no harm, and a pattern has to be a valid expression on its own, so one like `a)|(?:` is rejected. When several policies match, an exact policy wins, and among regex policies the one with the lowest `priority` wins. Only the winner applies, and policies are not merged. [`lookup_storage_policy`](#lookup_storage_policy) lists every match in this order.

An option given in the `WITH` clause of the statement overrides the same option from the policy, aliases included, and `WITH (tsdb.hypertable = false)` opts the table out. A statement with `WITH (tsdb.columnstore = false)` also leaves out the columnstore settings and the compression age of the policy. Temporary tables, partitions, tables created with `INHERITS` or `OF`, and tables created by an extension script never get a policy. Tables created inside `CREATE SCHEMA` get the policy that matches each of them. The setting `timescaledb.enable_storage_policies` (default `on`) turns policies off for the tables a session creates, and [`explain_storage_policy`](#explain_storage_policy) then says that none applies. [`check_storage_policy_drift`](#check_storage_policy_drift) and [`apply_storage_policy`](#apply_storage_policy) are asked for by name and ignore it. A table in the temporary schema never gets a policy, also when it is created as `pg_temp.name`.

A policy that matches but cannot be applied fails the `CREATE TABLE` and names the policy, as a table that silently stayed plain would be found only after it had grown too large to convert. A policy applies only when a table is created. A table keeps its settings when the policy is later changed, and [`check_storage_policy_drift`](#check_storage_policy_drift) and [`apply_storage_policy`](#apply_storage_policy) find and fix the difference.

Only the database owner or a superuser can add, alter or remove a policy, as a policy decides how other roles' tables are stored. The policy applies as the role that runs `CREATE TABLE`, so it grants nothing that role doesn't have, and `updated_by` records the role that made the change. Policies are included in a dump and are not applied again when it is restored.

The policy is a JSON document. Unknown keys are rejected when the policy is stored, and so are a `chunk_interval` or an age that is not a positive interval or integer, and a `partition_column` or a `columnstore` list that is not a non-empty string. Whether the columns exist, and whether the chunk interval fits the type of the column, can only be checked when a table is created, and then a failure names the policy.

| Key | Description |
|---|---|
| `partition_column` | Column to partition by. |
| `chunk_interval` | Chunk interval, as an interval for a time column or an integer for an integer column. |
| `create_default_indexes` | `true` or `false`. |
| `columnstore` | An object with `segmentby`, `orderby` and `index`, each a string. When `orderby` leaves out the time column, TimescaleDB adds it at the end, descending, and this is not reported as drift. |
| `compression` | An object with `mode` (`AFTER` or `NEVER`) and, for `AFTER`, `after`, a positive interval. |
| `retention` | The same form as `compression`. A retention policy drops chunks older than `after`. |

With `NEVER`, a table is expected to have no compression or retention policy, so [`check_storage_policy_drift`](#check_storage_policy_drift) reports one that was added by hand and [`apply_storage_policy`](#apply_storage_policy) removes it.

`columnstore`, `compression` and `retention` need the Timescale License, and a policy that sets them is rejected under the Apache license. `compression.after` and `retention` do not work on integer time columns, as the compression and retention policies there need an integer and an `integer_now` function. A `tiering` key is not supported yet.

## Usage Example <a id="storage-policy-example"></a>

This policy covers one table by name, partitioned by `ts` in one-day chunks, compressed after a week and dropped after 90 days.

```SQL
SELECT add_storage_policy('sensor_data', 'public', 'sensor_data', '{
    "partition_column": "ts",
    "chunk_interval": "1 day",
    "columnstore": {"segmentby": "sensor_id"},
    "compression": {"mode": "AFTER", "after": "7 days"},
    "retention": {"mode": "AFTER", "after": "90 days"}
}');
```
```
 add_storage_policy
--------------------
                  1
```

A plain `CREATE TABLE` now makes a hypertable.

```SQL
CREATE TABLE public.sensor_data (ts timestamptz NOT NULL, sensor_id int, value float8);
```
```
NOTICE:  applied storage policy "sensor_data" to "public"."sensor_data"
```

[`explain_storage_policy`](#explain_storage_policy) shows what a `CREATE TABLE` would do before any table is created.

```SQL
SELECT * FROM explain_storage_policy('public', 'sensor_data');
```
```
        setting        |    value    |    source
-----------------------+-------------+------------
 storage_policy        | sensor_data | exact match
 tsdb.hypertable       | t           | policy
 tsdb.partition_column | ts          | policy
 tsdb.chunk_interval   | 1 day       | policy
 tsdb.segmentby        | sensor_id   | policy
 tsdb.compress_after   | 7 days      | policy
 retention_after       | 90 days     | policy
```

## Command List (A-Z) <a id="storage-policy-api"></a>
Policy management
> - [add_storage_policy](#add_storage_policy)
> - [alter_storage_policy](#alter_storage_policy)
> - [remove_storage_policy](#remove_storage_policy)

Inspection
> - [apply_storage_policy](#apply_storage_policy)
> - [check_storage_policy_drift](#check_storage_policy_drift)
> - [explain_storage_policy](#explain_storage_policy)
> - [generate_storage_policy](#generate_storage_policy)
> - [lookup_storage_policy](#lookup_storage_policy)
> - [timescaledb_information.storage_policies](#storage_policies)

---

## **add_storage_policy** <a id="add_storage_policy"></a>
```SQL
add_storage_policy(
    name NAME,
    schema_name TEXT,
    table_name TEXT,
    policy JSONB,
    match_kind TEXT = 'exact',
    priority INTEGER = NULL,
    enabled BOOL = true,
    if_not_exists BOOL = false
) RETURNS INTEGER
```

Creates a storage policy. The policy document is validated when it is stored, and for `regex` the patterns are checked to compile. Adding a policy warns when it overlaps one of the other kind, as an exact policy always wins over a regex one. Two regex policies are not compared, as their priority settles which applies.

### Required Arguments <a id="add_storage_policy-required-arguments"></a>
|Name| Type |Description|
|---|---|---|
| `name` | `NAME` | Unique name of the policy. |
| `schema_name` | `TEXT` | Schema name, or a pattern when `match_kind` is `regex`. |
| `table_name` | `TEXT` | Table name, or a pattern when `match_kind` is `regex`. |
| `policy` | `JSONB` | The [policy document](#storage-policy-details). |

### Optional Arguments <a id="add_storage_policy-optional-arguments"></a>
|Name| Type |Description|
|---|---|---|
| `match_kind` | `TEXT` | `exact` (default) or `regex`. |
| `priority` | `INTEGER` | Required for `regex` and not allowed for `exact`. The lowest value wins, and values are unique. |
| `enabled` | `BOOL` | A disabled policy applies to nothing. Default `true`. |
| `if_not_exists` | `BOOL` | Do nothing, with a notice, if a policy of this name exists. Default `false`. |

### Returns

|Column|Type|Description|
|---|---|---|
| `add_storage_policy` | `INTEGER` | The id of the policy, or `NULL` if `if_not_exists` skipped it. |
<br>

### Sample Usages <a id="add_storage_policy-examples"></a>
Every table in schema `fhr` whose name starts with `ev_` is partitioned by `ts`, unless a policy with a lower priority or an exact one matches it.

```SQL
SELECT add_storage_policy('events', 'fhr', 'ev_.*',
    '{"partition_column": "ts", "chunk_interval": "1 day"}',
    match_kind => 'regex', priority => 10);
```

---

## **alter_storage_policy** <a id="alter_storage_policy"></a>
```SQL
alter_storage_policy(
    name NAME,
    schema_name TEXT = NULL,
    table_name TEXT = NULL,
    policy JSONB = NULL,
    priority INTEGER = NULL,
    enabled BOOL = NULL
) RETURNS VOID
```

Changes a policy. Only the arguments that are given change, and the match kind cannot be changed. A new `policy` replaces the whole document. Existing tables keep the settings they were created with, and [`check_storage_policy_drift`](#check_storage_policy_drift) lists them. Changing what a policy matches, or enabling it, warns about overlaps as [`add_storage_policy`](#add_storage_policy) does.

### Required Arguments <a id="alter_storage_policy-required-arguments"></a>
|Name| Type |Description|
|---|---|---|
| `name` | `NAME` | Name of the policy to change. |

The other arguments are those of [`add_storage_policy`](#add_storage_policy), and are all optional here.

### Sample Usages <a id="alter_storage_policy-examples"></a>
Swap the order of two regex policies by changing a priority, and switch a policy off.

```SQL
SELECT alter_storage_policy('events', priority => 1);
SELECT alter_storage_policy('sensor_data', enabled => false);
```

---

## **remove_storage_policy** <a id="remove_storage_policy"></a>
```SQL
remove_storage_policy(
    name NAME,
    if_exists BOOL = false
) RETURNS VOID
```

Removes a policy. Tables that were created under it are not changed.

### Required Arguments <a id="remove_storage_policy-required-arguments"></a>
|Name| Type |Description|
|---|---|---|
| `name` | `NAME` | Name of the policy. |

### Optional Arguments <a id="remove_storage_policy-optional-arguments"></a>
|Name| Type |Description|
|---|---|---|
| `if_exists` | `BOOL` | Do nothing, instead of an error, if the policy doesn't exist. Default `false`. |

---

## **lookup_storage_policy** <a id="lookup_storage_policy"></a>
```SQL
lookup_storage_policy(
    schema_name NAME,
    table_name NAME
) RETURNS TABLE (rank INTEGER, name NAME, match_kind TEXT, priority INTEGER, policy JSONB)
```

Lists every enabled policy that a table name matches, in the order of precedence. Rank 1 is the policy a `CREATE TABLE` applies, and each next rank is the one that would apply if those before it were removed or disabled.

### Required Arguments <a id="lookup_storage_policy-required-arguments"></a>
|Name| Type |Description|
|---|---|---|
| `schema_name` | `NAME` | Schema of the table. |
| `table_name` | `NAME` | Name of the table. |

### Returns

|Column|Type|Description|
|---|---|---|
| `rank` | `INTEGER` | Position in the order of precedence, starting at 1. |
| `name` | `NAME` | Name of the policy. |
| `match_kind` | `TEXT` | `exact` or `regex`. |
| `priority` | `INTEGER` | The priority of a `regex` policy, `NULL` for `exact`. |
| `policy` | `JSONB` | The policy document. |
<br>

### Sample Usages <a id="lookup_storage_policy-examples"></a>
```SQL
SELECT rank, name, match_kind, priority FROM lookup_storage_policy('fhr', 'ev_slow_2');
```
```
 rank |     name      | match_kind | priority
------+---------------+------------+----------
    1 | events_slow_2 | exact      |
    2 | events_slow   | regex      |        5
    3 | events        | regex      |       10
```

---

## **explain_storage_policy** <a id="explain_storage_policy"></a>
```SQL
explain_storage_policy(
    schema_name NAME,
    table_name NAME,
    options JSONB = NULL
) RETURNS TABLE (setting TEXT, value TEXT, source TEXT)
```

Shows what a `CREATE TABLE` with this name, and optionally these `WITH` options, would do, one row per setting with where it comes from. It uses the same lookup as `CREATE TABLE` and creates nothing. When no policy applies, the first row says why. Temporary tables, partitions and tables created with `INHERITS` or `OF` never get a policy, and a name alone doesn't say whether a table is one of them.

### Required Arguments <a id="explain_storage_policy-required-arguments"></a>
|Name| Type |Description|
|---|---|---|
| `schema_name` | `NAME` | Schema of the table. |
| `table_name` | `NAME` | Name of the table. |

### Optional Arguments <a id="explain_storage_policy-optional-arguments"></a>
|Name| Type |Description|
|---|---|---|
| `options` | `JSONB` | The `tsdb.*` options of the statement as a JSON object, such as `'{"segment_by": "dev"}'`. They win over the policy. |

### Returns

|Column|Type|Description|
|---|---|---|
| `setting` | `TEXT` | The setting. The first row, `storage_policy`, names the policy. |
| `value` | `TEXT` | The value, or `NULL` when no policy applies. |
| `source` | `TEXT` | `policy` or `statement`. For `storage_policy`, how it matched, or why none applies. |
<br>

### Sample Usages <a id="explain_storage_policy-examples"></a>
```SQL
SELECT * FROM explain_storage_policy('public', 'sensor_data', '{"chunk_interval": "1 hour"}');
```

---

## **check_storage_policy_drift** <a id="check_storage_policy_drift"></a>
```SQL
check_storage_policy_drift(
    schema_name NAME = NULL,
    table_name NAME = NULL,
    only_drift BOOL = true,
    include_plain BOOL = false
) RETURNS TABLE (hypertable_schema NAME, hypertable_name NAME, policy_name NAME,
                 setting TEXT, expected TEXT, actual TEXT, in_sync BOOL)
```

Compares existing hypertables with what [`explain_storage_policy`](#explain_storage_policy) gives a new table of the same name, and returns the settings that differ. This finds tables that were created before a policy changed, were changed by hand, or were created with their own options. The settings compared are the partition column, chunk interval, columnstore `segmentby` and `orderby`, and the ages of the compression and retention policies. Only what the policy declares is compared, and a hypertable that no policy matches is not listed. With `include_plain => true` a plain table that a policy matches is listed too, with one row, `tsdb.hypertable`, expected `true` and actual `false`. Such a table was created before the policy, or with `tsdb.hypertable = false`, and the check cannot tell the two apart, so plain tables are off by default. Temporary tables, partitions, tables that inherit or are typed, and tables of an extension never get a policy and are not listed. Unlogged tables do get a policy, and so are listed. A policy that is changed while the check runs may give anomalous results. It changes nothing.

### Optional Arguments <a id="check_storage_policy_drift-optional-arguments"></a>
|Name| Type |Description|
|---|---|---|
| `schema_name` | `NAME` | Only this schema. By default every hypertable is checked. |
| `table_name` | `NAME` | Only this table, or with no schema, tables of this name in any schema. |
| `only_drift` | `BOOL` | With `false`, every compared setting is returned and `in_sync` says whether it matches. Default `true`. |
| `include_plain` | `BOOL` | With `true`, plain tables that a policy matches are listed too. Default `false`. |

### Returns

|Column|Type|Description|
|---|---|---|
| `hypertable_schema`, `hypertable_name` | `NAME` | The table: a hypertable, or with `include_plain` a plain table. |
| `policy_name` | `NAME` | The policy that applies to it today. |
| `setting` | `TEXT` | The setting, as in [`explain_storage_policy`](#explain_storage_policy). |
| `expected` | `TEXT` | The value in the policy. |
| `actual` | `TEXT` | The value on the table, `NULL` if there is none. Intervals are shown in PostgreSQL's output form, such as `@ 90 days`. |
| `in_sync` | `BOOL` | Whether the two match. |
<br>

### Sample Usages <a id="check_storage_policy_drift-examples"></a>
After the retention in a policy was shortened from 90 to 30 days:

```SQL
SELECT hypertable_name, setting, expected, actual FROM check_storage_policy_drift('fhr', 'ev_1');
```
```
 hypertable_name |     setting     | expected |  actual
-----------------+-----------------+----------+-----------
 ev_1            | retention_after | 30 days  | @ 90 days
```

---

## **generate_storage_policy** <a id="generate_storage_policy"></a>
```SQL
generate_storage_policy(
    schema_name NAME,
    table_name NAME
) RETURNS JSONB
```

Describes how an existing hypertable is stored today as a policy document, which can be passed to [`add_storage_policy`](#add_storage_policy) or [`alter_storage_policy`](#alter_storage_policy). It reads the partition column and chunk interval, the columnstore `segmentby` and `orderby`, and the ages of the compression and retention policies. A setting that the table does not have is left out, so the document names only what the table has, and a policy made from it shows no drift for that table. Not captured are tiering and the schedule or other configuration of a job. A table whose compression policy has no age of its own is described with the age the policy uses, which is the chunk interval. A plain table has nothing to describe and gives an error.

### Required arguments

|Name|Type|Description|
|---|---|---|
| `schema_name` | `NAME` | Schema of the hypertable. |
| `table_name` | `NAME` | Name of the hypertable. |

### Returns

|Column|Type|Description|
|---|---|---|
| `generate_storage_policy` | `JSONB` | The policy document. |
<br>

### Sample Usages
Make an exact policy from a table that was set up by hand:

```SQL
SELECT add_storage_policy('ev_1', 'fhr', 'ev_1', generate_storage_policy('fhr', 'ev_1'));
```

---

## **apply_storage_policy** <a id="apply_storage_policy"></a>
```SQL
apply_storage_policy(
    schema_name NAME,
    table_name NAME,
    dry_run BOOL = false,
    allow_retention BOOL = false
) RETURNS TABLE (setting TEXT, expected TEXT, actual TEXT, action TEXT)
```

Brings an existing table up to its policy, by applying what [`check_storage_policy_drift`](#check_storage_policy_drift) reports. A hypertable gets the chunk interval, columnstore settings and compression and retention policies of the policy. An empty plain table is converted to a hypertable as `CREATE TABLE` would make it, with the columnstore enabled and a compression policy at the chunk interval unless the policy says otherwise. `create_default_indexes` and the columnstore `index` of the policy are applied. A plain table with rows is an error, as converting it has to move the data, so use `create_hypertable(..., migrate_data => true)` and apply again. The partition column of a hypertable cannot be changed and is an error.

A new chunk interval only applies to chunks created from now on, and existing chunks keep their size. In the same way, chunks that are compressed already keep the columnstore settings they were compressed with, and the rows of those settings say so. Retention can delete data that is already there, so it is only applied with `allow_retention => true`, and otherwise its row says it was skipped. Removing a retention policy, when the policy says `NEVER`, deletes nothing and needs no flag. `dry_run => true` lists the same steps as the real run, and fails for a caller who does not own the table, for a partition column of a type that cannot be partitioned, and for a primary key or unique index without the partition column. A policy that is changed while it runs may give anomalous results. The function runs with the privileges of the caller, who must own the table, as if the changes were made by hand. If a step fails, the steps before it are undone.

### Required Arguments <a id="apply_storage_policy-required-arguments"></a>
|Name| Type |Description|
|---|---|---|
| `schema_name` | `NAME` | Schema of the table. |
| `table_name` | `NAME` | Name of the table. |

### Optional Arguments <a id="apply_storage_policy-optional-arguments"></a>
|Name| Type |Description|
|---|---|---|
| `dry_run` | `BOOL` | Return what would be done and change nothing. Default `false`. |
| `allow_retention` | `BOOL` | Also apply the retention policy. Default `false`. |

### Returns

|Column|Type|Description|
|---|---|---|
| `setting` | `TEXT` | The setting that was changed. |
| `expected` | `TEXT` | The value in the policy. |
| `actual` | `TEXT` | The value before, `NULL` if there was none. |
| `action` | `TEXT` | `applied`, `would apply`, `converted`, `would convert`, or `skipped` with the reason. |
<br>

### Sample Usages <a id="apply_storage_policy-examples"></a>
Look at what would change, then do it.

```SQL
SELECT * FROM apply_storage_policy('fhr', 'ev_1', dry_run => true);
SELECT * FROM apply_storage_policy('fhr', 'ev_1');
```

---

## **timescaledb_information.storage_policies** <a id="storage_policies"></a>

A view over the policies, in the order they are matched: exact policies first, then regex policies by priority.

|Column|Type|Description|
|---|---|---|
| `name` | `NAME` | Name of the policy. |
| `schema_name`, `table_name` | `TEXT` | The names or patterns it matches. |
| `match_kind` | `TEXT` | `exact` or `regex`. |
| `priority` | `INTEGER` | The priority of a `regex` policy. |
| `policy` | `JSONB` | The policy document. |
| `enabled` | `BOOL` | Whether the policy applies. |
| `created_at`, `updated_at` | `TIMESTAMPTZ` | When it was created and last changed. |
| `updated_by` | `NAME` | The role that last changed it. |
