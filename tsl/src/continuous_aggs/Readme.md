# Continuous Aggregates #

A continuous aggregate is a special kind of materialized VIEW for aggregates.
The continuous aggregate maintains a hypertable containing the materialized form
of the aggregate (really the aggregate partial, see later), and updates this
table in the background. This enables queries on the continuous aggregate to
bypass the raw data, and read directly from the aggregated form.

Currently the aggregates are accurate but lag behind the raw data, all INSERTs
UPDATEs and DELETEs to the materialized range are eventually propagated, though
only after the background worker next runs; at some point in the future we
plan add the option to create or query aggregates that is aware of new data and
invalidations.

**NOTE**: The bucket containing INT64_MAX can never be materialized, and
    materialization usually is set to lag begind the insertion point by a
    fixed amount

## State ##

Each continuous aggregate maintains 4 pieces of state used to provide the
materialized view:

1. A materialization table, containing the materialized version of the query for
   various ranges
2. An invalidation threshold, below which might be materialized. Mutations below
   this threshold must be logged, so the range can be re-materialized.
3. A completed threshold, below which the aggregate was materialized. It is
   useless to read the materialization table above this threshold.
4. An invalidation log. Anyting range in this log must be re-materialized, as an
   INSERT, UPDATE, or DELETE may have altered it in a way which invalidated the
   materialization. There are actually two copies of this log, one which is
   per-hypertable, and updated by the mutating transaction, and a copy of this
   which is only accessed by the materializer. Since right now we only allow one
   continuous aggregate per hypertable, we only ever materialize the
   per-aggregate log in memory, and don't bother storing it in a real table. It
   is anticipated that this will change in the future.

## Views ##

In addition, the continuous aggs create the following VIEWs

1. A user view, which is the view queried by the enduser.
2. A partial view, which is used on materialization.
3. A direct view, which contains the query that was used to create the view.

## SELECT ##

The materialization does not store the aggregate's output, but rather the
a `bytea` containing the partial the aggregate creates. These partials are
what're used in parallelizable aggregates; multiple aggregates can be combined
to create a new partial, and the partial can be finalized to create the
aggregate's actual output. (We plan to eventually use this to create aggregates
with multiple time-resolutions).

The partials in the materialization table are keyed based on
`(time_bucket, chunk_id)`. During a SELECT we combine the partials for a given
time range, then finalize the resulting partials to ge the output. All of this
work if performed by the user view.

See [`create.c`](/tsl/src/continuous_aggs/create.c), and
[`partialize_finalize.c`](/tsl/src/partialize_finalize.c) for more details.

## INSERT/UPDATE/DELETE ##

Mutating transaction must check if the range they edit may be materialized, and
if so, record the range they edit in the invalidation log, so that the
materializer knows to re-materialize this range. In the interest of not
degrading efficiency, we do this in a TRIGGER on the raw hypertable which is
only instantiated when the continuous aggregate is created.

A statement must record an invalidation if:

1. It is an INSERT, UPDATE, or DELETE on a hypertable with a continuous
   aggregate
2. It is either stronger than READ COMMITTED _or_ the lowest value it touches is
   lower than the invalidation threshold for the hypertable.

To reduce the write-amplification, we only record one invalidation-range
per-transaction. This may increase the amount of materialization we re-do, but
we expect out-of-order mutations to be relatively rare, and don't want to
penalize mutations unnecessarily.

See [`insert.c`](/tsl/src/continuous_aggs/insert.c) for more details.

## Materialization Path ##

Materialization happens in three transactions:

1. We find the point we will materialize new data until, and update the
   invalidation threshold to that point.
2. We move the invalidation log from the hypertable to the log(s) of the
   continuous aggregate(s) (not yet implemented, as we only allow a single one)
3. We perform the actual materialization, of both the new and invalidated,
   ranges, and update the completed threshold.

We perform the materialization like this since we want to block mutations to the
raw hypertable for as little time as possible. The invalidation threshold must
be updated, and visible to all mustating transaction, _strictly before_ we
insert new values into the materialization table; if we didn't do this, a
concurrent INSERT could invalidate a materialized value without us realizing. We
enforce this by updating the invalidation threshold under an AccessExclusive,
ensuring no mutations are in progress. Since this is a contentious operation, we
do this as close to the end of a transaction as possible.

## Lock Ordering ##

Since there are so many objects accessed by the various submodules, it's
important that the locks don't deadlock.

The materializer worker grabs locks in the following order, at the beginning of
the first transaction, and holds them until all transactions complete:

1. raw hypertable: AccessShare
2. materialization table: ShareRowExclusiveLock
3. partial view: ShareRowExclusiveLock

In the first transaction it then grabs:

3. completed threshold: AccessShareLock
4. invalidation threshold: AccessExclusiveLock

In the second transaction it grabs:

5. invalidation log: RowExclusive
6. completed threshold: ??? (as this is only touched by the materialization
   worker, and those are excluded by the lock on the materialization table, it
   is unclear if it matters)
7. invalidation threshold: AccessShareLock


The INSERT/UPDATE/DELETE path grabs:

1. raw hypertable: RowExclusive or stronger
2. Invalidation Threshold: AccessShare (optional)
3. invalidation log: RowExclusive (note: this ordering does not conflict with
   the materialization worker's second transaction, because both transactions
   only grab a row-exclusive lock on the invalidation log, so the two locks
   don't conflict)

SELECTs grab

1. the user view: AccessShare?
2. the raw hypertable: the same
