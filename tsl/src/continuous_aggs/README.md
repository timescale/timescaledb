# Continuous Aggregates #

A continuous aggregate is a special kind of materialized view for
aggregates that can be partially and continuously refreshed, either
manually or automated by a policy that runs in the background. Unlike
a regular materialized view, a continuous aggregate doesn't require
complete re-materialization on every refresh. Instead, it is possible
to refresh a subset of the continuous aggregate at relatively low
cost, thus enabling continuous aggregation as new data is written or
old data is updated and/or backfilled.

To enable continuous aggregation, a continuous aggregate stores
partial aggregations for every time bucket in an internal
hypertable. The advantage of this configuration is that each time
bucket can be recomputed individually, without requiring updates to
other buckets, and buckets can be combined to form more granular
aggregates (e.g., hourly buckets can be combined to form daily
buckets). Finalization of the partial buckets happens automatically at
query time. Although such finalization gives slightly higher querying
times, it is offset by more efficient refreshes that only recompute
the buckets that have been "invalidated" by changes in the raw data.

A continuous aggregate policy automates the refreshing, allowing the
aggregate to stay up-to-date without manual intervention. A policy can
be configured to only refresh the most recent data (e.g., just the
last hour's worth of data) or ensure that the continuous aggregate is
always up-to-date with the underlying source data. Policies that focus
on recent data allow older parts of the continuous aggregate to stay
the same or be governed by manual refreshes.

## Bookkeeping and Internal State ##

TimescaleDB does bookkeeping for each continuous aggregate to know
which buckets of the aggregates require refreshing. Whenever a
modification happens to the source data, an invalidation for the
modified region is written to an invalidation log. However,
invalidations are not written after the *invalidation threshold*,
which tracks the latest bucket materialized thus far. This threshold
allows write amplification to be kept to a minimum by not writing
invalidations for "hot" time buckets that are assumed to still have
data being written to them.

Thus, to store, maintain, and query aggregations, continuous
aggregates consist of the following objects:

1. A user view, which queries and finalizes the aggregations and is
   also the object that users interact with.
2. A partial view, which is used to materialize new data.
3. A direct view, which holds the original query that users specified.
4. An internal materialization hypertable, containing the materialized
   data as partial aggregates for each time bucket.
5. An invalidation threshold, which is a timestamp that tracks the
   latest materialization. Invalidations that occur before this
   timestamp will be logged, while invalidations after it will not be
   logged.
6. A trigger on the source hypertable that writes invalidations to the
   hypertable invalidation log at transaction end, based on INSERT,
   UPDATE, and DELETE statements that mutate the data.
7. A hypertable invalidation log that tracks invalidated regions of
   data for each hypertable. Entries in this log contain time ranges
   that need to be re-materialized across all the hypertable's
   continuous aggregates.
8. A materialization invalidation log. Once a refresh runs on a given
   continuous aggregate, this log tracks how invalidations from the
   hypertable invalidation log are processed against the refresh
   window for the refreshed continuous aggregate. Thus, a single
   invalidation in the hypertable invalidation log becomes one entry
   per continuous aggregate in the materialization invalidation log.

## The materialized hypertable ##

The materialized hypertable does not store the aggregate's output, but
rather the partial aggregate state. For instance, in case of an
average, each bucket stores the sum and count in an internal binary
form. The partial aggregates are what gives continuous aggregates
flexibility; buckets can be individually updated and multiple partial
aggregates can be combined to form new partials. Future enhancements
may allow aggregating at different time resolutions using the the same
underlying continuous aggregate.

## The Invalidation Log and Threshold ##

There are two mechanisms for collecting hypertable invalidations:
1. A trigger on the hypertable, which writes to a dedicated
   *hypertable invalidation log* table.
2. A logical decoding plugin, which scans the WAL to produce
   invalidation entries.

### Invalidation Log Table

Mutating transactions must record their mutations in the invalidation
log, so that a refresh knows to re-materialize the invalidated
range. When using the trigger approach, this happens
by installing a trigger on the source hypertable when the first
continuous aggregate on that hypertable is created. The trigger
writes entries to the hypertable invalidation log.

To reduce the extra writes by the trigger, only one invalidation range
(lowest and highest modified value) is written at the end of a
mutating transaction. As a result, a refresh might materialize more
data than necessary, but the insert incurs a smaller overhead
instead. Write amplification is further reduced by never writing
invalidations after the invalidation threshold, which can be
configured to lag behind the time bucket that sees the most writes.

Whenever a refresh occurs across a time range that is newer than the
current invalidation threshold, the threshold must first be moved to
the end of the refreshed region so that new invalidations are recorded
in the region after the refresh. However, mutations in the refreshed
region can also happen concurrently with the refresh, so, in order to
not lose any invalidations, the invalidation threshold must be moved
in its own transaction before the new region is materialized.

Thus, every refresh may happen across two transactions; first one that
moves the invalidation threshold (if necessary) and a second one that
does the actual materialization of new data.

The second transaction of the refresh will only materialize regions
that are recorded as invalid in the invalidation log. Thus, the
initial state of a continuous aggregate is to have an entry in the
invalidation log that invalidates the entire range of the
aggregate. During the refresh, the log is processed and invalidations
are cut against the given refresh window, leaving only invalidation
entries that are outside the refresh window. Subsequently, if the
refresh window does not match any invalidations, there is nothing to
refresh either.

### Invalidations Plugin

The invalidation plugin is available in `invalidation_plugin.c` and is
used to generate "invalidations" for continuous aggregates. An
invalidation is a time range that has been modified in the original
hypertable that is the source of data for refreshing a continuous
aggregate.

To avoid dependencies on the TimescaleDB extension, the invalidation
plugin is made as simple as possible and does not use the metadata
tables of the TimescaleDB extension and instead is configured to read
specific relids and attribute names when fetching data.

While processing the WAL, the timestamps are collected into ranges and
(similarly to when using the trigger to record changes) only one
invalidation range (lowest and highest modified value in microseconds
since UNIX Epoch) is produced for each modified hypertable in each
mutating transaction. For the same reasons as above, this means that a
refresh might materialize more data than necessary.

The modifications are represented as tuples consisting of the relid
for the change and the start and end timestamp as int64, which is the
internal representation of timestamp values. It is up to the receiver
to use the relid and timestamps correctly.

To be able to collect a unique key for each changed row when doing a
`DELETE` or an `UPDATE`, it is required that the hypertable has a
replica identity set. This can either be the primary key of the
hypertable, or a unique index, or the full row if neither is suitable.

## Distribution of functions across files

Each source file has an associated header file that should be included
to use functions from the corresponding file.

<dl>
<dt>`common.c`</dt>
<dd>This file contains the functions common in all scenarios of creating a continuous aggregates.</dd>

<dt>`create.c`</dt>
<dd>This file contains the functions that are directly responsible for the creation of the continuous aggregates,
like creating hypertable, catalog_entry, view, etc.</dd>

<dt>`finalize.c`</dt>
<dd>This file contains the specific functions for the case when continous aggregates are created in old format.</dd>

<dt>`materialize.c`</dt>
<dd>This file contains the functions directly dealing with the materialization of the continuous aggregates.</dd>

<dt>`repair.c`</dt>
<dd>The repair and rebuilding related functions are put together in this file.</dd>
</dl>

<dt>`invalidation.c`</dt>
<dd>Functions related to invalidation processing for continuous aggregates.</dd>
</dl>

<dt>`invalidation_plugin.c`</dt>
<dd>The invalidation plugin.</dd>

<dt>`invalidation_plugin_cache.c`</dt>
<dd>The invalidation cache for the invalidation plugin.</dd>

<dt>`invalidation_record.c`</dt>
<dd>Functions to work with invalidation records.</dd>
</dl>

