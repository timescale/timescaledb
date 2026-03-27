# Continuous Aggregates: Comprehensive Research Report

## Table of Contents
1. [Requirements / Features / Capabilities List](#1-requirements--features--capabilities-list)
2. [How TimescaleDB Implements Continuous Aggregates](#2-how-timescaledb-implements-continuous-aggregates)
3. [Alternatives Comparison](#3-alternatives-comparison)
4. [Pros, Cons, Performance Regressions, and Required Improvements](#4-pros-cons-performance-regressions-and-required-improvements)

---

## 1. Requirements / Features / Capabilities List

This section defines what an ideal incremental materialized view system should provide. We use this list later to compare TimescaleDB against alternatives.

### 1.1 Core Functional Requirements

| ID | Requirement | Description |
|----|------------|-------------|
| F1 | **Incremental Refresh** | Only recompute data that has changed since last refresh, not the entire dataset |
| F2 | **Automatic Change Tracking** | Detect INSERT/UPDATE/DELETE on source tables without manual intervention |
| F3 | **Time-Bucketed Aggregation** | Support grouping by time intervals (1 min, 1 hour, 1 day, etc.) |
| F4 | **Variable-Width Buckets** | Support calendar-aware intervals (1 month, 1 year) that vary in length |
| F5 | **Standard Aggregation Functions** | Support COUNT, SUM, AVG, MIN, MAX, and ideally percentiles, STDDEV, etc. |
| F6 | **GROUP BY Multiple Columns** | Aggregate by time bucket + arbitrary grouping columns |
| F7 | **Real-Time Data Access** | Query returns up-to-date results even if materialization hasn't run yet |
| F8 | **Hierarchical Aggregation** | Build coarser aggregates on top of finer ones (hourly -> daily -> monthly) |
| F9 | **JOIN Support** | Allow joining hypertable with dimension/lookup tables in the aggregate definition |
| F10 | **Compression Integration** | Compressed storage of materialized results for space efficiency |

### 1.2 Operational Requirements

| ID | Requirement | Description |
|----|------------|-------------|
| O1 | **Automatic Background Refresh** | Policies/schedules that refresh without user intervention |
| O2 | **Manual Refresh with Window** | Ability to refresh a specific time range on demand |
| O3 | **Configurable Refresh Granularity** | Control how much data is refreshed per cycle (batch size) |
| O4 | **Concurrent Refresh** | Multiple refresh policies can run in parallel on non-overlapping ranges |
| O5 | **Retention Policy Integration** | Automatic cleanup of old materialized data |
| O6 | **Schema Evolution** | ALTER the aggregate definition without full rebuild (add/remove columns) |
| O7 | **Monitoring & Observability** | Visibility into refresh lag, materialization progress, invalidation backlog |
| O8 | **Repair & Recovery** | Ability to force full re-materialization of corrupted or drifted data |

### 1.3 Performance Requirements

| ID | Requirement | Description |
|----|------------|-------------|
| P1 | **Low Refresh Latency** | Time between data change and materialized availability should be configurable and low |
| P2 | **Minimal Write Amplification** | Invalidation tracking should not significantly slow down source table writes |
| P3 | **Efficient Chunk Pruning** | Refresh should only scan source chunks overlapping the invalidated range |
| P4 | **Query Performance** | Queries on materialized data should be orders of magnitude faster than raw queries |
| P5 | **Storage Efficiency** | Materialized data should be compact (compression, deduplication) |
| P6 | **Scalable Invalidation** | System should handle high-throughput writes (100K+ rows/sec) without invalidation log bloat |
| P7 | **Timezone/DST Correctness** | Bucketing must handle DST transitions, timezone changes without errors or data loss |

### 1.4 Correctness Requirements

| ID | Requirement | Description |
|----|------------|-------------|
| C1 | **Exact Results** | Materialized data must exactly match a fresh `GROUP BY` query on the same range |
| C2 | **No Partial Buckets** | Refresh should not produce half-materialized time buckets |
| C3 | **Transactional Consistency** | Reads during refresh should not see partial updates |
| C4 | **Update/Delete Tracking** | Changes to existing rows (not just inserts) must trigger re-materialization |
| C5 | **Idempotent Refresh** | Running refresh twice on the same range produces the same result |

### 1.5 Usability Requirements

| ID | Requirement | Description |
|----|------------|-------------|
| U1 | **Standard SQL Interface** | Define aggregates using familiar SQL (CREATE MATERIALIZED VIEW ... AS SELECT ...) |
| U2 | **Transparent Querying** | Users query the view like a normal table; real-time merge is invisible |
| U3 | **Clear Error Messages** | Failures in refresh, validation, or policy execution should be diagnosable |
| U4 | **Documentation & Examples** | Comprehensive docs covering common patterns and edge cases |
| U5 | **Migration Path** | Ability to convert existing materialized views or queries to incremental ones |

---

## 2. How TimescaleDB Implements Continuous Aggregates

### 2.1 Architecture Overview

TimescaleDB continuous aggregates use a **three-view architecture** on top of a **materialization hypertable**:

```
┌─────────────────────────────────────────────────────┐
│                   User View                          │
│  SELECT * FROM materialized WHERE ts < watermark     │
│  UNION ALL                                           │
│  SELECT * FROM source WHERE ts >= watermark           │
└─────────────────────────────────────────────────────┘
        │                           │
        ▼                           ▼
┌──────────────────┐    ┌──────────────────────────┐
│ Materialization   │    │ Source Hypertable         │
│ Hypertable        │    │ (raw data)               │
│ (pre-aggregated)  │    └──────────────────────────┘
└──────────────────┘
```

Three internal views are created:
1. **User View**: Public-facing, implements UNION ALL for real-time mode
2. **Partial View**: Stores partial aggregation query (used during refresh)
3. **Direct View**: Preserves original user query for `pg_get_viewdef()`

### 2.2 Invalidation Tracking

**Trigger-based tracking** (default) uses PostgreSQL triggers on source hypertable DML:

```
INSERT/UPDATE/DELETE on source hypertable
    → Per-transaction hash table tracks (chunk_relid → min/max modified time)
    → On COMMIT: write to continuous_aggs_hypertable_invalidation_log
    → On REFRESH: copy to continuous_aggs_materialization_invalidation_log (per-CAgg)
    → Process: expand invalidations to bucket boundaries, DELETE+INSERT or MERGE
```

**Watermark dampening**: Modifications *after* the invalidation threshold are not logged (they're in the "hot zone" handled by real-time queries). This prevents write amplification from high-frequency inserts at the leading edge.

### 2.3 Refresh Mechanism

Two-phase process (separate transactions):

**Phase 1**: Update invalidation threshold, move hypertable-level invalidations to per-CAgg log.

**Phase 2**: Read per-CAgg invalidations, expand to bucket boundaries, execute materialization SQL.

Materialization SQL strategy:
- **PG15+**: MERGE statement (atomic upsert)
- **Older PG**: DELETE old rows + INSERT new aggregated rows

Bucketing strategies:
- **Inscribed** (conservative): Only include complete buckets within the refresh window
- **Circumscribed** (aggressive): Include all buckets overlapping the window

### 2.4 Variable-Width Buckets

Monthly/yearly buckets cannot use simple integer arithmetic. TimescaleDB uses dedicated functions:
- `ts_compute_inscribed_bucketed_refresh_window_variable()`
- `ts_compute_circumscribed_bucketed_refresh_window_variable()`

Restrictions: Cannot mix months with days/hours (e.g., `'1 month 2 days'` is invalid).

### 2.5 Hierarchical CAggs

CAggs can be stacked: `source → hourly CAgg → daily CAgg → monthly CAgg`. The child's materialization table becomes the parent's source. Invalidations propagate through the hierarchy.

### 2.6 Compression Integration

Materialization hypertables support TimescaleDB compression. A newer optimization ("direct compress on CAgg refresh") orders materialization output by compression segment columns, enabling direct compression without decompression.

### 2.7 Feature Coverage vs. Requirements

| Requirement | TimescaleDB Status | Notes |
|------------|-------------------|-------|
| F1 Incremental Refresh | **Full** | Via invalidation log + bucketed refresh |
| F2 Auto Change Tracking | **Full** | Trigger-based on source hypertable |
| F3 Time-Bucketed Aggregation | **Full** | `time_bucket()` function |
| F4 Variable-Width Buckets | **Full** | Monthly/yearly with timezone support |
| F5 Standard Aggregation | **Full** | COUNT, SUM, AVG, MIN, MAX, etc. |
| F6 Multi-Column GROUP BY | **Full** | Arbitrary grouping columns |
| F7 Real-Time Data Access | **Full** | UNION ALL with watermark boundary |
| F8 Hierarchical Aggregation | **Full** | CAgg on CAgg |
| F9 JOIN Support | **Partial** | Single hypertable + regular tables; changes to joined tables not tracked |
| F10 Compression | **Full** | Columnstore compression on materialization table |
| O1 Auto Refresh | **Full** | Background worker policies |
| O2 Manual Refresh | **Full** | `refresh_continuous_aggregate()` with window |
| O3 Configurable Granularity | **Full** | `buckets_per_batch` parameter |
| O4 Concurrent Refresh | **Full** | Since v2.21 |
| O5 Retention Integration | **Full** | Retention policies on materialization table |
| O6 Schema Evolution | **None** | Must drop and recreate |
| O7 Monitoring | **Partial** | `timescaledb_information.continuous_aggregates` view; no detailed refresh metrics |
| O8 Repair & Recovery | **Partial** | Manual full refresh possible; no automated corruption detection |
| P1 Low Refresh Latency | **Medium** | Seconds to minutes depending on invalidation volume |
| P2 Minimal Write Amplification | **Good** | Watermark dampening reduces invalidation log writes |
| P3 Chunk Pruning | **Partial** | Known issues with timezone-aware bucketing (GitHub #4662) |
| P4 Query Performance | **Excellent** | 5x-1000x improvement reported in benchmarks |
| P5 Storage Efficiency | **Good** | With compression, 33x ratios reported |
| P6 Scalable Invalidation | **Good** | 100K+ rows/sec reported in production |
| P7 Timezone/DST | **Partial** | Known DST bugs (GitHub #8898) |
| C1 Exact Results | **Full** | DELETE+INSERT or MERGE guarantees exactness |
| C2 No Partial Buckets | **Full** | Inscribed bucketing prevents this |
| C3 Transactional Consistency | **Full** | Separate transactions for threshold vs. materialization |
| C4 Update/Delete Tracking | **Full** | Triggers track all DML |
| C5 Idempotent Refresh | **Full** | DELETE+INSERT is naturally idempotent |
| U1 Standard SQL | **Full** | `CREATE MATERIALIZED VIEW ... WITH (timescaledb.continuous)` |
| U2 Transparent Querying | **Full** | Real-time mode is transparent |
| U3 Clear Errors | **Partial** | Known usability issues (GitHub #5179) |
| U4 Documentation | **Good** | Comprehensive official docs |
| U5 Migration Path | **None** | No conversion from plain matviews |

---

## 3. Alternatives Comparison

### 3.1 Systems Evaluated

| System | Type | Approach |
|--------|------|----------|
| **TimescaleDB** | PostgreSQL extension | Deferred incremental refresh via invalidation log |
| **pg_ivm** | PostgreSQL extension | Immediate IVM via triggers |
| **ClickHouse** | Analytical database | Insert-triggered materialized views |
| **Materialize** | Streaming database | Differential Dataflow (continuous IVM) |
| **RisingWave** | Streaming database | Streaming IVM with PostgreSQL-compatible SQL |
| **Apache Flink SQL** | Stream processor | Continuous queries on changelog streams |
| **Oracle** | Enterprise RDBMS | Fast Refresh with MV logs |

### 3.2 Feature Comparison Matrix

| Requirement | TimescaleDB | pg_ivm | ClickHouse | Materialize | RisingWave | Flink SQL | Oracle |
|------------|:-----------:|:------:|:----------:|:-----------:|:----------:|:---------:|:------:|
| **F1** Incremental Refresh | Full | Full | Partial (insert-only) | Full | Full | Full | Full |
| **F2** Auto Change Tracking | Full | Full | Insert-only | Full (WAL) | Full | Full (CDC) | Full (MV logs) |
| **F3** Time-Bucketed Agg | Full | Manual | Full | Full | Full | Full (windows) | Full |
| **F4** Variable-Width Buckets | Full | N/A | Manual | Manual | Manual | Full (windows) | Manual |
| **F5** Standard Agg Functions | Full | Most | Full | Full | Full | Full | Full |
| **F6** Multi-Column GROUP BY | Full | Full | Full | Full | Full | Full | Full |
| **F7** Real-Time Data Access | Full | Full (immediate) | No (eventual) | Full (sub-second) | Full (sub-second) | Full (streaming) | Partial (on-demand) |
| **F8** Hierarchical Aggregation | Full (built-in) | Manual | Manual | Manual | Manual | Manual | Manual |
| **F9** JOIN Support | Partial | Full | Full | Full | Full | Full | Full |
| **F10** Compression | Full | No | Full (native) | No | No | N/A | Full |
| **O1** Auto Background Refresh | Full | Immediate | On insert | Continuous | Continuous | Continuous | On-demand/scheduled |
| **O4** Concurrent Refresh | Full | N/A | N/A | N/A | N/A | N/A | Full |
| **O6** Schema Evolution | None | None | None | Partial | Partial | Partial | Partial |
| **P1** Refresh Latency | Seconds-minutes | Milliseconds | Milliseconds (insert) | Sub-second | Sub-100ms | Sub-second | Seconds |
| **P2** Write Amplification | Low (dampening) | High (immediate) | Low | Medium | Medium | Medium | Medium (MV logs) |
| **P4** Query Performance | Excellent | Excellent | Excellent | Good | Good | N/A (streaming) | Excellent |
| **P7** Timezone/DST | Partial (bugs) | N/A | Manual | Manual | Manual | Full (windows) | Manual |
| **C4** Update/Delete Tracking | Full | Full | None | Full | Full | Full | Full |

### 3.3 Detailed Comparison

#### pg_ivm (PostgreSQL Extension)

**Approach**: Immediate view maintenance via triggers. Every INSERT/UPDATE/DELETE on base tables instantly updates the materialized view.

**Strengths vs. TimescaleDB**:
- Zero-latency freshness (immediate, not deferred)
- Full JOIN support (multi-table)
- No background worker/policy configuration needed

**Weaknesses vs. TimescaleDB**:
- Higher write overhead (every DML triggers view update)
- No time-bucket-aware optimizations
- No compression integration
- No hierarchical aggregation built-in
- Not available on managed PostgreSQL services
- No "dampening" for hot zones — every write pays the cost

**Best for**: Low-throughput OLTP workloads needing always-fresh aggregates.

#### ClickHouse Materialized Views

**Approach**: Materialized views act as triggers on INSERT. Each insert block is transformed and inserted into a target table. Supports two modes: incremental (trigger-based, insert-only) and refreshable (periodic full recomputation).

**Strengths vs. TimescaleDB**:
- Sub-millisecond latency for new inserts
- No invalidation log overhead
- Excellent compression and query performance natively
- Full SQL in MV definition including JOINs and subqueries

**Weaknesses vs. TimescaleDB**:
- **Does not handle UPDATE or DELETE** — append-only by design
- Workarounds for updates (ReplacingMergeTree) add complexity and are eventually consistent
- No real-time mode (stale until next insert or full refresh)
- No built-in hierarchical aggregation
- Different SQL dialect (not PostgreSQL-compatible)
- Refreshable MVs require full recomputation (no incremental for updates)

**Best for**: Append-only time-series and event streams with very high throughput.

#### Materialize

**Approach**: Built on Differential Dataflow. Ingests change streams (PostgreSQL WAL or Kafka CDC) and maintains views incrementally in real-time. All dependent views are updated atomically.

**Strengths vs. TimescaleDB**:
- True real-time IVM (sub-second freshness, not polling-based)
- Handles arbitrary SQL including multi-way JOINs, subqueries
- Strong consistency across dependent views
- Handles UPDATE and DELETE natively
- No invalidation log — changes flow through dataflow graph

**Weaknesses vs. TimescaleDB**:
- Separate system (not a PostgreSQL extension — requires running a separate database)
- Higher memory usage (maintains state for all intermediate computations)
- No built-in time-bucket-aware optimization
- No compression for materialized output
- Operational complexity (WAL slots, source management)
- Higher cost for very large historical datasets
- Query performance for point lookups may be slower than pre-aggregated hypertable

**Best for**: Real-time analytics dashboards, event-driven architectures needing sub-second freshness.

#### RisingWave

**Approach**: Streaming database with PostgreSQL-compatible SQL. Materialized views are incrementally maintained as data streams in.

**Strengths vs. TimescaleDB**:
- Sub-100ms end-to-end freshness
- PostgreSQL-compatible SQL
- Strong snapshot consistency
- Built-in source connectors (Kafka, CDC, etc.)
- 10-20ms p99 query latency on materialized views

**Weaknesses vs. TimescaleDB**:
- Separate system (not a PostgreSQL extension)
- Designed for streaming, not historical batch analytics
- No built-in compression for materialized data
- Less mature ecosystem
- No hierarchical aggregation built-in

**Best for**: Real-time streaming analytics with PostgreSQL compatibility.

#### Apache Flink SQL

**Approach**: Distributed stream processing. SQL queries run continuously over changelog streams. State is managed in checkpointed backends.

**Strengths vs. TimescaleDB**:
- True streaming IVM with event-time semantics
- Excellent windowing support (tumbling, sliding, session windows)
- Handles late data with watermarks
- Scales horizontally across clusters
- Handles complex event processing patterns

**Weaknesses vs. TimescaleDB**:
- Much higher operational complexity (JVM, cluster management, checkpointing)
- Not a database — needs external sink for queryable results
- State management is complex and resource-intensive
- No compression for output tables
- Steep learning curve

**Best for**: Large-scale real-time stream processing pipelines.

#### Oracle Fast Refresh

**Approach**: MV logs (physical tables tracking DML) enable incremental refresh. Partition Change Tracking for partitioned tables.

**Strengths vs. TimescaleDB**:
- Very mature (decades of production use)
- Full DML tracking (INSERT/UPDATE/DELETE)
- Partition-level refresh granularity
- Rich query rewrite capabilities (transparent query routing to MVs)
- ON COMMIT and ON DEMAND refresh modes

**Weaknesses vs. TimescaleDB**:
- Commercial license, expensive
- Not PostgreSQL ecosystem
- MV logs consume storage and impact write performance
- Complex configuration for optimal performance
- No time-series-specific optimizations

**Best for**: Enterprise data warehousing on Oracle.

### 3.4 Summary: When to Use What

| Scenario | Best Choice | Runner-Up |
|----------|------------|-----------|
| Time-series with PostgreSQL ecosystem | **TimescaleDB** | pg_ivm (if low write throughput) |
| Append-only high-throughput analytics | **ClickHouse** | TimescaleDB |
| Real-time dashboards (sub-second) | **Materialize** | RisingWave |
| Streaming analytics pipeline | **Flink SQL** | RisingWave |
| PostgreSQL compatibility + real-time | **RisingWave** | Materialize |
| Enterprise data warehouse | **Oracle** | TimescaleDB |
| Simple PostgreSQL aggregates | **pg_ivm** | TimescaleDB |

---

## 4. Pros, Cons, Performance Regressions, and Required Improvements

### 4.1 Pros of TimescaleDB Continuous Aggregates

1. **PostgreSQL-native**: Runs as an extension — no separate system to operate. Uses standard PostgreSQL SQL, transactions, and tooling.

2. **Correct incremental refresh**: DELETE+INSERT or MERGE guarantees exact results. No approximate or eventually-consistent semantics.

3. **Real-time mode**: UNION ALL architecture provides fresh results without waiting for refresh, with minimal query overhead.

4. **Watermark dampening**: Smart invalidation threshold prevents write amplification for hot (recent) data.

5. **Compression integration**: 33x storage reduction reported in production. Direct compression on refresh is a significant optimization.

6. **Hierarchical aggregation**: Built-in support for multi-level rollups (hourly → daily → monthly) with automatic invalidation propagation.

7. **Variable-width buckets**: First-class support for calendar-aware intervals (monthly, yearly) — a requirement many alternatives lack.

8. **Battle-tested at scale**: Cloudflare production deployment at 100K rows/sec ingestion with 5-35x query speedups.

9. **Batched and concurrent refresh**: Since v2.21, refresh can be parallelized and batched, reducing lock contention and latency.

10. **MERGE-based refresh**: On PG15+, atomic MERGE replaces the DELETE+INSERT pattern, reducing index churn and WAL volume.

### 4.2 Cons of TimescaleDB Continuous Aggregates

1. **Refresh latency**: Deferred refresh means data is stale for the configured interval (typically minutes). Real-time mode mitigates this but adds query overhead.

2. **Single hypertable limitation**: Only one hypertable in the FROM clause. Multi-hypertable aggregates are not supported.

3. **No schema evolution**: Cannot ALTER a continuous aggregate definition. Must drop and recreate (losing materialized data or requiring migration).

4. **JOIN tracking blind spot**: Changes to regular (non-hypertable) tables in JOINs are not tracked. Stale dimension data silently corrupts results.

5. **Query restrictions**: No DISTINCT, no FILTER, no ORDER BY in definition, no subqueries, no CTEs, no GROUPING SETS/ROLLUP/CUBE, no LIMIT/OFFSET. These restrictions are significant for complex analytics.

6. **TSL licensing**: Continuous aggregates require the Timescale License (not Apache). This matters for self-hosted deployments and forks.

7. **Operational complexity**: Requires understanding of invalidation logs, watermarks, refresh windows, and bucket alignment. Misconfigured policies can cause data gaps or excessive resource usage.

8. **Limited monitoring**: No built-in dashboards or metrics for refresh lag, invalidation backlog, or materialization throughput.

### 4.3 Known Performance Regressions

#### 4.3.1 Timezone-Aware Refresh (GitHub #4662)

**Problem**: Refreshing a CAgg with `time_bucket(interval, ts, timezone)` can consume 100% CPU and memory for 30+ minutes, while the equivalent direct query completes in seconds.

**Root Cause**: The refresh mechanism cannot constrain queries to relevant chunks when timezone parameters are involved, resulting in full-table scans.

**Impact**: Severe — renders timezone-aware CAggs unusable at scale.

**Status**: Partially addressed but still a pain point.

#### 4.3.2 DST Transition Failures (GitHub #8898)

**Problem**: Sub-hour CAgg refreshes fail with "range lower bound must be less than or equal to range upper bound" after DST fallback transitions.

**Root Cause**: Inverted materialization windows when DST causes time to "go back."

**Impact**: Moderate — affects sub-hour CAggs in DST-observing timezones twice a year.

#### 4.3.3 Excessive Disk Space (GitHub #3325)

**Problem**: Materialization tables store one row per source chunk per group, leading to larger-than-expected storage.

**Root Cause**: When source hypertable has hourly chunks and CAgg uses daily buckets, 24 partial rows are stored per day per group instead of 1.

**Impact**: Moderate — affects storage costs and index sizes.

#### 4.3.4 Hierarchical CAgg Refresh Lag (GitHub #5726)

**Problem**: Hierarchical CAggs with `end_offset=NULL` and `materialized_only=true` fail to refresh to the latest parent results.

**Root Cause**: Watermark calculation doesn't account for parent CAgg's refresh window correctly.

**Impact**: Moderate — affects hierarchical CAgg deployments.

#### 4.3.5 Slow Initial Creation (GitHub #4444)

**Problem**: Creating CAggs before inserting data leads to performance degradation. Dropping and recreating after data insertion helps.

**Root Cause**: Planner statistics and chunk layout are suboptimal when CAgg is created on empty table.

**Impact**: Low — workaround exists (create after data).

### 4.4 Required Improvements

#### Critical Priority

1. **Fix timezone-aware refresh performance**: The full-table-scan behavior during timezone-aware refresh (#4662) is a showstopper for many real-world use cases. Chunk pruning must work correctly with timezone parameters.

2. **Fix DST transition handling**: Sub-hour CAggs must handle DST transitions without errors (#8898). This requires careful interval arithmetic at DST boundaries.

3. **Schema evolution support**: `ALTER MATERIALIZED VIEW ... AS SELECT ...` should allow modifying the aggregate definition without full rebuild. At minimum: adding/removing non-key columns, changing aggregation functions.

#### High Priority

4. **Multi-hypertable JOINs**: Support JOINs between multiple hypertables in the aggregate definition. This is the #1 requested feature beyond current capabilities.

5. **Tracked JOINs**: When a CAgg joins a hypertable with a regular table, changes to the regular table should be optionally trackable (via triggers or polling).

6. **Richer query support**: Allow DISTINCT, FILTER clause, and HAVING in CAgg definitions. These are common in analytics queries and their absence forces users into workarounds.

7. **Better monitoring**: Expose metrics for refresh lag, invalidation backlog size, materialization throughput, and refresh duration via `pg_stat` views or similar.

#### Medium Priority

8. **Configurable refresh latency target**: Allow users to specify a target freshness (e.g., "within 30 seconds") and have the system auto-tune refresh frequency.

9. **Partial materialization**: For very large time ranges, support materializing only "hot" ranges while leaving cold data to be computed on demand.

10. **Materialization storage deduplication**: When source chunks align poorly with CAgg buckets, deduplicate partial rows to reduce storage overhead (#3325).

11. **Streaming/low-latency mode**: Optional immediate materialization (like pg_ivm) for CAggs where sub-second freshness matters and write throughput is low enough.

12. **Migration tooling**: Tools to convert existing PostgreSQL materialized views or plain aggregate queries into continuous aggregates with minimal downtime.

#### Low Priority

13. **Window function support**: Allow window functions in CAgg definitions (currently experimental/gated).

14. **GROUPING SETS/ROLLUP/CUBE**: Support multi-level aggregation in a single CAgg definition.

15. **Cross-database materialization**: Materialize aggregates from data in external sources (foreign data wrappers, logical replication).

---

## Appendix A: Theoretical Background

### Incremental View Maintenance (IVM) Theory

The academic state of the art distinguishes several approaches:

1. **Delta Queries**: Traditional approach. For each DML, compute the delta on the view and apply it. Works well for simple queries but O(N) for joins.

2. **Differential Dataflow**: Maintains versioned state and computes diffs. Foundation for Materialize. Optimal for streaming workloads.

3. **DBSP (Database Stream Processing)**: Formalized in VLDB 2023. Uses four operators from digital signal processing to express any relational query as an incremental dataflow. Foundation for Feldera.

4. **Heavy/Light Partitioning**: Adaptive strategy for join queries. Partitions keys by frequency to optimize update time. Achieves O(N^1/2) for triangle queries.

**Theoretical limits**: For Q-hierarchical queries, constant-time updates with constant-delay enumeration are achievable and provably optimal. For non-hierarchical queries (e.g., triangle queries), constant-time updates and constant-delay enumeration cannot be achieved simultaneously (conditional on OMv conjecture).

TimescaleDB's approach sits in the "delta queries with deferred batch application" category — invalidations are accumulated and applied in batch during refresh. This is simpler than differential dataflow but has inherently higher latency.

### Key Insight

The fundamental trade-off in IVM is **write-time cost vs. read-time freshness**:

- **Immediate IVM** (pg_ivm, Materialize): Every write pays the cost of updating all dependent views. Reads are always fresh.
- **Deferred IVM** (TimescaleDB, Oracle): Writes are fast (only log invalidation). Reads may be stale. Refresh batches reduce overhead.
- **Insert-triggered** (ClickHouse): Writes pay moderate cost. Only inserts are handled — updates/deletes require workarounds.

TimescaleDB's deferred approach is well-suited for time-series workloads where:
- Write throughput is high (100K+ rows/sec)
- Data is mostly append-only (inserts >> updates/deletes)
- Staleness of seconds-to-minutes is acceptable
- PostgreSQL ecosystem compatibility is required

---

## Appendix B: Sources

### Official Documentation
- TimescaleDB Docs: About Continuous Aggregates
- TimescaleDB Docs: Create a Continuous Aggregate
- TimescaleDB Docs: Real-Time Aggregates
- TimescaleDB Docs: Hierarchical Continuous Aggregates
- TimescaleDB Docs: Troubleshooting

### GitHub Issues
- #4662: Refresh vs. direct query performance (timezone)
- #4444: Very slow continuous aggregate
- #3325: Excessive disk space
- #4950: Lag behind one period
- #8898: DST transition failures
- #5726: Hierarchical CAgg refresh issues
- #5179: Better error messages

### Production Case Studies
- Cloudflare: TimescaleDB for Zero Trust analytics (July 2025)

### Academic Papers
- DBSP: Automatic Incremental View Maintenance (VLDB 2023)
- Recent Increments in Incremental View Maintenance (PODS 2024)
- Materialized View Maintenance: Issues, Classification, and Open Challenges (2019 Survey)

### Alternative Systems
- pg_ivm: PostgreSQL Incremental View Maintenance extension
- ClickHouse: Materialized Views documentation
- Materialize: Incremental Computation guide
- RisingWave: Materialized Views documentation
- Apache Flink: Continuous Queries on Dynamic Tables
- Oracle: Fast Refresh documentation
