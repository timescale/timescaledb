/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <access/table.h>
#include <access/genam.h>
#include <access/tableam.h>
#include <access/htup_details.h>
#include <catalog/pg_class.h>
#include <catalog/indexing.h>
#include <catalog/namespace.h>
#include <catalog/pg_namespace.h>
#include <utils/builtins.h>
#include <utils/syscache.h>
#include <utils/snapmgr.h>
#include <storage/lmgr.h>
#include <fmgr.h>

#include "stats.h"
#include "ts_catalog/catalog.h"
#include "ts_catalog/continuous_agg.h"
#include "chunk.h"
#include "extension.h"
#include "hypertable_cache.h"
#include "debug_point.h"
#include "utils.h"

typedef struct StatsContext
{
	TelemetryStats *stats;
	Snapshot snapshot;
} StatsContext;

/*
 * Determine the type of a hypertable.
 */
static StatsRelType
classify_hypertable(const Hypertable *ht)
{
	if (TS_HYPERTABLE_IS_INTERNAL_COMPRESSION_TABLE(ht))
	{
		/*
		 * This is an internal compression table, but could be for a
		 * regular hypertable, a distributed member hypertable, or for
		 * an internal materialized hypertable (cagg). The latter case
		 * is currently not handled
		 */
		return RELTYPE_COMPRESSION_HYPERTABLE;
	}
	else
	{
		/*
		 * Not dealing with an internal compression hypertable, but
		 * could be a materialized hypertable (cagg) unless it is
		 * distributed.
		 */
		switch (ht->fd.replication_factor)
		{
			case HYPERTABLE_DISTRIBUTED_MEMBER:
				return RELTYPE_DISTRIBUTED_HYPERTABLE_MEMBER;
			case HYPERTABLE_REGULAR:
			{
				const ContinuousAgg *cagg = ts_continuous_agg_find_by_mat_hypertable_id(ht->fd.id);

				if (cagg)
					return RELTYPE_MATERIALIZED_HYPERTABLE;

				return RELTYPE_HYPERTABLE;
			}
			default:
				Assert(ht->fd.replication_factor >= 1);
				return RELTYPE_DISTRIBUTED_HYPERTABLE;
		}
	}
}

static StatsRelType
classify_chunk(Cache *htcache, const Hypertable **ht, const Chunk *chunk)
{
	StatsRelType parent_reltype;

	Assert(NULL != chunk);
	/* Classify the chunk's parent */
	*ht = ts_hypertable_cache_get_entry(htcache, chunk->hypertable_relid, CACHE_FLAG_MISSING_OK);
	Assert(NULL != *ht);
	parent_reltype = classify_hypertable(*ht);

	/* Classify the chunk's parent */
	switch (parent_reltype)
	{
		case RELTYPE_HYPERTABLE:
			return RELTYPE_CHUNK;
		case RELTYPE_DISTRIBUTED_HYPERTABLE:
			return RELTYPE_DISTRIBUTED_CHUNK;
		case RELTYPE_DISTRIBUTED_HYPERTABLE_MEMBER:
			return RELTYPE_DISTRIBUTED_CHUNK_MEMBER;
		case RELTYPE_MATERIALIZED_HYPERTABLE:
			return RELTYPE_MATERIALIZED_CHUNK;
		case RELTYPE_COMPRESSION_HYPERTABLE:
			return RELTYPE_COMPRESSION_CHUNK;
		default:
			/* Shouldn't really get here */
			return RELTYPE_OTHER;
	}
}

static StatsRelType
classify_table(const Form_pg_class class, Cache *htcache, const Hypertable **ht,
			   const Chunk **chunk)
{
	Assert(class->relkind == RELKIND_RELATION);

	if (class->relispartition)
		return RELTYPE_PARTITION;

	/* Check if it is a hypertable */
	*ht = ts_hypertable_cache_get_entry(htcache, class->oid, CACHE_FLAG_MISSING_OK);

	if (*ht)
		return classify_hypertable(*ht);

	/* Check if it is a chunk */
	*chunk = ts_chunk_get_by_relid(class->oid, false);

	if (NULL != *chunk)
		return classify_chunk(htcache, ht, *chunk);

	return RELTYPE_TABLE;
}

static StatsRelType
classify_partitioned_table(const Form_pg_class class)
{
	Assert(class->relkind == RELKIND_PARTITIONED_TABLE);

	/*
	 * If the partitioned table itself is a partition, then it is a partition
	 * in a multi-dimensional partitioned table. Treat it as a partition so
	 * that only "root" tables are counted as partitioned tables.
	 */
	if (class->relispartition)
		return RELTYPE_PARTITION;

	return RELTYPE_PARTITIONED_TABLE;
}

static StatsRelType
classify_foreign_table(Cache *htcache, Oid relid, const Hypertable **ht, const Chunk **chunk)
{
	*chunk = ts_chunk_get_by_relid(relid, false);

	if (*chunk)
		return classify_chunk(htcache, ht, *chunk);

	/*
	 * Currently don't care about non-chunk foreign tables, so classify as
	 * "other".
	 */
	return RELTYPE_OTHER;
}

static StatsRelType
classify_view(const Form_pg_class class, Cache *htcache, const ContinuousAgg **cagg)
{
	const Catalog *catalog = ts_catalog_get();

	if (class->relnamespace == catalog->extension_schema_id[TS_INTERNAL_SCHEMA])
		return RELTYPE_OTHER;

	*cagg = ts_continuous_agg_find_by_relid(class->oid);

	if (*cagg)
		return RELTYPE_CONTINUOUS_AGG;

	return RELTYPE_VIEW;
}

static StatsRelType
classify_relation(const Form_pg_class class, Cache *htcache, const Hypertable **ht,
				  const Chunk **chunk, const ContinuousAgg **cagg)
{
	*chunk = NULL;
	*ht = NULL;
	*cagg = NULL;

	switch (class->relkind)
	{
		case RELKIND_RELATION:
			return classify_table(class, htcache, ht, chunk);
		case RELKIND_PARTITIONED_TABLE:
			return classify_partitioned_table(class);
		case RELKIND_FOREIGN_TABLE:
			return classify_foreign_table(htcache, class->oid, ht, chunk);
		case RELKIND_MATVIEW:
			return RELTYPE_MATVIEW;
		case RELKIND_VIEW:
			return classify_view(class, htcache, cagg);
		default:
			return RELTYPE_OTHER;
	}
}

static void
add_storage(StorageStats *stats, Form_pg_class class)
{
	RelationSize relsize;

	relsize = ts_relation_size_impl(class->oid);
	stats->relsize.total_size += relsize.total_size;
	stats->relsize.heap_size += relsize.heap_size;
	stats->relsize.toast_size += relsize.toast_size;
	stats->relsize.index_size += relsize.index_size;
}

static void
process_relation(BaseStats *stats, Form_pg_class class)
{
	stats->relcount++;

	/*
	 * As of PG14, pg_class.reltuples is set to -1 when the row count is
	 * unknown. Make sure we only add the count when the information is
	 * available.
	 */
	if (class->reltuples > 0)
		stats->reltuples += class->reltuples;

	if (RELKIND_HAS_STORAGE(class->relkind))
		add_storage((StorageStats *) stats, class);
}

static void
process_hypertable(HyperStats *hyp, Form_pg_class class, const Hypertable *ht)
{
	process_relation(&hyp->storage.base, class);

	if (TS_HYPERTABLE_HAS_COMPRESSION_ENABLED(ht))
		hyp->compressed_hypertable_count++;
}

static void
process_distributed_hypertable(HyperStats *hyp, Form_pg_class class, const Hypertable *ht)
{
	hyp->storage.base.relcount++;

	if (TS_HYPERTABLE_HAS_COMPRESSION_ENABLED(ht))
		hyp->compressed_hypertable_count++;

	if (ht->fd.replication_factor > 1)
		hyp->replicated_hypertable_count++;
}

static void
process_continuous_agg(CaggStats *cs, Form_pg_class class, const ContinuousAgg *cagg)
{
	const Hypertable *mat_ht = ts_hypertable_get_by_id(cagg->data.mat_hypertable_id);
	const Hypertable *raw_ht = ts_hypertable_get_by_id(cagg->data.raw_hypertable_id);

	Assert(cagg);

	process_relation(&cs->hyp.storage.base, class);

	if (TS_HYPERTABLE_HAS_COMPRESSION_ENABLED(mat_ht))
		cs->hyp.compressed_hypertable_count++;

	if (hypertable_is_distributed(raw_ht))
		cs->on_distributed_hypertable_count++;

	if (!cagg->data.materialized_only)
		cs->uses_real_time_aggregation_count++;

	if (ContinuousAggIsFinalized(cagg))
		cs->finalized++;

	if (cagg->data.parent_mat_hypertable_id != INVALID_HYPERTABLE_ID)
		cs->nested++;
}

static void
process_partition(HyperStats *stats, Form_pg_class class, bool ischunk)
{
	stats->child_count++;
	/*
	 * Note that reltuples should be correct even for compressed chunks, since
	 * we "freeze" those stats when a chunk is compressed, and for foreign
	 * table chunks, since we import those stats from data nodes.
	 *
	 * Also, as of PG14, the parent tables include the cumulative stats for
	 * all children, so no need to count the partitions separately since the
	 * sum will be in the root.
	 */
	if (
#if PG14_GE
		ischunk &&
#endif
		class->reltuples > 0)
	{
		stats->storage.base.reltuples += class->reltuples;
	}

	add_storage(&stats->storage, class);
}

/*
 * Add a chunk's stats to the parent table.
 */
static void
add_chunk_stats(HyperStats *stats, Form_pg_class class, const Chunk *chunk,
				const Form_compression_chunk_size fd_compr)
{
	process_partition(stats, class, true);

	if (ts_chunk_is_compressed(chunk))
		stats->compressed_chunk_count++;

	/* Add replica chunks, if any. Only count the extra replicas */
	if (list_length(chunk->data_nodes) > 1)
		stats->replica_chunk_count += (list_length(chunk->data_nodes) - 1);

	/*
	 * A chunk on a distributed hypertable can be marked as compressed but
	 * have no compression stats (the stats exists on the data node and might
	 * not be "imported"). Therefore, the check here is not the same as
	 * above.
	 */
	if (NULL != fd_compr)
	{
		stats->compressed_heap_size += fd_compr->compressed_heap_size;
		stats->compressed_indexes_size += fd_compr->compressed_index_size;
		stats->compressed_toast_size += fd_compr->compressed_toast_size;
		stats->uncompressed_heap_size += fd_compr->uncompressed_heap_size;
		stats->uncompressed_indexes_size += fd_compr->uncompressed_index_size;
		stats->uncompressed_toast_size += fd_compr->uncompressed_toast_size;
		stats->uncompressed_row_count += fd_compr->numrows_pre_compression;
		stats->compressed_row_count += fd_compr->numrows_post_compression;

		/* Also add compressed sizes to total number for entire table */
		stats->storage.relsize.heap_size += fd_compr->compressed_heap_size;
		stats->storage.relsize.toast_size += fd_compr->compressed_toast_size;
		stats->storage.relsize.index_size += fd_compr->compressed_index_size;
	}
}

static bool
get_chunk_compression_stats(StatsContext *statsctx, const Chunk *chunk,
							Form_compression_chunk_size compr_stats)
{
	TupleInfo *ti;
	ScanIterator it;
	bool found = false;

	if (!ts_chunk_is_compressed(chunk))
		return false;

	it = ts_scan_iterator_create(COMPRESSION_CHUNK_SIZE, AccessShareLock, CurrentMemoryContext);
	ts_scan_iterator_set_index(&it, COMPRESSION_CHUNK_SIZE, COMPRESSION_CHUNK_SIZE_PKEY);
	it.ctx.snapshot = statsctx->snapshot;

	ts_scan_iterator_scan_key_reset(&it);
	ts_scan_iterator_scan_key_init(&it,
								   Anum_compression_chunk_size_pkey_chunk_id,
								   BTEqualStrategyNumber,
								   F_INT4EQ,
								   Int32GetDatum(chunk->fd.id));
	ts_scan_iterator_start_or_restart_scan(&it);
	ti = ts_scan_iterator_next(&it);

	if (ti)
	{
		Form_compression_chunk_size fd;
		bool should_free;
		HeapTuple tuple = ts_scan_iterator_fetch_heap_tuple(&it, false, &should_free);

		fd = (Form_compression_chunk_size) GETSTRUCT(tuple);
		memcpy(compr_stats, fd, sizeof(*fd));

		if (should_free)
			heap_freetuple(tuple);

		found = true;
	}

	ts_scan_iterator_close(&it);

	return found;
}

/*
 * Process a relation identified as being a chunk.
 *
 * The chunk could be part of a
 *
 *  - Hypertable
 *  - Distributed hypertable
 *  - Distributed hypertable member
 *  - Materialized hypertable (cagg) chunk
 *  - Internal compression table for hypertable
 *  - Internal compression table for materialized hypertable (cagg)
 *
 * Note that we want to count regular chunks and compressed chunks as part of
 * the same hypertable, although they are children of different tables
 * internally. The same applies to chunks that belong to a continuous
 * aggregate, although in that case there is actually a two-level indirection:
 * The main cagg view is the user-facing relation we'd like to collect stats
 * for, while its chunks are actually stored in a materialized hypertable,
 * and, in a second tier, in a compressed hypertable.
 */
static void
process_chunk(StatsContext *statsctx, StatsRelType chunk_reltype, Form_pg_class class,
			  const Chunk *chunk)
{
	TelemetryStats *stats = statsctx->stats;
	FormData_compression_chunk_size comp_stats_data;
	Form_compression_chunk_size compr_stats = NULL;

	Assert(chunk);

	/*
	 * Ignore compression chunks since we have a separate metadata table with
	 * stats for them
	 */
	if (chunk_reltype == RELTYPE_COMPRESSION_CHUNK)
		return;

	if (get_chunk_compression_stats(statsctx, chunk, &comp_stats_data))
		compr_stats = &comp_stats_data;

	switch (chunk_reltype)
	{
		case RELTYPE_CHUNK:
			add_chunk_stats(&stats->hypertables, class, chunk, compr_stats);
			break;
		case RELTYPE_DISTRIBUTED_CHUNK:
			add_chunk_stats(&stats->distributed_hypertables, class, chunk, compr_stats);
			break;
		case RELTYPE_DISTRIBUTED_CHUNK_MEMBER:
			add_chunk_stats(&stats->distributed_hypertable_members, class, chunk, compr_stats);
			break;
		case RELTYPE_MATERIALIZED_CHUNK:
			add_chunk_stats(&stats->continuous_aggs.hyp, class, chunk, compr_stats);
			break;
		default:
			pg_unreachable();
			break;
	}
}

static bool
is_pg_schema(Oid namespaceid)
{
	static Oid information_schema_oid = InvalidOid;

	if (namespaceid == PG_CATALOG_NAMESPACE || namespaceid == PG_TOAST_NAMESPACE)
		return true;

	if (!OidIsValid(information_schema_oid))
		information_schema_oid = get_namespace_oid("information_schema", false);

	return namespaceid == information_schema_oid;
}

static bool
is_ts_schema(const Catalog *catalog, Oid namespaceid)
{
	int i;

	for (i = 0; i < _TS_MAX_SCHEMA; i++)
	{
		if (namespaceid != catalog->extension_schema_id[TS_INTERNAL_SCHEMA] &&
			namespaceid == catalog->extension_schema_id[i])
			return true;
	}

	return false;
}

static bool
should_ignore_relation(const Catalog *catalog, Form_pg_class class)
{
	return (is_pg_schema(class->relnamespace) || isAnyTempNamespace(class->relnamespace) ||
			is_ts_schema(catalog, class->relnamespace) || ts_is_catalog_table(class->oid));
}

/*
 * Scan the entire pg_class catalog table for all relations. For each
 * relation, classify it and gather stats based on the classification.
 */
void
ts_telemetry_stats_gather(TelemetryStats *stats)
{
	const Catalog *catalog = ts_catalog_get();
	Relation rel;
	SysScanDesc scan;
	Cache *htcache = ts_hypertable_cache_pin();
	MemoryContext oldmcxt, relmcxt;
	StatsContext statsctx = {
		.stats = stats,
		.snapshot = GetActiveSnapshot(),
	};

	MemSet(stats, 0, sizeof(*stats));
	rel = table_open(RelationRelationId, AccessShareLock);
	scan = systable_beginscan(rel, ClassOidIndexId, false, NULL, 0, NULL);
	relmcxt = AllocSetContextCreate(CurrentMemoryContext, "RelationStats", ALLOCSET_DEFAULT_SIZES);

	while (true)
	{
		HeapTuple tup;
		Form_pg_class class;
		StatsRelType reltype;
		const Chunk *chunk = NULL;
		const Hypertable *ht = NULL;
		const ContinuousAgg *cagg = NULL;

		tup = systable_getnext(scan);

		if (!HeapTupleIsValid(tup))
			break;

		class = (Form_pg_class) GETSTRUCT(tup);

		if (should_ignore_relation(catalog, class))
			continue;

		/* Lock the relation to ensure it does not disappear while we process
		 * it */
		LockRelationOid(class->oid, AccessShareLock);

		/* Now that the lock is acquired, ensure the relation still
		 * exists. Otherwise, ignore the relation and release the useless
		 * lock. */
		if (!SearchSysCacheExists1(RELOID, ObjectIdGetDatum(class->oid)))
		{
			UnlockRelationOid(class->oid, AccessShareLock);
			continue;
		}

		/*
		 * Use temporary per-relation memory context to not accumulate cruft
		 * during processing of pg_class.
		 */
		oldmcxt = MemoryContextSwitchTo(relmcxt);
		MemoryContextReset(relmcxt);

		reltype = classify_relation(class, htcache, &ht, &chunk, &cagg);

		DEBUG_WAITPOINT("telemetry_classify_relation");

		switch (reltype)
		{
			case RELTYPE_HYPERTABLE:
				Assert(NULL != ht);
				process_hypertable(&stats->hypertables, class, ht);
				break;
			case RELTYPE_DISTRIBUTED_HYPERTABLE:
				Assert(NULL != ht);
				process_distributed_hypertable(&stats->distributed_hypertables, class, ht);
				break;
			case RELTYPE_DISTRIBUTED_HYPERTABLE_MEMBER:
				/*
				 * Since this is just a hypertable on a data node, process as
				 * a regular hypertable.
				 */
				Assert(NULL != ht);
				process_hypertable(&stats->distributed_hypertable_members, class, ht);
				break;
			case RELTYPE_TABLE:
				process_relation(&stats->tables.base, class);
				break;
			case RELTYPE_PARTITIONED_TABLE:
				process_relation(&stats->partitioned_tables.storage.base, class);
				break;
			case RELTYPE_CHUNK:
			case RELTYPE_DISTRIBUTED_CHUNK:
			case RELTYPE_DISTRIBUTED_CHUNK_MEMBER:
			case RELTYPE_COMPRESSION_CHUNK:
			case RELTYPE_MATERIALIZED_CHUNK:
				Assert(NULL != chunk);
				process_chunk(&statsctx, reltype, class, chunk);
				break;
			case RELTYPE_PARTITION:
				process_partition(&stats->partitioned_tables, class, false);
				break;
			case RELTYPE_VIEW:
				/* Filter internal cagg views */
				if (class->relnamespace != catalog->extension_schema_id[TS_INTERNAL_SCHEMA])
					process_relation(&stats->views, class);
				break;
			case RELTYPE_MATVIEW:
				process_relation(&stats->materialized_views.base, class);
				break;
			case RELTYPE_CONTINUOUS_AGG:
				Assert(NULL != cagg);
				process_continuous_agg(&stats->continuous_aggs, class, cagg);
				break;
				/* No stats collected for types below */
			case RELTYPE_COMPRESSION_HYPERTABLE:
			case RELTYPE_MATERIALIZED_HYPERTABLE:
			case RELTYPE_OTHER:
				break;
		}

		UnlockRelationOid(class->oid, AccessShareLock);
		MemoryContextSwitchTo(oldmcxt);
	}

	systable_endscan(scan);
	table_close(rel, AccessShareLock);
	ts_cache_release(htcache);
	MemoryContextDelete(relmcxt);
}
