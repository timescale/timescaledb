/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

/* This file contains the implementation for SQL utility functions that
 *  compress and decompress chunks
 */
#include <postgres.h>
#include <access/tableam.h>
#include <access/xact.h>
#include <catalog/dependency.h>
#include <catalog/index.h>
#include <commands/event_trigger.h>
#include <commands/tablecmds.h>
#include <commands/trigger.h>
#include <libpq-fe.h>
#include <miscadmin.h>
#include <nodes/makefuncs.h>
#include <nodes/parsenodes.h>
#include <nodes/pg_list.h>
#include <parser/parse_func.h>
#include <storage/lmgr.h>
#include <trigger.h>
#include <utils/builtins.h>
#include <utils/elog.h>
#include <utils/fmgrprotos.h>
#include <utils/inval.h>
#include <utils/snapmgr.h>

#include "compat/compat.h"
#include "api.h"
#include "cache.h"
#include "chunk.h"
#include "compression.h"
#include "compression_storage.h"
#include "compressionam_handler.h"
#include "create.h"
#include "debug_point.h"
#include "error_utils.h"
#include "errors.h"
#include "hypercube.h"
#include "hypertable.h"
#include "hypertable_cache.h"
#include "scan_iterator.h"
#include "scanner.h"
#include "ts_catalog/array_utils.h"
#include "ts_catalog/catalog.h"
#include "ts_catalog/chunk_column_stats.h"
#include "ts_catalog/compression_chunk_size.h"
#include "ts_catalog/compression_settings.h"
#include "ts_catalog/continuous_agg.h"
#include "utils.h"
#include "wal_utils.h"

typedef struct CompressChunkCxt
{
	Hypertable *srcht;
	Chunk *srcht_chunk;		 /* chunk from srcht */
	Hypertable *compress_ht; /*compressed table for srcht */
} CompressChunkCxt;

static Oid get_compressed_chunk_index_for_recompression(Chunk *uncompressed_chunk);
static Oid recompress_chunk_segmentwise_impl(Chunk *chunk);

static Node *
create_dummy_query()
{
	RawStmt *query = NULL;
	query = makeNode(RawStmt);
	query->stmt = (Node *) makeNode(SelectStmt);
	return (Node *) query;
}

void
compression_chunk_size_catalog_insert(int32 src_chunk_id, const RelationSize *src_size,
									  int32 compress_chunk_id, const RelationSize *compress_size,
									  int64 rowcnt_pre_compression, int64 rowcnt_post_compression,
									  int64 rowcnt_frozen)
{
	Catalog *catalog = ts_catalog_get();
	Relation rel;
	TupleDesc desc;
	CatalogSecurityContext sec_ctx;

	Datum values[Natts_compression_chunk_size];
	bool nulls[Natts_compression_chunk_size] = { false };

	rel = table_open(catalog_get_table_id(catalog, COMPRESSION_CHUNK_SIZE), RowExclusiveLock);
	desc = RelationGetDescr(rel);

	memset(values, 0, sizeof(values));

	values[AttrNumberGetAttrOffset(Anum_compression_chunk_size_chunk_id)] =
		Int32GetDatum(src_chunk_id);
	values[AttrNumberGetAttrOffset(Anum_compression_chunk_size_compressed_chunk_id)] =
		Int32GetDatum(compress_chunk_id);
	values[AttrNumberGetAttrOffset(Anum_compression_chunk_size_uncompressed_heap_size)] =
		Int64GetDatum(src_size->heap_size);
	values[AttrNumberGetAttrOffset(Anum_compression_chunk_size_uncompressed_toast_size)] =
		Int64GetDatum(src_size->toast_size);
	values[AttrNumberGetAttrOffset(Anum_compression_chunk_size_uncompressed_index_size)] =
		Int64GetDatum(src_size->index_size);
	values[AttrNumberGetAttrOffset(Anum_compression_chunk_size_compressed_heap_size)] =
		Int64GetDatum(compress_size->heap_size);
	values[AttrNumberGetAttrOffset(Anum_compression_chunk_size_compressed_toast_size)] =
		Int64GetDatum(compress_size->toast_size);
	values[AttrNumberGetAttrOffset(Anum_compression_chunk_size_compressed_index_size)] =
		Int64GetDatum(compress_size->index_size);
	values[AttrNumberGetAttrOffset(Anum_compression_chunk_size_numrows_pre_compression)] =
		Int64GetDatum(rowcnt_pre_compression);
	values[AttrNumberGetAttrOffset(Anum_compression_chunk_size_numrows_post_compression)] =
		Int64GetDatum(rowcnt_post_compression);
	values[AttrNumberGetAttrOffset(Anum_compression_chunk_size_numrows_frozen_immediately)] =
		Int64GetDatum(rowcnt_frozen);

	ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);
	ts_catalog_insert_values(rel, desc, values, nulls);
	ts_catalog_restore_user(&sec_ctx);
	table_close(rel, RowExclusiveLock);
}

static int
compression_chunk_size_catalog_update_merged(int32 chunk_id, const RelationSize *size,
											 int32 merge_chunk_id, const RelationSize *merge_size,
											 int64 merge_rowcnt_pre_compression,
											 int64 merge_rowcnt_post_compression)
{
	ScanIterator iterator =
		ts_scan_iterator_create(COMPRESSION_CHUNK_SIZE, RowExclusiveLock, CurrentMemoryContext);
	bool updated = false;

	iterator.ctx.index =
		catalog_get_index(ts_catalog_get(), COMPRESSION_CHUNK_SIZE, COMPRESSION_CHUNK_SIZE_PKEY);
	ts_scan_iterator_scan_key_init(&iterator,
								   Anum_compression_chunk_size_pkey_chunk_id,
								   BTEqualStrategyNumber,
								   F_INT4EQ,
								   Int32GetDatum(chunk_id));
	ts_scanner_foreach(&iterator)
	{
		Datum values[Natts_compression_chunk_size];
		bool replIsnull[Natts_compression_chunk_size] = { false };
		bool repl[Natts_compression_chunk_size] = { false };
		bool should_free;
		TupleInfo *ti = ts_scan_iterator_tuple_info(&iterator);
		HeapTuple tuple = ts_scanner_fetch_heap_tuple(ti, false, &should_free);
		HeapTuple new_tuple;
		heap_deform_tuple(tuple, ts_scanner_get_tupledesc(ti), values, replIsnull);

		/* Increment existing sizes with sizes from uncompressed chunk. */
		values[AttrNumberGetAttrOffset(Anum_compression_chunk_size_uncompressed_heap_size)] =
			Int64GetDatum(size->heap_size +
						  DatumGetInt64(values[AttrNumberGetAttrOffset(
							  Anum_compression_chunk_size_uncompressed_heap_size)]));
		repl[AttrNumberGetAttrOffset(Anum_compression_chunk_size_uncompressed_heap_size)] = true;
		values[AttrNumberGetAttrOffset(Anum_compression_chunk_size_uncompressed_toast_size)] =
			Int64GetDatum(size->toast_size +
						  DatumGetInt64(values[AttrNumberGetAttrOffset(
							  Anum_compression_chunk_size_uncompressed_toast_size)]));
		repl[AttrNumberGetAttrOffset(Anum_compression_chunk_size_uncompressed_toast_size)] = true;
		values[AttrNumberGetAttrOffset(Anum_compression_chunk_size_uncompressed_index_size)] =
			Int64GetDatum(size->index_size +
						  DatumGetInt64(values[AttrNumberGetAttrOffset(
							  Anum_compression_chunk_size_uncompressed_index_size)]));
		repl[AttrNumberGetAttrOffset(Anum_compression_chunk_size_uncompressed_index_size)] = true;
		values[AttrNumberGetAttrOffset(Anum_compression_chunk_size_compressed_heap_size)] =
			Int64GetDatum(merge_size->heap_size);
		repl[AttrNumberGetAttrOffset(Anum_compression_chunk_size_compressed_heap_size)] = true;
		values[AttrNumberGetAttrOffset(Anum_compression_chunk_size_compressed_toast_size)] =
			Int64GetDatum(merge_size->toast_size);
		repl[AttrNumberGetAttrOffset(Anum_compression_chunk_size_compressed_toast_size)] = true;
		values[AttrNumberGetAttrOffset(Anum_compression_chunk_size_compressed_index_size)] =
			Int64GetDatum(merge_size->index_size);
		repl[AttrNumberGetAttrOffset(Anum_compression_chunk_size_compressed_index_size)] = true;
		values[AttrNumberGetAttrOffset(Anum_compression_chunk_size_numrows_pre_compression)] =
			Int64GetDatum(merge_rowcnt_pre_compression +
						  DatumGetInt64(values[AttrNumberGetAttrOffset(
							  Anum_compression_chunk_size_numrows_pre_compression)]));
		repl[AttrNumberGetAttrOffset(Anum_compression_chunk_size_numrows_pre_compression)] = true;
		values[AttrNumberGetAttrOffset(Anum_compression_chunk_size_numrows_post_compression)] =
			Int64GetDatum(merge_rowcnt_post_compression +
						  DatumGetInt64(values[AttrNumberGetAttrOffset(
							  Anum_compression_chunk_size_numrows_post_compression)]));
		repl[AttrNumberGetAttrOffset(Anum_compression_chunk_size_numrows_post_compression)] = true;

		new_tuple =
			heap_modify_tuple(tuple, ts_scanner_get_tupledesc(ti), values, replIsnull, repl);
		ts_catalog_update(ti->scanrel, new_tuple);
		heap_freetuple(new_tuple);

		if (should_free)
			heap_freetuple(tuple);

		updated = true;
		break;
	}

	ts_scan_iterator_end(&iterator);
	ts_scan_iterator_close(&iterator);
	return updated;
}

static void
get_hypertable_or_cagg_name(Hypertable *ht, Name objname)
{
	ContinuousAggHypertableStatus status = ts_continuous_agg_hypertable_status(ht->fd.id);
	if (status == HypertableIsNotContinuousAgg || status == HypertableIsRawTable)
		namestrcpy(objname, NameStr(ht->fd.table_name));
	else if (status == HypertableIsMaterialization)
	{
		ContinuousAgg *cagg = ts_continuous_agg_find_by_mat_hypertable_id(ht->fd.id, false);
		namestrcpy(objname, NameStr(cagg->data.user_view_name));
	}
	else
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("unexpected hypertable status for %s %d",
						NameStr(ht->fd.table_name),
						status)));
	}
}

static void
compresschunkcxt_init(CompressChunkCxt *cxt, Cache *hcache, Oid hypertable_relid, Oid chunk_relid)
{
	Hypertable *srcht = ts_hypertable_cache_get_entry(hcache, hypertable_relid, CACHE_FLAG_NONE);
	Hypertable *compress_ht;
	Chunk *srcchunk;

	ts_hypertable_permissions_check(srcht->main_table_relid, GetUserId());

	if (!TS_HYPERTABLE_HAS_COMPRESSION_TABLE(srcht))
	{
		NameData cagg_ht_name;
		get_hypertable_or_cagg_name(srcht, &cagg_ht_name);
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("compression not enabled on \"%s\"", NameStr(cagg_ht_name)),
				 errdetail("It is not possible to compress chunks on a hypertable or"
						   " continuous aggregate that does not have compression enabled."),
				 errhint("Enable compression using ALTER TABLE/MATERIALIZED VIEW with"
						 " the timescaledb.compress option.")));
	}
	compress_ht = ts_hypertable_get_by_id(srcht->fd.compressed_hypertable_id);
	if (compress_ht == NULL)
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("missing compress hypertable")));
	/* user has to be the owner of the compression table too */
	ts_hypertable_permissions_check(compress_ht->main_table_relid, GetUserId());

	if (!srcht->space) /* something is wrong */
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR), errmsg("missing hyperspace for hypertable")));
	/* refetch the srcchunk with all attributes filled in */
	srcchunk = ts_chunk_get_by_relid(chunk_relid, true);
	ts_chunk_validate_chunk_status_for_operation(srcchunk, CHUNK_COMPRESS, true);
	cxt->srcht = srcht;
	cxt->compress_ht = compress_ht;
	cxt->srcht_chunk = srcchunk;
}

static Chunk *
find_chunk_to_merge_into(Hypertable *ht, Chunk *current_chunk)
{
	int64 max_chunk_interval, current_chunk_interval = 0, compressed_chunk_interval = 0;
	Chunk *previous_chunk;
	Point *p;

	const Dimension *time_dim = hyperspace_get_open_dimension(ht->space, 0);

	if (!time_dim || time_dim->fd.compress_interval_length == 0)
		return NULL;

	Assert(current_chunk->cube->num_slices > 0);
	Assert(current_chunk->cube->slices[0]->fd.dimension_id == time_dim->fd.id);

	max_chunk_interval = time_dim->fd.compress_interval_length;

	p = ts_point_create(current_chunk->cube->num_slices);

	/* First coordinate is the time coordinates and we want it to fall into previous chunk
	 * hence we reduce it by 1
	 */
	p->coordinates[p->num_coords++] = current_chunk->cube->slices[0]->fd.range_start - 1;
	current_chunk_interval = current_chunk->cube->slices[0]->fd.range_end -
							 current_chunk->cube->slices[0]->fd.range_start;

	for (int i = p->num_coords; i < current_chunk->cube->num_slices; i++)
	{
		p->coordinates[p->num_coords++] = current_chunk->cube->slices[i]->fd.range_start;
	}

	previous_chunk = ts_hypertable_find_chunk_for_point(ht, p);

	/* If there is no previous adjacent chunk along the time dimension or
	 * if it hasn't been compressed yet, we can't merge.
	 */
	if (!previous_chunk || !OidIsValid(previous_chunk->fd.compressed_chunk_id))
		return NULL;

	Assert(previous_chunk->cube->num_slices > 0);
	Assert(previous_chunk->cube->slices[0]->fd.dimension_id == time_dim->fd.id);

	compressed_chunk_interval = previous_chunk->cube->slices[0]->fd.range_end -
								previous_chunk->cube->slices[0]->fd.range_start;

	/* If the slices do not match (except on time dimension), we cannot merge the chunks. */
	if (previous_chunk->cube->num_slices != current_chunk->cube->num_slices)
		return NULL;

	for (int i = 1; i < previous_chunk->cube->num_slices; i++)
	{
		if (previous_chunk->cube->slices[i]->fd.id != current_chunk->cube->slices[i]->fd.id)
		{
			return NULL;
		}
	}

	/* If the compressed chunk is full, we can't merge any more. */
	if (compressed_chunk_interval == 0 ||
		compressed_chunk_interval + current_chunk_interval > max_chunk_interval)
		return NULL;

	/* Get reloid of the previous compressed chunk */
	Oid prev_comp_reloid = ts_chunk_get_relid(previous_chunk->fd.compressed_chunk_id, false);
	CompressionSettings *prev_comp_settings = ts_compression_settings_get(prev_comp_reloid);
	CompressionSettings *ht_comp_settings = ts_compression_settings_get(ht->main_table_relid);
	if (!ts_compression_settings_equal(ht_comp_settings, prev_comp_settings))
		return NULL;

	/* We don't support merging chunks with sequence numbers */
	if (get_attnum(prev_comp_reloid, COMPRESSION_COLUMN_METADATA_SEQUENCE_NUM_NAME) !=
		InvalidAttrNumber)
		return NULL;

	return previous_chunk;
}

/* Check if compression order is violated by merging in a new chunk
 * Because data merged in uses higher sequence numbers than any data already in the chunk,
 * the only way the order is guaranteed can be if we know the data we are merging in would come
 * after the existing data according to the compression order. This is true if the data being merged
 * in has timestamps greater than the existing data and the first column in the order by is time
 * ASC.
 */
static bool
check_is_chunk_order_violated_by_merge(CompressChunkCxt *cxt, const Dimension *time_dim,
									   Chunk *mergable_chunk)
{
	const DimensionSlice *mergable_slice =
		ts_hypercube_get_slice_by_dimension_id(mergable_chunk->cube, time_dim->fd.id);
	if (!mergable_slice)
		elog(ERROR, "mergeable chunk has no time dimension slice");
	const DimensionSlice *compressed_slice =
		ts_hypercube_get_slice_by_dimension_id(cxt->srcht_chunk->cube, time_dim->fd.id);
	if (!compressed_slice)
		elog(ERROR, "compressed chunk has no time dimension slice");

	if (mergable_slice->fd.range_start > compressed_slice->fd.range_start &&
		mergable_slice->fd.range_end > compressed_slice->fd.range_start)
	{
		return true;
	}

	CompressionSettings *ht_settings =
		ts_compression_settings_get(mergable_chunk->hypertable_relid);

	char *attname = get_attname(cxt->srcht->main_table_relid, time_dim->column_attno, false);
	int index = ts_array_position(ht_settings->fd.orderby, attname);

	/* Primary dimension column should be first compress_orderby column. */
	if (index != 1)
		return true;

	/*
	 * Sort order must not be DESC for merge. We don't need to check
	 * NULLS FIRST/LAST here because partitioning columns have NOT NULL
	 * constraint.
	 */
	if (ts_array_get_element_bool(ht_settings->fd.orderby_desc, index))
		return true;

	return false;
}

static Oid
compress_chunk_impl(Oid hypertable_relid, Oid chunk_relid)
{
	Oid result_chunk_id = chunk_relid;
	CompressChunkCxt cxt = { 0 };
	Chunk *compress_ht_chunk, *mergable_chunk;
	Cache *hcache;
	RelationSize before_size, after_size;
	CompressionStats cstat;
	bool new_compressed_chunk = false;

	hcache = ts_hypertable_cache_pin();
	compresschunkcxt_init(&cxt, hcache, hypertable_relid, chunk_relid);

	/* acquire locks on src and compress hypertable and src chunk */
	ereport(LOG,
			(errmsg("acquiring locks for compressing \"%s.%s\"",
					get_namespace_name(get_rel_namespace(chunk_relid)),
					get_rel_name(chunk_relid))));
	LockRelationOid(cxt.srcht->main_table_relid, AccessShareLock);
	LockRelationOid(cxt.compress_ht->main_table_relid, AccessShareLock);
	LockRelationOid(cxt.srcht_chunk->table_id, ExclusiveLock);

	/* acquire locks on catalog tables to keep till end of txn */
	LockRelationOid(catalog_get_table_id(ts_catalog_get(), CHUNK), RowExclusiveLock);
	ereport(LOG,
			(errmsg("locks acquired for compressing \"%s.%s\"",
					get_namespace_name(get_rel_namespace(chunk_relid)),
					get_rel_name(chunk_relid))));

	DEBUG_WAITPOINT("compress_chunk_impl_start");

	/*
	 * Re-read the state of the chunk after all locks have been acquired and ensure
	 * it is still uncompressed. Another process running in parallel might have
	 * already performed the compression while we were waiting for the locks to be
	 * acquired.
	 */
	Chunk *chunk_state_after_lock = ts_chunk_get_by_relid(chunk_relid, true);

	/* Throw error if chunk has invalid status for operation */
	ts_chunk_validate_chunk_status_for_operation(chunk_state_after_lock, CHUNK_COMPRESS, true);

	/* get compression properties for hypertable */
	mergable_chunk = find_chunk_to_merge_into(cxt.srcht, cxt.srcht_chunk);
	if (!mergable_chunk)
	{
		/*
		 * Set up a dummy parsetree since we're calling AlterTableInternal
		 * inside create_compress_chunk(). We can use anything here because we
		 * are not calling EventTriggerDDLCommandEnd but we use a parse tree
		 * type that CreateCommandTag can handle to avoid spurious printouts
		 * in the event that EventTriggerDDLCommandEnd is called.
		 */
		EventTriggerAlterTableStart(create_dummy_query());
		/* create compressed chunk and a new table */
		compress_ht_chunk = create_compress_chunk(cxt.compress_ht, cxt.srcht_chunk, InvalidOid);
		new_compressed_chunk = true;
		ereport(LOG,
				(errmsg("new compressed chunk \"%s.%s\" created",
						NameStr(compress_ht_chunk->fd.schema_name),
						NameStr(compress_ht_chunk->fd.table_name))));
		EventTriggerAlterTableEnd();
	}
	else
	{
		/* use an existing compressed chunk to compress into */
		compress_ht_chunk = ts_chunk_get_by_id(mergable_chunk->fd.compressed_chunk_id, true);
		result_chunk_id = mergable_chunk->table_id;
		ereport(LOG,
				(errmsg("merge into existing compressed chunk \"%s.%s\"",
						NameStr(compress_ht_chunk->fd.schema_name),
						NameStr(compress_ht_chunk->fd.table_name))));
	}

	/* Since the compressed relation is created in the same transaction as the tuples that will be
	 * written by the compressor, we can insert the tuple directly in frozen state. This is the same
	 * logic as performed in COPY INSERT FROZEN.
	 *
	 * Note: Tuples inserted with HEAP_INSERT_FROZEN become immediately visible to all transactions
	 * (they violate the MVCC pattern). So, this flag can only be used when creating the compressed
	 * chunk in the same transaction as the compressed tuples are inserted.
	 *
	 * If this isn't the case, then tuples can be seen multiple times by parallel readers - once in
	 * the uncompressed part of the hypertable (since they are not deleted in the transaction) and
	 * once in the compressed part of the hypertable since the MVCC semantic is violated due to the
	 * flag.
	 *
	 * In contrast, when the compressed chunk part is created in the same transaction as the tuples
	 * are written, the compressed chunk (i.e., the catalog entry) becomes visible to other
	 * transactions only after the transaction that performs the compression is committed and
	 * the uncompressed chunk is truncated.
	 */
	int insert_options = new_compressed_chunk ? HEAP_INSERT_FROZEN : 0;

	before_size = ts_relation_size_impl(cxt.srcht_chunk->table_id);

	/*
	 * Calculate and add the column dimension ranges for the src chunk. This has to
	 * be done before the compression. In case of recompression, the logic will get the
	 * min/max entries for the uncompressed portion and reconcile and update the existing
	 * entry for ht/chunk/column combination. This case handles:
	 *
	 * * INSERTs into uncompressed chunk
	 * * UPDATEs into uncompressed chunk
	 *
	 * In case of DELETEs, the entries won't exist in the uncompressed chunk, but since
	 * we are deleting, we will stay within the earlier computed max/min range. This
	 * means that the chunk will not get pruned for a larger range of values. This will
	 * work ok enough if only a few of the compressed chunks get DELETEs down the line.
	 * In the future, we can look at computing min/max entries in the compressed chunk
	 * using the batch metadata and then recompute the range to handle DELETE cases.
	 */
	if (cxt.srcht->range_space)
		ts_chunk_column_stats_calculate(cxt.srcht, cxt.srcht_chunk);

	cstat = compress_chunk(cxt.srcht_chunk->table_id, compress_ht_chunk->table_id, insert_options);

	after_size = ts_relation_size_impl(compress_ht_chunk->table_id);

	if (new_compressed_chunk)
	{
		compression_chunk_size_catalog_insert(cxt.srcht_chunk->fd.id,
											  &before_size,
											  compress_ht_chunk->fd.id,
											  &after_size,
											  cstat.rowcnt_pre_compression,
											  cstat.rowcnt_post_compression,
											  cstat.rowcnt_frozen);

		/* Copy chunk constraints (including fkey) to compressed chunk.
		 * Do this after compressing the chunk to avoid holding strong, unnecessary locks on the
		 * referenced table during compression.
		 */
		ts_chunk_constraints_create(cxt.compress_ht, compress_ht_chunk);
		ts_trigger_create_all_on_chunk(compress_ht_chunk);
		ts_chunk_set_compressed_chunk(cxt.srcht_chunk, compress_ht_chunk->fd.id);
	}
	else
	{
		compression_chunk_size_catalog_update_merged(mergable_chunk->fd.id,
													 &before_size,
													 compress_ht_chunk->fd.id,
													 &after_size,
													 cstat.rowcnt_pre_compression,
													 cstat.rowcnt_post_compression);

		const Dimension *time_dim = hyperspace_get_open_dimension(cxt.srcht->space, 0);
		Assert(time_dim != NULL);

		bool chunk_unordered =
			check_is_chunk_order_violated_by_merge(&cxt, time_dim, mergable_chunk);

		ts_chunk_merge_on_dimension(cxt.srcht, mergable_chunk, cxt.srcht_chunk, time_dim->fd.id);

		if (chunk_unordered)
		{
			ts_chunk_set_unordered(mergable_chunk);
			tsl_compress_chunk_wrapper(mergable_chunk, true, false);
		}
	}

	ts_cache_release(hcache);
	return result_chunk_id;
}

static void
decompress_chunk_impl(Chunk *uncompressed_chunk, bool if_compressed)
{
	Cache *hcache;
	Hypertable *uncompressed_hypertable =
		ts_hypertable_cache_get_cache_and_entry(uncompressed_chunk->hypertable_relid,
												CACHE_FLAG_NONE,
												&hcache);
	Hypertable *compressed_hypertable;
	Chunk *compressed_chunk;

	ts_hypertable_permissions_check(uncompressed_hypertable->main_table_relid, GetUserId());

	if (TS_HYPERTABLE_IS_INTERNAL_COMPRESSION_TABLE(uncompressed_hypertable))
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("decompress_chunk must not be called on the internal compressed chunk")));

	compressed_hypertable =
		ts_hypertable_get_by_id(uncompressed_hypertable->fd.compressed_hypertable_id);
	if (compressed_hypertable == NULL)
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("missing compressed hypertable")));

	if (uncompressed_chunk->fd.hypertable_id != uncompressed_hypertable->fd.id)
		elog(ERROR, "hypertable and chunk do not match");

	if (uncompressed_chunk->fd.compressed_chunk_id == INVALID_CHUNK_ID)
	{
		ts_cache_release(hcache);
		ereport((if_compressed ? NOTICE : ERROR),
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("chunk \"%s\" is not compressed",
						get_rel_name(uncompressed_chunk->table_id))));
		return;
	}

	write_logical_replication_msg_decompression_start();

	ts_chunk_validate_chunk_status_for_operation(uncompressed_chunk, CHUNK_DECOMPRESS, true);
	compressed_chunk = ts_chunk_get_by_id(uncompressed_chunk->fd.compressed_chunk_id, true);

	ereport(LOG,
			(errmsg("acquiring locks for decompressing \"%s.%s\"",
					NameStr(uncompressed_chunk->fd.schema_name),
					NameStr(uncompressed_chunk->fd.table_name))));
	/* acquire locks on src and compress hypertable and src chunk */
	LockRelationOid(uncompressed_hypertable->main_table_relid, AccessShareLock);
	LockRelationOid(compressed_hypertable->main_table_relid, AccessShareLock);

	/*
	 * Acquire an ExclusiveLock on the uncompressed and the compressed
	 * chunk (the chunks can still be accessed by reads).
	 *
	 * The lock on the compressed chunk is needed because it gets deleted
	 * after decompression. The lock on the uncompressed chunk is needed
	 * to avoid deadlocks (e.g., caused by later lock upgrades or parallel
	 * started chunk compressions).
	 *
	 * Note: Also the function decompress_chunk() will request an
	 *       ExclusiveLock on the compressed and on the uncompressed
	 *       chunk. See the comments in function about the concurrency of
	 *       operations.
	 */
	LockRelationOid(uncompressed_chunk->table_id, ExclusiveLock);
	LockRelationOid(compressed_chunk->table_id, ExclusiveLock);

	/* acquire locks on catalog tables to keep till end of txn */
	LockRelationOid(catalog_get_table_id(ts_catalog_get(), CHUNK), RowExclusiveLock);
	ereport(LOG,
			(errmsg("locks acquired for decompressing \"%s.%s\"",
					NameStr(uncompressed_chunk->fd.schema_name),
					NameStr(uncompressed_chunk->fd.table_name))));

	DEBUG_WAITPOINT("decompress_chunk_impl_start");

	/*
	 * Re-read the state of the chunk after all locks have been acquired and ensure
	 * it is still compressed. Another process running in parallel might have
	 * already performed the decompression while we were waiting for the locks to be
	 * acquired.
	 */
	Chunk *chunk_state_after_lock = ts_chunk_get_by_id(uncompressed_chunk->fd.id, true);

	/* Throw error if chunk has invalid status for operation */
	ts_chunk_validate_chunk_status_for_operation(chunk_state_after_lock, CHUNK_DECOMPRESS, true);

	decompress_chunk(compressed_chunk->table_id, uncompressed_chunk->table_id);

	/* Delete the compressed chunk */
	ts_compression_chunk_size_delete(uncompressed_chunk->fd.id);
	ts_chunk_clear_compressed_chunk(uncompressed_chunk);
	ts_compression_settings_delete(compressed_chunk->table_id);

	/*
	 * Lock the compressed chunk that is going to be deleted. At this point,
	 * the reference to the compressed chunk is already removed from the
	 * catalog but we need to block readers from accessing this chunk
	 * until the catalog changes are visible to them.
	 *
	 * Note: Calling performMultipleDeletions in chunk_index_tuple_delete
	 * also requests an AccessExclusiveLock on the compressed_chunk. However,
	 * this call makes the lock on the chunk explicit.
	 */
	LockRelationOid(uncompressed_chunk->table_id, AccessExclusiveLock);
	LockRelationOid(compressed_chunk->table_id, AccessExclusiveLock);
	ts_chunk_drop(compressed_chunk, DROP_RESTRICT, -1);
	ts_cache_release(hcache);
	write_logical_replication_msg_decompression_end();
}

/*
 * Create a new compressed chunk using existing table with compressed data.
 *
 * chunk_relid - non-compressed chunk relid
 * chunk_table - table containing compressed data
 */
Datum
tsl_create_compressed_chunk(PG_FUNCTION_ARGS)
{
	Oid chunk_relid = PG_GETARG_OID(0);
	Oid chunk_table = PG_GETARG_OID(1);
	RelationSize uncompressed_size = { .heap_size = PG_GETARG_INT64(2),
									   .toast_size = PG_GETARG_INT64(3),
									   .index_size = PG_GETARG_INT64(4) };
	RelationSize compressed_size = { .heap_size = PG_GETARG_INT64(5),
									 .toast_size = PG_GETARG_INT64(6),
									 .index_size = PG_GETARG_INT64(7) };
	int64 numrows_pre_compression = PG_GETARG_INT64(8);
	int64 numrows_post_compression = PG_GETARG_INT64(9);
	Chunk *chunk;
	Chunk *compress_ht_chunk;
	Cache *hcache;
	CompressChunkCxt cxt;
	bool chunk_was_compressed;

	Assert(!PG_ARGISNULL(0));
	Assert(!PG_ARGISNULL(1));

	ts_feature_flag_check(FEATURE_HYPERTABLE_COMPRESSION);
	TS_PREVENT_FUNC_IF_READ_ONLY();

	chunk = ts_chunk_get_by_relid(chunk_relid, true);
	hcache = ts_hypertable_cache_pin();
	compresschunkcxt_init(&cxt, hcache, chunk->hypertable_relid, chunk_relid);

	/* Acquire locks on src and compress hypertable and src chunk */
	LockRelationOid(cxt.srcht->main_table_relid, AccessShareLock);
	LockRelationOid(cxt.compress_ht->main_table_relid, AccessShareLock);
	LockRelationOid(cxt.srcht_chunk->table_id, ShareLock);

	/* Acquire locks on catalog tables to keep till end of txn */
	LockRelationOid(catalog_get_table_id(ts_catalog_get(), CHUNK), RowExclusiveLock);

	/*
	 * Set up a dummy parsetree since we're calling AlterTableInternal inside
	 * create_compress_chunk(). We can use anything here because we are not
	 * calling EventTriggerDDLCommandEnd but we use a parse tree type that
	 * CreateCommandTag can handle to avoid spurious printouts.
	 */
	EventTriggerAlterTableStart(create_dummy_query());
	/* Create compressed chunk using existing table */
	compress_ht_chunk = create_compress_chunk(cxt.compress_ht, cxt.srcht_chunk, chunk_table);
	EventTriggerAlterTableEnd();

	/* Copy chunk constraints (including fkey) to compressed chunk */
	ts_chunk_constraints_create(cxt.compress_ht, compress_ht_chunk);
	ts_trigger_create_all_on_chunk(compress_ht_chunk);

	/* Insert empty stats to compression_chunk_size */
	compression_chunk_size_catalog_insert(cxt.srcht_chunk->fd.id,
										  &uncompressed_size,
										  compress_ht_chunk->fd.id,
										  &compressed_size,
										  numrows_pre_compression,
										  numrows_post_compression,
										  0);

	chunk_was_compressed = ts_chunk_is_compressed(cxt.srcht_chunk);
	ts_chunk_set_compressed_chunk(cxt.srcht_chunk, compress_ht_chunk->fd.id);
	if (!chunk_was_compressed && ts_table_has_tuples(cxt.srcht_chunk->table_id, AccessShareLock))
	{
		/* The chunk was not compressed before it had the compressed chunk
		 * attached to it, and it contains rows, so we set it to be partial.
		 */
		ts_chunk_set_partial(cxt.srcht_chunk);
	}
	ts_cache_release(hcache);

	PG_RETURN_OID(chunk_relid);
}

Datum
tsl_compress_chunk(PG_FUNCTION_ARGS)
{
	Oid uncompressed_chunk_id = PG_ARGISNULL(0) ? InvalidOid : PG_GETARG_OID(0);
	bool if_not_compressed = PG_ARGISNULL(1) ? true : PG_GETARG_BOOL(1);
	bool recompress = PG_ARGISNULL(2) ? false : PG_GETARG_BOOL(2);

	ts_feature_flag_check(FEATURE_HYPERTABLE_COMPRESSION);

	TS_PREVENT_FUNC_IF_READ_ONLY();
	Chunk *chunk = ts_chunk_get_by_relid(uncompressed_chunk_id, true);

	uncompressed_chunk_id = tsl_compress_chunk_wrapper(chunk, if_not_compressed, recompress);

	PG_RETURN_OID(uncompressed_chunk_id);
}

Oid
tsl_compress_chunk_wrapper(Chunk *chunk, bool if_not_compressed, bool recompress)
{
	Oid uncompressed_chunk_id = chunk->table_id;

	write_logical_replication_msg_compression_start();

	if (ts_chunk_is_compressed(chunk))
	{
		if (recompress)
		{
			CompressionSettings *ht_settings = ts_compression_settings_get(chunk->hypertable_relid);
			Oid compressed_chunk_relid = ts_chunk_get_relid(chunk->fd.compressed_chunk_id, true);
			CompressionSettings *chunk_settings =
				ts_compression_settings_get(compressed_chunk_relid);

			if (!ts_compression_settings_equal(ht_settings, chunk_settings))
			{
				decompress_chunk_impl(chunk, false);
				compress_chunk_impl(chunk->hypertable_relid, chunk->table_id);
				write_logical_replication_msg_compression_end();
				return uncompressed_chunk_id;
			}
		}
		if (!ts_chunk_needs_recompression(chunk))
		{
			write_logical_replication_msg_compression_end();
			ereport((if_not_compressed ? NOTICE : ERROR),
					(errcode(ERRCODE_DUPLICATE_OBJECT),
					 errmsg("chunk \"%s\" is already compressed", get_rel_name(chunk->table_id))));
			return uncompressed_chunk_id;
		}

		if (ts_chunk_is_partial(chunk) && get_compressed_chunk_index_for_recompression(chunk))
		{
			uncompressed_chunk_id = recompress_chunk_segmentwise_impl(chunk);
		}
		else
		{
			decompress_chunk_impl(chunk, false);
			compress_chunk_impl(chunk->hypertable_relid, chunk->table_id);
		}
	}
	else
	{
		uncompressed_chunk_id = compress_chunk_impl(chunk->hypertable_relid, chunk->table_id);
	}

	write_logical_replication_msg_compression_end();

	return uncompressed_chunk_id;
}

Datum
tsl_decompress_chunk(PG_FUNCTION_ARGS)
{
	Oid uncompressed_chunk_id = PG_ARGISNULL(0) ? InvalidOid : PG_GETARG_OID(0);
	bool if_compressed = PG_ARGISNULL(1) ? true : PG_GETARG_BOOL(1);
	int32 chunk_id;

	ts_feature_flag_check(FEATURE_HYPERTABLE_COMPRESSION);

	TS_PREVENT_FUNC_IF_READ_ONLY();
	Chunk *uncompressed_chunk = ts_chunk_get_by_relid(uncompressed_chunk_id, true);
	chunk_id = uncompressed_chunk->fd.id;

	Hypertable *ht = ts_hypertable_get_by_id(uncompressed_chunk->fd.hypertable_id);
	ts_hypertable_permissions_check(ht->main_table_relid, GetUserId());

	if (!ht->fd.compressed_hypertable_id)
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("missing compressed hypertable")));

	if (!ts_chunk_is_compressed(uncompressed_chunk))
	{
		ereport((if_compressed ? NOTICE : ERROR),
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("chunk \"%s\" is not compressed", get_rel_name(uncompressed_chunk_id))));

		PG_RETURN_NULL();
	}

	decompress_chunk_impl(uncompressed_chunk, if_compressed);

	/*
	 * Post decompression regular DML can happen into this chunk. So, we update
	 * chunk_column_stats entries for this chunk to min/max entries now.
	 */
	ts_chunk_column_stats_reset_by_chunk_id(chunk_id);

	PG_RETURN_OID(uncompressed_chunk_id);
}

/* Sort the tuples and recompress them */
static void
recompress_segment(Tuplesortstate *tuplesortstate, Relation compressed_chunk_rel,
				   RowCompressor *row_compressor)
{
	tuplesort_performsort(tuplesortstate);
	row_compressor_reset(row_compressor);
	row_compressor_append_sorted_rows(row_compressor,
									  tuplesortstate,
									  RelationGetDescr(compressed_chunk_rel),
									  compressed_chunk_rel);
	tuplesort_end(tuplesortstate);
	CommandCounterIncrement();
}

static bool
compressed_chunk_column_is_segmentby(PerCompressedColumn per_compressed_col)
{
	/* not compressed and not metadata, offset should be -1 for metadata */
	return !per_compressed_col.is_compressed && per_compressed_col.decompressed_column_offset >= 0;
}

static void
decompress_segment_update_current_segment(CompressedSegmentInfo **current_segment,
										  TupleTableSlot *slot, PerCompressedColumn *per_col,
										  int16 *segby_col_offsets_compressed, int nsegmentby_cols)
{
	Datum val;
	bool is_null;
	int seg_idx = 0;
	for (int i = 0; i < nsegmentby_cols; i++)
	{
		int16 col_offset = segby_col_offsets_compressed[i];
		if (compressed_chunk_column_is_segmentby(per_col[col_offset]))
		{
			val = slot_getattr(slot, AttrOffsetGetAttrNumber(col_offset), &is_null);
			/* new segment, need to do per-segment processing */
			if (current_segment[seg_idx]->segment_info)
				pfree(current_segment[seg_idx]->segment_info);
			SegmentInfo *segment_info =
				segment_info_new(TupleDescAttr(slot->tts_tupleDescriptor, col_offset));
			segment_info_update(segment_info, val, is_null);
			current_segment[seg_idx]->segment_info = segment_info;
			current_segment[seg_idx]->decompressed_chunk_offset =
				per_col[col_offset].decompressed_column_offset;
			seg_idx++;
		}
	}
}

static bool
decompress_segment_changed_group(CompressedSegmentInfo **current_segment, TupleTableSlot *slot,
								 PerCompressedColumn *per_col, int16 *segby_col_offsets_compressed,
								 int nsegmentby_cols)
{
	Datum val;
	bool is_null;
	bool changed_segment = false;
	int seg_idx = 0;
	for (int i = 0; i < nsegmentby_cols; i++)
	{
		int16 col_offset = segby_col_offsets_compressed[i];
		if (compressed_chunk_column_is_segmentby(per_col[col_offset]))
		{
			val = slot_getattr(slot, AttrOffsetGetAttrNumber(col_offset), &is_null);
			if (!segment_info_datum_is_in_group(current_segment[seg_idx++]->segment_info,
												val,
												is_null))
			{
				changed_segment = true;
				break;
			}
		}
	}
	return changed_segment;
}

/*
 * This is hacky but it doesn't matter. We just want to check for the existence of such an index
 * on the compressed chunk.
 */
extern Datum
tsl_get_compressed_chunk_index_for_recompression(PG_FUNCTION_ARGS)
{
	ts_feature_flag_check(FEATURE_HYPERTABLE_COMPRESSION);
	Oid uncompressed_chunk_id = PG_ARGISNULL(0) ? InvalidOid : PG_GETARG_OID(0);

	Chunk *uncompressed_chunk = ts_chunk_get_by_relid(uncompressed_chunk_id, true);

	Oid index_oid = get_compressed_chunk_index_for_recompression(uncompressed_chunk);

	if (OidIsValid(index_oid))
	{
		PG_RETURN_OID(index_oid);
	}
	else
		PG_RETURN_NULL();
}

static Oid
get_compressed_chunk_index_for_recompression(Chunk *uncompressed_chunk)
{
	Chunk *compressed_chunk = ts_chunk_get_by_id(uncompressed_chunk->fd.compressed_chunk_id, true);

	Relation uncompressed_chunk_rel = table_open(uncompressed_chunk->table_id, ShareLock);
	Relation compressed_chunk_rel = table_open(compressed_chunk->table_id, ShareLock);

	CompressionSettings *settings = ts_compression_settings_get(compressed_chunk->table_id);

	ResultRelInfo *indstate = ts_catalog_open_indexes(compressed_chunk_rel);
	Oid index_oid = get_compressed_chunk_index(indstate, settings);

	ts_catalog_close_indexes(indstate);

	table_close(compressed_chunk_rel, NoLock);
	table_close(uncompressed_chunk_rel, NoLock);

	return index_oid;
}

/*
 * This function fetches the remaining uncompressed chunk rows into
 * the tuplesort for recompression.
 * `unmatched` here means that no existing segment in the compressed
 * chunk matches the segmentby column values for these remaining rows,
 * so new segments must be created for them.
 */
static void
fetch_unmatched_uncompressed_chunk_into_tuplesort(Tuplesortstate *segment_tuplesortstate,
												  Relation uncompressed_chunk_rel,
												  bool *unmatched_rows_exist)
{
	TableScanDesc scan;
	TupleTableSlot *slot = table_slot_create(uncompressed_chunk_rel, NULL);
	Snapshot snapshot = GetLatestSnapshot();
	ScanKeyData scankey = {
		/* Let compression TAM know it should only return tuples from the
		 * non-compressed relation. No actual scankey necessary */
		.sk_flags = SK_NO_COMPRESSED,
	};

	scan = table_beginscan(uncompressed_chunk_rel, snapshot, 0, &scankey);

	while (table_scan_getnextslot(scan, ForwardScanDirection, slot))
	{
		if (!(*unmatched_rows_exist))
			*unmatched_rows_exist = true;

		slot_getallattrs(slot);
		tuplesort_puttupleslot(segment_tuplesortstate, slot);
		simple_table_tuple_delete(uncompressed_chunk_rel, &slot->tts_tid, snapshot);
	}
	ExecDropSingleTupleTableSlot(slot);
	table_endscan(scan);
}

static bool
fetch_matching_uncompressed_chunk_into_tuplesort(Tuplesortstate *segment_tuplesortstate,
												 int nsegmentby_cols,
												 Relation uncompressed_chunk_rel,
												 CompressedSegmentInfo **current_segment)
{
	TableScanDesc scan;
	Snapshot snapshot;
	int index = 0;
	int nsegbycols_nonnull = 0;
	int num_scankeys = 1;
	Bitmapset *null_segbycols = NULL;
	bool matching_exist = false;

	for (int seg_col = 0; seg_col < nsegmentby_cols; seg_col++)
	{
		if (!current_segment[seg_col]->segment_info->is_null)
			nsegbycols_nonnull++;
		/* also build a bms of null columns to check later on */
		else
		{
			int16 attno = current_segment[seg_col]->decompressed_chunk_offset + 1;
			null_segbycols = bms_add_member(null_segbycols, attno);
		}
	}

	if (nsegbycols_nonnull > 0)
		num_scankeys = nsegbycols_nonnull;

	ScanKeyData *scankey = palloc0(sizeof(*scankey) * num_scankeys);

	for (int seg_col = 0; seg_col < nsegmentby_cols; seg_col++)
	{
		Datum val = current_segment[seg_col]->segment_info->val;
		/* Get the attno of this segby col in the uncompressed chunk */
		int16 attno =
			current_segment[seg_col]->decompressed_chunk_offset + 1; /* offset is attno - 1 */

		if (current_segment[seg_col]->segment_info->is_null)
			continue;
		ScanKeyEntryInitializeWithInfo(&scankey[index],
									   0 /*flags */,
									   attno,
									   BTEqualStrategyNumber,
									   InvalidOid,
									   current_segment[seg_col]->segment_info->collation,
									   &current_segment[seg_col]->segment_info->eq_fn,
									   val);
		index++;
	}

	snapshot = GetLatestSnapshot();
	/* Let compression TAM know it should only return tuples from the
	 * non-compressed relation. */
	scankey->sk_flags = SK_NO_COMPRESSED;
	scan = table_beginscan(uncompressed_chunk_rel, snapshot, nsegbycols_nonnull, scankey);
	TupleTableSlot *slot = table_slot_create(uncompressed_chunk_rel, NULL);

	while (table_scan_getnextslot(scan, ForwardScanDirection, slot))
	{
		bool valid = true;
		/* check for NULL values in this segment manually */
		for (int attno = bms_next_member(null_segbycols, -1); attno >= 0;
			 attno = bms_next_member(null_segbycols, attno))
		{
			if (!slot_attisnull(slot, attno))
			{
				valid = false;
				break;
			}
		}
		if (valid)
		{
			matching_exist = true;
			slot_getallattrs(slot);
			tuplesort_puttupleslot(segment_tuplesortstate, slot);
			/* simple_table_tuple_delete since we don't expect concurrent
			 * updates, have exclusive lock on the relation */
			simple_table_tuple_delete(uncompressed_chunk_rel, &slot->tts_tid, snapshot);
		}
	}
	ExecDropSingleTupleTableSlot(slot);
	table_endscan(scan);

	if (null_segbycols != NULL)
		pfree(null_segbycols);

	pfree(scankey);

	return matching_exist;
}

/*
 * Recompress an existing chunk by decompressing the batches
 * that are affected by the addition of newer data. The existing
 * compressed chunk will not be recreated but modified in place.
 *
 * 0 uncompressed_chunk_id REGCLASS
 * 1 if_not_compressed BOOL = false
 */
Datum
tsl_recompress_chunk_segmentwise(PG_FUNCTION_ARGS)
{
	Oid uncompressed_chunk_id = PG_ARGISNULL(0) ? InvalidOid : PG_GETARG_OID(0);
	bool if_not_compressed = PG_ARGISNULL(1) ? true : PG_GETARG_BOOL(1);

	ts_feature_flag_check(FEATURE_HYPERTABLE_COMPRESSION);
	TS_PREVENT_FUNC_IF_READ_ONLY();
	Chunk *chunk = ts_chunk_get_by_relid(uncompressed_chunk_id, true);

	if (!ts_chunk_is_partial(chunk))
	{
		int elevel = if_not_compressed ? NOTICE : ERROR;
		elog(elevel,
			 "nothing to recompress in chunk %s.%s",
			 NameStr(chunk->fd.schema_name),
			 NameStr(chunk->fd.table_name));
	}
	else
	{
		uncompressed_chunk_id = recompress_chunk_segmentwise_impl(chunk);
	}

	PG_RETURN_OID(uncompressed_chunk_id);
}

static Oid
recompress_chunk_segmentwise_impl(Chunk *uncompressed_chunk)
{
	Oid uncompressed_chunk_id = uncompressed_chunk->table_id;

	/*
	 * only proceed if status in (3, 9, 11)
	 * 1: compressed
	 * 2: compressed_unordered
	 * 4: frozen
	 * 8: compressed_partial
	 */
	if (!ts_chunk_is_compressed(uncompressed_chunk) && ts_chunk_is_partial(uncompressed_chunk))
		elog(ERROR,
			 "unexpected chunk status %d in chunk %s.%s",
			 uncompressed_chunk->fd.status,
			 NameStr(uncompressed_chunk->fd.schema_name),
			 NameStr(uncompressed_chunk->fd.table_name));

	/* need it to find the segby cols from the catalog */
	Chunk *compressed_chunk = ts_chunk_get_by_id(uncompressed_chunk->fd.compressed_chunk_id, true);
	CompressionSettings *settings = ts_compression_settings_get(compressed_chunk->table_id);

	int nsegmentby_cols = ts_array_length(settings->fd.segmentby);

	/* new status after recompress should simply be compressed (1)
	 * It is ok to update this early on in the transaction as it keeps a lock
	 * on the updated tuple in the CHUNK table potentially preventing other transaction
	 * from updating it
	 */
	if (ts_chunk_clear_status(uncompressed_chunk,
							  CHUNK_STATUS_COMPRESSED_UNORDERED | CHUNK_STATUS_COMPRESSED_PARTIAL))
		ereport(LOG,
				(errmsg("cleared chunk status for recompression: \"%s.%s\"",
						NameStr(uncompressed_chunk->fd.schema_name),
						NameStr(uncompressed_chunk->fd.table_name))));

	ereport(LOG,
			(errmsg("acquiring locks for recompression: \"%s.%s\"",
					NameStr(uncompressed_chunk->fd.schema_name),
					NameStr(uncompressed_chunk->fd.table_name))));
	/* lock both chunks, compressed and uncompressed */
	/* TODO: Take RowExclusive locks instead of AccessExclusive */
	Relation uncompressed_chunk_rel = table_open(uncompressed_chunk->table_id, ExclusiveLock);
	Relation compressed_chunk_rel = table_open(compressed_chunk->table_id, ExclusiveLock);

	/*
	 * Calculate and add the column dimension ranges for the src chunk. This has to
	 * be done before the compression. In case of recompression, the logic will get the
	 * min/max entries for the uncompressed portion and reconcile and update the existing
	 * entry for ht/chunk/column combination. This case handles:
	 *
	 * * INSERTs into uncompressed chunk
	 * * UPDATEs into uncompressed chunk
	 *
	 * In case of DELETEs, the entries won't exist in the uncompressed chunk, but since
	 * we are deleting, we will stay within the earlier computed max/min range. This
	 * means that the chunk will not get pruned for a larger range of values. This will
	 * work ok enough if only a few of the compressed chunks get DELETEs down the line.
	 * In the future, we can look at computing min/max entries in the compressed chunk
	 * using the batch metadata and then recompute the range to handle DELETE cases.
	 */
	Hypertable *ht = ts_hypertable_get_by_id(uncompressed_chunk->fd.hypertable_id);
	if (ht->range_space)
		ts_chunk_column_stats_calculate(ht, uncompressed_chunk);

	/*************** tuplesort state *************************/
	Tuplesortstate *segment_tuplesortstate;
	TupleDesc compressed_rel_tupdesc = RelationGetDescr(compressed_chunk_rel);
	TupleDesc uncompressed_rel_tupdesc = RelationGetDescr(uncompressed_chunk_rel);

	int n_keys = ts_array_length(settings->fd.segmentby) + ts_array_length(settings->fd.orderby);
	AttrNumber *sort_keys = palloc(sizeof(*sort_keys) * n_keys);
	Oid *sort_operators = palloc(sizeof(*sort_operators) * n_keys);
	Oid *sort_collations = palloc(sizeof(*sort_collations) * n_keys);
	bool *nulls_first = palloc(sizeof(*nulls_first) * n_keys);

	int num_segmentby = ts_array_length(settings->fd.segmentby);
	int num_orderby = ts_array_length(settings->fd.orderby);
	n_keys = num_segmentby + num_orderby;

	for (int n = 0; n < n_keys; n++)
	{
		const char *attname;
		int position;
		if (n < num_segmentby)
		{
			position = n + 1;
			attname = ts_array_get_element_text(settings->fd.segmentby, position);
		}
		else
		{
			position = n - num_segmentby + 1;
			attname = ts_array_get_element_text(settings->fd.orderby, position);
		}
		compress_chunk_populate_sort_info_for_column(settings,
													 RelationGetRelid(uncompressed_chunk_rel),
													 attname,
													 &sort_keys[n],
													 &sort_operators[n],
													 &sort_collations[n],
													 &nulls_first[n]);
	}

	segment_tuplesortstate = tuplesort_begin_heap(uncompressed_rel_tupdesc,
												  n_keys,
												  sort_keys,
												  sort_operators,
												  sort_collations,
												  nulls_first,
												  maintenance_work_mem,
												  NULL,
												  false);

	/******************** row decompressor **************/

	RowDecompressor decompressor = build_decompressor(compressed_chunk_rel, uncompressed_chunk_rel);
	/********** row compressor *******************/
	RowCompressor row_compressor;
	row_compressor_init(settings,
						&row_compressor,
						uncompressed_chunk_rel,
						compressed_chunk_rel,
						compressed_rel_tupdesc->natts,
						true /*need_bistate*/,
						0 /*insert options*/);

	/* create an array of the segmentby column offsets in the compressed chunk */
	int16 *segmentby_column_offsets_compressed =
		palloc(sizeof(*segmentby_column_offsets_compressed) * nsegmentby_cols);
	int seg_idx = 0;
	for (int col = 0; col < decompressor.num_compressed_columns; col++)
	{
		if (!compressed_chunk_column_is_segmentby(decompressor.per_compressed_cols[col]))
			continue;
		segmentby_column_offsets_compressed[seg_idx++] = col;
	}

	HeapTuple compressed_tuple;

	IndexScanDesc index_scan;
	bool changed_segment = false;
	/************ current segment **************/
	CompressedSegmentInfo **current_segment =
		palloc(sizeof(CompressedSegmentInfo *) * nsegmentby_cols);

	for (int i = 0; i < nsegmentby_cols; i++)
	{
		current_segment[i] = palloc(sizeof(CompressedSegmentInfo));
		current_segment[i]->segment_info = NULL;
	}
	bool current_segment_init = false;
	bool skip_current_segment = false;

	/************** snapshot ****************************/
	Snapshot snapshot = RegisterSnapshot(GetTransactionSnapshot());

	/* Index scan */
	Relation index_rel = index_open(row_compressor.index_oid, ExclusiveLock);
	ereport(LOG,
			(errmsg("locks acquired for recompression: \"%s.%s\"",
					NameStr(uncompressed_chunk->fd.schema_name),
					NameStr(uncompressed_chunk->fd.table_name))));

	index_scan = index_beginscan(compressed_chunk_rel, index_rel, snapshot, 0, 0);
	TupleTableSlot *slot = table_slot_create(compressed_chunk_rel, NULL);
	index_rescan(index_scan, NULL, 0, NULL, 0);

	while (index_getnext_slot(index_scan, ForwardScanDirection, slot))
	{
		slot_getallattrs(slot);

		if (!current_segment_init)
		{
			current_segment_init = true;
			decompress_segment_update_current_segment(current_segment,
													  slot, /*slot from compressed chunk*/
													  decompressor.per_compressed_cols,
													  segmentby_column_offsets_compressed,
													  nsegmentby_cols);

			skip_current_segment =
				!fetch_matching_uncompressed_chunk_into_tuplesort(segment_tuplesortstate,
																  nsegmentby_cols,
																  uncompressed_chunk_rel,
																  current_segment);
		}
		/* we have a segment already, so compare those */
		changed_segment = decompress_segment_changed_group(current_segment,
														   slot,
														   decompressor.per_compressed_cols,
														   segmentby_column_offsets_compressed,
														   nsegmentby_cols);
		if (changed_segment)
		{
			if (!skip_current_segment)
			{
				recompress_segment(segment_tuplesortstate, uncompressed_chunk_rel, &row_compressor);

				/* reinit tuplesort */
				segment_tuplesortstate = tuplesort_begin_heap(uncompressed_rel_tupdesc,
															  n_keys,
															  sort_keys,
															  sort_operators,
															  sort_collations,
															  nulls_first,
															  maintenance_work_mem,
															  NULL,
															  false);
			}

			decompress_segment_update_current_segment(current_segment,
													  slot, /*slot from compressed chunk*/
													  decompressor.per_compressed_cols,
													  segmentby_column_offsets_compressed,
													  nsegmentby_cols);

			changed_segment = false;
			skip_current_segment =
				!fetch_matching_uncompressed_chunk_into_tuplesort(segment_tuplesortstate,
																  nsegmentby_cols,
																  uncompressed_chunk_rel,
																  current_segment);
		}

		if (skip_current_segment)
		{
			continue;
		}

		/* Didn't change group and we are not skipping the current segment
		 * add it to the tuplesort
		 */
		bool should_free;

		compressed_tuple = ExecFetchSlotHeapTuple(slot, false, &should_free);

		heap_deform_tuple(compressed_tuple,
						  compressed_rel_tupdesc,
						  decompressor.compressed_datums,
						  decompressor.compressed_is_nulls);

		row_decompressor_decompress_row_to_tuplesort(&decompressor, segment_tuplesortstate);

		simple_table_tuple_delete(compressed_chunk_rel, &(slot->tts_tid), snapshot);
		CommandCounterIncrement();

		if (should_free)
			heap_freetuple(compressed_tuple);
	}

	ExecClearTuple(slot);

	/* never changed segment, but still need to perform the tuplesort and add everything to the
	 * compressed chunk
	 * the current segment could not be initialized in the case where two recompress operations
	 * execute concurrently: one blocks on the Exclusive lock but has already read the chunk
	 * status and determined that there is data in the uncompressed chunk */
	if (!changed_segment && !skip_current_segment && current_segment_init)
	{
		recompress_segment(segment_tuplesortstate, uncompressed_chunk_rel, &row_compressor);
	}
	/* done with the compressed chunk segments that had new entries in the uncompressed
	 but there could be rows inserted into the uncompressed that don't already have a corresponding
	 compressed segment, we need to compress those as well */
	segment_tuplesortstate = tuplesort_begin_heap(uncompressed_rel_tupdesc,
												  n_keys,
												  sort_keys,
												  sort_operators,
												  sort_collations,
												  nulls_first,
												  maintenance_work_mem,
												  NULL,
												  false);

	bool unmatched_rows_exist = false;
	fetch_unmatched_uncompressed_chunk_into_tuplesort(segment_tuplesortstate,
													  uncompressed_chunk_rel,
													  &unmatched_rows_exist);

	if (unmatched_rows_exist)
	{
		tuplesort_performsort(segment_tuplesortstate);
		row_compressor_reset(&row_compressor);
		row_compressor_append_sorted_rows(&row_compressor,
										  segment_tuplesortstate,
										  RelationGetDescr(uncompressed_chunk_rel),
										  uncompressed_chunk_rel);
		tuplesort_end(segment_tuplesortstate);

		/* make changes visible */
		CommandCounterIncrement();
	}

	row_compressor_close(&row_compressor);
	ExecDropSingleTupleTableSlot(slot);
	index_endscan(index_scan);
	UnregisterSnapshot(snapshot);
	index_close(index_rel, NoLock);
	row_decompressor_close(&decompressor);

	/* changed chunk status, so invalidate any plans involving this chunk */
	CacheInvalidateRelcacheByRelid(uncompressed_chunk_id);

	/* Need to rebuild indexes if the relation is using compression TAM */
	if (uncompressed_chunk_rel->rd_tableam == compressionam_routine())
	{
#if PG14_GE
		ReindexParams params = {
			.options = 0,
			.tablespaceOid = InvalidOid,
		};

		reindex_relation(RelationGetRelid(uncompressed_chunk_rel), 0, &params);
#else
		reindex_relation(RelationGetRelid(uncompressed_chunk_rel), 0, 0);
#endif
	}

	table_close(uncompressed_chunk_rel, NoLock);
	table_close(compressed_chunk_rel, NoLock);

	PG_RETURN_OID(uncompressed_chunk_id);
}
