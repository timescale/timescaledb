/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <access/genam.h>
#include <access/sdir.h>
#include <access/tableam.h>
#include <access/valid.h>
#include <catalog/pg_am.h>
#include <nodes/nodeFuncs.h>
#include <optimizer/optimizer.h>
#include <parser/parse_coerce.h>
#include <parser/parse_relation.h>
#include <parser/parsetree.h>
#include <utils/lsyscache.h>
#include <utils/relcache.h>
#include <utils/snapmgr.h>
#include <utils/typcache.h>

#include <compat/compat.h>
#include <compression/compression.h>
#include <compression/compression_dml.h>
#include <compression/create.h>
#include <compression/wal_utils.h>
#include <expression_utils.h>
#include <indexing.h>
#include <nodes/chunk_dispatch/chunk_dispatch.h>
#include <nodes/chunk_dispatch/chunk_insert_state.h>
#include <nodes/hypertable_modify.h>
#include <ts_catalog/array_utils.h>

static struct decompress_batches_stats
decompress_batches_scan(Relation in_rel, Relation out_rel, Relation index_rel, Snapshot snapshot,
						ScanKeyData *index_scankeys, int num_index_scankeys,
						ScanKeyData *heap_scankeys, int num_heap_scankeys,
						ScanKeyData *mem_scankeys, int num_mem_scankeys,
						tuple_filtering_constraints *constraints, bool *skip_current_tuple,
						bool delete_only, Bitmapset *null_columns, List *is_nulls);

static bool batch_matches(RowDecompressor *decompressor, ScanKeyData *scankeys, int num_scankeys,
						  tuple_filtering_constraints *constraints, bool *skip_current_tuple);
static void process_predicates(Chunk *ch, CompressionSettings *settings, List *predicates,
							   ScanKeyData **mem_scankeys, int *num_mem_scankeys,
							   List **heap_filters, List **index_filters, List **is_null);
static Relation find_matching_index(Relation comp_chunk_rel, List **index_filters,
									List **heap_filters);
static tuple_filtering_constraints *
get_batch_keys_for_unique_constraints(const ChunkInsertState *cis, Relation relation);
static BatchFilter *make_batchfilter(char *column_name, StrategyNumber strategy, Oid collation,
									 RegProcedure opcode, Const *value, bool is_null_check,
									 bool is_null, bool is_array_op);
static inline TM_Result delete_compressed_tuple(RowDecompressor *decompressor, Snapshot snapshot,
												HeapTuple compressed_tuple);
static void report_error(TM_Result result);

static bool key_column_is_null(tuple_filtering_constraints *constraints, Relation chunk_rel,
							   Oid ht_relid, TupleTableSlot *slot);
static bool can_delete_without_decompression(HypertableModifyState *ht_state,
											 CompressionSettings *settings, Chunk *chunk,
											 List *predicates);

void
decompress_batches_for_insert(const ChunkInsertState *cis, TupleTableSlot *slot)
{
	/*
	 * This is supposed to be called with the actual tuple that is being
	 * inserted, so it cannot be empty.
	 */
	Assert(!TTS_EMPTY(slot));

	Relation out_rel = cis->rel;

	if (!ts_indexing_relation_has_primary_or_unique_index(out_rel))
	{
		/*
		 * If there are no unique constraints there is nothing to do here.
		 */
		return;
	}

	if (!ts_guc_enable_dml_decompression)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("inserting into compressed chunk with unique constraints disabled"),
				 errhint("Set timescaledb.enable_dml_decompression to TRUE.")));

	tuple_filtering_constraints *constraints = get_batch_keys_for_unique_constraints(cis, out_rel);
	if (key_column_is_null(constraints, out_rel, cis->hypertable_relid, slot))
	{
		/* When any key column is NULL and NULLs are distinct there is no
		 * decompression to be done as the tuple will not conflict with any
		 * existing tuples.
		 */
		return;
	}

	Assert(OidIsValid(cis->compressed_chunk_table_id));
	Relation in_rel = relation_open(cis->compressed_chunk_table_id, RowExclusiveLock);
	CompressionSettings *settings = ts_compression_settings_get(cis->compressed_chunk_table_id);
	Assert(settings);

	Bitmapset *index_columns = NULL;
	Bitmapset *null_columns = NULL;
	struct decompress_batches_stats stats;

	/* the scan keys used for in memory tests of the decompressed tuples */
	int num_mem_scankeys = 0;
	ScanKeyData *mem_scankeys = NULL;
	int num_index_scankeys = 0;
	ScanKeyData *index_scankeys = NULL;
	Relation index_rel = NULL;
	ScanKeyData *heap_scankeys = NULL;
	int num_heap_scankeys = 0;
	Bitmapset *key_columns = constraints->key_columns;

	if (ts_guc_enable_dml_decompression_tuple_filtering)
	{
		mem_scankeys = build_mem_scankeys_from_slot(cis->hypertable_relid,
													settings,
													out_rel,
													constraints,
													slot,
													&num_mem_scankeys);

		index_scankeys = build_index_scankeys_using_slot(cis->hypertable_relid,
														 in_rel,
														 out_rel,
														 constraints->key_columns,
														 slot,
														 &index_rel,
														 &index_columns,
														 &num_index_scankeys);
	}

	bool skip_current_tuple = false;
	if (index_rel)
	{
		/*
		 * Prepare the heap scan keys for all
		 * key columns not found in the index
		 */
		key_columns = bms_difference(constraints->key_columns, index_columns);
	}

	heap_scankeys = build_heap_scankeys(cis->hypertable_relid,
										in_rel,
										out_rel,
										settings,
										key_columns,
										&null_columns,
										slot,
										&num_heap_scankeys);

	/* no null column check for non-segmentby columns in case of index scan */
	if (index_rel)
		null_columns = NULL;

	/*
	 * Using latest snapshot to scan the heap since we are doing this to build
	 * the index on the uncompressed chunks in order to do speculative insertion
	 * which is always built from all tuples (even in higher levels of isolation).
	 */
	stats = decompress_batches_scan(in_rel,
									out_rel,
									index_rel,
									GetLatestSnapshot(),
									index_scankeys,
									num_index_scankeys,
									heap_scankeys,
									num_heap_scankeys,
									mem_scankeys,
									num_mem_scankeys,
									constraints,
									&skip_current_tuple,
									false,
									null_columns, /* no null column check for non-segmentby
											 columns */
									NIL);
	if (index_rel)
		index_close(index_rel, AccessShareLock);

	Assert(cis->cds != NULL);
	if (skip_current_tuple)
	{
		cis->cds->skip_current_tuple = true;
	}

	cis->cds->batches_deleted += stats.batches_deleted;
	cis->cds->batches_filtered += stats.batches_filtered;
	cis->cds->batches_decompressed += stats.batches_decompressed;
	cis->cds->tuples_decompressed += stats.tuples_decompressed;

	CommandCounterIncrement();
	table_close(in_rel, NoLock);
}

/*
 * This method will:
 *  1. Evaluate WHERE clauses and check if SEGMENT BY columns
 *     are specified or not.
 *  2. Build scan keys for SEGMENT BY columns.
 *  3. Move scanned rows to staging area.
 *  4. Update catalog table to change status of moved chunk.
 *
 *  Returns true if it decompresses any data.
 */
static bool
decompress_batches_for_update_delete(HypertableModifyState *ht_state, Chunk *chunk,
									 List *predicates, EState *estate, bool has_joins)
{
	/* process each chunk with its corresponding predicates */

	List *heap_filters = NIL;
	List *index_filters = NIL;
	List *is_null = NIL;
	ListCell *lc = NULL;
	Relation chunk_rel;
	Relation comp_chunk_rel;
	Relation matching_index_rel = NULL;
	Chunk *comp_chunk;
	BatchFilter *filter;

	ScanKeyData *scankeys = NULL;
	Bitmapset *null_columns = NULL;
	int num_scankeys = 0;
	ScanKeyData *index_scankeys = NULL;
	int num_index_scankeys = 0;
	struct decompress_batches_stats stats;
	int num_mem_scankeys = 0;
	ScanKeyData *mem_scankeys = NULL;

	comp_chunk = ts_chunk_get_by_id(chunk->fd.compressed_chunk_id, true);
	CompressionSettings *settings = ts_compression_settings_get(comp_chunk->table_id);
	bool delete_only = ht_state->mt->operation == CMD_DELETE && !has_joins &&
					   can_delete_without_decompression(ht_state, settings, chunk, predicates);

	process_predicates(chunk,
					   settings,
					   predicates,
					   &mem_scankeys,
					   &num_mem_scankeys,
					   &heap_filters,
					   &index_filters,
					   &is_null);

	chunk_rel = table_open(chunk->table_id, RowExclusiveLock);
	comp_chunk_rel = table_open(comp_chunk->table_id, RowExclusiveLock);

	if (index_filters)
	{
		matching_index_rel = find_matching_index(comp_chunk_rel, &index_filters, &heap_filters);
	}

	if (heap_filters)
	{
		scankeys = build_update_delete_scankeys(comp_chunk_rel,
												heap_filters,
												&num_scankeys,
												&null_columns);
	}

	if (matching_index_rel)
	{
		index_scankeys =
			build_index_scankeys(matching_index_rel, index_filters, &num_index_scankeys);
	}

	stats = decompress_batches_scan(comp_chunk_rel,
									chunk_rel,
									matching_index_rel,
									GetTransactionSnapshot(),
									index_scankeys,
									num_index_scankeys,
									scankeys,
									num_scankeys,
									mem_scankeys,
									num_mem_scankeys,
									NULL,
									NULL,
									delete_only,
									null_columns,
									is_null);

	/* close the selected index */
	if (matching_index_rel)
		index_close(matching_index_rel, AccessShareLock);

	/*
	 * tuples from compressed chunk has been decompressed and moved
	 * to staging area, thus mark this chunk as partially compressed
	 */
	if (stats.batches_decompressed > 0)
		ts_chunk_set_partial(chunk);

	table_close(chunk_rel, NoLock);
	table_close(comp_chunk_rel, NoLock);

	foreach (lc, heap_filters)
	{
		filter = lfirst(lc);
		pfree(filter);
	}
	foreach (lc, index_filters)
	{
		filter = lfirst(lc);
		pfree(filter);
	}
	ht_state->batches_deleted += stats.batches_deleted;
	ht_state->batches_filtered += stats.batches_filtered;
	ht_state->batches_decompressed += stats.batches_decompressed;
	ht_state->tuples_decompressed += stats.tuples_decompressed;

	return stats.batches_decompressed > 0;
}

typedef struct DecompressBatchScanData
{
	TableScanDesc scan;
	IndexScanDesc index_scan;
} DecompressBatchScanData;

typedef struct DecompressBatchScanData *DecompressBatchScanDesc;

static DecompressBatchScanDesc
decompress_batch_beginscan(Relation in_rel, Relation index_rel, Snapshot snapshot, int num_scankeys,
						   ScanKeyData *scankeys)
{
	DecompressBatchScanDesc scan;
	scan = (DecompressBatchScanDesc) palloc(sizeof(DecompressBatchScanData));

	if (index_rel)
	{
		scan->index_scan = index_beginscan(in_rel, index_rel, snapshot, num_scankeys, 0);
		index_rescan(scan->index_scan, scankeys, num_scankeys, NULL, 0);
		scan->scan = NULL;
	}
	else
	{
		scan->scan = table_beginscan(in_rel, snapshot, num_scankeys, scankeys);
		scan->index_scan = NULL;
	}

	return scan;
}

static bool
decompress_batch_scan_getnext_slot(DecompressBatchScanDesc scan, ScanDirection direction,
								   struct TupleTableSlot *slot)
{
	if (scan->index_scan)
	{
		return index_getnext_slot(scan->index_scan, direction, slot);
	}
	else
	{
		return table_scan_getnextslot(scan->scan, direction, slot);
	}
}

static void
decompress_batch_endscan(DecompressBatchScanDesc scan)
{
	if (scan->index_scan)
	{
		index_endscan(scan->index_scan);
	}
	else
	{
		table_endscan(scan->scan);
	}

	pfree(scan);
}

/*
 * This method will:
 *  1.Scan the index created with SEGMENT BY columns or the entire compressed chunk
 *  2.Fetch matching rows and decompress the row
 *  3.Delete this row from compressed chunk
 *  4.Insert decompressed rows to uncompressed chunk
 *
 *  Returns whether we decompressed anything.
 *
 */
static struct decompress_batches_stats
decompress_batches_scan(Relation in_rel, Relation out_rel, Relation index_rel, Snapshot snapshot,
						ScanKeyData *index_scankeys, int num_index_scankeys,
						ScanKeyData *heap_scankeys, int num_heap_scankeys,
						ScanKeyData *mem_scankeys, int num_mem_scankeys,
						tuple_filtering_constraints *constraints, bool *skip_current_tuple,
						bool delete_only, Bitmapset *null_columns, List *is_nulls)
{
	HeapTuple compressed_tuple;
	RowDecompressor decompressor;
	bool decompressor_initialized = false;
	bool valid = false;
	int num_scanned_rows = 0;
	int num_filtered_rows = 0;
	TM_Result result;
	DecompressBatchScanDesc scan = NULL;

	struct decompress_batches_stats stats = { 0 };

	/* TODO: Optimization by reusing the index scan while working on a single chunk */

	if (index_rel)
	{
		scan = decompress_batch_beginscan(in_rel,
										  index_rel,
										  snapshot,
										  num_index_scankeys,
										  index_scankeys);
	}
	else
	{
		scan = decompress_batch_beginscan(in_rel, NULL, snapshot, num_heap_scankeys, heap_scankeys);
	}
	TupleTableSlot *slot = table_slot_create(in_rel, NULL);

	while (decompress_batch_scan_getnext_slot(scan, ForwardScanDirection, slot))
	{
		num_scanned_rows++;

		/* Deconstruct the tuple */
		Assert(slot->tts_ops->get_heap_tuple);
		compressed_tuple = slot->tts_ops->get_heap_tuple(slot);

		if (index_rel && num_heap_scankeys)
		{
			/* filter tuple based on compress_orderby columns */
			valid = false;
#if PG16_LT
			HeapKeyTest(compressed_tuple,
						RelationGetDescr(in_rel),
						num_heap_scankeys,
						heap_scankeys,
						valid);
#else
			valid = HeapKeyTest(compressed_tuple,
								RelationGetDescr(in_rel),
								num_heap_scankeys,
								heap_scankeys);
#endif
			if (!valid)
			{
				num_filtered_rows++;
				continue;
			}
		}

		int attrno = bms_next_member(null_columns, -1);
		int pos = 0;
		bool is_null_condition = 0;
		bool seg_col_is_null = false;
		valid = true;
		/*
		 * Since the heap scan API does not support SK_SEARCHNULL we have to check
		 * for NULL values manually when those are part of the constraints.
		 */
		for (; attrno >= 0; attrno = bms_next_member(null_columns, attrno))
		{
			is_null_condition = is_nulls && list_nth_int(is_nulls, pos);
			seg_col_is_null = slot_attisnull(slot, attrno);
			if ((seg_col_is_null && !is_null_condition) || (!seg_col_is_null && is_null_condition))
			{
				/*
				 * if segment by column in the scanned tuple has non null value
				 * and IS NULL is specified, OR segment by column has null value
				 * and IS NOT NULL is specified then skip this tuple
				 */
				valid = false;
				break;
			}
			pos++;
		}

		if (!valid)
		{
			num_filtered_rows++;
			continue;
		}

		if (!decompressor_initialized)
		{
			decompressor = build_decompressor(in_rel, out_rel);
			decompressor.delete_only = delete_only;
			decompressor_initialized = true;
		}

		heap_deform_tuple(compressed_tuple,
						  decompressor.in_desc,
						  decompressor.compressed_datums,
						  decompressor.compressed_is_nulls);

		if (num_mem_scankeys && !batch_matches(&decompressor,
											   mem_scankeys,
											   num_mem_scankeys,
											   constraints,
											   skip_current_tuple))
		{
			row_decompressor_reset(&decompressor);
			stats.batches_filtered++;
			continue;
		}

		if (skip_current_tuple && *skip_current_tuple)
		{
			row_decompressor_close(&decompressor);
			decompress_batch_endscan(scan);
			ExecDropSingleTupleTableSlot(slot);
			return stats;
		}
		write_logical_replication_msg_decompression_start();
		result = delete_compressed_tuple(&decompressor, snapshot, compressed_tuple);
		/* skip reporting error if isolation level is < Repeatable Read
		 * since somebody decompressed the data concurrently, we need to take
		 * that data into account as well when in Read Committed level
		 */
		if (result == TM_Deleted && !IsolationUsesXactSnapshot())
		{
			write_logical_replication_msg_decompression_end();
			stats.batches_decompressed++;
			continue;
		}
		if (result != TM_Ok)
		{
			write_logical_replication_msg_decompression_end();
			row_decompressor_close(&decompressor);
			decompress_batch_endscan(scan);
			report_error(result);
			return stats;
		}
		if (decompressor.delete_only)
		{
			stats.batches_deleted++;
		}
		else
		{
			stats.tuples_decompressed += row_decompressor_decompress_row_to_table(&decompressor);
			stats.batches_decompressed++;
		}
		write_logical_replication_msg_decompression_end();
	}
	ExecDropSingleTupleTableSlot(slot);
	decompress_batch_endscan(scan);
	if (decompressor_initialized)
	{
		row_decompressor_close(&decompressor);
	}

	if (ts_guc_debug_compression_path_info)
	{
		elog(INFO,
			 "Number of compressed rows fetched from %s: %d. "
			 "Number of compressed rows filtered%s: %d.",
			 index_rel ? "index" : "table scan",
			 num_scanned_rows,
			 index_rel ? " by heap filters" : "",
			 num_filtered_rows);
	}

	return stats;
}

static bool
batch_matches(RowDecompressor *decompressor, ScanKeyData *scankeys, int num_scankeys,
			  tuple_filtering_constraints *constraints, bool *skip_current_tuple)
{
	int num_tuples = decompress_batch(decompressor);

	bool valid = false;

	for (int row = 0; row < num_tuples; row++)
	{
		TupleTableSlot *decompressed_slot = decompressor->decompressed_slots[row];
		HeapTuple tuple = decompressed_slot->tts_ops->get_heap_tuple(decompressed_slot);
#if PG16_LT
		HeapKeyTest(tuple, decompressor->out_desc, num_scankeys, scankeys, valid);
#else
		valid = HeapKeyTest(tuple, decompressor->out_desc, num_scankeys, scankeys);
#endif
		if (valid)
		{
			if (constraints)
			{
				if (constraints->on_conflict == ONCONFLICT_NONE)
				{
					ereport(ERROR,
							(errcode(ERRCODE_UNIQUE_VIOLATION),
							 errmsg("duplicate key value violates unique constraint \"%s\"",
									get_rel_name(constraints->index_relid))

								 ));
				}
				if (constraints->on_conflict == ONCONFLICT_NOTHING && skip_current_tuple)
				{
					*skip_current_tuple = true;
				}
			}
			return true;
		}
	}

	return false;
}

/*
 * Traverse the plan tree to look for Scan nodes on uncompressed chunks.
 * Once Scan node is found check if chunk is compressed, if so then
 * decompress those segments which match the filter conditions if present.
 */

struct decompress_chunk_context
{
	List *relids;
	HypertableModifyState *ht_state;
	/* indicates decompression actually occurred */
	bool batches_decompressed;
	bool has_joins;
};

static bool decompress_chunk_walker(PlanState *ps, struct decompress_chunk_context *ctx);

bool
decompress_target_segments(HypertableModifyState *ht_state)
{
	ModifyTableState *ps =
		linitial_node(ModifyTableState, castNode(CustomScanState, ht_state)->custom_ps);

	struct decompress_chunk_context ctx = {
		.ht_state = ht_state,
		.relids = castNode(ModifyTable, ps->ps.plan)->resultRelations,
	};
	Assert(ctx.relids);

	decompress_chunk_walker(&ps->ps, &ctx);
	return ctx.batches_decompressed;
}

static bool
decompress_chunk_walker(PlanState *ps, struct decompress_chunk_context *ctx)
{
	RangeTblEntry *rte = NULL;
	bool needs_decompression = false;
	bool should_rescan = false;
	bool batches_decompressed = false;
	List *predicates = NIL;
	Chunk *current_chunk;
	if (ps == NULL)
		return false;

	switch (nodeTag(ps))
	{
		/* Note: IndexOnlyScans will never be selected for target
		 * tables because system columns are necessary in order to modify the
		 * data and those columns cannot be a part of the index
		 */
		case T_IndexScanState:
		{
			/* Get the index quals on the original table and also include
			 * any filters that are used for filtering heap tuples
			 */
			predicates = list_union(((IndexScan *) ps->plan)->indexqualorig, ps->plan->qual);
			needs_decompression = true;
			break;
		}
		case T_BitmapHeapScanState:
			predicates = list_union(((BitmapHeapScan *) ps->plan)->bitmapqualorig, ps->plan->qual);
			needs_decompression = true;
			should_rescan = true;
			break;
		case T_SeqScanState:
		case T_SampleScanState:
		case T_TidScanState:
		case T_TidRangeScanState:
		{
			predicates = list_copy(ps->plan->qual);
			needs_decompression = true;
			break;
		}
		case T_NestLoopState:
		case T_MergeJoinState:
		case T_HashJoinState:
		{
			ctx->has_joins = true;
			break;
		}
		default:
			break;
	}
	if (needs_decompression)
	{
		/*
		 * We are only interested in chunk scans of chunks that are the
		 * target of the DML statement not chunk scan on joined hypertables
		 * even when it is a self join
		 */
		int scanrelid = ((Scan *) ps->plan)->scanrelid;
		if (list_member_int(ctx->relids, scanrelid))
		{
			rte = rt_fetch(scanrelid, ps->state->es_range_table);
			current_chunk = ts_chunk_get_by_relid(rte->relid, false);
			if (current_chunk && ts_chunk_is_compressed(current_chunk))
			{
				if (!ts_guc_enable_dml_decompression)
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("UPDATE/DELETE is disabled on compressed chunks"),
							 errhint("Set timescaledb.enable_dml_decompression to TRUE.")));

				batches_decompressed = decompress_batches_for_update_delete(ctx->ht_state,
																			current_chunk,
																			predicates,
																			ps->state,
																			ctx->has_joins);
				ctx->batches_decompressed |= batches_decompressed;

				/* This is a workaround specifically for bitmap heap scans:
				 * during node initialization, initialize the scan state with the active snapshot
				 * but since we are inserting data to be modified during the same query, they end up
				 * missing that data by using a snapshot which doesn't account for this decompressed
				 * data. To circumvent this issue, we change the internal scan state to use the
				 * transaction snapshot and execute a rescan so the scan state is set correctly and
				 * includes the new data.
				 */
				if (should_rescan)
				{
					ScanState *ss = ((ScanState *) ps);
					if (ss && ss->ss_currentScanDesc)
					{
						ss->ss_currentScanDesc->rs_snapshot = GetTransactionSnapshot();
						ExecReScan(ps);
					}
				}
			}
		}
	}

	if (predicates)
		pfree(predicates);

	return planstate_tree_walker(ps, decompress_chunk_walker, ctx);
}

/*
 * For insert into compressed chunks with unique index determine the
 * columns which can be used for INSERT batch filtering.
 * The passed in relation is the uncompressed chunk.
 *
 * In case of multiple unique indexes we have to return the shared columns.
 * For expression indexes we ignore the columns with expressions, for partial
 * indexes we ignore predicate.
 *
 */
static tuple_filtering_constraints *
get_batch_keys_for_unique_constraints(const ChunkInsertState *cis, Relation relation)
{
	tuple_filtering_constraints *constraints = palloc0(sizeof(tuple_filtering_constraints));
	constraints->on_conflict = ONCONFLICT_UPDATE;
	ListCell *lc;

	/* Fast path if definitely no indexes */
	if (!RelationGetForm(relation)->relhasindex)
		return constraints;

	List *indexoidlist = RelationGetIndexList(relation);

	/* Fall out if no indexes (but relhasindex was set) */
	if (indexoidlist == NIL)
		return constraints;

	foreach (lc, indexoidlist)
	{
		Oid indexOid = lfirst_oid(lc);
		Relation indexDesc = index_open(indexOid, AccessShareLock);

		/*
		 * We are only interested in unique indexes. PRIMARY KEY indexes also have
		 * indisunique set to true so we do not need to check for them separately.
		 */
		if (!indexDesc->rd_index->indislive || !indexDesc->rd_index->indisvalid ||
			!indexDesc->rd_index->indisunique)
		{
			index_close(indexDesc, AccessShareLock);
			continue;
		}

		Bitmapset *idx_attrs = NULL;
		/*
		 * Collect attributes of current index.
		 * For covering indexes we need to ignore the included columns.
		 */
		for (int i = 0; i < indexDesc->rd_index->indnkeyatts; i++)
		{
			int attno = indexDesc->rd_index->indkey.values[i];
			/* We are not interested in expression columns which will have attno = 0 */
			if (!attno)
				continue;

			idx_attrs = bms_add_member(idx_attrs, attno - FirstLowInvalidHeapAttributeNumber);
		}
		index_close(indexDesc, AccessShareLock);

		if (!constraints->key_columns)
		{
			/* First iteration */
			constraints->key_columns = bms_copy(idx_attrs);
			/*
			 * We only optimize unique constraint checks for non-partial and
			 * non-expression indexes. For partial and expression indexes we
			 * can still do batch filtering, just not make decisions about
			 * constraint violations.
			 */
			constraints->covered = indexDesc->rd_indexprs == NIL && indexDesc->rd_indpred == NIL;
			constraints->index_relid = indexDesc->rd_id;
		}
		else
		{
			/* more than one unique constraint */
			constraints->key_columns = bms_intersect(idx_attrs, constraints->key_columns);
			constraints->covered = false;
		}

		/* When multiple unique indexes are present, in theory there could be no shared
		 * columns even though that is very unlikely as they will probably at least share
		 * the partitioning columns. But since we are looking at chunk indexes here that
		 * is not guaranteed.
		 */
		if (!constraints->key_columns)
			return constraints;
	}

	if (constraints->covered && cis->cds->dispatch)
	{
		constraints->on_conflict = ts_chunk_dispatch_get_on_conflict_action(cis->cds->dispatch);
	}

	return constraints;
}

/*
 * This method will evaluate the predicates, extract
 * left and right operands, check if any of the operands
 * can be used for batch filtering and if so, it will
 * create a BatchFilter object and add it to the corresponding
 * list.
 * Any segmentby filter is put into index_filters list other
 * filters are put into heap_filters list.
 */
static void
process_predicates(Chunk *ch, CompressionSettings *settings, List *predicates,
				   ScanKeyData **mem_scankeys, int *num_mem_scankeys, List **heap_filters,
				   List **index_filters, List **is_null)
{
	ListCell *lc;
	if (ts_guc_enable_dml_decompression_tuple_filtering)
	{
		*mem_scankeys = palloc0(sizeof(ScanKeyData) * list_length(predicates));
	}
	*num_mem_scankeys = 0;

	/*
	 * We dont want to forward boundParams from the execution state here
	 * as we dont want to constify join params in the predicates.
	 * Constifying JOIN params would not be safe as we don't redo
	 * this part in rescan.
	 */
	PlannerGlobal glob = { .boundParams = NULL };
	PlannerInfo root = { .glob = &glob };

	foreach (lc, predicates)
	{
		Node *node = copyObject(lfirst(lc));
		Var *var;
		Expr *expr;
		Oid collation, opno;
		RegProcedure opcode;
		char *column_name;

		switch (nodeTag(node))
		{
			case T_OpExpr:
			{
				OpExpr *opexpr = castNode(OpExpr, node);
				collation = opexpr->inputcollid;
				Const *arg_value;

				if (!ts_extract_expr_args(&opexpr->xpr, &var, &expr, &opno, &opcode))
					continue;

				if (!IsA(expr, Const))
				{
					expr = (Expr *) estimate_expression_value(&root, (Node *) expr);

					if (!IsA(expr, Const))
						continue;
				}

				arg_value = castNode(Const, expr);

				column_name = get_attname(ch->table_id, var->varattno, false);
				TypeCacheEntry *tce = lookup_type_cache(var->vartype, TYPECACHE_BTREE_OPFAMILY);
				int op_strategy = get_op_opfamily_strategy(opno, tce->btree_opf);

				if (ts_array_is_member(settings->fd.segmentby, column_name))
				{
					switch (op_strategy)
					{
						case BTEqualStrategyNumber:
						case BTLessStrategyNumber:
						case BTLessEqualStrategyNumber:
						case BTGreaterStrategyNumber:
						case BTGreaterEqualStrategyNumber:
						{
							/* save segment by column name and its corresponding value specified in
							 * WHERE */
							*index_filters = lappend(*index_filters,
													 make_batchfilter(column_name,
																	  op_strategy,
																	  collation,
																	  opcode,
																	  arg_value,
																	  false, /* is_null_check */
																	  false, /* is_null */
																	  false	 /* is_array_op */
																	  ));
						}
					}
					continue;
				}

				/*
				 * Segmentby columns are checked as part of batch scan so no need to redo the check.
				 */
				if (ts_guc_enable_dml_decompression_tuple_filtering)
				{
					ScanKeyEntryInitialize(&(*mem_scankeys)[(*num_mem_scankeys)++],
										   arg_value->constisnull ? SK_ISNULL : 0,
										   var->varattno,
										   op_strategy,
										   arg_value->consttype,
										   arg_value->constcollid,
										   opcode,
										   arg_value->constisnull ? 0 : arg_value->constvalue);
				}

				int min_attno = compressed_column_metadata_attno(settings,
																 ch->table_id,
																 var->varattno,
																 settings->fd.relid,
																 "min");
				int max_attno = compressed_column_metadata_attno(settings,
																 ch->table_id,
																 var->varattno,
																 settings->fd.relid,
																 "max");

				if (min_attno != InvalidAttrNumber && max_attno != InvalidAttrNumber)
				{
					switch (op_strategy)
					{
						case BTEqualStrategyNumber:
						{
							/* orderby col = value implies min <= value and max >= value */
							*heap_filters = lappend(*heap_filters,
													make_batchfilter(get_attname(settings->fd.relid,
																				 min_attno,
																				 false),
																	 BTLessEqualStrategyNumber,
																	 collation,
																	 opcode,
																	 arg_value,
																	 false, /* is_null_check */
																	 false, /* is_null */
																	 false	/* is_array_op */
																	 ));
							*heap_filters = lappend(*heap_filters,
													make_batchfilter(get_attname(settings->fd.relid,
																				 max_attno,
																				 false),
																	 BTGreaterEqualStrategyNumber,
																	 collation,
																	 opcode,
																	 arg_value,
																	 false, /* is_null_check */
																	 false, /* is_null */
																	 false	/* is_array_op */
																	 ));
						}
						break;
						case BTLessStrategyNumber:
						case BTLessEqualStrategyNumber:
						{
							/* orderby col <[=] value implies min <[=] value */
							*heap_filters = lappend(*heap_filters,
													make_batchfilter(get_attname(settings->fd.relid,
																				 min_attno,
																				 false),
																	 op_strategy,
																	 collation,
																	 opcode,
																	 arg_value,
																	 false, /* is_null_check */
																	 false, /* is_null */
																	 false	/* is_array_op */
																	 ));
						}
						break;
						case BTGreaterStrategyNumber:
						case BTGreaterEqualStrategyNumber:
						{
							/* orderby col >[=] value implies max >[=] value */
							*heap_filters = lappend(*heap_filters,
													make_batchfilter(get_attname(settings->fd.relid,
																				 max_attno,
																				 false),
																	 op_strategy,
																	 collation,
																	 opcode,
																	 arg_value,
																	 false, /* is_null_check */
																	 false, /* is_null */
																	 false	/* is_array_op */
																	 ));
						}
					}
				}
			}
			break;
			case T_ScalarArrayOpExpr:
			{
				ScalarArrayOpExpr *sa_expr = castNode(ScalarArrayOpExpr, node);
				if (!ts_extract_expr_args(&sa_expr->xpr, &var, &expr, &opno, &opcode))
					continue;

				if (!IsA(expr, Const))
				{
					expr = (Expr *) estimate_expression_value(&root, (Node *) expr);
					if (!IsA(expr, Const))
						continue;
				}

				Const *arg_value = castNode(Const, expr);
				collation = sa_expr->inputcollid;

				column_name = get_attname(ch->table_id, var->varattno, false);
				TypeCacheEntry *tce = lookup_type_cache(var->vartype, TYPECACHE_BTREE_OPFAMILY);
				int op_strategy = get_op_opfamily_strategy(opno, tce->btree_opf);
				if (ts_array_is_member(settings->fd.segmentby, column_name))
				{
					switch (op_strategy)
					{
						case BTEqualStrategyNumber:
						case BTLessStrategyNumber:
						case BTLessEqualStrategyNumber:
						case BTGreaterStrategyNumber:
						case BTGreaterEqualStrategyNumber:
						{
							/* save segment by column name and its corresponding value specified in
							 * WHERE */
							*index_filters = lappend(*index_filters,
													 make_batchfilter(column_name,
																	  op_strategy,
																	  collation,
																	  opcode,
																	  arg_value,
																	  false, /* is_null_check */
																	  false, /* is_null */
																	  true	 /* is_array_op */
																	  ));
						}
					}
					continue;
				}

				break;
			}
			case T_NullTest:
			{
				NullTest *ntest = (NullTest *) node;
				if (IsA(ntest->arg, Var))
				{
					var = (Var *) ntest->arg;
					/* ignore system-defined attributes */
					if (var->varattno <= 0)
						continue;
					column_name = get_attname(ch->table_id, var->varattno, false);
					if (ts_array_is_member(settings->fd.segmentby, column_name))
					{
						*index_filters =
							lappend(*index_filters,
									make_batchfilter(column_name,
													 InvalidStrategy,
													 InvalidOid,
													 InvalidOid,
													 NULL,
													 true, /* is_null_check */
													 ntest->nulltesttype == IS_NULL, /* is_null */
													 false /* is_array_op */
													 ));
						if (ntest->nulltesttype == IS_NULL)
							*is_null = lappend_int(*is_null, 1);
						else
							*is_null = lappend_int(*is_null, 0);
					}
					/* We cannot optimize filtering decompression using ORDERBY
					 * metadata and null check qualifiers. We could possibly do that by checking the
					 * compressed data in combination with the ORDERBY nulls first setting and
					 * verifying that the first or last tuple of a segment contains a NULL value.
					 * This is left for future optimization */
				}
			}
			break;
			default:
				break;
		}
	}
}

static BatchFilter *
make_batchfilter(char *column_name, StrategyNumber strategy, Oid collation, RegProcedure opcode,
				 Const *value, bool is_null_check, bool is_null, bool is_array_op)
{
	BatchFilter *segment_filter = palloc0(sizeof(*segment_filter));

	*segment_filter = (BatchFilter){
		.strategy = strategy,
		.collation = collation,
		.opcode = opcode,
		.value = value,
		.is_null_check = is_null_check,
		.is_null = is_null,
		.is_array_op = is_array_op,
	};
	namestrcpy(&segment_filter->column_name, column_name);

	return segment_filter;
}

/*
 * A compressed chunk can have multiple indexes. For a given list
 * of columns in index_filters, find the matching index which has
 * the most columns based on index_filters and adjust the filters
 * if necessary.
 * Return matching index if found else return NULL.
 *
 * Note: This method will find the best matching index based on
 * number of filters it matches. If an index matches all the filters,
 * it will be chosen. Otherwise, it will try to select the index
 * which has most matches. If there are multiple indexes have
 * the same number of matches, it will pick the first one it finds.
 * For example
 * for a given condition like "WHERE X = 10 AND Y = 8"
 * if there are multiple indexes like
 * 1. index (a,b,c,x)
 * 2. index (a,x,y)
 * 3. index (x)
 * In this case 2nd index is returned. If that one didn't exist,
 * it would return the 1st index.
 */
static Relation
find_matching_index(Relation comp_chunk_rel, List **index_filters, List **heap_filters)
{
	List *index_oids;
	ListCell *lc;
	int total_filters = list_length(*index_filters);
	int max_match_count = 0;
	Relation result_rel = NULL;

	/* get list of indexes defined on compressed chunk */
	index_oids = RelationGetIndexList(comp_chunk_rel);
	foreach (lc, index_oids)
	{
		int match_count = 0;
		Relation index_rel = index_open(lfirst_oid(lc), AccessShareLock);
		IndexInfo *index_info = BuildIndexInfo(index_rel);

		/* Can't use partial or expression indexes */
		if (index_info->ii_Predicate != NIL || index_info->ii_Expressions != NIL)
		{
			index_close(index_rel, AccessShareLock);
			continue;
		}

		/* Can only use Btree indexes */
		if (index_info->ii_Am != BTREE_AM_OID)
		{
			index_close(index_rel, AccessShareLock);
			continue;
		}

		ListCell *li;
		foreach (li, *index_filters)
		{
			for (int i = 0; i < index_rel->rd_index->indnatts; i++)
			{
				AttrNumber attnum = index_rel->rd_index->indkey.values[i];
				char *attname = get_attname(RelationGetRelid(comp_chunk_rel), attnum, false);
				BatchFilter *sf = lfirst(li);
				/* ensure column exists in index relation */
				if (!strcmp(attname, NameStr(sf->column_name)))
				{
					match_count++;
					break;
				}
			}
		}
		if (match_count == total_filters)
		{
			/* found index which has all columns specified in WHERE */
			if (result_rel)
				index_close(result_rel, AccessShareLock);
			if (ts_guc_debug_compression_path_info)
				elog(INFO, "Index \"%s\" is used for scan. ", RelationGetRelationName(index_rel));
			return index_rel;
		}

		if (match_count > max_match_count)
		{
			max_match_count = match_count;
			result_rel = index_rel;
			continue;
		}
		index_close(index_rel, AccessShareLock);
	}

	/* No matching index whatsoever */
	if (!result_rel)
	{
		*heap_filters = list_concat(*heap_filters, *index_filters);
		*index_filters = list_truncate(*index_filters, 0);
		return NULL;
	}

	/* We found an index which matches partially.
	 * It can be used but we need to transfer the unmatched
	 * filters from index_filters to heap filters.
	 */
	for (int i = 0; i < list_length(*index_filters); i++)
	{
		BatchFilter *sf = list_nth(*index_filters, i);
		bool match = false;
		for (int j = 0; j < result_rel->rd_index->indnatts; j++)
		{
			AttrNumber attnum = result_rel->rd_index->indkey.values[j];
			char *attname = get_attname(RelationGetRelid(comp_chunk_rel), attnum, false);
			/* ensure column exists in index relation */
			if (!strcmp(attname, NameStr(sf->column_name)))
			{
				match = true;
				break;
			}
		}

		if (!match)
		{
			*heap_filters = lappend(*heap_filters, sf);
			*index_filters = list_delete_nth_cell(*index_filters, i);
		}
	}
	if (ts_guc_debug_compression_path_info)
		elog(INFO, "Index \"%s\" is used for scan. ", RelationGetRelationName(result_rel));
	return result_rel;
}

static inline TM_Result
delete_compressed_tuple(RowDecompressor *decompressor, Snapshot snapshot,
						HeapTuple compressed_tuple)
{
	TM_FailureData tmfd;
	TM_Result result;
	result = table_tuple_delete(decompressor->in_rel,
								&compressed_tuple->t_self,
								decompressor->mycid,
								snapshot,
								InvalidSnapshot,
								true,
								&tmfd,
								false);
	return result;
}

static void
report_error(TM_Result result)
{
	switch (result)
	{
		case TM_Deleted:
		{
			if (IsolationUsesXactSnapshot())
			{
				/* For Repeatable Read isolation level report error */
				ereport(ERROR,
						(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
						 errmsg("could not serialize access due to concurrent update")));
			}
		}
		break;
		/*
		 * If another transaction is updating the compressed data,
		 * we have to abort the transaction to keep consistency.
		 */
		case TM_Updated:
		{
			elog(ERROR, "tuple concurrently updated");
		}
		break;
		case TM_Invisible:
		{
			elog(ERROR, "attempted to lock invisible tuple");
		}
		break;
		case TM_Ok:
			break;
		default:
		{
			elog(ERROR, "unexpected tuple operation result: %d", result);
		}
		break;
	}
}

/*
 * If key_columns are including all unique constraint columns and NULLS
 * are not DISTINCT any NULL value in the key columns allows us to skip
 * finding matching batches as it will not create a constraint violation.
 */
static bool
key_column_is_null(tuple_filtering_constraints *constraints, Relation chunk_rel, Oid ht_relid,
				   TupleTableSlot *slot)
{
	if (!constraints->covered || constraints->nullsnotdistinct)
		return false;

	int i = -1;
	while ((i = bms_next_member(constraints->key_columns, i)) > 0)
	{
		AttrNumber chunk_attno = i + FirstLowInvalidHeapAttributeNumber;

		/*
		 * slot has the physical layout of the hypertable, so we need to
		 * get the attribute number of the hypertable for the column.
		 */
		const NameData *attname = attnumAttName(chunk_rel, chunk_attno);

		AttrNumber ht_attno = get_attnum(ht_relid, NameStr(*attname));
		if (slot_attisnull(slot, ht_attno))
			return true;
	}

	return false;
}

static bool
can_delete_without_decompression(HypertableModifyState *ht_state, CompressionSettings *settings,
								 Chunk *chunk, List *predicates)
{
	ListCell *lc;

	if (!ts_guc_enable_compressed_direct_batch_delete)
		return false;

	/*
	 * If there is a RETURNING clause we skip the optimization to delete compressed batches directly
	 */
	if (ht_state->mt->returningLists)
		return false;

	/*
	 * If there are any DELETE row triggers on the hypertable we skip the optimization
	 * to delete compressed batches directly.
	 */
	ModifyTableState *ps =
		linitial_node(ModifyTableState, castNode(CustomScanState, ht_state)->custom_ps);
	if (ps->rootResultRelInfo->ri_TrigDesc)
	{
		TriggerDesc *trigdesc = ps->rootResultRelInfo->ri_TrigDesc;
		if (trigdesc->trig_delete_before_row || trigdesc->trig_delete_after_row ||
			trigdesc->trig_delete_instead_row)
		{
			return false;
		}
	}

	foreach (lc, predicates)
	{
		Node *node = lfirst(lc);
		Var *var;
		Expr *arg_value;
		Oid opno;

		if (ts_extract_expr_args((Expr *) node, &var, &arg_value, &opno, NULL))
		{
			if (!IsA(arg_value, Const))
			{
				return false;
			}
			char *column_name = get_attname(chunk->table_id, var->varattno, false);
			if (ts_array_is_member(settings->fd.segmentby, column_name))
			{
				continue;
			}
		}
		return false;
	}
	return true;
}
