/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <access/tableam.h>
#include <access/htup_details.h>
#include <access/multixact.h>
#include <access/valid.h>
#include <access/xact.h>
#include <catalog/heap.h>
#include <catalog/index.h>
#include <catalog/namespace.h>
#include <catalog/pg_am.h>
#include <catalog/pg_attribute.h>
#include <catalog/pg_type.h>
#include <common/base64.h>
#include <executor/nodeIndexscan.h>
#include <executor/tuptable.h>
#include <libpq/pqformat.h>
#include <miscadmin.h>
#include <nodes/nodeFuncs.h>
#include <nodes/execnodes.h>
#include <nodes/pg_list.h>
#include <nodes/print.h>
#include <parser/parsetree.h>
#include <parser/parse_coerce.h>
#include <storage/lmgr.h>
#include <storage/predicate.h>
#include <utils/builtins.h>
#include <utils/datum.h>
#include <utils/fmgroids.h>
#include <utils/lsyscache.h>
#include <utils/memutils.h>
#include <utils/portal.h>
#include <utils/rel.h>
#include <utils/relcache.h>
#include <utils/snapmgr.h>
#include <utils/syscache.h>
#include <utils/tuplesort.h>
#include <utils/typcache.h>
#include <replication/message.h>

#include "compat/compat.h"

#include "array.h"
#include "chunk.h"
#include "compression.h"
#include "create.h"
#include "custom_type_cache.h"
#include "debug_assert.h"
#include "debug_point.h"
#include "deltadelta.h"
#include "dictionary.h"
#include "gorilla.h"
#include "guc.h"
#include "nodes/chunk_dispatch/chunk_insert_state.h"
#include "nodes/hypertable_modify.h"
#include "indexing.h"
#include "segment_meta.h"
#include "ts_catalog/array_utils.h"
#include "ts_catalog/catalog.h"
#include "ts_catalog/compression_settings.h"
#include "ts_catalog/compression_chunk_size.h"

static const CompressionAlgorithmDefinition definitions[_END_COMPRESSION_ALGORITHMS] = {
	[COMPRESSION_ALGORITHM_ARRAY] = ARRAY_ALGORITHM_DEFINITION,
	[COMPRESSION_ALGORITHM_DICTIONARY] = DICTIONARY_ALGORITHM_DEFINITION,
	[COMPRESSION_ALGORITHM_GORILLA] = GORILLA_ALGORITHM_DEFINITION,
	[COMPRESSION_ALGORITHM_DELTADELTA] = DELTA_DELTA_ALGORITHM_DEFINITION,
};

#if PG14_GE
/* The prefix of a logical replication message which is inserted into the
 * replication stream right before decompression inserts are happening
 */
#define DECOMPRESSION_MARKER_START "::timescaledb-decompression-start"
/* The prefix of a logical replication message which is inserted into the
 * replication stream right after all decompression inserts have finished
 */
#define DECOMPRESSION_MARKER_END "::timescaledb-decompression-end"
#endif

static inline void
write_logical_replication_msg_decompression_start()
{
#if PG14_GE
	if (ts_guc_enable_decompression_logrep_markers && XLogLogicalInfoActive())
	{
		LogLogicalMessage(DECOMPRESSION_MARKER_START, "", 0, true);
	}
#endif
}

static inline void
write_logical_replication_msg_decompression_end()
{
#if PG14_GE
	if (ts_guc_enable_decompression_logrep_markers && XLogLogicalInfoActive())
	{
		LogLogicalMessage(DECOMPRESSION_MARKER_END, "", 0, true);
	}
#endif
}

static Compressor *
compressor_for_type(Oid type)
{
	CompressionAlgorithm algorithm = compression_get_default_algorithm(type);
	if (algorithm >= _END_COMPRESSION_ALGORITHMS)
		elog(ERROR, "invalid compression algorithm %d", algorithm);

	return definitions[algorithm].compressor_for_type(type);
}

DecompressionIterator *(*tsl_get_decompression_iterator_init(CompressionAlgorithm algorithm,
															 bool reverse))(Datum, Oid)
{
	if (algorithm >= _END_COMPRESSION_ALGORITHMS)
		elog(ERROR, "invalid compression algorithm %d", algorithm);

	if (reverse)
		return definitions[algorithm].iterator_init_reverse;
	else
		return definitions[algorithm].iterator_init_forward;
}

DecompressAllFunction
tsl_get_decompress_all_function(CompressionAlgorithm algorithm, Oid type)
{
	if (algorithm >= _END_COMPRESSION_ALGORITHMS)
		elog(ERROR, "invalid compression algorithm %d", algorithm);

	if (type != TEXTOID &&
		(algorithm == COMPRESSION_ALGORITHM_DICTIONARY || algorithm == COMPRESSION_ALGORITHM_ARRAY))
	{
		/* Bulk decompression of array and dictionary is only supported for text. */
		return NULL;
	}

	return definitions[algorithm].decompress_all;
}

static Tuplesortstate *compress_chunk_sort_relation(CompressionSettings *settings, Relation in_rel);
static void row_compressor_process_ordered_slot(RowCompressor *row_compressor, TupleTableSlot *slot,
												CommandId mycid);
static void row_compressor_update_group(RowCompressor *row_compressor, TupleTableSlot *row);
static bool row_compressor_new_row_is_in_new_group(RowCompressor *row_compressor,
												   TupleTableSlot *row);
static void row_compressor_append_row(RowCompressor *row_compressor, TupleTableSlot *row);
static void row_compressor_flush(RowCompressor *row_compressor, CommandId mycid,
								 bool changed_groups);

static int create_segment_filter_scankey(RowDecompressor *decompressor,
										 char *segment_filter_col_name, StrategyNumber strategy,
										 ScanKeyData *scankeys, int num_scankeys,
										 Bitmapset **null_columns, Datum value, bool is_null_check);
static void create_per_compressed_column(RowDecompressor *decompressor);

/********************
 ** compress_chunk **
 ********************/

static CompressedDataHeader *
get_compressed_data_header(Datum data)
{
	CompressedDataHeader *header = (CompressedDataHeader *) PG_DETOAST_DATUM(data);

	if (header->compression_algorithm >= _END_COMPRESSION_ALGORITHMS)
		elog(ERROR, "invalid compression algorithm %d", header->compression_algorithm);

	return header;
}

/* Truncate the relation WITHOUT applying triggers. This is the
 * main difference with ExecuteTruncate. Triggers aren't applied
 * because the data remains, just in compressed form. Also don't
 * restart sequences. Use the transactional branch through ExecuteTruncate.
 */
static void
truncate_relation(Oid table_oid)
{
	List *fks = heap_truncate_find_FKs(list_make1_oid(table_oid));
	/* Take an access exclusive lock now. Note that this may very well
	 *  be a lock upgrade. */
	Relation rel = table_open(table_oid, AccessExclusiveLock);
	Oid toast_relid;

	/* Chunks should never have fks into them, but double check */
	if (fks != NIL)
		elog(ERROR, "found a FK into a chunk while truncating");

	CheckTableForSerializableConflictIn(rel);

#if PG16_LT
	RelationSetNewRelfilenode(rel, rel->rd_rel->relpersistence);
#else
	RelationSetNewRelfilenumber(rel, rel->rd_rel->relpersistence);
#endif

	toast_relid = rel->rd_rel->reltoastrelid;

	table_close(rel, NoLock);

	if (OidIsValid(toast_relid))
	{
		rel = table_open(toast_relid, AccessExclusiveLock);
#if PG16_LT
		RelationSetNewRelfilenode(rel, rel->rd_rel->relpersistence);
#else
		RelationSetNewRelfilenumber(rel, rel->rd_rel->relpersistence);
#endif
		Assert(rel->rd_rel->relpersistence != RELPERSISTENCE_UNLOGGED);
		table_close(rel, NoLock);
	}

#if PG14_LT
	int options = 0;
#else
	ReindexParams params = { 0 };
	ReindexParams *options = &params;
#endif
	reindex_relation(table_oid, REINDEX_REL_PROCESS_TOAST, options);
	rel = table_open(table_oid, AccessExclusiveLock);
	CommandCounterIncrement();
	table_close(rel, NoLock);
}

/*
 * Use reltuples as an estimate for the number of rows that will get compressed. This value
 * might be way off the mark in case analyze hasn't happened in quite a while on this input
 * chunk. But that's the best guesstimate to start off with.
 *
 * We will report progress for every 10% of reltuples compressed. If rel or reltuples is not valid
 * or it's just too low then we just assume reporting every 100K tuples for now.
 */
#define RELTUPLES_REPORT_DEFAULT 100000
static int64
calculate_reltuples_to_report(Relation rel)
{
	int64 report_reltuples = RELTUPLES_REPORT_DEFAULT;

	if (rel != NULL && rel->rd_rel->reltuples > 0)
	{
		report_reltuples = (int64) (0.1 * rel->rd_rel->reltuples);
		/* either analyze has not been done or table doesn't have a lot of rows */
		if (report_reltuples < RELTUPLES_REPORT_DEFAULT)
			report_reltuples = RELTUPLES_REPORT_DEFAULT;
	}

	return report_reltuples;
}

CompressionStats
compress_chunk(Oid in_table, Oid out_table, int insert_options)
{
	int n_keys;
	ListCell *lc;
	int indexscan_direction = NoMovementScanDirection;
	Relation matched_index_rel = NULL;
	TupleTableSlot *slot;
	IndexScanDesc index_scan;
	CommandId mycid = GetCurrentCommandId(true);
	HeapTuple in_table_tp = NULL, index_tp = NULL;
	Form_pg_attribute in_table_attr_tp, index_attr_tp;
	CompressionStats cstat;
	CompressionSettings *settings = ts_compression_settings_get(out_table);
	int64 report_reltuples;

	/* We want to prevent other compressors from compressing this table,
	 * and we want to prevent INSERTs or UPDATEs which could mess up our compression.
	 * We may as well allow readers to keep reading the uncompressed data while
	 * we are compressing, so we only take an ExclusiveLock instead of AccessExclusive.
	 */
	Relation in_rel = table_open(in_table, ExclusiveLock);
	/* We are _just_ INSERTing into the out_table so in principle we could take
	 * a RowExclusive lock, and let other operations read and write this table
	 * as we work. However, we currently compress each table as a oneshot, so
	 * we're taking the stricter lock to prevent accidents.
	 */
	Relation out_rel = relation_open(out_table, ExclusiveLock);

	/* Sanity check we are dealing with relations */
	Ensure(in_rel->rd_rel->relkind == RELKIND_RELATION, "compress_chunk called on non-relation");
	Ensure(out_rel->rd_rel->relkind == RELKIND_RELATION, "compress_chunk called on non-relation");

	TupleDesc in_desc = RelationGetDescr(in_rel);
	TupleDesc out_desc = RelationGetDescr(out_rel);
	/* Before calling row compressor relation should be segmented and sorted as configured
	 * by compress_segmentby and compress_orderby.
	 * Cost of sorting can be mitigated if we find an existing BTREE index defined for
	 * uncompressed chunk otherwise expensive tuplesort will come into play.
	 *
	 * The following code is trying to find an existing index that
	 * matches the configuration so that we can skip sequential scan and
	 * tuplesort.
	 */
	if (ts_guc_enable_compression_indexscan)
	{
		List *in_rel_index_oids = RelationGetIndexList(in_rel);
		foreach (lc, in_rel_index_oids)
		{
			Oid index_oid = lfirst_oid(lc);
			Relation index_rel = index_open(index_oid, AccessShareLock);
			IndexInfo *index_info = BuildIndexInfo(index_rel);

			if (index_info->ii_Predicate != 0)
			{
				/*
				 * Can't use partial indexes for compression because they refer
				 * only to a subset of all rows.
				 */
				index_close(index_rel, AccessShareLock);
				continue;
			}

			int previous_direction = NoMovementScanDirection;
			int current_direction = NoMovementScanDirection;

			n_keys =
				ts_array_length(settings->fd.segmentby) + ts_array_length(settings->fd.orderby);

			if (n_keys <= index_info->ii_NumIndexKeyAttrs && index_info->ii_Am == BTREE_AM_OID)
			{
				int i;
				for (i = 0; i < n_keys; i++)
				{
					const char *attname;
					int16 position;
					bool is_orderby_asc = true;
					bool is_null_first = false;

					if (i < ts_array_length(settings->fd.segmentby))
					{
						position = i + 1;
						attname = ts_array_get_element_text(settings->fd.segmentby, position);
					}
					else
					{
						position = i - ts_array_length(settings->fd.segmentby) + 1;
						attname = ts_array_get_element_text(settings->fd.orderby, position);
						is_orderby_asc =
							!ts_array_get_element_bool(settings->fd.orderby_desc, position);
						is_null_first =
							ts_array_get_element_bool(settings->fd.orderby_nullsfirst, position);
					}
					int16 att_num = get_attnum(in_table, attname);

					int16 option = index_rel->rd_indoption[i];
					bool index_orderby_asc = ((option & INDOPTION_DESC) == 0);
					bool index_null_first = ((option & INDOPTION_NULLS_FIRST) != 0);

					if (att_num == 0 || index_info->ii_IndexAttrNumbers[i] != att_num)
					{
						break;
					}

					in_table_tp = SearchSysCacheAttNum(in_table, att_num);
					if (!HeapTupleIsValid(in_table_tp))
						elog(ERROR,
							 "table \"%s\" does not have column \"%s\"",
							 get_rel_name(in_table),
							 attname);

					index_tp = SearchSysCacheAttNum(index_oid, i + 1);
					if (!HeapTupleIsValid(index_tp))
						elog(ERROR,
							 "index \"%s\" does not have column \"%s\"",
							 get_rel_name(index_oid),
							 attname);

					in_table_attr_tp = (Form_pg_attribute) GETSTRUCT(in_table_tp);
					index_attr_tp = (Form_pg_attribute) GETSTRUCT(index_tp);

					if (index_orderby_asc == is_orderby_asc && index_null_first == is_null_first &&
						in_table_attr_tp->attcollation == index_attr_tp->attcollation)
					{
						current_direction = ForwardScanDirection;
					}
					else if (index_orderby_asc != is_orderby_asc &&
							 index_null_first != is_null_first &&
							 in_table_attr_tp->attcollation == index_attr_tp->attcollation)
					{
						current_direction = BackwardScanDirection;
					}
					else
					{
						current_direction = NoMovementScanDirection;
						break;
					}

					ReleaseSysCache(in_table_tp);
					in_table_tp = NULL;
					ReleaseSysCache(index_tp);
					index_tp = NULL;
					if (previous_direction == NoMovementScanDirection)
					{
						previous_direction = current_direction;
					}
					else if (previous_direction != current_direction)
					{
						break;
					}
				}

				if (n_keys == i && (previous_direction == current_direction &&
									current_direction != NoMovementScanDirection))
				{
					matched_index_rel = index_rel;
					indexscan_direction = current_direction;
					break;
				}
				else
				{
					if (HeapTupleIsValid(in_table_tp))
					{
						ReleaseSysCache(in_table_tp);
						in_table_tp = NULL;
					}
					if (HeapTupleIsValid(index_tp))
					{
						ReleaseSysCache(index_tp);
						index_tp = NULL;
					}
					index_close(index_rel, AccessShareLock);
				}
			}
			else
			{
				index_close(index_rel, AccessShareLock);
			}
		}
	}

	RowCompressor row_compressor;
	/* Reset sequence is used for resetting the sequence without
	 * having to fetch the existing sequence for an individual
	 * sequence group. If we are rolling up an uncompressed chunk
	 * into an existing compressed chunk, we have to fetch
	 * sequence numbers for each batch of the segment using
	 * the respective index. In normal compression, this is
	 * not necessary because we start segment numbers from zero.
	 * We distinguish these cases by insert_options being zero for rollups.
	 *
	 * Sequence numbers are planned to be removed in the near future,
	 * which will render this flag obsolete.
	 */
	bool reset_sequence = insert_options > 0;
	row_compressor_init(settings,
						&row_compressor,
						in_rel,
						out_rel,
						out_desc->natts,
						true /*need_bistate*/,
						reset_sequence,
						insert_options);

	if (matched_index_rel != NULL)
	{
		int64 nrows_processed = 0;

		/*
		 * even though we log the information below, this debug info
		 * is still used for INFO messages to clients and our tests.
		 */
		if (ts_guc_debug_compression_path_info)
		{
			elog(INFO,
				 "compress_chunk_indexscan_start matched index \"%s\"",
				 get_rel_name(matched_index_rel->rd_id));
		}

		elog(LOG,
			 "using index \"%s\" to scan rows for compression",
			 get_rel_name(matched_index_rel->rd_id));

		index_scan = index_beginscan(in_rel, matched_index_rel, GetTransactionSnapshot(), 0, 0);
		slot = table_slot_create(in_rel, NULL);
		index_rescan(index_scan, NULL, 0, NULL, 0);
		report_reltuples = calculate_reltuples_to_report(in_rel);
		while (index_getnext_slot(index_scan, indexscan_direction, slot))
		{
			row_compressor_process_ordered_slot(&row_compressor, slot, mycid);
			if ((++nrows_processed % report_reltuples) == 0)
				elog(LOG,
					 "compressed " INT64_FORMAT " rows from \"%s\"",
					 nrows_processed,
					 RelationGetRelationName(in_rel));
		}

		if (row_compressor.rows_compressed_into_current_value > 0)
			row_compressor_flush(&row_compressor, mycid, true);

		elog(LOG,
			 "finished compressing " INT64_FORMAT " rows from \"%s\"",
			 nrows_processed,
			 RelationGetRelationName(in_rel));

		ExecDropSingleTupleTableSlot(slot);
		index_endscan(index_scan);
		index_close(matched_index_rel, AccessShareLock);
	}
	else
	{
		/*
		 * even though we log the information below, this debug info
		 * is still used for INFO messages to clients and our tests.
		 */
		if (ts_guc_debug_compression_path_info)
		{
			elog(INFO, "compress_chunk_tuplesort_start");
		}

		elog(LOG,
			 "using tuplesort to scan rows from \"%s\" for compression",
			 RelationGetRelationName(in_rel));

		Tuplesortstate *sorted_rel = compress_chunk_sort_relation(settings, in_rel);
		row_compressor_append_sorted_rows(&row_compressor, sorted_rel, in_desc, in_rel);
		tuplesort_end(sorted_rel);
	}

	row_compressor_close(&row_compressor);
	DEBUG_WAITPOINT("compression_done_before_truncate_uncompressed");
	truncate_relation(in_table);

	table_close(out_rel, NoLock);
	table_close(in_rel, NoLock);
	cstat.rowcnt_pre_compression = row_compressor.rowcnt_pre_compression;
	cstat.rowcnt_post_compression = row_compressor.num_compressed_rows;

	if ((insert_options & HEAP_INSERT_FROZEN) == HEAP_INSERT_FROZEN)
		cstat.rowcnt_frozen = row_compressor.num_compressed_rows;
	else
		cstat.rowcnt_frozen = 0;

	return cstat;
}

static Tuplesortstate *
compress_chunk_sort_relation(CompressionSettings *settings, Relation in_rel)
{
	TupleDesc tupDesc = RelationGetDescr(in_rel);
	Tuplesortstate *tuplesortstate;
	TableScanDesc scan;
	TupleTableSlot *slot;

	int num_segmentby = ts_array_length(settings->fd.segmentby);
	int num_orderby = ts_array_length(settings->fd.orderby);
	int n_keys = num_segmentby + num_orderby;

	AttrNumber *sort_keys = palloc(sizeof(*sort_keys) * n_keys);
	Oid *sort_operators = palloc(sizeof(*sort_operators) * n_keys);
	Oid *sort_collations = palloc(sizeof(*sort_collations) * n_keys);
	bool *nulls_first = palloc(sizeof(*nulls_first) * n_keys);
	int n;

	for (n = 0; n < n_keys; n++)
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
													 RelationGetRelid(in_rel),
													 attname,
													 &sort_keys[n],
													 &sort_operators[n],
													 &sort_collations[n],
													 &nulls_first[n]);
	}

	tuplesortstate = tuplesort_begin_heap(tupDesc,
										  n_keys,
										  sort_keys,
										  sort_operators,
										  sort_collations,
										  nulls_first,
										  maintenance_work_mem,
										  NULL,
										  false /*=randomAccess*/);

	scan = table_beginscan(in_rel, GetLatestSnapshot(), 0, (ScanKey) NULL);
	slot = table_slot_create(in_rel, NULL);

	while (table_scan_getnextslot(scan, ForwardScanDirection, slot))
	{
		if (!TTS_EMPTY(slot))
		{
			/*    This may not be the most efficient way to do things.
			 *     Since we use begin_heap() the tuplestore expects tupleslots,
			 *      so ISTM that the options are this or maybe putdatum().
			 */
			tuplesort_puttupleslot(tuplesortstate, slot);
		}
	}

	table_endscan(scan);

	ExecDropSingleTupleTableSlot(slot);

	tuplesort_performsort(tuplesortstate);

	return tuplesortstate;
}

void
compress_chunk_populate_sort_info_for_column(CompressionSettings *settings, Oid table,
											 const char *attname, AttrNumber *att_nums,
											 Oid *sort_operator, Oid *collation, bool *nulls_first)
{
	HeapTuple tp;
	Form_pg_attribute att_tup;
	TypeCacheEntry *tentry;

	tp = SearchSysCacheAttName(table, attname);
	if (!HeapTupleIsValid(tp))
		elog(ERROR, "table \"%s\" does not have column \"%s\"", get_rel_name(table), attname);

	att_tup = (Form_pg_attribute) GETSTRUCT(tp);
	/* Other validation checks beyond just existence of a valid comparison operator could be useful
	 */

	*att_nums = att_tup->attnum;
	*collation = att_tup->attcollation;

	tentry = lookup_type_cache(att_tup->atttypid, TYPECACHE_LT_OPR | TYPECACHE_GT_OPR);

	if (ts_array_is_member(settings->fd.segmentby, attname))
	{
		*nulls_first = false;
		*sort_operator = tentry->lt_opr;
	}
	else
	{
		Assert(ts_array_is_member(settings->fd.orderby, attname));
		int position = ts_array_position(settings->fd.orderby, attname);
		*nulls_first = ts_array_get_element_bool(settings->fd.orderby_nullsfirst, position);

		if (ts_array_get_element_bool(settings->fd.orderby_desc, position))
			*sort_operator = tentry->gt_opr;
		else
			*sort_operator = tentry->lt_opr;
	}

	if (!OidIsValid(*sort_operator))
		elog(ERROR,
			 "no valid sort operator for column \"%s\" of type \"%s\"",
			 attname,
			 format_type_be(att_tup->atttypid));

	ReleaseSysCache(tp);
}

/*
 * Find segment by index for setting the correct sequence number if
 * we are trying to roll up chunks while compressing
 */
Oid
get_compressed_chunk_index(ResultRelInfo *resultRelInfo, CompressionSettings *settings)
{
	int num_segmentby_columns = ts_array_length(settings->fd.segmentby);

	for (int i = 0; i < resultRelInfo->ri_NumIndices; i++)
	{
		bool matches = true;
		Relation index_relation = resultRelInfo->ri_IndexRelationDescs[i];
		IndexInfo *index_info = resultRelInfo->ri_IndexRelationInfo[i];

		/* the index must include all segment by columns and sequence number */
		if (index_info->ii_NumIndexKeyAttrs != num_segmentby_columns + 1)
			continue;

		for (int j = 0; j < index_info->ii_NumIndexKeyAttrs - 1; j++)
		{
			const char *attname =
				get_attname(index_relation->rd_id, AttrOffsetGetAttrNumber(j), false);

			if (!ts_array_is_member(settings->fd.segmentby, attname))
			{
				matches = false;
				break;
			}
		}

		if (!matches)
			continue;

		/* Check last index column is sequence number */
		const char *attname =
			get_attname(index_relation->rd_id, index_info->ii_NumIndexKeyAttrs, false);

		if (strncmp(attname, COMPRESSION_COLUMN_METADATA_SEQUENCE_NUM_NAME, NAMEDATALEN) == 0)
			return index_relation->rd_id;
	}

	return InvalidOid;
}

static int32
index_scan_sequence_number(Relation table_rel, Oid index_oid, ScanKeyData *scankey,
						   int num_scankeys)
{
	int32 result = 0;
	bool is_null;
	Relation index_rel = index_open(index_oid, AccessShareLock);

	IndexScanDesc index_scan =
		index_beginscan(table_rel, index_rel, GetTransactionSnapshot(), num_scankeys, 0);
	index_scan->xs_want_itup = true;
	index_rescan(index_scan, scankey, num_scankeys, NULL, 0);

	if (index_getnext_tid(index_scan, BackwardScanDirection))
	{
		result = index_getattr(index_scan->xs_itup,
							   index_scan->xs_itupdesc
								   ->natts, /* Last attribute of the index is sequence number. */
							   index_scan->xs_itupdesc,
							   &is_null);
		if (is_null)
			result = 0;
	}

	index_endscan(index_scan);
	index_close(index_rel, AccessShareLock);

	return result;
}

static int32
table_scan_sequence_number(Relation table_rel, int16 seq_num_column_num, ScanKeyData *scankey,
						   int num_scankeys)
{
	int32 max_seq_num = 0;
	TupleTableSlot *slot;
	TableScanDesc scan;

	slot = table_slot_create(table_rel, NULL);
	scan = table_beginscan(table_rel, GetLatestSnapshot(), num_scankeys, scankey);

	while (table_scan_getnextslot(scan, ForwardScanDirection, slot))
	{
		bool is_null;
		Datum seq_num = slot_getattr(slot, seq_num_column_num, &is_null);

		if (!is_null)
		{
			int32 curr_seq_num = DatumGetInt32(seq_num);

			if (max_seq_num < curr_seq_num)
				max_seq_num = curr_seq_num;
		}
	}

	table_endscan(scan);
	ExecDropSingleTupleTableSlot(slot);

	return max_seq_num;
}

/* Scan compressed chunk to get the sequence number for current group.
 * This is necessary to do when merging chunks. If the chunk is empty,
 * scan will always return 0 and the sequence number will start from
 * SEQUENCE_NUM_GAP.
 */
static int32
get_sequence_number_for_current_group(Relation table_rel, Oid index_oid,
									  int16 *uncompressed_col_to_compressed_col,
									  PerColumn *per_column, int n_input_columns,
									  int16 seq_num_column_num)
{
	/* If there is a suitable index, use index scan otherwise fallback to heap scan. */
	bool is_index_scan = OidIsValid(index_oid);

	int i, num_scankeys = 0;
	int32 result = 0;

	for (i = 0; i < n_input_columns; i++)
	{
		if (per_column[i].segmentby_column_index < 1)
			continue;

		num_scankeys++;
	}

	MemoryContext scan_ctx = AllocSetContextCreate(CurrentMemoryContext,
												   "get max sequence number scan",
												   ALLOCSET_DEFAULT_SIZES);
	MemoryContext old_ctx;
	old_ctx = MemoryContextSwitchTo(scan_ctx);

	ScanKeyData *scankey = NULL;

	if (num_scankeys > 0)
	{
		scankey = palloc0(sizeof(ScanKeyData) * num_scankeys);

		for (i = 0; i < n_input_columns; i++)
		{
			if (per_column[i].segmentby_column_index < 1)
				continue;

			PerColumn col = per_column[i];
			int16 attno = is_index_scan ?
							  col.segmentby_column_index :
							  AttrOffsetGetAttrNumber(uncompressed_col_to_compressed_col[i]);

			if (col.segment_info->is_null)
			{
				ScanKeyEntryInitialize(&scankey[col.segmentby_column_index - 1],
									   SK_ISNULL | SK_SEARCHNULL,
									   attno,
									   InvalidStrategy, /* no strategy */
									   InvalidOid,		/* no strategy subtype */
									   InvalidOid,		/* no collation */
									   InvalidOid,		/* no reg proc for this */
									   (Datum) 0);		/* constant */
			}
			else
			{
				ScanKeyEntryInitializeWithInfo(&scankey[col.segmentby_column_index - 1],
											   0, /* flags */
											   attno,
											   BTEqualStrategyNumber,
											   InvalidOid, /* No strategy subtype. */
											   col.segment_info->collation,
											   &col.segment_info->eq_fn,
											   col.segment_info->val);
			}
		}
	}

	if (is_index_scan)
	{
		/* Index scan should always use at least one scan key to get the sequence number. */
		Assert(num_scankeys > 0);

		result = index_scan_sequence_number(table_rel, index_oid, scankey, num_scankeys);
	}
	else
	{
		/* Table scan can work without scan keys. */
		result = table_scan_sequence_number(table_rel, seq_num_column_num, scankey, num_scankeys);
	}

	MemoryContextSwitchTo(old_ctx);
	MemoryContextDelete(scan_ctx);

	return result + SEQUENCE_NUM_GAP;
}

static void
build_column_map(CompressionSettings *settings, Relation uncompressed_table,
				 Relation compressed_table, PerColumn **pcolumns, int16 **pmap)
{
	Oid compressed_data_type_oid = ts_custom_type_cache_get(CUSTOM_TYPE_COMPRESSED_DATA)->type_oid;
	TupleDesc out_desc = RelationGetDescr(compressed_table);
	TupleDesc in_desc = RelationGetDescr(uncompressed_table);

	PerColumn *columns = palloc0(sizeof(PerColumn) * in_desc->natts);
	int16 *map = palloc0(sizeof(int16) * in_desc->natts);

	for (int i = 0; i < in_desc->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(in_desc, i);

		if (attr->attisdropped)
			continue;

		PerColumn *column = &columns[AttrNumberGetAttrOffset(attr->attnum)];
		AttrNumber compressed_colnum = get_attnum(compressed_table->rd_id, NameStr(attr->attname));
		Form_pg_attribute compressed_column_attr =
			TupleDescAttr(out_desc, AttrNumberGetAttrOffset(compressed_colnum));
		map[AttrNumberGetAttrOffset(attr->attnum)] = AttrNumberGetAttrOffset(compressed_colnum);

		bool is_segmentby = ts_array_is_member(settings->fd.segmentby, NameStr(attr->attname));
		bool is_orderby = ts_array_is_member(settings->fd.orderby, NameStr(attr->attname));

		if (!is_segmentby)
		{
			if (compressed_column_attr->atttypid != compressed_data_type_oid)
				elog(ERROR,
					 "expected column '%s' to be a compressed data type",
					 NameStr(attr->attname));

			AttrNumber segment_min_attr_number =
				compressed_column_metadata_attno(settings,
												 uncompressed_table->rd_id,
												 attr->attnum,
												 compressed_table->rd_id,
												 "min");
			AttrNumber segment_max_attr_number =
				compressed_column_metadata_attno(settings,
												 uncompressed_table->rd_id,
												 attr->attnum,
												 compressed_table->rd_id,
												 "max");
			int16 segment_min_attr_offset = segment_min_attr_number - 1;
			int16 segment_max_attr_offset = segment_max_attr_number - 1;

			SegmentMetaMinMaxBuilder *segment_min_max_builder = NULL;
			if (segment_min_attr_number != InvalidAttrNumber ||
				segment_max_attr_number != InvalidAttrNumber)
			{
				Ensure(segment_min_attr_number != InvalidAttrNumber,
					   "could not find the min metadata column");
				Ensure(segment_max_attr_number != InvalidAttrNumber,
					   "could not find the min metadata column");
				segment_min_max_builder =
					segment_meta_min_max_builder_create(attr->atttypid, attr->attcollation);
			}

			Ensure(!is_orderby || segment_min_max_builder != NULL,
				   "orderby columns must have minmax metadata");

			*column = (PerColumn){
				.compressor = compressor_for_type(attr->atttypid),
				.min_metadata_attr_offset = segment_min_attr_offset,
				.max_metadata_attr_offset = segment_max_attr_offset,
				.min_max_metadata_builder = segment_min_max_builder,
				.segmentby_column_index = -1,
			};
		}
		else
		{
			if (attr->atttypid != compressed_column_attr->atttypid)
				elog(ERROR,
					 "expected segment by column \"%s\" to be same type as uncompressed column",
					 NameStr(attr->attname));
			int16 index = ts_array_position(settings->fd.segmentby, NameStr(attr->attname));
			*column = (PerColumn){
				.segment_info = segment_info_new(attr),
				.segmentby_column_index = index,
				.min_metadata_attr_offset = -1,
				.max_metadata_attr_offset = -1,
			};
		}
	}
	*pcolumns = columns;
	*pmap = map;
}

/********************
 ** row_compressor **
 ********************/
void
row_compressor_init(CompressionSettings *settings, RowCompressor *row_compressor,
					Relation uncompressed_table, Relation compressed_table,
					int16 num_columns_in_compressed_table, bool need_bistate, bool reset_sequence,
					int insert_options)
{
	Name count_metadata_name = DatumGetName(
		DirectFunctionCall1(namein, CStringGetDatum(COMPRESSION_COLUMN_METADATA_COUNT_NAME)));
	Name sequence_num_metadata_name = DatumGetName(
		DirectFunctionCall1(namein,
							CStringGetDatum(COMPRESSION_COLUMN_METADATA_SEQUENCE_NUM_NAME)));
	AttrNumber count_metadata_column_num =
		get_attnum(compressed_table->rd_id, NameStr(*count_metadata_name));
	AttrNumber sequence_num_column_num =
		get_attnum(compressed_table->rd_id, NameStr(*sequence_num_metadata_name));

	if (count_metadata_column_num == InvalidAttrNumber)
		elog(ERROR,
			 "missing metadata column '%s' in compressed table",
			 COMPRESSION_COLUMN_METADATA_COUNT_NAME);

	if (sequence_num_column_num == InvalidAttrNumber)
		elog(ERROR,
			 "missing metadata column '%s' in compressed table",
			 COMPRESSION_COLUMN_METADATA_SEQUENCE_NUM_NAME);

	*row_compressor = (RowCompressor){
		.per_row_ctx = AllocSetContextCreate(CurrentMemoryContext,
											 "compress chunk per-row",
											 ALLOCSET_DEFAULT_SIZES),
		.compressed_table = compressed_table,
		.bistate = need_bistate ? GetBulkInsertState() : NULL,
		.resultRelInfo = ts_catalog_open_indexes(compressed_table),
		.n_input_columns = RelationGetDescr(uncompressed_table)->natts,
		.count_metadata_column_offset = AttrNumberGetAttrOffset(count_metadata_column_num),
		.sequence_num_metadata_column_offset = AttrNumberGetAttrOffset(sequence_num_column_num),
		.compressed_values = palloc(sizeof(Datum) * num_columns_in_compressed_table),
		.compressed_is_null = palloc(sizeof(bool) * num_columns_in_compressed_table),
		.rows_compressed_into_current_value = 0,
		.rowcnt_pre_compression = 0,
		.num_compressed_rows = 0,
		.sequence_num = SEQUENCE_NUM_GAP,
		.reset_sequence = reset_sequence,
		.first_iteration = true,
		.insert_options = insert_options,
	};

	memset(row_compressor->compressed_is_null, 1, sizeof(bool) * num_columns_in_compressed_table);

	build_column_map(settings,
					 uncompressed_table,
					 compressed_table,
					 &row_compressor->per_column,
					 &row_compressor->uncompressed_col_to_compressed_col);

	row_compressor->index_oid = get_compressed_chunk_index(row_compressor->resultRelInfo, settings);
}

void
row_compressor_append_sorted_rows(RowCompressor *row_compressor, Tuplesortstate *sorted_rel,
								  TupleDesc sorted_desc, Relation in_rel)
{
	CommandId mycid = GetCurrentCommandId(true);
	TupleTableSlot *slot = MakeTupleTableSlot(sorted_desc, &TTSOpsMinimalTuple);
	bool got_tuple;
	int64 nrows_processed = 0;
	int64 report_reltuples;

	report_reltuples = calculate_reltuples_to_report(in_rel);

	for (got_tuple = tuplesort_gettupleslot(sorted_rel,
											true /*=forward*/,
											false /*=copy*/,
											slot,
											NULL /*=abbrev*/);
		 got_tuple;
		 got_tuple = tuplesort_gettupleslot(sorted_rel,
											true /*=forward*/,
											false /*=copy*/,
											slot,
											NULL /*=abbrev*/))
	{
		row_compressor_process_ordered_slot(row_compressor, slot, mycid);
		if ((++nrows_processed % report_reltuples) == 0)
			elog(LOG,
				 "compressed " INT64_FORMAT " rows from \"%s\"",
				 nrows_processed,
				 RelationGetRelationName(in_rel));
	}

	if (row_compressor->rows_compressed_into_current_value > 0)
		row_compressor_flush(row_compressor, mycid, true);
	elog(LOG,
		 "finished compressing " INT64_FORMAT " rows from \"%s\"",
		 nrows_processed,
		 RelationGetRelationName(in_rel));

	ExecDropSingleTupleTableSlot(slot);
}

static void
row_compressor_process_ordered_slot(RowCompressor *row_compressor, TupleTableSlot *slot,
									CommandId mycid)
{
	MemoryContext old_ctx;
	slot_getallattrs(slot);
	old_ctx = MemoryContextSwitchTo(row_compressor->per_row_ctx);
	if (row_compressor->first_iteration)
	{
		row_compressor_update_group(row_compressor, slot);
		row_compressor->first_iteration = false;
	}
	bool changed_groups = row_compressor_new_row_is_in_new_group(row_compressor, slot);
	bool compressed_row_is_full =
		row_compressor->rows_compressed_into_current_value >= MAX_ROWS_PER_COMPRESSION;
	if (compressed_row_is_full || changed_groups)
	{
		if (row_compressor->rows_compressed_into_current_value > 0)
			row_compressor_flush(row_compressor, mycid, changed_groups);
		if (changed_groups)
			row_compressor_update_group(row_compressor, slot);
	}

	row_compressor_append_row(row_compressor, slot);
	MemoryContextSwitchTo(old_ctx);
	ExecClearTuple(slot);
}

static void
row_compressor_update_group(RowCompressor *row_compressor, TupleTableSlot *row)
{
	int col;
	/* save original memory context */
	const MemoryContext oldcontext = CurrentMemoryContext;

	Assert(row_compressor->rows_compressed_into_current_value == 0);
	Assert(row_compressor->n_input_columns <= row->tts_nvalid);

	MemoryContextSwitchTo(row_compressor->per_row_ctx->parent);
	for (col = 0; col < row_compressor->n_input_columns; col++)
	{
		PerColumn *column = &row_compressor->per_column[col];
		Datum val;
		bool is_null;

		if (column->segment_info == NULL)
			continue;

		Assert(column->compressor == NULL);

		/* Performance Improvement: We should just use array access here; everything is guaranteed
		   to be fetched */
		val = slot_getattr(row, AttrOffsetGetAttrNumber(col), &is_null);
		segment_info_update(column->segment_info, val, is_null);
	}
	/* switch to original memory context */
	MemoryContextSwitchTo(oldcontext);

	/*
	 * The sequence number of the compressed tuple is per segment by grouping
	 * and should be reset when the grouping changes to prevent overflows with
	 * many segmentby columns.
	 *
	 */
	if (row_compressor->reset_sequence)
		row_compressor->sequence_num = SEQUENCE_NUM_GAP; /* Start sequence from beginning */
	else
		row_compressor->sequence_num =
			get_sequence_number_for_current_group(row_compressor->compressed_table,
												  row_compressor->index_oid,
												  row_compressor
													  ->uncompressed_col_to_compressed_col,
												  row_compressor->per_column,
												  row_compressor->n_input_columns,
												  AttrOffsetGetAttrNumber(
													  row_compressor
														  ->sequence_num_metadata_column_offset));
}

static bool
row_compressor_new_row_is_in_new_group(RowCompressor *row_compressor, TupleTableSlot *row)
{
	int col;
	for (col = 0; col < row_compressor->n_input_columns; col++)
	{
		PerColumn *column = &row_compressor->per_column[col];
		Datum datum = CharGetDatum(0);
		bool is_null;

		if (column->segment_info == NULL)
			continue;

		Assert(column->compressor == NULL);

		datum = slot_getattr(row, AttrOffsetGetAttrNumber(col), &is_null);

		if (!segment_info_datum_is_in_group(column->segment_info, datum, is_null))
			return true;
	}

	return false;
}

static void
row_compressor_append_row(RowCompressor *row_compressor, TupleTableSlot *row)
{
	int col;
	for (col = 0; col < row_compressor->n_input_columns; col++)
	{
		Compressor *compressor = row_compressor->per_column[col].compressor;
		bool is_null;
		Datum val;

		/* if there is no compressor, this must be a segmenter, so just skip */
		if (compressor == NULL)
			continue;

		/* Performance Improvement: Since we call getallatts at the beginning, slot_getattr is
		 * useless overhead here, and we should just access the array directly.
		 */
		val = slot_getattr(row, AttrOffsetGetAttrNumber(col), &is_null);
		if (is_null)
		{
			compressor->append_null(compressor);
			if (row_compressor->per_column[col].min_max_metadata_builder != NULL)
				segment_meta_min_max_builder_update_null(
					row_compressor->per_column[col].min_max_metadata_builder);
		}
		else
		{
			compressor->append_val(compressor, val);
			if (row_compressor->per_column[col].min_max_metadata_builder != NULL)
				segment_meta_min_max_builder_update_val(row_compressor->per_column[col]
															.min_max_metadata_builder,
														val);
		}
	}

	row_compressor->rows_compressed_into_current_value += 1;
}

static void
row_compressor_flush(RowCompressor *row_compressor, CommandId mycid, bool changed_groups)
{
	HeapTuple compressed_tuple;

	for (int col = 0; col < row_compressor->n_input_columns; col++)
	{
		PerColumn *column = &row_compressor->per_column[col];
		Compressor *compressor;
		int16 compressed_col;
		if (column->compressor == NULL && column->segment_info == NULL)
			continue;

		compressor = column->compressor;
		compressed_col = row_compressor->uncompressed_col_to_compressed_col[col];

		Assert(compressed_col >= 0);

		if (compressor != NULL)
		{
			void *compressed_data;
			Assert(column->segment_info == NULL);

			compressed_data = compressor->finish(compressor);

			/* non-segment columns are NULL iff all the values are NULL */
			row_compressor->compressed_is_null[compressed_col] = compressed_data == NULL;
			if (compressed_data != NULL)
				row_compressor->compressed_values[compressed_col] =
					PointerGetDatum(compressed_data);

			if (column->min_max_metadata_builder != NULL)
			{
				Assert(column->min_metadata_attr_offset >= 0);
				Assert(column->max_metadata_attr_offset >= 0);

				if (!segment_meta_min_max_builder_empty(column->min_max_metadata_builder))
				{
					Assert(compressed_data != NULL);
					row_compressor->compressed_is_null[column->min_metadata_attr_offset] = false;
					row_compressor->compressed_is_null[column->max_metadata_attr_offset] = false;

					row_compressor->compressed_values[column->min_metadata_attr_offset] =
						segment_meta_min_max_builder_min(column->min_max_metadata_builder);
					row_compressor->compressed_values[column->max_metadata_attr_offset] =
						segment_meta_min_max_builder_max(column->min_max_metadata_builder);
				}
				else
				{
					Assert(compressed_data == NULL);
					row_compressor->compressed_is_null[column->min_metadata_attr_offset] = true;
					row_compressor->compressed_is_null[column->max_metadata_attr_offset] = true;
				}
			}
		}
		else if (column->segment_info != NULL)
		{
			row_compressor->compressed_values[compressed_col] = column->segment_info->val;
			row_compressor->compressed_is_null[compressed_col] = column->segment_info->is_null;
		}
	}

	row_compressor->compressed_values[row_compressor->count_metadata_column_offset] =
		Int32GetDatum(row_compressor->rows_compressed_into_current_value);
	row_compressor->compressed_is_null[row_compressor->count_metadata_column_offset] = false;

	row_compressor->compressed_values[row_compressor->sequence_num_metadata_column_offset] =
		Int32GetDatum(row_compressor->sequence_num);
	row_compressor->compressed_is_null[row_compressor->sequence_num_metadata_column_offset] = false;

	/* overflow could happen only if chunk has more than 200B rows */
	if (row_compressor->sequence_num > PG_INT32_MAX - SEQUENCE_NUM_GAP)
		elog(ERROR, "sequence id overflow");

	row_compressor->sequence_num += SEQUENCE_NUM_GAP;

	compressed_tuple = heap_form_tuple(RelationGetDescr(row_compressor->compressed_table),
									   row_compressor->compressed_values,
									   row_compressor->compressed_is_null);
	Assert(row_compressor->bistate != NULL);
	heap_insert(row_compressor->compressed_table,
				compressed_tuple,
				mycid,
				row_compressor->insert_options /*=options*/,
				row_compressor->bistate);
	if (row_compressor->resultRelInfo->ri_NumIndices > 0)
	{
		ts_catalog_index_insert(row_compressor->resultRelInfo, compressed_tuple);
	}

	heap_freetuple(compressed_tuple);

	/* free the compressed values now that we're done with them (the old compressor is freed in
	 * finish()) */
	for (int col = 0; col < row_compressor->n_input_columns; col++)
	{
		PerColumn *column = &row_compressor->per_column[col];
		int16 compressed_col;
		if (column->compressor == NULL && column->segment_info == NULL)
			continue;

		compressed_col = row_compressor->uncompressed_col_to_compressed_col[col];
		Assert(compressed_col >= 0);
		if (row_compressor->compressed_is_null[compressed_col])
			continue;

		/* don't free the segment-bys if we've overflowed the row, we still need them */
		if (column->segment_info != NULL && !changed_groups)
			continue;

		if (column->compressor != NULL || !column->segment_info->typ_by_val)
			pfree(DatumGetPointer(row_compressor->compressed_values[compressed_col]));

		if (column->min_max_metadata_builder != NULL)
		{
			/* segment_meta_min_max_builder_reset will free the values, so  clear here */
			if (!row_compressor->compressed_is_null[column->min_metadata_attr_offset])
			{
				row_compressor->compressed_values[column->min_metadata_attr_offset] = 0;
				row_compressor->compressed_is_null[column->min_metadata_attr_offset] = true;
			}
			if (!row_compressor->compressed_is_null[column->max_metadata_attr_offset])
			{
				row_compressor->compressed_values[column->max_metadata_attr_offset] = 0;
				row_compressor->compressed_is_null[column->max_metadata_attr_offset] = true;
			}
			segment_meta_min_max_builder_reset(column->min_max_metadata_builder);
		}

		row_compressor->compressed_values[compressed_col] = 0;
		row_compressor->compressed_is_null[compressed_col] = true;
	}
	row_compressor->rowcnt_pre_compression += row_compressor->rows_compressed_into_current_value;
	row_compressor->num_compressed_rows++;
	row_compressor->rows_compressed_into_current_value = 0;

	MemoryContextReset(row_compressor->per_row_ctx);
}

void
row_compressor_reset(RowCompressor *row_compressor)
{
	row_compressor->first_iteration = true;
}

void
row_compressor_close(RowCompressor *row_compressor)
{
	if (row_compressor->bistate)
		FreeBulkInsertState(row_compressor->bistate);
	ts_catalog_close_indexes(row_compressor->resultRelInfo);
}

/******************
 ** segment_info **
 ******************/

SegmentInfo *
segment_info_new(Form_pg_attribute column_attr)
{
	TypeCacheEntry *tce = lookup_type_cache(column_attr->atttypid, TYPECACHE_EQ_OPR_FINFO);

	if (!OidIsValid(tce->eq_opr_finfo.fn_oid))
		elog(ERROR, "no equality function for column \"%s\"", NameStr(column_attr->attname));

	SegmentInfo *segment_info = palloc(sizeof(*segment_info));

	*segment_info = (SegmentInfo){
		.typlen = column_attr->attlen,
		.typ_by_val = column_attr->attbyval,
	};

	fmgr_info_cxt(tce->eq_opr_finfo.fn_oid, &segment_info->eq_fn, CurrentMemoryContext);

	segment_info->eq_fcinfo = HEAP_FCINFO(2);
	segment_info->collation = column_attr->attcollation;
	InitFunctionCallInfoData(*segment_info->eq_fcinfo,
							 &segment_info->eq_fn /*=Flinfo*/,
							 2 /*=Nargs*/,
							 column_attr->attcollation /*=Collation*/,
							 NULL, /*=Context*/
							 NULL  /*=ResultInfo*/
	);

	return segment_info;
}

void
segment_info_update(SegmentInfo *segment_info, Datum val, bool is_null)
{
	segment_info->is_null = is_null;
	if (is_null)
		segment_info->val = 0;
	else
		segment_info->val = datumCopy(val, segment_info->typ_by_val, segment_info->typlen);
}

bool
segment_info_datum_is_in_group(SegmentInfo *segment_info, Datum datum, bool is_null)
{
	Datum data_is_eq;
	FunctionCallInfo eq_fcinfo;
	/* if one of the datums is null and the other isn't, we must be in a new group */
	if (segment_info->is_null != is_null)
		return false;

	/* they're both null */
	if (segment_info->is_null)
		return true;

	/* neither is null, call the eq function */
	eq_fcinfo = segment_info->eq_fcinfo;

	FC_SET_ARG(eq_fcinfo, 0, segment_info->val);
	FC_SET_ARG(eq_fcinfo, 1, datum);

	data_is_eq = FunctionCallInvoke(eq_fcinfo);

	if (eq_fcinfo->isnull)
		return false;

	return DatumGetBool(data_is_eq);
}

/**********************
 ** decompress_chunk **
 **********************/

RowDecompressor
build_decompressor(Relation in_rel, Relation out_rel)
{
	TupleDesc in_desc = RelationGetDescr(in_rel);
	TupleDesc out_desc = CreateTupleDescCopyConstr(RelationGetDescr(out_rel));

	RowDecompressor decompressor = {
		.num_compressed_columns = in_desc->natts,

		.in_desc = in_desc,
		.in_rel = in_rel,

		.out_desc = out_desc,
		.out_rel = out_rel,
		.indexstate = ts_catalog_open_indexes(out_rel),

		.mycid = GetCurrentCommandId(true),
		.bistate = GetBulkInsertState(),

		.compressed_datums = palloc(sizeof(Datum) * in_desc->natts),
		.compressed_is_nulls = palloc(sizeof(bool) * in_desc->natts),

		/* cache memory used to store the decompressed datums/is_null for form_tuple */
		.decompressed_datums = palloc(sizeof(Datum) * out_desc->natts),
		.decompressed_is_nulls = palloc(sizeof(bool) * out_desc->natts),

		.per_compressed_row_ctx = AllocSetContextCreate(CurrentMemoryContext,
														"decompress chunk per-compressed row",
														ALLOCSET_DEFAULT_SIZES),
		.estate = CreateExecutorState(),

		.decompressed_slots = palloc0(sizeof(void *) * GLOBAL_MAX_ROWS_PER_COMPRESSION),
	};

	create_per_compressed_column(&decompressor);

	/*
	 * We need to make sure decompressed_is_nulls is in a defined state. While this
	 * will get written for normal columns it will not get written for dropped columns
	 * since dropped columns don't exist in the compressed chunk so we initialize
	 * with true here.
	 */
	memset(decompressor.decompressed_is_nulls, true, out_desc->natts);

	detoaster_init(&decompressor.detoaster, CurrentMemoryContext);

	return decompressor;
}

void
row_decompressor_close(RowDecompressor *decompressor)
{
	FreeBulkInsertState(decompressor->bistate);
	MemoryContextDelete(decompressor->per_compressed_row_ctx);
	ts_catalog_close_indexes(decompressor->indexstate);
	FreeExecutorState(decompressor->estate);
	detoaster_close(&decompressor->detoaster);
}

void
decompress_chunk(Oid in_table, Oid out_table)
{
	/*
	 * Locks are taken in the order uncompressed table then compressed table
	 * for consistency with compress_chunk.
	 * We are _just_ INSERTing into the out_table so in principle we could take
	 * a RowExclusive lock, and let other operations read and write this table
	 * as we work. However, we currently compress each table as a oneshot, so
	 * we're taking the stricter lock to prevent accidents.
	 * We want to prevent other decompressors from decompressing this table,
	 * and we want to prevent INSERTs or UPDATEs which could mess up our decompression.
	 * We may as well allow readers to keep reading the compressed data while
	 * we are compressing, so we only take an ExclusiveLock instead of AccessExclusive.
	 */
	Relation out_rel = table_open(out_table, AccessExclusiveLock);
	Relation in_rel = table_open(in_table, ExclusiveLock);
	int64 nrows_processed = 0;

	RowDecompressor decompressor = build_decompressor(in_rel, out_rel);
	TupleTableSlot *slot = table_slot_create(in_rel, NULL);
	TableScanDesc scan = table_beginscan(in_rel, GetLatestSnapshot(), 0, (ScanKey) NULL);
	int64 report_reltuples = calculate_reltuples_to_report(in_rel);

	while (table_scan_getnextslot(scan, ForwardScanDirection, slot))
	{
		bool should_free;
		HeapTuple tuple = ExecFetchSlotHeapTuple(slot, false, &should_free);

		heap_deform_tuple(tuple,
						  decompressor.in_desc,
						  decompressor.compressed_datums,
						  decompressor.compressed_is_nulls);

		if (should_free)
			heap_freetuple(tuple);

		row_decompressor_decompress_row_to_table(&decompressor);

		if ((++nrows_processed % report_reltuples) == 0)
			elog(LOG,
				 "decompressed " INT64_FORMAT " rows from \"%s\"",
				 nrows_processed,
				 RelationGetRelationName(in_rel));
	}

	elog(LOG,
		 "finished decompressing " INT64_FORMAT " rows from \"%s\"",
		 nrows_processed,
		 RelationGetRelationName(in_rel));
	table_endscan(scan);
	ExecDropSingleTupleTableSlot(slot);
	row_decompressor_close(&decompressor);

	table_close(out_rel, NoLock);
	table_close(in_rel, NoLock);
}

static void
create_per_compressed_column(RowDecompressor *decompressor)
{
	Oid compressed_data_type_oid = ts_custom_type_cache_get(CUSTOM_TYPE_COMPRESSED_DATA)->type_oid;
	Assert(OidIsValid(compressed_data_type_oid));

	decompressor->per_compressed_cols =
		palloc(sizeof(*decompressor->per_compressed_cols) * decompressor->in_desc->natts);

	Assert(OidIsValid(compressed_data_type_oid));

	for (int col = 0; col < decompressor->in_desc->natts; col++)
	{
		Oid decompressed_type;
		bool is_compressed;
		int16 decompressed_column_offset;
		PerCompressedColumn *per_compressed_col = &decompressor->per_compressed_cols[col];
		Form_pg_attribute compressed_attr = TupleDescAttr(decompressor->in_desc, col);
		char *col_name = NameStr(compressed_attr->attname);
		if (strcmp(col_name, COMPRESSION_COLUMN_METADATA_COUNT_NAME) == 0)
		{
			decompressor->count_compressed_attindex = col;
		}

		/* find the mapping from compressed column to uncompressed column, setting
		 * the index of columns that don't have an uncompressed version
		 * (such as metadata) to -1
		 * Assumption: column names are the same on compressed and
		 *       uncompressed chunk.
		 */
		AttrNumber decompressed_colnum = get_attnum(decompressor->out_rel->rd_id, col_name);
		if (!AttributeNumberIsValid(decompressed_colnum))
		{
			*per_compressed_col = (PerCompressedColumn){
				.decompressed_column_offset = -1,
			};
			continue;
		}

		decompressed_column_offset = AttrNumberGetAttrOffset(decompressed_colnum);

		decompressed_type =
			TupleDescAttr(decompressor->out_desc, decompressed_column_offset)->atttypid;

		/* determine if the data is compressed or not */
		is_compressed = compressed_attr->atttypid == compressed_data_type_oid;
		if (!is_compressed && compressed_attr->atttypid != decompressed_type)
			elog(ERROR,
				 "compressed table type '%s' does not match decompressed table type '%s' for "
				 "segment-by column \"%s\"",
				 format_type_be(compressed_attr->atttypid),
				 format_type_be(decompressed_type),
				 col_name);

		*per_compressed_col = (PerCompressedColumn){
			.decompressed_column_offset = decompressed_column_offset,
			.is_compressed = is_compressed,
			.decompressed_type = decompressed_type,
		};
	}
}

/*
 * Decompresses the current compressed batch into decompressed_slots, and returns
 * the number of rows in batch.
 */
static int
decompress_batch(RowDecompressor *decompressor)
{
	MemoryContext old_ctx = MemoryContextSwitchTo(decompressor->per_compressed_row_ctx);

	/*
	 * Set segmentbys and compressed columns with default value.
	 */
	for (int input_column = 0; input_column < decompressor->num_compressed_columns; input_column++)
	{
		PerCompressedColumn *column_info = &decompressor->per_compressed_cols[input_column];
		const int output_index = column_info->decompressed_column_offset;

		/* Metadata column. */
		if (output_index < 0)
		{
			continue;
		}

		/* Segmentby column. */
		if (!column_info->is_compressed)
		{
			decompressor->decompressed_datums[output_index] =
				decompressor->compressed_datums[input_column];
			decompressor->decompressed_is_nulls[output_index] =
				decompressor->compressed_is_nulls[input_column];
			continue;
		}

		/* Compressed column with default value. */
		if (decompressor->compressed_is_nulls[input_column])
		{
			column_info->iterator = NULL;
			decompressor->decompressed_datums[output_index] =
				getmissingattr(decompressor->out_desc,
							   output_index + 1,
							   &decompressor->decompressed_is_nulls[output_index]);

			continue;
		}

		/* Normal compressed column. */
		Datum compressed_datum = PointerGetDatum(
			detoaster_detoast_attr((struct varlena *) DatumGetPointer(
									   decompressor->compressed_datums[input_column]),
								   &decompressor->detoaster));
		CompressedDataHeader *header = get_compressed_data_header(compressed_datum);
		column_info->iterator =
			definitions[header->compression_algorithm]
				.iterator_init_forward(PointerGetDatum(header), column_info->decompressed_type);
	}

	/*
	 * Set the number of batch rows from count metadata column.
	 */
	const int n_batch_rows =
		DatumGetInt32(decompressor->compressed_datums[decompressor->count_compressed_attindex]);
	CheckCompressedData(n_batch_rows > 0);
	CheckCompressedData(n_batch_rows <= GLOBAL_MAX_ROWS_PER_COMPRESSION);

	/*
	 * Decompress all compressed columns for each row of the batch.
	 */
	for (int current_row = 0; current_row < n_batch_rows; current_row++)
	{
		for (int16 col = 0; col < decompressor->num_compressed_columns; col++)
		{
			PerCompressedColumn *column_info = &decompressor->per_compressed_cols[col];
			if (column_info->iterator == NULL)
			{
				continue;
			}
			Assert(column_info->is_compressed);

			const int output_index = column_info->decompressed_column_offset;
			const DecompressResult value = column_info->iterator->try_next(column_info->iterator);
			CheckCompressedData(!value.is_done);
			decompressor->decompressed_datums[output_index] = value.val;
			decompressor->decompressed_is_nulls[output_index] = value.is_null;
		}

		/*
		 * Form the heap tuple for this decompressed rows and save it for later
		 * processing.
		 */
		if (decompressor->decompressed_slots[current_row] == NULL)
		{
			MemoryContextSwitchTo(old_ctx);
			decompressor->decompressed_slots[current_row] =
				MakeSingleTupleTableSlot(decompressor->out_desc, &TTSOpsHeapTuple);
			MemoryContextSwitchTo(decompressor->per_compressed_row_ctx);
		}
		else
		{
			ExecClearTuple(decompressor->decompressed_slots[current_row]);
		}

		TupleTableSlot *decompressed_slot = decompressor->decompressed_slots[current_row];

		HeapTuple decompressed_tuple = heap_form_tuple(decompressor->out_desc,
													   decompressor->decompressed_datums,
													   decompressor->decompressed_is_nulls);
		decompressed_tuple->t_tableOid = decompressor->out_rel->rd_id;

		ExecStoreHeapTuple(decompressed_tuple, decompressed_slot, /* should_free = */ false);
	}

	/*
	 * Verify that all other columns have ended, i.e. their length is consistent
	 * with the count metadata column.
	 */
	for (int16 col = 0; col < decompressor->num_compressed_columns; col++)
	{
		PerCompressedColumn *column_info = &decompressor->per_compressed_cols[col];
		if (column_info->iterator == NULL)
		{
			continue;
		}
		Assert(column_info->is_compressed);
		const DecompressResult value = column_info->iterator->try_next(column_info->iterator);
		CheckCompressedData(value.is_done);
	}
	MemoryContextSwitchTo(old_ctx);

	decompressor->batches_decompressed++;
	decompressor->tuples_decompressed += n_batch_rows;

	return n_batch_rows;
}

void
row_decompressor_decompress_row_to_table(RowDecompressor *decompressor)
{
	const int n_batch_rows = decompress_batch(decompressor);

	MemoryContext old_ctx = MemoryContextSwitchTo(decompressor->per_compressed_row_ctx);

	/* Insert all decompressed rows into table using the bulk insert API. */
	table_multi_insert(decompressor->out_rel,
					   decompressor->decompressed_slots,
					   n_batch_rows,
					   decompressor->mycid,
					   /* options = */ 0,
					   decompressor->bistate);

	/*
	 * Now, update the indexes. If we have several indexes, we want to first
	 * insert the entire batch into one index, then into another, and so on.
	 * Working with one index at a time gives better data access locality,
	 * which reduces the load on shared buffers cache.
	 * The normal Postgres code inserts each row into all indexes, so to do it
	 * the other way around, we create a temporary ResultRelInfo that only
	 * references one index. Then we loop over indexes, and for each index we
	 * set it to this temporary ResultRelInfo, and insert all rows into this
	 * single index.
	 */
	if (decompressor->indexstate->ri_NumIndices > 0)
	{
		ResultRelInfo indexstate_copy = *decompressor->indexstate;
		Relation single_index_relation;
		IndexInfo *single_index_info;
		indexstate_copy.ri_NumIndices = 1;
		indexstate_copy.ri_IndexRelationDescs = &single_index_relation;
		indexstate_copy.ri_IndexRelationInfo = &single_index_info;
		for (int i = 0; i < decompressor->indexstate->ri_NumIndices; i++)
		{
			single_index_relation = decompressor->indexstate->ri_IndexRelationDescs[i];
			single_index_info = decompressor->indexstate->ri_IndexRelationInfo[i];
			for (int row = 0; row < n_batch_rows; row++)
			{
				TupleTableSlot *decompressed_slot = decompressor->decompressed_slots[row];
				EState *estate = decompressor->estate;
				ExprContext *econtext = GetPerTupleExprContext(estate);

				/* Arrange for econtext's scan tuple to be the tuple under test */
				econtext->ecxt_scantuple = decompressed_slot;
#if PG14_LT
				estate->es_result_relation_info = &indexstate_copy;
#endif
				ExecInsertIndexTuplesCompat(&indexstate_copy,
											decompressed_slot,
											estate,
											false,
											false,
											NULL,
											NIL,
											false);
			}
		}
	}

	MemoryContextSwitchTo(old_ctx);
	MemoryContextReset(decompressor->per_compressed_row_ctx);
}

void
row_decompressor_decompress_row_to_tuplesort(RowDecompressor *decompressor,
											 Tuplesortstate *tuplesortstate)
{
	const int n_batch_rows = decompress_batch(decompressor);

	MemoryContext old_ctx = MemoryContextSwitchTo(decompressor->per_compressed_row_ctx);

	for (int i = 0; i < n_batch_rows; i++)
	{
		tuplesort_puttupleslot(tuplesortstate, decompressor->decompressed_slots[i]);
	}

	MemoryContextSwitchTo(old_ctx);
	MemoryContextReset(decompressor->per_compressed_row_ctx);
}

/********************/
/*** SQL Bindings ***/
/********************/

Datum
tsl_compressed_data_decompress_forward(PG_FUNCTION_ARGS)
{
	CompressedDataHeader *header;
	FuncCallContext *funcctx;
	MemoryContext oldcontext;
	DecompressionIterator *iter;
	DecompressResult res;

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	if (SRF_IS_FIRSTCALL())
	{
		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		header = get_compressed_data_header(PG_GETARG_DATUM(0));

		iter = definitions[header->compression_algorithm]
				   .iterator_init_forward(PointerGetDatum(header),
										  get_fn_expr_argtype(fcinfo->flinfo, 1));

		funcctx->user_fctx = iter;
		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();

	iter = funcctx->user_fctx;
	res = iter->try_next(iter);

	if (res.is_done)
		SRF_RETURN_DONE(funcctx);

	if (res.is_null)
		SRF_RETURN_NEXT_NULL(funcctx);

	SRF_RETURN_NEXT(funcctx, res.val);
}

Datum
tsl_compressed_data_decompress_reverse(PG_FUNCTION_ARGS)
{
	CompressedDataHeader *header;
	FuncCallContext *funcctx;
	MemoryContext oldcontext;
	DecompressionIterator *iter;
	DecompressResult res;

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	if (SRF_IS_FIRSTCALL())
	{
		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		header = get_compressed_data_header(PG_GETARG_DATUM(0));

		iter = definitions[header->compression_algorithm]
				   .iterator_init_reverse(PointerGetDatum(header),
										  get_fn_expr_argtype(fcinfo->flinfo, 1));

		funcctx->user_fctx = iter;
		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();

	iter = funcctx->user_fctx;
	res = iter->try_next(iter);

	if (res.is_done)
		SRF_RETURN_DONE(funcctx);

	if (res.is_null)
		SRF_RETURN_NEXT_NULL(funcctx);

	SRF_RETURN_NEXT(funcctx, res.val);
	;
}

Datum
tsl_compressed_data_send(PG_FUNCTION_ARGS)
{
	CompressedDataHeader *header = get_compressed_data_header(PG_GETARG_DATUM(0));
	StringInfoData buf;

	pq_begintypsend(&buf);
	pq_sendbyte(&buf, header->compression_algorithm);

	definitions[header->compression_algorithm].compressed_data_send(header, &buf);

	PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}

Datum
tsl_compressed_data_recv(PG_FUNCTION_ARGS)
{
	StringInfo buf = (StringInfo) PG_GETARG_POINTER(0);
	CompressedDataHeader header = { .vl_len_ = { 0 } };

	header.compression_algorithm = pq_getmsgbyte(buf);

	if (header.compression_algorithm >= _END_COMPRESSION_ALGORITHMS)
		elog(ERROR, "invalid compression algorithm %d", header.compression_algorithm);

	return definitions[header.compression_algorithm].compressed_data_recv(buf);
}

extern Datum
tsl_compressed_data_in(PG_FUNCTION_ARGS)
{
	const char *input = PG_GETARG_CSTRING(0);
	size_t input_len = strlen(input);
	int decoded_len;
	char *decoded;
	StringInfoData data;
	Datum result;

	if (input_len > PG_INT32_MAX)
		elog(ERROR, "input too long");

	decoded_len = pg_b64_dec_len(input_len);
	decoded = palloc(decoded_len + 1);
	decoded_len = pg_b64_decode(input, input_len, decoded, decoded_len);

	if (decoded_len < 0)
		elog(ERROR, "could not decode base64-encoded compressed data");

	decoded[decoded_len] = '\0';
	data = (StringInfoData){
		.data = decoded,
		.len = decoded_len,
		.maxlen = decoded_len,
	};

	result = DirectFunctionCall1(tsl_compressed_data_recv, PointerGetDatum(&data));

	PG_RETURN_DATUM(result);
}

extern Datum
tsl_compressed_data_out(PG_FUNCTION_ARGS)
{
	Datum bytes_data = DirectFunctionCall1(tsl_compressed_data_send, PG_GETARG_DATUM(0));
	bytea *bytes = DatumGetByteaP(bytes_data);
	int raw_len = VARSIZE_ANY_EXHDR(bytes);
	const char *raw_data = VARDATA(bytes);
	int encoded_len = pg_b64_enc_len(raw_len);
	char *encoded = palloc(encoded_len + 1);
	encoded_len = pg_b64_encode(raw_data, raw_len, encoded, encoded_len);

	if (encoded_len < 0)
		elog(ERROR, "could not base64-encode compressed data");

	encoded[encoded_len] = '\0';

	PG_RETURN_CSTRING(encoded);
}

extern CompressionStorage
compression_get_toast_storage(CompressionAlgorithm algorithm)
{
	if (algorithm == _INVALID_COMPRESSION_ALGORITHM || algorithm >= _END_COMPRESSION_ALGORITHMS)
		elog(ERROR, "invalid compression algorithm %d", algorithm);
	return definitions[algorithm].compressed_data_storage;
}

/*
 * Return a default compression algorithm suitable
 * for the type. The actual algorithm used for a
 * type might be different though since the compressor
 * can deviate from the default. The actual algorithm
 * used for a specific batch can only be determined
 * by reading the batch header.
 */
extern CompressionAlgorithm
compression_get_default_algorithm(Oid typeoid)
{
	switch (typeoid)
	{
		case INT4OID:
		case INT2OID:
		case INT8OID:
		case DATEOID:
		case TIMESTAMPOID:
		case TIMESTAMPTZOID:
			return COMPRESSION_ALGORITHM_DELTADELTA;

		case FLOAT4OID:
		case FLOAT8OID:
			return COMPRESSION_ALGORITHM_GORILLA;

		case NUMERICOID:
			return COMPRESSION_ALGORITHM_ARRAY;

		default:
		{
			/* use dictitionary if possible, otherwise use array */
			TypeCacheEntry *tentry =
				lookup_type_cache(typeoid, TYPECACHE_EQ_OPR_FINFO | TYPECACHE_HASH_PROC_FINFO);
			if (tentry->hash_proc_finfo.fn_addr == NULL || tentry->eq_opr_finfo.fn_addr == NULL)
				return COMPRESSION_ALGORITHM_ARRAY;
			return COMPRESSION_ALGORITHM_DICTIONARY;
		}
	}
}

/*
 * Build scankeys for decompression of specific batches. key_columns references the
 * columns of the uncompressed chunk.
 */
static ScanKeyData *
build_scankeys(Oid hypertable_relid, Oid out_rel, RowDecompressor *decompressor,
			   Bitmapset *key_columns, Bitmapset **null_columns, TupleTableSlot *slot,
			   int *num_scankeys)
{
	int key_index = 0;
	ScanKeyData *scankeys = NULL;

	CompressionSettings *settings = ts_compression_settings_get(out_rel);
	Assert(settings);

	if (!bms_is_empty(key_columns))
	{
		scankeys = palloc0(bms_num_members(key_columns) * 2 * sizeof(ScanKeyData));
		int i = -1;
		while ((i = bms_next_member(key_columns, i)) > 0)
		{
			AttrNumber attno = i + FirstLowInvalidHeapAttributeNumber;
			char *attname = get_attname(decompressor->out_rel->rd_id, attno, false);
			bool isnull;
			AttrNumber ht_attno = get_attnum(hypertable_relid, attname);
			Datum value = slot_getattr(slot, ht_attno, &isnull);
			/*
			 * There are 3 possible scenarios we have to consider
			 * when dealing with columns which are part of unique
			 * constraints.
			 *
			 * 1. Column is segmentby-Column
			 * In this case we can add a single ScanKey with an
			 * equality check for the value.
			 * 2. Column is orderby-Column
			 * In this we can add 2 ScanKeys with range constraints
			 * utilizing batch metadata.
			 * 3. Column is neither segmentby nor orderby
			 * In this case we cannot utilize this column for
			 * batch filtering as the values are compressed and
			 * we have no metadata.
			 */
			if (ts_array_is_member(settings->fd.segmentby, attname))
			{
				key_index = create_segment_filter_scankey(decompressor,
														  attname,
														  BTEqualStrategyNumber,
														  scankeys,
														  key_index,
														  null_columns,
														  value,
														  isnull);
			}
			if (ts_array_is_member(settings->fd.orderby, attname))
			{
				/* Cannot optimize orderby columns with NULL values since those
				 * are not visible in metadata
				 */
				if (isnull)
					continue;

				int16 index = ts_array_position(settings->fd.orderby, attname);

				key_index = create_segment_filter_scankey(decompressor,
														  column_segment_min_name(index),
														  BTLessEqualStrategyNumber,
														  scankeys,
														  key_index,
														  null_columns,
														  value,
														  false); /* is_null_check */
				key_index = create_segment_filter_scankey(decompressor,
														  column_segment_max_name(index),
														  BTGreaterEqualStrategyNumber,
														  scankeys,
														  key_index,
														  null_columns,
														  value,
														  false); /* is_null_check */
			}
		}
	}

	*num_scankeys = key_index;
	return scankeys;
}

static int
create_segment_filter_scankey(RowDecompressor *decompressor, char *segment_filter_col_name,
							  StrategyNumber strategy, ScanKeyData *scankeys, int num_scankeys,
							  Bitmapset **null_columns, Datum value, bool is_null_check)
{
	AttrNumber cmp_attno = get_attnum(decompressor->in_rel->rd_id, segment_filter_col_name);
	Assert(cmp_attno != InvalidAttrNumber);
	/* This should never happen but if it does happen, we can't generate a scan key for
	 * the filter column so just skip it */
	if (cmp_attno == InvalidAttrNumber)
		return num_scankeys;

	/*
	 * In PG versions <= 14 NULL values are always considered distinct
	 * from other NULL values and therefore NULLABLE multi-columnn
	 * unique constraints might expose unexpected behaviour in the
	 * presence of NULL values.
	 * Since SK_SEARCHNULL is not supported by heap scans we cannot
	 * build a ScanKey for NOT NULL and instead have to do those
	 * checks manually.
	 */
	if (is_null_check)
	{
		*null_columns = bms_add_member(*null_columns, cmp_attno);
		return num_scankeys;
	}

	Oid atttypid = decompressor->in_desc->attrs[AttrNumberGetAttrOffset(cmp_attno)].atttypid;

	TypeCacheEntry *tce = lookup_type_cache(atttypid, TYPECACHE_BTREE_OPFAMILY);
	if (!OidIsValid(tce->btree_opf))
		elog(ERROR, "no btree opfamily for type \"%s\"", format_type_be(atttypid));

	Oid opr = get_opfamily_member(tce->btree_opf, atttypid, atttypid, strategy);

	/*
	 * Fall back to btree operator input type when it is binary compatible with
	 * the column type and no operator for column type could be found.
	 */
	if (!OidIsValid(opr) && IsBinaryCoercible(atttypid, tce->btree_opintype))
	{
		opr =
			get_opfamily_member(tce->btree_opf, tce->btree_opintype, tce->btree_opintype, strategy);
	}

	/* No operator could be found so we can't create the scankey. */
	if (!OidIsValid(opr))
		return num_scankeys;

	opr = get_opcode(opr);
	Assert(OidIsValid(opr));
	/* We should never end up here but: no opcode, no optimization */
	if (!OidIsValid(opr))
		return num_scankeys;

	ScanKeyEntryInitialize(&scankeys[num_scankeys++],
						   0, /* flags */
						   cmp_attno,
						   strategy,
						   InvalidOid, /* No strategy subtype. */
						   decompressor->in_desc->attrs[AttrNumberGetAttrOffset(cmp_attno)]
							   .attcollation,
						   opr,
						   value);

	return num_scankeys;
}

/*
 * For insert into compressed chunks with unique index determine the
 * columns which are safe to use for batch filtering.
 *
 * This is based on RelationGetIndexAttrBitmap from postgres with changes
 * to also track unique expression indexes.
 */
static Bitmapset *
compressed_insert_key_columns(Relation relation)
{
	Bitmapset *indexattrs = NULL; /* indexed columns */
	ListCell *l;

	/* Fast path if definitely no indexes */
	if (!RelationGetForm(relation)->relhasindex)
		return NULL;

	List *indexoidlist = RelationGetIndexList(relation);

	/* Fall out if no indexes (but relhasindex was set) */
	if (indexoidlist == NIL)
		return NULL;

	/*
	 * For each index, add referenced attributes to indexattrs.
	 *
	 * Note: we consider all indexes returned by RelationGetIndexList, even if
	 * they are not indisready or indisvalid.  This is important because an
	 * index for which CREATE INDEX CONCURRENTLY has just started must be
	 * included in HOT-safety decisions (see README.HOT).  If a DROP INDEX
	 * CONCURRENTLY is far enough along that we should ignore the index, it
	 * won't be returned at all by RelationGetIndexList.
	 */
	foreach (l, indexoidlist)
	{
		Oid indexOid = lfirst_oid(l);
		Relation indexDesc = index_open(indexOid, AccessShareLock);

		if (!indexDesc->rd_index->indisunique)
		{
			index_close(indexDesc, AccessShareLock);
			continue;
		}

		/* Collect simple attribute references.
		 * For covering indexes we only need to collect the key attributes.
		 * Unlike RelationGetIndexAttrBitmap we allow expression indexes
		 * but we do not extract attributes from the expressions as that
		 * would not be a safe filter as the expression can alter attributes
		 * which would not make them sufficient for batch filtering.
		 */
		for (int i = 0; i < indexDesc->rd_index->indnkeyatts; i++)
		{
			int attrnum = indexDesc->rd_index->indkey.values[i];
			if (attrnum != 0)
			{
				indexattrs =
					bms_add_member(indexattrs, attrnum - FirstLowInvalidHeapAttributeNumber);
			}
		}
		index_close(indexDesc, AccessShareLock);
	}

	return indexattrs;
}

void
decompress_batches_for_insert(const ChunkInsertState *cis, TupleTableSlot *slot)
{
	/* COPY operation can end up flushing an empty buffer which
	 * could in turn send an empty slot our way. No need to decompress
	 * anything if that happens.
	 */
	if (TTS_EMPTY(slot))
	{
		return;
	}

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

	Oid comp_relid = ts_chunk_get_relid(cis->compressed_chunk_id, false);
	Relation in_rel = relation_open(comp_relid, RowExclusiveLock);

	RowDecompressor decompressor = build_decompressor(in_rel, out_rel);
	Bitmapset *key_columns = compressed_insert_key_columns(out_rel);
	Bitmapset *null_columns = NULL;

	int num_scankeys;
	ScanKeyData *scankeys = build_scankeys(cis->hypertable_relid,
										   in_rel->rd_id,
										   &decompressor,
										   key_columns,
										   &null_columns,
										   slot,
										   &num_scankeys);

	bms_free(key_columns);

	/*
	 * Using latest snapshot to scan the heap since we are doing this to build
	 * the index on the uncompressed chunks in order to do speculative insertion
	 * which is always built from all tuples (even in higher levels of isolation).
	 */
	TupleTableSlot *compressed_slot = table_slot_create(in_rel, NULL);
	Snapshot snapshot = GetLatestSnapshot();
	TableScanDesc scan = table_beginscan(in_rel, snapshot, num_scankeys, scankeys);

	while (table_scan_getnextslot(scan, ForwardScanDirection, compressed_slot))
	{
		bool valid = true;

		/*
		 * Since the heap scan API does not support SK_SEARCHNULL we have to check
		 * for NULL values manually when those are part of the constraints.
		 */
		for (int attno = bms_next_member(null_columns, -1); attno >= 0;
			 attno = bms_next_member(null_columns, attno))
		{
			if (!slot_attisnull(compressed_slot, attno))
			{
				valid = false;
				break;
			}
		}

		/*
		 * Skip if NULL check failed.
		 */
		if (!valid)
			continue;

		bool should_free;
		HeapTuple tuple = ExecFetchSlotHeapTuple(compressed_slot, false, &should_free);
		heap_deform_tuple(tuple,
						  decompressor.in_desc,
						  decompressor.compressed_datums,
						  decompressor.compressed_is_nulls);

		if (should_free)
			heap_freetuple(tuple);

		write_logical_replication_msg_decompression_start();
		row_decompressor_decompress_row_to_table(&decompressor);
		write_logical_replication_msg_decompression_end();

		TM_FailureData tmfd;
		TM_Result result pg_attribute_unused();
		result = table_tuple_delete(in_rel,
									&compressed_slot->tts_tid,
									decompressor.mycid,
									snapshot,
									InvalidSnapshot,
									true,
									&tmfd,
									false);
		Assert(result == TM_Ok);

		Assert(cis->cds != NULL);
		cis->cds->batches_decompressed += decompressor.batches_decompressed;
		cis->cds->tuples_decompressed += decompressor.tuples_decompressed;
	}

	table_endscan(scan);
	ExecDropSingleTupleTableSlot(compressed_slot);
	row_decompressor_close(&decompressor);

	CommandCounterIncrement();
	table_close(in_rel, NoLock);
}

const CompressionAlgorithmDefinition *
algorithm_definition(CompressionAlgorithm algo)
{
	Assert(algo > 0 && algo < _END_COMPRESSION_ALGORITHMS);
	return &definitions[algo];
}

#if PG14_GE
static BatchFilter *
make_batchfilter(char *column_name, StrategyNumber strategy, Oid collation, RegProcedure opcode,
				 Const *value, bool is_null_check, bool is_null)
{
	BatchFilter *segment_filter = palloc0(sizeof(*segment_filter));

	*segment_filter = (BatchFilter){
		.strategy = strategy,
		.collation = collation,
		.opcode = opcode,
		.value = value,
		.is_null_check = is_null_check,
		.is_null = is_null,
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

/*
 * This method will evaluate the predicates, extract
 * left and right operands, check if one of the operands is
 * a simple Var type. If its Var type extract its corresponding
 * column name from hypertable_compression catalog table.
 * If extracted column is a SEGMENT BY column then save column
 * name, value specified in the predicate. This information will
 * be used to build scan keys later.
 */
static void
fill_predicate_context(Chunk *ch, CompressionSettings *settings, List *predicates,
					   List **heap_filters, List **index_filters, List **is_null)
{
	ListCell *lc;
	foreach (lc, predicates)
	{
		Node *node = copyObject(lfirst(lc));

		Var *var;
		char *column_name;
		switch (nodeTag(node))
		{
			case T_OpExpr:
			{
				OpExpr *opexpr = (OpExpr *) node;
				RegProcedure opcode = opexpr->opfuncid;
				Oid collation = opexpr->inputcollid;
				Expr *leftop, *rightop;
				Const *arg_value;

				leftop = linitial(opexpr->args);
				rightop = lsecond(opexpr->args);

				if (IsA(leftop, RelabelType))
					leftop = ((RelabelType *) leftop)->arg;
				if (IsA(rightop, RelabelType))
					rightop = ((RelabelType *) rightop)->arg;

				if (IsA(leftop, Var) && IsA(rightop, Const))
				{
					var = (Var *) leftop;
					arg_value = (Const *) rightop;
				}
				else if (IsA(rightop, Var) && IsA(leftop, Const))
				{
					var = (Var *) rightop;
					arg_value = (Const *) leftop;
				}
				else
					continue;

				/* ignore system-defined attributes */
				if (var->varattno <= 0)
					continue;
				column_name = get_attname(ch->table_id, var->varattno, false);
				TypeCacheEntry *tce = lookup_type_cache(var->vartype, TYPECACHE_BTREE_OPFAMILY);
				int op_strategy = get_op_opfamily_strategy(opexpr->opno, tce->btree_opf);
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
																	  false,   /* is_null_check */
																	  false)); /* is_null */
						}
					}
				}
				else if (ts_array_is_member(settings->fd.orderby, column_name))
				{
					int16 index = ts_array_position(settings->fd.orderby, column_name);
					switch (op_strategy)
					{
						case BTEqualStrategyNumber:
						{
							/* orderby col = value implies min <= value and max >= value */
							*heap_filters = lappend(*heap_filters,
													make_batchfilter(column_segment_min_name(index),
																	 BTLessEqualStrategyNumber,
																	 collation,
																	 opcode,
																	 arg_value,
																	 false,	  /* is_null_check */
																	 false)); /* is_null */
							*heap_filters = lappend(*heap_filters,
													make_batchfilter(column_segment_max_name(index),
																	 BTGreaterEqualStrategyNumber,
																	 collation,
																	 opcode,
																	 arg_value,
																	 false,	  /* is_null_check */
																	 false)); /* is_null */
						}
						break;
						case BTLessStrategyNumber:
						case BTLessEqualStrategyNumber:
						{
							/* orderby col <[=] value implies min <[=] value */
							*heap_filters = lappend(*heap_filters,
													make_batchfilter(column_segment_min_name(index),
																	 op_strategy,
																	 collation,
																	 opcode,
																	 arg_value,
																	 false,	  /* is_null_check */
																	 false)); /* is_null */
						}
						break;
						case BTGreaterStrategyNumber:
						case BTGreaterEqualStrategyNumber:
						{
							/* orderby col >[=] value implies max >[=] value */
							*heap_filters = lappend(*heap_filters,
													make_batchfilter(column_segment_max_name(index),
																	 op_strategy,
																	 collation,
																	 opcode,
																	 arg_value,
																	 false,	  /* is_null_check */
																	 false)); /* is_null */
						}
					}
				}
			}
			break;
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
													 ntest->nulltesttype == IS_NULL)); /* is_null */
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

/*
 * This method will build scan keys for predicates including
 * SEGMENT BY column with attribute number from compressed chunk
 * if condition is like <segmentbycol> = <const value>, else
 * OUT param null_columns is saved with column attribute number.
 */
static ScanKeyData *
build_update_delete_scankeys(RowDecompressor *decompressor, List *heap_filters, int *num_scankeys,
							 Bitmapset **null_columns)
{
	ListCell *lc;
	BatchFilter *filter;
	int key_index = 0;

	ScanKeyData *scankeys = palloc0(heap_filters->length * sizeof(ScanKeyData));

	foreach (lc, heap_filters)
	{
		filter = lfirst(lc);
		AttrNumber attno = get_attnum(decompressor->in_rel->rd_id, NameStr(filter->column_name));
		if (attno == InvalidAttrNumber)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_COLUMN),
					 errmsg("column \"%s\" of relation \"%s\" does not exist",
							NameStr(filter->column_name),
							RelationGetRelationName(decompressor->in_rel))));

		key_index = create_segment_filter_scankey(decompressor,
												  NameStr(filter->column_name),
												  filter->strategy,
												  scankeys,
												  key_index,
												  null_columns,
												  filter->value ? filter->value->constvalue : 0,
												  filter->is_null_check);
	}
	*num_scankeys = key_index;
	return scankeys;
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
 * This method will:
 *  1.scan compressed chunk
 *  2.decompress the row
 *  3.delete this row from compressed chunk
 *  4.insert decompressed rows to uncompressed chunk
 *
 * Return value:
 * if all 4 steps defined above pass set chunk_status_changed to true and return true
 * if step 4 fails return false. Step 3 will fail if there are conflicting concurrent operations on
 * same chunk.
 */
static bool
decompress_batches(RowDecompressor *decompressor, ScanKeyData *scankeys, int num_scankeys,
				   Bitmapset *null_columns, List *is_nulls, bool *chunk_status_changed)
{
	Snapshot snapshot = GetTransactionSnapshot();

	TupleTableSlot *slot = table_slot_create(decompressor->in_rel, NULL);
	TableScanDesc scan = table_beginscan(decompressor->in_rel, snapshot, num_scankeys, scankeys);
	int num_scanned_rows = 0;
	int num_filtered_rows = 0;

	while (table_scan_getnextslot(scan, ForwardScanDirection, slot))
	{
		num_scanned_rows++;
		bool skip_tuple = false;
		int attrno = bms_next_member(null_columns, -1);
		int pos = 0;
		bool is_null_condition = 0;
		bool seg_col_is_null = false;
		for (; attrno >= 0; attrno = bms_next_member(null_columns, attrno))
		{
			is_null_condition = list_nth_int(is_nulls, pos);
			seg_col_is_null = slot_attisnull(slot, attrno);
			if ((seg_col_is_null && !is_null_condition) || (!seg_col_is_null && is_null_condition))
			{
				/*
				 * if segment by column in the scanned tuple has non null value
				 * and IS NULL is specified, OR segment by column has null value
				 * and IS NOT NULL is specified then skip this tuple
				 */
				skip_tuple = true;
				break;
			}
			pos++;
		}
		if (skip_tuple)
		{
			num_filtered_rows++;
			continue;
		}

		TM_FailureData tmfd;
		TM_Result result;
		bool should_free;
		HeapTuple compressed_tuple = ExecFetchSlotHeapTuple(slot, false, &should_free);

		heap_deform_tuple(compressed_tuple,
						  decompressor->in_desc,
						  decompressor->compressed_datums,
						  decompressor->compressed_is_nulls);

		if (should_free)
			heap_freetuple(compressed_tuple);

		result = table_tuple_delete(decompressor->in_rel,
									&slot->tts_tid,
									decompressor->mycid,
									snapshot,
									InvalidSnapshot,
									true,
									&tmfd,
									false);

		if (result != TM_Ok)
		{
			table_endscan(scan);
			report_error(result);
		}
		row_decompressor_decompress_row_to_table(decompressor);
		*chunk_status_changed = true;
	}
	if (scankeys)
		pfree(scankeys);
	table_endscan(scan);
	ExecDropSingleTupleTableSlot(slot);

	if (ts_guc_debug_compression_path_info)
	{
		elog(INFO,
			 "Number of compressed rows fetched from table scan: %d. "
			 "Number of compressed rows filtered: %d.",
			 num_scanned_rows,
			 num_filtered_rows);
	}

	return true;
}

/*
 * This method will build scan keys required to do index
 * scans on compressed chunks.
 */
static ScanKeyData *
build_index_scankeys(Relation index_rel, List *index_filters, int *num_scankeys)
{
	ListCell *lc;
	BatchFilter *filter = NULL;
	*num_scankeys = list_length(index_filters);
	ScanKeyData *scankey = palloc0(sizeof(ScanKeyData) * (*num_scankeys));
	int idx = 0;
	int flags;

	/* Order scankeys based on index attribute order */
	for (int attno = 1; attno <= index_rel->rd_index->indnatts && idx < *num_scankeys; attno++)
	{
		char *attname = get_attname(RelationGetRelid(index_rel), attno, false);
		foreach (lc, index_filters)
		{
			filter = lfirst(lc);
			if (!strcmp(attname, NameStr(filter->column_name)))
			{
				flags = 0;
				if (filter->is_null_check)
				{
					flags = SK_ISNULL | (filter->is_null ? SK_SEARCHNULL : SK_SEARCHNOTNULL);
				}

				ScanKeyEntryInitialize(&scankey[idx++],
									   flags,
									   attno,
									   filter->strategy,
									   InvalidOid, /* no strategy subtype */
									   filter->collation,
									   filter->opcode,
									   filter->value ? filter->value->constvalue : 0);
				break;
			}
		}
	}

	Assert(idx == *num_scankeys);
	return scankey;
}

/*
 * This method will:
 *  1.Scan the index created with SEGMENT BY columns.
 *  2.Fetch matching rows and decompress the row
 *  3.insert decompressed rows to uncompressed chunk
 *  4.delete this row from compressed chunk
 */
static bool
decompress_batches_using_index(RowDecompressor *decompressor, Relation index_rel,
							   ScanKeyData *index_scankeys, int num_index_scankeys,
							   ScanKeyData *scankeys, int num_scankeys, Bitmapset *null_columns,
							   List *is_nulls, bool *chunk_status_changed)
{
	Snapshot snapshot = GetTransactionSnapshot();
	int num_segmentby_filtered_rows = 0;
	int num_heap_filtered_rows = 0;

	IndexScanDesc scan =
		index_beginscan(decompressor->in_rel, index_rel, snapshot, num_index_scankeys, 0);
	TupleTableSlot *slot = table_slot_create(decompressor->in_rel, NULL);
	index_rescan(scan, index_scankeys, num_index_scankeys, NULL, 0);

	while (index_getnext_slot(scan, ForwardScanDirection, slot))
	{
		TM_Result result;
		TM_FailureData tmfd;
		bool should_free;
		HeapTuple compressed_tuple;

		compressed_tuple = ExecFetchSlotHeapTuple(slot, false, &should_free);

		num_segmentby_filtered_rows++;
		if (num_scankeys)
		{
			/* filter tuple based on compress_orderby columns */
			bool valid = false;
#if PG16_LT
			HeapKeyTest(compressed_tuple,
						RelationGetDescr(decompressor->in_rel),
						num_scankeys,
						scankeys,
						valid);
#else
			valid = HeapKeyTest(compressed_tuple,
								RelationGetDescr(decompressor->in_rel),
								num_scankeys,
								scankeys);
#endif
			if (!valid)
			{
				num_heap_filtered_rows++;

				if (should_free)
					heap_freetuple(compressed_tuple);
				continue;
			}
		}

		bool skip_tuple = false;
		int attrno = bms_next_member(null_columns, -1);
		int pos = 0;
		bool is_null_condition = 0;
		bool seg_col_is_null = false;
		for (; attrno >= 0; attrno = bms_next_member(null_columns, attrno))
		{
			is_null_condition = list_nth_int(is_nulls, pos);
			seg_col_is_null = slot_attisnull(slot, attrno);
			if ((seg_col_is_null && !is_null_condition) || (!seg_col_is_null && is_null_condition))
			{
				/*
				 * if segment by column in the scanned tuple has non null value
				 * and IS NULL is specified, OR segment by column has null value
				 * and IS NOT NULL is specified then skip this tuple
				 */
				skip_tuple = true;
				break;
			}
			pos++;
		}
		if (skip_tuple)
		{
			num_heap_filtered_rows++;
			continue;
		}

		heap_deform_tuple(compressed_tuple,
						  decompressor->in_desc,
						  decompressor->compressed_datums,
						  decompressor->compressed_is_nulls);

		if (should_free)
			heap_freetuple(compressed_tuple);

		result = table_tuple_delete(decompressor->in_rel,
									&slot->tts_tid,
									decompressor->mycid,
									snapshot,
									InvalidSnapshot,
									true,
									&tmfd,
									false);

		/* skip reporting error if isolation level is < Repeatable Read */
		if (result == TM_Deleted && !IsolationUsesXactSnapshot())
			continue;

		if (result != TM_Ok)
		{
			index_endscan(scan);
			index_close(index_rel, AccessShareLock);
			report_error(result);
		}
		row_decompressor_decompress_row_to_table(decompressor);
		*chunk_status_changed = true;
	}

	if (ts_guc_debug_compression_path_info)
	{
		elog(INFO,
			 "Number of compressed rows fetched from index: %d. "
			 "Number of compressed rows filtered by heap filters: %d.",
			 num_segmentby_filtered_rows,
			 num_heap_filtered_rows);
	}

	ExecDropSingleTupleTableSlot(slot);
	index_endscan(scan);
	CommandCounterIncrement();
	return true;
}

/*
 * This method will:
 *  1. Evaluate WHERE clauses and check if SEGMENT BY columns
 *     are specified or not.
 *  2. Build scan keys for SEGMENT BY columns.
 *  3. Move scanned rows to staging area.
 *  4. Update catalog table to change status of moved chunk.
 */
static void
decompress_batches_for_update_delete(HypertableModifyState *ht_state, Chunk *chunk,
									 List *predicates, EState *estate)
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
	RowDecompressor decompressor;
	BatchFilter *filter;

	bool chunk_status_changed = false;
	ScanKeyData *scankeys = NULL;
	Bitmapset *null_columns = NULL;
	int num_scankeys = 0;
	ScanKeyData *index_scankeys = NULL;
	int num_index_scankeys = 0;

	comp_chunk = ts_chunk_get_by_id(chunk->fd.compressed_chunk_id, true);
	CompressionSettings *settings = ts_compression_settings_get(comp_chunk->table_id);

	fill_predicate_context(chunk, settings, predicates, &heap_filters, &index_filters, &is_null);

	chunk_rel = table_open(chunk->table_id, RowExclusiveLock);
	comp_chunk_rel = table_open(comp_chunk->table_id, RowExclusiveLock);
	decompressor = build_decompressor(comp_chunk_rel, chunk_rel);

	if (index_filters)
	{
		matching_index_rel = find_matching_index(comp_chunk_rel, &index_filters, &heap_filters);
	}

	write_logical_replication_msg_decompression_start();
	if (heap_filters)
	{
		scankeys =
			build_update_delete_scankeys(&decompressor, heap_filters, &num_scankeys, &null_columns);
	}
	if (matching_index_rel)
	{
		index_scankeys =
			build_index_scankeys(matching_index_rel, index_filters, &num_index_scankeys);
		decompress_batches_using_index(&decompressor,
									   matching_index_rel,
									   index_scankeys,
									   num_index_scankeys,
									   scankeys,
									   num_scankeys,
									   null_columns,
									   is_null,
									   &chunk_status_changed);
		/* close the selected index */
		index_close(matching_index_rel, AccessShareLock);
	}
	else
	{
		decompress_batches(&decompressor,
						   scankeys,
						   num_scankeys,
						   null_columns,
						   is_null,
						   &chunk_status_changed);
	}
	write_logical_replication_msg_decompression_end();

	/*
	 * tuples from compressed chunk has been decompressed and moved
	 * to staging area, thus mark this chunk as partially compressed
	 */
	if (chunk_status_changed == true)
		ts_chunk_set_partial(chunk);

	row_decompressor_close(&decompressor);

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
	ht_state->batches_decompressed += decompressor.batches_decompressed;
	ht_state->tuples_decompressed += decompressor.tuples_decompressed;
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

	return decompress_chunk_walker(&ps->ps, &ctx);
}

static bool
decompress_chunk_walker(PlanState *ps, struct decompress_chunk_context *ctx)
{
	RangeTblEntry *rte = NULL;
	bool needs_decompression = false;
	bool should_rescan = false;
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

				decompress_batches_for_update_delete(ctx->ht_state,
													 current_chunk,
													 predicates,
													 ps->state);

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

#endif
