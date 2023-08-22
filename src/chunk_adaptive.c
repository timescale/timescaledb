/*
 * The contents and feature provided by this file (and its associated header
 * file) -- adaptive chunking -- are currently in BETA.
 * Feedback, suggestions, and bugs should be reported
 * on the GitHub repository issues page, or in our public slack.
 */
#include <postgres.h>
#include <catalog/pg_proc.h>
#include <catalog/pg_type.h>
#include <utils/acl.h>
#include <utils/syscache.h>
#include <utils/lsyscache.h>
#include <utils/guc.h>
#include <utils/builtins.h>
#include <utils/array.h>
#include <utils/snapmgr.h>
#include <utils/typcache.h>
#include <funcapi.h>
#include <math.h>
#include <parser/parse_func.h>
#include <miscadmin.h>

#include "hypertable_cache.h"
#include "errors.h"
#include "compat/compat.h"
#include "chunk_adaptive.h"
#include "chunk.h"
#include "hypercube.h"
#include "utils.h"

#define DEFAULT_CHUNK_SIZING_FN_NAME "calculate_chunk_interval"

/* This can be set to a positive number (and non-zero) value from tests to
 * simulate memory cache size. This makes it possible to run tests
 * deterministically. */
static int64 fixed_memory_cache_size = -1;

/*
 * Takes a PostgreSQL text representation of data (e.g., 40MB) and converts it
 * into a int64 for calculations
 */
static int64
convert_text_memory_amount_to_bytes(const char *memory_amount)
{
	const char *hintmsg;
	int nblocks;
	int64 bytes;

	if (NULL == memory_amount)
		elog(ERROR, "invalid memory amount");

	if (!parse_int(memory_amount, &nblocks, GUC_UNIT_BLOCKS, &hintmsg))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid data amount"),
				 errhint("%s", hintmsg)));

	bytes = nblocks;
	bytes *= BLCKSZ;

	return bytes;
}

/*
 * Exposed for testing purposes to be able to simulate a different memory
 * cache size in tests.
 */
TS_FUNCTION_INFO_V1(ts_set_memory_cache_size);

Datum
ts_set_memory_cache_size(PG_FUNCTION_ARGS)
{
	const char *memory_amount = text_to_cstring(PG_GETARG_TEXT_P(0));

	fixed_memory_cache_size = convert_text_memory_amount_to_bytes(memory_amount);

	PG_RETURN_INT64(fixed_memory_cache_size);
}

/*
 * Get the amount of cache memory for chunks.
 * We use shared_buffers converted to bytes.
 */
static int64
get_memory_cache_size(void)
{
	const char *val;
	const char *hintmsg;
	int shared_buffers;
	int64 memory_bytes;

	if (fixed_memory_cache_size > 0)
		return fixed_memory_cache_size;

	val = GetConfigOption("shared_buffers", false, false);

	if (NULL == val)
		elog(ERROR, "missing configuration for 'shared_buffers'");

	if (!parse_int(val, &shared_buffers, GUC_UNIT_BLOCKS, &hintmsg))
		elog(ERROR, "could not parse 'shared_buffers' setting: %s", hintmsg);

	memory_bytes = shared_buffers;

	/* Value is in blocks, so convert to bytes */
	memory_bytes *= BLCKSZ;

	return memory_bytes;
}

/*
 * For chunk sizing, we don't want to set chunk size exactly the same as the
 * available cache memory, since chunk sizes won't be exact. We therefore give
 * some slack here.
 */
#define DEFAULT_CACHE_MEMORY_SLACK 0.9

extern inline int64
ts_chunk_calculate_initial_chunk_target_size(void)
{
	return (int64) ((double) get_memory_cache_size() * DEFAULT_CACHE_MEMORY_SLACK);
}

typedef enum MinMaxResult
{
	MINMAX_NO_INDEX,
	MINMAX_NO_TUPLES,
	MINMAX_FOUND,
} MinMaxResult;

/*
 * Use a heap scan to find the min and max of a given column of a chunk. This
 * could be a rather costly operation. Should figure out how to keep min-max
 * stats cached.
 */
static MinMaxResult
minmax_heapscan(Relation rel, Oid atttype, AttrNumber attnum, Datum minmax[2])
{
	TupleTableSlot *slot = table_slot_create(rel, NULL);
	TableScanDesc scan;
	TypeCacheEntry *tce;
	bool nulls[2] = { true, true };

	/* Lookup the tuple comparison function from the type cache */
	tce = lookup_type_cache(atttype, TYPECACHE_CMP_PROC | TYPECACHE_CMP_PROC_FINFO);

	if (NULL == tce || !OidIsValid(tce->cmp_proc))
		elog(ERROR, "no comparison function for type %u", atttype);

	scan = table_beginscan(rel, GetTransactionSnapshot(), 0, NULL);

	while (table_scan_getnextslot(scan, ForwardScanDirection, slot))
	{
		bool isnull;
		Datum value = slot_getattr(slot, attnum, &isnull);

		if (isnull)
			continue;

		/* Check for new min */
		if (nulls[0] || DatumGetInt32(FunctionCall2(&tce->cmp_proc_finfo, value, minmax[0])) < 0)
		{
			nulls[0] = false;
			minmax[0] = value;
		}

		/* Check for new max */
		if (nulls[1] || DatumGetInt32(FunctionCall2(&tce->cmp_proc_finfo, value, minmax[1])) > 0)
		{
			nulls[1] = false;
			minmax[1] = value;
		}
	}

	table_endscan(scan);
	ExecDropSingleTupleTableSlot(slot);

	return (nulls[0] || nulls[1]) ? MINMAX_NO_TUPLES : MINMAX_FOUND;
}

/*
 * Use an index scan to find the min and max of a given column of a chunk.
 */
static MinMaxResult
minmax_indexscan(Relation rel, Relation idxrel, AttrNumber attnum, Datum minmax[2])
{
	IndexScanDesc scan = index_beginscan(rel, idxrel, GetTransactionSnapshot(), 0, 0);
	TupleTableSlot *slot = table_slot_create(rel, NULL);
	bool nulls[2] = { true, true };
	int i;

	for (i = 0; i < 2; i++)
	{
		static ScanDirection directions[2] = { BackwardScanDirection, ForwardScanDirection };
		bool found_tuple;
		bool isnull;

		index_rescan(scan, NULL, 0, NULL, 0);
		found_tuple = index_getnext_slot(scan, directions[i], slot);

		if (!found_tuple)
			break;

		minmax[i] = slot_getattr(slot, attnum, &isnull);
		nulls[i] = isnull;
	}

	index_endscan(scan);
	ExecDropSingleTupleTableSlot(slot);

	Assert((nulls[0] && nulls[1]) || (!nulls[0] && !nulls[1]));

	return nulls[0] ? MINMAX_NO_TUPLES : MINMAX_FOUND;
}

/*
 * Do a scan for min and max using and index on the given column.
 */
static MinMaxResult
relation_minmax_indexscan(Relation rel, Oid atttype, Name attname, AttrNumber attnum,
						  Datum minmax[2])
{
	List *indexlist = RelationGetIndexList(rel);
	ListCell *lc;
	MinMaxResult res = MINMAX_NO_INDEX;

	foreach (lc, indexlist)
	{
		Relation idxrel;
		Form_pg_attribute idxattr;

		idxrel = index_open(lfirst_oid(lc), AccessShareLock);
		idxattr = TupleDescAttr(idxrel->rd_att, 0);

		if (idxattr->atttypid == atttype && namestrcmp(&idxattr->attname, NameStr(*attname)) == 0)
			res = minmax_indexscan(rel, idxrel, attnum, minmax);

		index_close(idxrel, AccessShareLock);

		if (res == MINMAX_FOUND)
			break;
	}

	return res;
}

/*
 * Determines if a table has an appropriate index for finding the minimum and
 * maximum time value. This would be an index whose first column is the same as
 * the column used for time partitioning.
 */
static bool
table_has_minmax_index(Oid relid, Oid atttype, Name attname, AttrNumber attnum)
{
	Datum minmax[2];
	Relation rel = table_open(relid, AccessShareLock);
	MinMaxResult res = relation_minmax_indexscan(rel, atttype, attname, attnum, minmax);

	table_close(rel, AccessShareLock);

	return res != MINMAX_NO_INDEX;
}

/*
 * Get the min and max value for a given column of a chunk.
 *
 * Returns true iff min and max is found, otherwise false.
 */
static bool
chunk_get_minmax(Oid relid, Oid atttype, AttrNumber attnum, Datum minmax[2])
{
	Relation rel = table_open(relid, AccessShareLock);
	NameData attname;
	MinMaxResult res;

	namestrcpy(&attname, get_attname(relid, attnum, false));
	res = relation_minmax_indexscan(rel, atttype, &attname, attnum, minmax);

	if (res == MINMAX_NO_INDEX)
	{
		ereport(WARNING,
				(errmsg("no index on \"%s\" found for adaptive chunking on chunk \"%s\"",
						NameStr(attname),
						get_rel_name(relid)),
				 errdetail("Adaptive chunking works best with an index on the dimension being "
						   "adapted.")));

		res = minmax_heapscan(rel, atttype, attnum, minmax);
	}

	table_close(rel, AccessShareLock);

	return res == MINMAX_FOUND;
}

#define CHUNK_SIZING_FUNC_NARGS 3
#define DEFAULT_CHUNK_WINDOW 3

/* Tuples must have filled this fraction of the chunk interval to use it to
 * estimate a new chunk time interval */
#define INTERVAL_FILLFACTOR_THRESH 0.5
/* A chunk must fill this (extrapolated) fraction of the target size to use it
 * to estimate a new chunk time interval.  */
#define SIZE_FILLFACTOR_THRESH 0.15

/* The calculated chunk time interval must differ this much to actually change
 * the interval */
#define INTERVAL_MIN_CHANGE_THRESH 0.15

/* More than this number of intervals must be undersized in order to use the
 * undersized calculation path */
#define NUM_UNDERSIZED_INTERVALS 1

/* Threshold to boost to if there are only undersized intervals to make
 * predictions from. This should be slightly above the SIZE_FILLFACTOR_THRESH
 * so that the next chunks made with this are likely to meet that threshold
 * and be used in normal prediction mode */
#define UNDERSIZED_FILLFACTOR_THRESH (SIZE_FILLFACTOR_THRESH * 1.1)

TS_FUNCTION_INFO_V1(ts_calculate_chunk_interval);

/*
 * Calculate a new interval for a chunk in a given dimension.
 *
 * This function implements the main algorithm for adaptive chunking. Given a
 * dimension, coordinate (point) on the dimensional axis (e.g., point in time),
 * and a chunk target size (in bytes), the function should return a new
 * interval that best fills the chunk to the target size.
 *
 * The intuition behind the current implementation is to look back at the recent
 * past chunks in the dimension and look at how close they are to the target
 * size (the fillfactor) and then use that information to calculate a new
 * interval. I.e., if the fillfactor of a past chunk was below 1.0 we increase
 * the interval, and if it was above 1.0 we decrease it. Thus, for each past
 * chunk, we calculate the interval that would have filled the chunk to the
 * target size. Then, to calculate the new chunk interval, we average the
 * intervals of the past chunks.
 *
 * Note, however, that there are a couple of caveats. First, we cannot look back
 * at the most recently created chunks, because there is no guarantee that data
 * was written exactly in order of the dimension we are looking at. Therefore,
 * we "look back" along the dimension axis instead of by, e.g., chunk
 * ID. Second, chunks can be filled unevenly. Below are three examples of how
 * chunks can be filled ('*' represents data):
 *
 *' |--------|
 *' | * * * *|  1. Evenly filled (ideal)
 *' |--------|
 *
 *' |--------|
 *' |    ****|  2. Partially filled
 *' |--------|
 *
 *' |--------|
 *' |  * * **|  3. Unevenly filled
 *' |--------|
 *
 * Chunk (1) above represents the ideal situation. The chunk is evenly filled
 * across the entire chunk interval. This indicates a steady stream of data at
 * an even rate. Given the size and interval of this chunk, it would be
 * straightforward to calculate a new interval to hit a given target size.
 *
 * Chunk (2) has the same amount of data as (1), but it is reasonable to believe
 * that the following chunk will be fully filled with about twice the amount of
 * data. It is common for the first chunk in a hypertable to look like
 * this. Thus, to be able to use the first chunk for prediction, we compensate
 * by finding the MIN and MAX dimension values of the data in the chunk and then
 * use max-min (difference) as the interval instead of the chunk's actual
 * interval (i.e., since we are more interested in data rate/density we pretend
 * that this is a smaller chunk in terms of the given dimension.)
 *
 * Chunk (3) is probably a common real world scenario. We don't do anything
 * special to handle this case.
 *
 * We use a number of thresholds to avoid changing intervals
 * unnecessarily. I.e., if we are close to the target interval, we avoid
 * changing the interval since there might be a natural variance in the
 * fillfactor across chunks. This is intended to avoid flip-flopping or unstable
 * behavior.
 *
 * Additionally, two other thresholds govern much of the algorithm's behavior.
 * First is the SIZE_FILLFACTOR_THRESH, which is the minimum percentage of
 * the extrapolated size a chunk should fill to be used in computing a new
 * target size. We want a minimum so as to not overreact to a chunk that is too
 * small to get an accurate extrapolation from. For example, a chunk that is
 * only a percentage point or two of the extrapolated size (or less!) may not
 * contain enough data to give a true sense of the data rate, i.e., if it was
 * made in a particularly bursty or slow period.
 *
 * However, in the event that an initial chunk size was set
 * way too small, the algorithm will never adjust because
 * _all_ the chunks fall below this threshold. Therefore we have another
 * threshold -- NUM_UNDERSIZED_INTERVALS -- that helps our algorithm make
 * progress to the correct estimate. If there are _no_ chunks that
 * meet SIZE_FILLFACTOR_THRESH, and at least NUM_UNDERSIZED_INTERVALS chunks
 * we are sufficiently full, we use those chunks to adjust the target chunk
 * size so that the next chunks created at least meet SIZE_FILLFACTOR_THRESH.
 * This will then allow the algorithm to work in the normal way to adjust
 * further if needed.
 */
Datum
ts_calculate_chunk_interval(PG_FUNCTION_ARGS)
{
	int32 dimension_id = PG_GETARG_INT32(0);
	int64 dimension_coord = PG_GETARG_INT64(1);
	int64 chunk_target_size_bytes = PG_GETARG_INT64(2);
	int64 chunk_interval = 0;
	int64 undersized_intervals = 0;
	int64 current_interval;
	int32 hypertable_id;
	Hypertable *ht;
	const Dimension *dim;
	List *chunks = NIL;
	ListCell *lc;
	int num_intervals = 0;
	int num_undersized_intervals = 0;
	double interval_diff;
	double undersized_fillfactor = 0.0;
	AclResult acl_result;

	if (PG_NARGS() != CHUNK_SIZING_FUNC_NARGS)
		elog(ERROR, "invalid number of arguments");

	if (chunk_target_size_bytes < 0)
		elog(ERROR, "chunk_target_size must be positive");

	elog(DEBUG1, "[adaptive] chunk_target_size_bytes=" UINT64_FORMAT, chunk_target_size_bytes);

	hypertable_id = ts_dimension_get_hypertable_id(dimension_id);

	if (hypertable_id <= 0)
		elog(ERROR, "could not find a matching hypertable for dimension %u", dimension_id);

	ht = ts_hypertable_get_by_id(hypertable_id);

	Assert(ht != NULL);

	acl_result = pg_class_aclcheck(ht->main_table_relid, GetUserId(), ACL_SELECT);
	if (acl_result != ACLCHECK_OK)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("permission denied for table %s", ht->fd.table_name.data)));

	if (hypertable_is_distributed(ht))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("adaptive chunking not supported on distributed hypertables")));

	dim = ts_hyperspace_get_dimension_by_id(ht->space, dimension_id);

	Assert(dim != NULL);

	current_interval = dim->fd.interval_length;

	/* Get a window of recent chunks */
	chunks = ts_chunk_get_window(dimension_id,
								 dimension_coord,
								 DEFAULT_CHUNK_WINDOW,
								 CurrentMemoryContext);

	foreach (lc, chunks)
	{
		Chunk *chunk = lfirst(lc);
		const DimensionSlice *slice =
			ts_hypercube_get_slice_by_dimension_id(chunk->cube, dimension_id);
		int64 chunk_size, slice_interval;
		Datum minmax[2];
		AttrNumber attno = ts_map_attno(ht->main_table_relid, chunk->table_id, dim->column_attno);

		Assert(NULL != slice);

		chunk_size = DatumGetInt64(
			DirectFunctionCall1(pg_total_relation_size, ObjectIdGetDatum(chunk->table_id)));

		slice_interval = slice->fd.range_end - slice->fd.range_start;

		if (chunk_get_minmax(chunk->table_id, dim->fd.column_type, attno, minmax))
		{
			int64 min = ts_time_value_to_internal(minmax[0], dim->fd.column_type);
			int64 max = ts_time_value_to_internal(minmax[1], dim->fd.column_type);
			double interval_fillfactor, size_fillfactor;
			int64 extrapolated_chunk_size;

			/*
			 * The fillfactor of the slice interval that the data actually
			 * spans
			 */
			interval_fillfactor = ((double) max - min) / slice_interval;

			/*
			 * Extrapolate the size the chunk would have if it spanned the
			 * entire interval
			 */
			extrapolated_chunk_size = chunk_size / interval_fillfactor;
			size_fillfactor = ((double) extrapolated_chunk_size) / chunk_target_size_bytes;

			elog(DEBUG2,
				 "[adaptive] slice_interval=" UINT64_FORMAT " interval_fillfactor=%lf"
				 " current_chunk_size=" UINT64_FORMAT " extrapolated_chunk_size=" UINT64_FORMAT
				 " size_fillfactor=%lf",
				 slice_interval,
				 interval_fillfactor,
				 chunk_size,
				 extrapolated_chunk_size,
				 size_fillfactor);

			/*
			 * If the chunk is sufficiently filled with data and its
			 * extrapolated size is large enough to make a good estimate, use
			 * it
			 */
			if (interval_fillfactor > INTERVAL_FILLFACTOR_THRESH &&
				size_fillfactor > SIZE_FILLFACTOR_THRESH)
			{
				chunk_interval += (slice_interval / size_fillfactor);
				num_intervals++;
			}

			/*
			 * If the chunk is sufficiently filled with data but its
			 * extrapolated size is too small, track it and maybe use it if it
			 * is all we have
			 */
			else if (interval_fillfactor > INTERVAL_FILLFACTOR_THRESH)
			{
				elog(DEBUG2,
					 "[adaptive] chunk sufficiently full, "
					 "but undersized. may use for prediction.");
				undersized_intervals += slice_interval;
				undersized_fillfactor += size_fillfactor;
				num_undersized_intervals++;
			}
		}
	}

	elog(DEBUG1,
		 "[adaptive] current interval=" UINT64_FORMAT
		 " num_intervals=%d num_undersized_intervals=%d",
		 current_interval,
		 num_intervals,
		 num_undersized_intervals);

	/*
	 * No full sized intervals, but enough undersized intervals to adjust
	 * higher. We only want to do this if there are no sufficiently sized
	 * intervals to use for a normal adjustment. This keeps us from getting
	 * stuck with a really small interval size.
	 */
	if (num_intervals == 0 && num_undersized_intervals > NUM_UNDERSIZED_INTERVALS)
	{
		double avg_fillfactor = undersized_fillfactor / num_undersized_intervals;
		double incr_factor = UNDERSIZED_FILLFACTOR_THRESH / avg_fillfactor;
		int64 avg_interval = undersized_intervals / num_undersized_intervals;

		elog(DEBUG1,
			 "[adaptive] no sufficiently large intervals found, but "
			 "some undersized ones found. increase interval to probe for better"
			 " threshold. factor=%lf",
			 incr_factor);
		chunk_interval = (int64) (avg_interval * incr_factor);
	}
	/* No data & insufficient amount of undersized chunks, keep old interval */
	else if (num_intervals == 0)
	{
		elog(DEBUG1,
			 "[adaptive] no sufficiently large intervals found, "
			 "nor enough undersized chunks to estimate. "
			 "use previous size of " UINT64_FORMAT,
			 current_interval);
		PG_RETURN_INT64(current_interval);
	}
	else
		chunk_interval /= num_intervals;

	/*
	 * If the interval hasn't really changed much from before, we keep the old
	 * interval to ensure we do not have fluctuating behavior around the
	 * target size.
	 */
	interval_diff = fabs(1.0 - ((double) chunk_interval / current_interval));

	if (interval_diff <= INTERVAL_MIN_CHANGE_THRESH)
	{
		elog(DEBUG1,
			 "[adaptive] calculated chunk interval=" UINT64_FORMAT
			 ", but is below change threshold, keeping old interval",
			 chunk_interval);
		chunk_interval = current_interval;
	}
	else
	{
		elog(LOG,
			 "[adaptive] calculated chunk interval=" UINT64_FORMAT
			 " for hypertable %d, making change",
			 chunk_interval,
			 hypertable_id);
	}

	PG_RETURN_INT64(chunk_interval);
}

/*
 * Validate that the provided function in the catalog can be used for
 * determining a new chunk size, i.e., has form (int,bigint,bigint) -> bigint.
 *
 * Parameter 'info' will be updated with the function's information
 */
void
ts_chunk_sizing_func_validate(regproc func, ChunkSizingInfo *info)
{
	HeapTuple tuple;
	Form_pg_proc form;
	Oid *typearr;

	if (!OidIsValid(func))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_FUNCTION), (errmsg("invalid chunk sizing function"))));

	tuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(func));

	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for function %u", func);

	form = (Form_pg_proc) GETSTRUCT(tuple);
	typearr = form->proargtypes.values;

	if (form->pronargs != CHUNK_SIZING_FUNC_NARGS || typearr[0] != INT4OID ||
		typearr[1] != INT8OID || typearr[2] != INT8OID || form->prorettype != INT8OID)
	{
		ReleaseSysCache(tuple);
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
				 errmsg("invalid function signature"),
				 errhint("A chunk sizing function's signature should be (int, bigint, bigint) -> "
						 "bigint")));
	}

	if (NULL != info)
	{
		info->func = func;
		namestrcpy(&info->func_schema, get_namespace_name(form->pronamespace));
		namestrcpy(&info->func_name, NameStr(form->proname));
	}

	ReleaseSysCache(tuple);
}

/*
 * Parse the target size text into an integer amount of bytes.
 *
 * 'off' / 'disable' - returns a target of 0
 * 'estimate' - returns a target based on number of bytes in shared memory
 * 'XXMB' / etc - converts from PostgreSQL pretty text into number of bytes
 */
static int64
chunk_target_size_in_bytes(const text *target_size_text)
{
	const char *target_size = text_to_cstring(target_size_text);
	int64 target_size_bytes = 0;

	if (pg_strcasecmp(target_size, "off") == 0 || pg_strcasecmp(target_size, "disable") == 0)
		return 0;

	if (pg_strcasecmp(target_size, "estimate") == 0)
		target_size_bytes = ts_chunk_calculate_initial_chunk_target_size();
	else
		target_size_bytes = convert_text_memory_amount_to_bytes(target_size);

	/* Disable if target size is zero or less */
	if (target_size_bytes <= 0)
		target_size_bytes = 0;

	return target_size_bytes;
}

#define MB (1024 * 1024)

void
ts_chunk_adaptive_sizing_info_validate(ChunkSizingInfo *info)
{
	AttrNumber attnum;
	NameData attname;
	Oid atttype;

	if (!OidIsValid(info->table_relid))
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_TABLE), errmsg("table does not exist")));

	ts_hypertable_permissions_check(info->table_relid, GetUserId());

	if (NULL == info->colname)
		ereport(ERROR,
				(errcode(ERRCODE_TS_DIMENSION_NOT_EXIST),
				 errmsg("no open dimension found for adaptive chunking")));

	attnum = get_attnum(info->table_relid, info->colname);
	namestrcpy(&attname, info->colname);
	atttype = get_atttype(info->table_relid, attnum);

	if (!OidIsValid(atttype))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_COLUMN),
				 errmsg("column \"%s\" does not exist", info->colname)));

	ts_chunk_sizing_func_validate(info->func, info);

	if (NULL == info->target_size)
		info->target_size_bytes = 0;
	else
		info->target_size_bytes = chunk_target_size_in_bytes(info->target_size);

	/* Don't validate further if disabled */
	if (info->target_size_bytes <= 0 || !OidIsValid(info->func))
		return;

	/* Warn of small target sizes */
	if (info->target_size_bytes > 0 && info->target_size_bytes < (10 * MB))
		elog(WARNING, "target chunk size for adaptive chunking is less than 10 MB");

	if (info->check_for_index &&
		!table_has_minmax_index(info->table_relid, atttype, &attname, attnum))
		ereport(WARNING,
				(errmsg("no index on \"%s\" found for adaptive chunking on hypertable \"%s\"",
						info->colname,
						get_rel_name(info->table_relid)),
				 errdetail("Adaptive chunking works best with an index on the dimension being "
						   "adapted.")));
}

TS_FUNCTION_INFO_V1(ts_chunk_adaptive_set);

/*
 * Change the settings for adaptive chunking.
 */
Datum
ts_chunk_adaptive_set(PG_FUNCTION_ARGS)
{
	ChunkSizingInfo info = {
		.table_relid = PG_GETARG_OID(0),
		.target_size = PG_ARGISNULL(1) ? NULL : PG_GETARG_TEXT_P(1),
		.func = PG_ARGISNULL(2) ? InvalidOid : PG_GETARG_OID(2),
		.colname = NULL,
		.check_for_index = true,
	};
	Hypertable *ht;
	const Dimension *dim;
	Cache *hcache;
	HeapTuple tuple;
	TupleDesc tupdesc;
	CatalogSecurityContext sec_ctx;
	Datum values[2];
	bool nulls[2] = { false, false };

	TS_PREVENT_FUNC_IF_READ_ONLY();
	if (PG_ARGISNULL(0))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid hypertable: cannot be NULL")));

	if (!OidIsValid(info.table_relid))
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_TABLE), errmsg("table does not exist")));

	ts_hypertable_permissions_check(info.table_relid, GetUserId());

	ht = ts_hypertable_cache_get_cache_and_entry(info.table_relid, CACHE_FLAG_NONE, &hcache);

	/* Get the first open dimension that we will adapt on */
	dim = ts_hyperspace_get_dimension(ht->space, DIMENSION_TYPE_OPEN, 0);

	if (NULL == dim)
		ereport(ERROR,
				(errcode(ERRCODE_TS_DIMENSION_NOT_EXIST),
				 errmsg("no open dimension found for adaptive chunking")));

	info.colname = NameStr(dim->fd.column_name);

	ts_chunk_adaptive_sizing_info_validate(&info);

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "function returning record called in context that cannot accept type record");

	tupdesc = BlessTupleDesc(tupdesc);

	if (OidIsValid(info.func))
	{
		ht->chunk_sizing_func = info.func;
		values[0] = ObjectIdGetDatum(info.func);
	}
	else if (OidIsValid(ht->chunk_sizing_func))
	{
		ts_chunk_sizing_func_validate(ht->chunk_sizing_func, &info);
		values[0] = ObjectIdGetDatum(ht->chunk_sizing_func);
	}
	else
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_FUNCTION), errmsg("invalid chunk sizing function")));

	values[1] = Int64GetDatum(info.target_size_bytes);

	/* Update the hypertable entry */
	ht->fd.chunk_target_size = info.target_size_bytes;
	ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);
	ts_hypertable_update(ht);
	ts_catalog_restore_user(&sec_ctx);

	ts_cache_release(hcache);

	tuple = heap_form_tuple(tupdesc, values, nulls);

	PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
}

static Oid
get_default_chunk_sizing_fn_oid()
{
	Oid chunkfnargtypes[] = { INT4OID, INT8OID, INT8OID };
	List *funcname =
		list_make2(makeString(FUNCTIONS_SCHEMA_NAME), makeString(DEFAULT_CHUNK_SIZING_FN_NAME));
	int nargs = sizeof(chunkfnargtypes) / sizeof(chunkfnargtypes[0]);
	Oid chunkfnoid = LookupFuncName(funcname, nargs, chunkfnargtypes, false);
	return chunkfnoid;
}

ChunkSizingInfo *
ts_chunk_sizing_info_get_default_disabled(Oid table_relid)
{
	ChunkSizingInfo *chunk_sizing_info = palloc(sizeof(*chunk_sizing_info));
	*chunk_sizing_info = (ChunkSizingInfo){
		.table_relid = table_relid,
		.target_size = NULL,
		.func = get_default_chunk_sizing_fn_oid(),
		.colname = NULL,
		.check_for_index = false,
	};
	return chunk_sizing_info;
}
