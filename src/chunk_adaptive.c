#include <postgres.h>
#include <catalog/pg_proc.h>
#include <catalog/pg_type.h>
#include <utils/syscache.h>
#include <utils/lsyscache.h>
#include <utils/guc.h>
#include <utils/builtins.h>
#include <utils/array.h>
#include <utils/snapmgr.h>
#include <utils/tqual.h>
#include <utils/typcache.h>
#include <funcapi.h>
#include <math.h>

#if defined(WIN32)
#include <windows.h>
#else
#include <unistd.h>
#endif

#include "hypertable_cache.h"
#include "errors.h"
#include "compat.h"
#include "chunk_adaptive.h"
#include "chunk.h"
#include "hypercube.h"
#include "utils.h"

/* This can be set to a positive number (and non-zero) value from tests to
 * simulate effective memory cache size. This makes it possible to run tests
 * deterministically. */
static int64 fixed_effective_memory_cache_size = -1;

/*
 * Get the available system memory.
 */
static int64
system_memory_bytes(void)
{
	int64		bytes;

#if defined(WIN32)
	MEMORYSTATUSEX status;

	status.dwLength = sizeof(status);
	GlobalMemoryStatusEx(&status);
	bytes = status.ullTotalPhys;

#elif defined(_SC_PHYS_PAGES) && defined(_SC_PAGESIZE)

	bytes = sysconf(_SC_PHYS_PAGES);
	bytes *= sysconf(_SC_PAGESIZE);
#else
#error "Unsupported platform"
#endif
	return bytes;
}

static int64
convert_text_memory_amount_to_bytes(const char *memory_amount)
{
	const char *hintmsg;
	int			nblocks;
	int64		bytes;

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

TS_FUNCTION_INFO_V1(set_effective_memory_cache_size);

Datum
set_effective_memory_cache_size(PG_FUNCTION_ARGS)
{
	const char *memory_amount = text_to_cstring(PG_GETARG_TEXT_P(0));

	fixed_effective_memory_cache_size = convert_text_memory_amount_to_bytes(memory_amount);

	PG_RETURN_INT64(fixed_effective_memory_cache_size);
}

/*
 * Estimate the effective memory available for caching. PostgreSQL generally
 * relies on both its own shared buffer cache and the OS file system cache. Thus
 * the cache memory available is the combination of these two caches. The
 * 'effective_cache_size' setting in PostgreSQL is supposed to give an estimate
 * of this combined memory cache and is probably the best value to use if
 * accurately set by the user (defaults to '4GB'). Note that
 * effective_cache_size is only used to inform the planner on how to plan
 * queries and does not affect the actual available cache memory (this is
 * limited by the free memory on the system, typically free+cached in top).
 *
 * A conservative setting for effective_cache_size is typically 1/2 the memory
 * of the system, while a common recommended setting for shared_buffers is 1/4
 * of system memory. In case shared_buffers is set higher than
 * effective_cache_size, we use the max of the two (a larger shared_buffers is a
 * strange setting though). Ultimately we are limited by system memory.
 *
 * Note that this relies on the user setting a good value for
 * effective_cache_size, or otherwise our estimate will be off. Alternatively,
 * we could just read to free memory on the system, but this won't account for
 * future concurrent usage by other processes.
 */
static int64
estimate_effective_memory_cache_size(void)
{
	const char *val;
	const char *hintmsg;
	int			shared_buffers,
				effective_cache_size;
	int64		memory_bytes;

	/* Use half of system memory as an upper bound */
	int64		sysmem_bound_bytes = system_memory_bytes() / 2;

	if (fixed_effective_memory_cache_size > 0)
		return fixed_effective_memory_cache_size;

	val = GetConfigOption("shared_buffers", false, false);

	if (NULL == val)
		elog(ERROR, "missing configuration for 'shared_buffers'");

	if (!parse_int(val, &shared_buffers, GUC_UNIT_BLOCKS, &hintmsg))
		elog(ERROR, "could not parse 'shared_buffers' setting: %s", hintmsg);

	val = GetConfigOption("effective_cache_size", false, false);

	if (NULL == val)
		elog(ERROR, "missing configuration for 'effective_cache_size'");

	if (!parse_int(val, &effective_cache_size, GUC_UNIT_BLOCKS, &hintmsg))
		ereport(ERROR,
				(errmsg("could not parse 'effective_cache_size' setting"),
				 errhint("%s", hintmsg)));

	memory_bytes = Max((int64) shared_buffers, (int64) effective_cache_size);

	/* Both values are in blocks, so convert to bytes */
	memory_bytes *= BLCKSZ;

	/*
	 * Upper bound on system memory in case of weird settings for
	 * effective_cache_size or shared_buffers
	 */
	if (memory_bytes > sysmem_bound_bytes)
		memory_bytes = sysmem_bound_bytes;

	return memory_bytes;
}

/* The default concurrency factor, i.e., the number of chunks we expect to fit
 * in memory at the same time */
#define DEFAULT_CONCURRENT_CHUNK_USAGE 4

static inline int64
calculate_initial_chunk_target_size(void)
{
	/*
	 * Simply use a quarter of estimated memory to account for keeping
	 * simultaneous chunks in memory. Alternatively, we could use a better
	 * estimate of, e.g., concurrent chunk usage, such as the number of
	 * hypertables in the database. However, that requires scanning for
	 * hypertables in all schemas and databases, and might not be a good
	 * estimate in case of many "old" (unused) hypertables.
	 */
	return estimate_effective_memory_cache_size() / DEFAULT_CONCURRENT_CHUNK_USAGE;
}

/*
 * Use a heap scan to find the min and max of a given column of a chunk. This
 * could be a rather costly operation. Should figure out how to keep min-max
 * stats cached.
 *
 * Returns true iff min and max is found.
 */
static bool
minmax_heapscan(Relation rel, Oid atttype, AttrNumber attnum, Datum minmax[2])
{
	HeapScanDesc scan;
	HeapTuple	tuple;
	TypeCacheEntry *tce;
	bool		minmaxnull[2] = {true};

	/* Lookup the tuple comparison function from the type cache */
	tce = lookup_type_cache(atttype, TYPECACHE_CMP_PROC | TYPECACHE_CMP_PROC_FINFO);

	if (NULL == tce || !OidIsValid(tce->cmp_proc))
		elog(ERROR, "no comparison function for type %u", atttype);

	scan = heap_beginscan(rel, GetTransactionSnapshot(), 0, NULL);

	while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL)
	{
		bool		isnull;
		Datum		value = heap_getattr(tuple, attnum, RelationGetDescr(rel), &isnull);

		/* Check for new min */
		if (minmaxnull[0] || DatumGetInt32(FunctionCall2(&tce->cmp_proc_finfo, value, minmax[0])) < 0)
		{
			minmaxnull[0] = false;
			minmax[0] = value;
		}

		/* Check for new max */
		if (minmaxnull[1] || DatumGetInt32(FunctionCall2(&tce->cmp_proc_finfo, value, minmax[1])) > 0)
		{
			minmaxnull[1] = false;
			minmax[1] = value;
		}
	}

	heap_endscan(scan);

	return !minmaxnull[0] && !minmaxnull[1];
}

/*
 * Use an index scan to find the min and max of a given column of a chunk.
 *
 * Returns true iff min and max is found.
 */
static bool
minmax_indexscan(Relation rel, Relation idxrel, AttrNumber attnum, Datum minmax[2])
{
	IndexScanDesc scan = index_beginscan(rel, idxrel, GetTransactionSnapshot(), 0, 0);
	HeapTuple	tuple;
	bool		isnull;
	int			n = 0;

	tuple = index_getnext(scan, BackwardScanDirection);

	if (HeapTupleIsValid(tuple))
		minmax[n++] = heap_getattr(tuple, attnum, RelationGetDescr(rel), &isnull);

	index_rescan(scan, NULL, 0, NULL, 0);
	tuple = index_getnext(scan, ForwardScanDirection);

	if (HeapTupleIsValid(tuple))
		minmax[n++] = heap_getattr(tuple, attnum, RelationGetDescr(rel), &isnull);

	index_endscan(scan);

	return n == 2;
}

/*
 * Get the min and max value for a given column of a chunk.
 *
 * Returns true iff min and max is found, otherwise false.
 */
static bool
chunk_get_minmax(Oid relid, Oid atttype, AttrNumber attnum, Datum minmax[2])
{
	Relation	rel = heap_open(relid, AccessShareLock);
	List	   *indexlist = RelationGetIndexList(rel);
	ListCell   *lc;
	bool		found = false;

	foreach(lc, indexlist)
	{
		Relation	idxrel;

		idxrel = index_open(lfirst_oid(lc), AccessShareLock);

		if (idxrel->rd_att->attrs[0]->attnum == attnum)
			found = minmax_indexscan(rel, idxrel, attnum, minmax);

		index_close(idxrel, AccessShareLock);

		if (found)
			break;
	}

	if (!found)
		found = minmax_heapscan(rel, atttype, attnum, minmax);

	heap_close(rel, AccessShareLock);

	return found;
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

TS_FUNCTION_INFO_V1(calculate_chunk_interval);

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
 * Finally, we use a number of thresholds to avoid changing intervals
 * unnecessarily. I.e., if we are close to the target interval, we avoid
 * changing the interval since there might be a natural variance in the
 * fillfactor across chunks. This is intended to avoid flip-flopping or unstable
 * behavior.
 */
Datum
calculate_chunk_interval(PG_FUNCTION_ARGS)
{
	int32		dimension_id = PG_GETARG_INT32(0);
	int64		dimension_coord = PG_GETARG_INT64(1);
	int64		chunk_target_size_bytes = PG_GETARG_INT64(2);
	int64		chunk_interval = 0;
	int64		undersized_intervals = 0;
	int32		hypertable_id;
	Hypertable *ht;
	Dimension  *dim;
	List	   *chunks = NIL;
	ListCell   *lc;
	int			num_intervals = 0;
	int			num_undersized_intervals = 0;
	double		interval_diff;
	double		undersized_fillfactor = 0.0;

	if (PG_NARGS() != CHUNK_SIZING_FUNC_NARGS)
		elog(ERROR, "invalid number of arguments");

	Assert(chunk_target_size_bytes >= 0);
	elog(DEBUG1, "[adaptive] chunk_target_size_bytes=" UINT64_FORMAT,
		 chunk_target_size_bytes);

	hypertable_id = dimension_get_hypertable_id(dimension_id);

	if (hypertable_id <= 0)
		elog(ERROR, "could not find a matching hypertable for dimension %u", dimension_id);

	ht = hypertable_get_by_id(hypertable_id);

	Assert(ht != NULL);

	dim = hyperspace_get_dimension_by_id(ht->space, dimension_id);

	Assert(dim != NULL);

	/* Get a window of recent chunks */
	chunks = chunk_get_window(hypertable_id,
							  dimension_coord,
							  DEFAULT_CHUNK_WINDOW,
							  CurrentMemoryContext);

	foreach(lc, chunks)
	{
		Chunk	   *chunk = lfirst(lc);
		DimensionSlice *slice = hypercube_get_slice_by_dimension_id(chunk->cube, dimension_id);
		int64		chunk_size,
					slice_interval;
		Datum		minmax[2];

		Assert(NULL != slice);

		chunk_size = DatumGetInt64(DirectFunctionCall1(pg_total_relation_size,
													   ObjectIdGetDatum(chunk->table_id)));

		slice_interval = slice->fd.range_end - slice->fd.range_start;

		if (chunk_get_minmax(chunk->table_id, dim->fd.column_type, dim->column_attno, minmax))
		{
			int64		min = time_value_to_internal(minmax[0], dim->fd.column_type, false);
			int64		max = time_value_to_internal(minmax[1], dim->fd.column_type, false);
			double		interval_fillfactor,
						size_fillfactor;
			int64		extrapolated_chunk_size;

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

			elog(DEBUG2, "[adaptive] slice_interval=" UINT64_FORMAT
				 " interval_fillfactor=%lf"
				 " current_chunk_size=" UINT64_FORMAT
				 " extrapolated_chunk_size=" UINT64_FORMAT
				 " size_fillfactor=%lf",
				 slice_interval,
				 interval_fillfactor,
				 chunk_size,
				 extrapolated_chunk_size,
				 size_fillfactor);


			if (interval_fillfactor > INTERVAL_FILLFACTOR_THRESH &&
				size_fillfactor > SIZE_FILLFACTOR_THRESH)
			{
				chunk_interval += (slice_interval / size_fillfactor);
				num_intervals++;
			}
			else if (interval_fillfactor > INTERVAL_FILLFACTOR_THRESH)
			{
				elog(DEBUG2, "[adaptive] chunk sufficiently full, "
					 "but undersized. may use for prediction.");
				undersized_intervals += slice_interval;
				undersized_fillfactor += size_fillfactor;
				num_undersized_intervals++;
			}
		}
	}

	elog(DEBUG1, "[adaptive] current interval is " UINT64_FORMAT, dim->fd.interval_length);
	elog(DEBUG1, "[adaptive] num_intervals %d num_undersized_intervals %d", num_intervals, num_undersized_intervals);

	/* No full sized intervals, but enough undersized intervals to probe */
	if (num_intervals == 0 && num_undersized_intervals > NUM_UNDERSIZED_INTERVALS)
	{
		double		avg_fillfactor = undersized_fillfactor / num_undersized_intervals;
		double		incr_factor = UNDERSIZED_FILLFACTOR_THRESH / avg_fillfactor;
		int64		avg_interval = undersized_intervals / num_undersized_intervals;

		elog(DEBUG1, "[adaptive] no sufficiently large intervals found, but "
			 "some undersized ones found. increase interval to probe for better"
			 " threshold. factor=%lf", incr_factor);
		chunk_interval = (int64) (avg_interval * incr_factor);
	}
	/* No data & insufficient amount of undersized chunks, keep old interval */
	else if (num_intervals == 0)
	{
		elog(DEBUG1, "[adaptive] no sufficiently large intervals found, "
			 "nor enough undersized chunks to estimate. "
			 "use previous size of " UINT64_FORMAT,
			 dim->fd.interval_length);
		PG_RETURN_INT64(dim->fd.interval_length);
	}
	else
		chunk_interval /= num_intervals;

	elog(DEBUG1, "[adaptive] calculated chunk interval is " UINT64_FORMAT "", chunk_interval);

	/*
	 * If the interval hasn't really changed much from before, we keep the old
	 * interval to ensure we do not have fluctuating behavior around the
	 * target size.
	 */
	interval_diff = fabs(1.0 - ((double) chunk_interval / dim->fd.interval_length));

	if (interval_diff <= INTERVAL_MIN_CHANGE_THRESH)
	{
		chunk_interval = dim->fd.interval_length;
		elog(DEBUG1, "[adaptive] change in chunk interval is below threshold, keeping old interval");
	}

	PG_RETURN_INT64(chunk_interval);
}

static void
chunk_sizing_func_validate(regproc func, ChunkSizingInfo *info)
{
	HeapTuple	tuple;
	Form_pg_proc form;
	Oid		   *typearr;

	if (!OidIsValid(func))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_FUNCTION),
				 (errmsg("invalid chunk sizing function"))));

	tuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(func));

	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for function %u", func);

	form = (Form_pg_proc) GETSTRUCT(tuple);
	typearr = form->proargtypes.values;

	if (form->pronargs != CHUNK_SIZING_FUNC_NARGS ||
		typearr[0] != INT4OID ||
		typearr[1] != INT8OID ||
		typearr[2] != INT8OID ||
		form->prorettype != INT8OID)
	{
		ReleaseSysCache(tuple);
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
				 errmsg("invalid number of function arguments"),
				 errhint("A chunk sizing function's signature should be (int, bigint, bigint) -> bigint")));
	}

	if (NULL != info)
	{
		info->func = func;
		namestrcpy(&info->func_schema, get_namespace_name(form->pronamespace));
		namestrcpy(&info->func_name, NameStr(form->proname));
	}

	ReleaseSysCache(tuple);
}

static int64
chunk_target_size_in_bytes(const text *target_size_text)
{
	const char *target_size = text_to_cstring(target_size_text);
	int64		target_size_bytes = 0;

	if (pg_strcasecmp(target_size, "off") == 0 ||
		pg_strcasecmp(target_size, "disable") == 0)
		return 0;

	if (pg_strcasecmp(target_size, "estimate") != 0)
		target_size_bytes = convert_text_memory_amount_to_bytes(target_size);

	if (target_size_bytes <= 0)
		target_size_bytes = calculate_initial_chunk_target_size();

	return target_size_bytes;
}

void
chunk_adaptive_validate_sizing_info(ChunkSizingInfo *info)
{
	chunk_sizing_func_validate(info->func, info);

	if (NULL != info->target_size)
		info->target_size_bytes = chunk_target_size_in_bytes(info->target_size);
	else
		info->target_size_bytes = 0;
}

TS_FUNCTION_INFO_V1(chunk_adaptive_set_chunk_sizing);

Datum
chunk_adaptive_set_chunk_sizing(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	ChunkSizingInfo info = {
		.func = PG_ARGISNULL(2) ? InvalidOid : PG_GETARG_OID(2),
		.target_size = PG_ARGISNULL(1) ? NULL : PG_GETARG_TEXT_P(1),
	};
	Hypertable *ht;
	Cache	   *hcache;
	HeapTuple	tuple;
	TupleDesc	tupdesc;
	CatalogSecurityContext sec_ctx;
	Datum		values[2];
	bool		nulls[2] = {false, false};

	if (!OidIsValid(relid))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_TABLE),
				 errmsg("table does not exist")));

	hcache = hypertable_cache_pin();
	ht = hypertable_cache_get_entry(hcache, relid);

	if (NULL == ht)
		ereport(ERROR,
				(errcode(ERRCODE_IO_HYPERTABLE_NOT_EXIST),
				 errmsg("table \"%s\" is not a hypertable",
						get_rel_name(relid))));

	chunk_adaptive_validate_sizing_info(&info);

	if (NULL != info.target_size)
		info.target_size_bytes = chunk_target_size_in_bytes(info.target_size);

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "function returning record called in context that cannot accept type record");

	tupdesc = BlessTupleDesc(tupdesc);

	if (OidIsValid(info.func))
	{
		ht->chunk_sizing_func = info.func;
		values[0] = DatumGetObjectId(info.func);
	}
	else if (OidIsValid(ht->chunk_sizing_func))
	{
		chunk_sizing_func_validate(ht->chunk_sizing_func, &info);
		values[0] = DatumGetObjectId(ht->chunk_sizing_func);
	}
	else
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_FUNCTION),
				 errmsg("invalid chunk sizing function")));

	values[1] = DatumGetInt64(info.target_size_bytes);

	/* Update the hypertable entry */
	ht->fd.chunk_target_size = info.target_size_bytes;
	catalog_become_owner(catalog_get(), &sec_ctx);
	hypertable_update(ht);
	catalog_restore_user(&sec_ctx);

	cache_release(hcache);

	tuple = heap_form_tuple(tupdesc, values, nulls);

	PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
}
