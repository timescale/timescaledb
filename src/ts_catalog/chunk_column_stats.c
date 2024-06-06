/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <funcapi.h>
#include <storage/lmgr.h>
#include <utils/syscache.h>

#include "compat/compat.h"
#include "chunk_column_stats.h"
#include "dimension_vector.h"

/*
 * Enable chunk column stats attributes
 */
enum Anum_enable_chunk_column_stats
{
	Anum_enable_chunk_column_stats_id = 1,
	Anum_enable_chunk_column_stats_enabled,
	_Anum_enable_chunk_column_stats_max,
};

#define Natts_enable_chunk_column_stats (_Anum_enable_chunk_column_stats_max - 1)

TS_FUNCTION_INFO_V1(ts_chunk_column_stats_enable);

/*
 * Disable chunk column stats attributes
 */
enum Anum_disable_chunk_column_stats
{
	Anum_disable_chunk_column_stats_hypertable_id = 1,
	Anum_disable_chunk_column_stats_column_name,
	Anum_disable_chunk_column_stats_disabled,
	_Anum_disable_chunk_column_stats_max,
};

#define Natts_disable_chunk_column_stats (_Anum_disable_chunk_column_stats_max - 1)

TS_FUNCTION_INFO_V1(ts_chunk_column_stats_disable);

/*
 * Create a datum to be returned by ts_chunk_column_stats_enable DDL function
 */
static Datum
chunk_column_stats_enable_datum(FunctionCallInfo fcinfo, int32 id, bool enabled)
{
	TupleDesc tupdesc;
	HeapTuple tuple;
	Datum values[Natts_enable_chunk_column_stats];
	bool nulls[Natts_enable_chunk_column_stats] = { false };

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("function returning record called in "
						"context that cannot accept type record")));

	tupdesc = BlessTupleDesc(tupdesc);

	Assert(tupdesc->natts == Natts_enable_chunk_column_stats);
	values[AttrNumberGetAttrOffset(Anum_enable_chunk_column_stats_id)] = Int32GetDatum(id);
	values[AttrNumberGetAttrOffset(Anum_enable_chunk_column_stats_enabled)] = BoolGetDatum(enabled);
	tuple = heap_form_tuple(tupdesc, values, nulls);

	return HeapTupleGetDatum(tuple);
}

/*
 * Create a datum to be returned by ts_chunk_column_stats_disable DDL function
 */
static Datum
chunk_column_stats_disable_datum(FunctionCallInfo fcinfo, int32 hypertable_id, Name colname,
								 bool disabled)
{
	TupleDesc tupdesc;
	HeapTuple tuple;
	Datum values[Natts_disable_chunk_column_stats];
	bool nulls[Natts_disable_chunk_column_stats] = { false };

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("function returning record called in "
						"context that cannot accept type record")));

	tupdesc = BlessTupleDesc(tupdesc);

	Assert(tupdesc->natts == Natts_disable_chunk_column_stats);
	values[AttrNumberGetAttrOffset(Anum_disable_chunk_column_stats_hypertable_id)] =
		Int32GetDatum(hypertable_id);
	values[AttrNumberGetAttrOffset(Anum_disable_chunk_column_stats_column_name)] =
		NameGetDatum(colname);
	values[AttrNumberGetAttrOffset(Anum_disable_chunk_column_stats_disabled)] =
		BoolGetDatum(disabled);
	tuple = heap_form_tuple(tupdesc, values, nulls);

	return HeapTupleGetDatum(tuple);
}

static int32
chunk_column_stats_insert_relation(const Relation rel, Form_chunk_column_stats info)
{
	TupleDesc desc = RelationGetDescr(rel);
	Datum values[Natts_chunk_column_stats] = { 0 };
	bool nulls[Natts_chunk_column_stats] = { false };
	CatalogSecurityContext sec_ctx;

	ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);
	info->id = ts_catalog_table_next_seq_id(ts_catalog_get(), CHUNK_COLUMN_STATS);
	values[AttrNumberGetAttrOffset(Anum_chunk_column_stats_id)] = Int32GetDatum(info->id);
	values[AttrNumberGetAttrOffset(Anum_chunk_column_stats_hypertable_id)] =
		Int32GetDatum(info->hypertable_id);
	values[AttrNumberGetAttrOffset(Anum_chunk_column_stats_chunk_id)] =
		Int32GetDatum(info->chunk_id);
	values[AttrNumberGetAttrOffset(Anum_chunk_column_stats_column_name)] =
		NameGetDatum(&info->column_name);
	values[AttrNumberGetAttrOffset(Anum_chunk_column_stats_range_start)] =
		Int64GetDatum(info->range_start);
	values[AttrNumberGetAttrOffset(Anum_chunk_column_stats_range_end)] =
		Int64GetDatum(info->range_end);
	values[AttrNumberGetAttrOffset(Anum_chunk_column_stats_valid)] = BoolGetDatum(info->valid);

	ts_catalog_insert_values(rel, desc, values, nulls);
	ts_catalog_restore_user(&sec_ctx);

	return info->id;
}

static int32
chunk_column_stats_insert(Form_chunk_column_stats info)
{
	Catalog *catalog = ts_catalog_get();
	Relation rel;
	int32 ccol_stats_id;

	rel = table_open(catalog_get_table_id(catalog, CHUNK_COLUMN_STATS), RowExclusiveLock);
	ccol_stats_id = chunk_column_stats_insert_relation(rel, info);
	table_close(rel, RowExclusiveLock);
	return ccol_stats_id;
}

static ScanTupleResult
chunk_column_stats_tuple_update(TupleInfo *ti, void *data)
{
	bool should_free;
	HeapTuple tuple = ts_scanner_fetch_heap_tuple(ti, false, &should_free);
	FormData_chunk_column_stats *fd = (FormData_chunk_column_stats *) data;

	Datum values[Natts_chunk_column_stats] = { 0 };
	bool isnull[Natts_chunk_column_stats] = { 0 };
	bool doReplace[Natts_chunk_column_stats] = { 0 };

	values[AttrNumberGetAttrOffset(Anum_chunk_column_stats_range_start)] =
		Int64GetDatum(fd->range_start);
	doReplace[AttrNumberGetAttrOffset(Anum_chunk_column_stats_range_start)] = true;

	values[AttrNumberGetAttrOffset(Anum_chunk_column_stats_range_end)] =
		Int64GetDatum(fd->range_end);
	doReplace[AttrNumberGetAttrOffset(Anum_chunk_column_stats_range_end)] = true;

	values[AttrNumberGetAttrOffset(Anum_chunk_column_stats_valid)] = BoolGetDatum(fd->valid);
	doReplace[AttrNumberGetAttrOffset(Anum_chunk_column_stats_valid)] = true;

	HeapTuple new_tuple =
		heap_modify_tuple(tuple, ts_scanner_get_tupledesc(ti), values, isnull, doReplace);

	ts_catalog_update(ti->scanrel, new_tuple);

	heap_freetuple(new_tuple);
	if (should_free)
		heap_freetuple(tuple);

	return SCAN_DONE;
}

static int
chunk_column_stats_scan_internal(ScanKeyData *scankey, int nkeys, tuple_found_func tuple_found,
								 void *data, int limit, int dimension_index, LOCKMODE lockmode,
								 MemoryContext mctx)
{
	Catalog *catalog = ts_catalog_get();
	ScannerCtx scanctx = {
		.table = catalog_get_table_id(catalog, CHUNK_COLUMN_STATS),
		.index = catalog_get_index(catalog, CHUNK_COLUMN_STATS, dimension_index),
		.nkeys = nkeys,
		.limit = limit,
		.scankey = scankey,
		.data = data,
		.tuple_found = tuple_found,
		.lockmode = lockmode,
		.scandirection = ForwardScanDirection,
		.result_mctx = mctx,
	};

	return ts_scanner_scan(&scanctx);
}

int
ts_chunk_column_stats_update_by_id(int32 chunk_column_stats_id,
								   FormData_chunk_column_stats *fd_range)
{
	ScanKeyData scankey[1];

	ScanKeyInit(&scankey[0],
				Anum_chunk_column_stats_id_idx_id,
				BTEqualStrategyNumber,
				F_INT4EQ,
				Int32GetDatum(chunk_column_stats_id));

	return chunk_column_stats_scan_internal(scankey,
											1,
											chunk_column_stats_tuple_update,
											fd_range,
											1,
											CHUNK_COLUMN_STATS_ID_IDX,
											RowExclusiveLock,
											CurrentMemoryContext);
}

static void
ts_chunk_column_stats_validate(Form_chunk_column_stats info, const Oid hypertable_relid,
							   bool if_not_exists)
{
	HeapTuple tuple;
	Datum datum;
	bool isnull;
	Oid column_type;

	/* Check that the column exists and has not been dropped */
	tuple = SearchSysCacheAttName(hypertable_relid, NameStr(info->column_name));

	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_COLUMN),
				 errmsg("column \"%s\" does not exist", NameStr(info->column_name))));

	datum = SysCacheGetAttr(ATTNAME, tuple, Anum_pg_attribute_atttypid, &isnull);
	Assert(!isnull);

	column_type = DatumGetObjectId(datum);

	ReleaseSysCache(tuple);

	/* we only support a subset of data types for range calculations right now */
	switch (column_type)
	{
		case INT2OID:
		case INT4OID:
		case INT8OID:
		case TIMESTAMPOID:
		case TIMESTAMPTZOID:
		case DATEOID:
			break;
		default:
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("data type \"%s\" unsupported for range calculation",
							format_type_be(column_type)),
					 errhint("Integer-like, timestamp-like data types supported currently")));
	}
}

/*
 * Track min/max range for a given column in a hypertable
 */
static Datum
ts_chunk_column_stats_add_internal(FunctionCallInfo fcinfo, Oid table_relid, Name colname,
								   bool if_not_exists)
{
	Hypertable *ht;
	Cache *hcache;
	Datum retval = 0;
	int32 ccol_stats_id = 0;
	FormData_chunk_column_stats fd = { 0 };
	Form_chunk_column_stats form;
	bool enabled = true;

	ts_hypertable_permissions_check(table_relid, GetUserId());
	namestrcpy(&fd.column_name, NameStr(*colname));
	LockRelationOid(table_relid, AccessShareLock);

	ts_chunk_column_stats_validate(&fd, table_relid, if_not_exists);

	ht = ts_hypertable_cache_get_cache_and_entry(table_relid, CACHE_FLAG_NONE, &hcache);

	/*
	 * Add an entry in the _timescaledb_catalog.chunk_column_stats table. We add
	 * a special entry in the catalog which contains the hypertable_id, the colname,
	 * an invalid id (for the chunk) and PG_INT64_MAX, PG_INT64_MIN as range values
	 * to indicate that ranges should be calculated for this column for chunks.
	 *
	 * We have a uniqueness check on ht_id, colname, chunk_id
	 *
	 * Check if the entry already exists, first.
	 */
	form = ts_chunk_column_stats_lookup(ht->fd.id, INVALID_CHUNK_ID, NameStr(*colname));
	if (form != NULL)
	{
		if (!if_not_exists)
		{
			ereport(ERROR,
					(errcode(ERRCODE_DUPLICATE_OBJECT),
					 errmsg("already enabled for column \"%s\"", NameStr(*colname))));
		}
		else
		{
			ereport(NOTICE,
					(errcode(ERRCODE_DUPLICATE_OBJECT),
					 errmsg("already enabled for column \"%s\", skipping", NameStr(*colname))));
			/* return the existing id */
			ccol_stats_id = form->id;
			/* we still return true since it's already enabled */
			enabled = true;
			goto do_return;
		}
	}

	fd.hypertable_id = ht->fd.id;
	fd.chunk_id = INVALID_CHUNK_ID;
	fd.range_start = PG_INT64_MIN;
	fd.range_end = PG_INT64_MAX;
	fd.valid = true;
	ccol_stats_id = chunk_column_stats_insert(&fd);

	/* refresh the ht entry to accommodate this new chunk_column_stats entry */
	if (ht->range_space)
		pfree(ht->range_space);
	ht->range_space = ts_chunk_column_stats_range_space_scan(ht->fd.id,
															 ht->main_table_relid,
															 ts_cache_memory_ctx(hcache));

	/*
	 * If the hypertable has chunks, to make it compatible
	 * we add artificial min/max range entries which will cover -inf / inf
	 * range for all these existing chunks.
	 *
	 * TODO: Maybe have a future version which calculates actual ranges for
	 * compressed chunks in this function itself? Or have an option to this
	 * function which specifies if we should calculate ranges for compressed
	 * chunks.
	 */
	if (ts_hypertable_has_chunks(ht->main_table_relid, AccessShareLock))
	{
		ListCell *lc;
		List *chunk_id_list = ts_chunk_get_chunk_ids_by_hypertable_id(ht->fd.id);

		foreach (lc, chunk_id_list)
		{
			/* other fields are set appropriately in fd above. Only change chunk_id */
			fd.chunk_id = lfirst_int(lc);
			chunk_column_stats_insert(&fd);
		}
	}

do_return:
	/* return the id of the main entry for this dimension range */
	fd.id = ccol_stats_id;
	retval = chunk_column_stats_enable_datum(fcinfo, fd.id, enabled);
	ts_cache_release(hcache);

	PG_RETURN_DATUM(retval);
}

/*
 * Add min/max range tracking for a column in a hypertable.
 *
 * Arguments:
 * 0. Relation ID of table
 * 1. Column name
 * 2. IF NOT EXISTS option (bool)
 */
Datum
ts_chunk_column_stats_enable(PG_FUNCTION_ARGS)
{
	Oid hypertable_relid;
	NameData colname;
	bool if_not_exists;

	TS_PREVENT_FUNC_IF_READ_ONLY();

	if (PG_ARGISNULL(0))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("hypertable cannot be NULL")));
	hypertable_relid = PG_GETARG_OID(0);

	if (PG_ARGISNULL(1))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("column name cannot be NULL")));
	namestrcpy(&colname, NameStr(*PG_GETARG_NAME(1)));

	if_not_exists = PG_ARGISNULL(2) ? false : PG_GETARG_BOOL(2);

	return ts_chunk_column_stats_add_internal(fcinfo, hypertable_relid, &colname, if_not_exists);
}

/*
 * Remove min/max range tracking for a column in a hypertable.
 *
 * Arguments:
 * 0. Relation ID of hypertable
 * 1. Column name
 * 2. IF NOT EXISTS option (bool)
 */
Datum
ts_chunk_column_stats_disable(PG_FUNCTION_ARGS)
{
	Oid hypertable_relid;
	NameData colname;
	bool if_not_exists;
	Hypertable *ht;
	Cache *hcache;
	Datum retval = 0;
	int delete_count = 0;

	TS_PREVENT_FUNC_IF_READ_ONLY();

	if (PG_ARGISNULL(0))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("hypertable cannot be NULL")));
	hypertable_relid = PG_GETARG_OID(0);

	if (PG_ARGISNULL(1))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("column name cannot be NULL")));
	namestrcpy(&colname, NameStr(*PG_GETARG_NAME(1)));

	if_not_exists = PG_ARGISNULL(2) ? false : PG_GETARG_BOOL(2);

	ts_hypertable_permissions_check(hypertable_relid, GetUserId());
	LockRelationOid(hypertable_relid, ShareUpdateExclusiveLock);
	ht = ts_hypertable_cache_get_cache_and_entry(hypertable_relid, CACHE_FLAG_NONE, &hcache);

	/*
	 * Remove entries from _timescaledb_catalog.chunk_column_stats table.
	 *
	 * There's a special entry in the catalog which contains the hypertable_id, the colname,
	 * an invalid id (for the chunk) and PG_INT64_MAX, PG_INT64_MIN as range values
	 * to indicate that ranges should be calculated for this column for chunks.
	 *
	 * Check if the entry already exists, first.
	 */
	if (ts_chunk_column_stats_lookup(ht->fd.id, INVALID_CHUNK_ID, NameStr(colname)) == NULL)
	{
		if (!if_not_exists)
		{
			ereport(ERROR,
					(errcode(ERRCODE_DUPLICATE_OBJECT),
					 errmsg("statistics not enabled for column \"%s\"", NameStr(colname))));
		}
		else
		{
			ereport(NOTICE,
					(errcode(ERRCODE_DUPLICATE_OBJECT),
					 errmsg("statistics not enabled for column \"%s\", skipping",
							NameStr(colname))));
			goto do_return;
		}
	}

	/* Delete all entries matching this hypertable_id and column_name. */
	delete_count = ts_chunk_column_stats_delete_by_ht_colname(ht->fd.id, NameStr(colname));

	/* refresh the ht entry to accommodate this deleted chunk_column_stats entry */
	if (ht->range_space)
		pfree(ht->range_space);
	ht->range_space = ts_chunk_column_stats_range_space_scan(ht->fd.id,
															 ht->main_table_relid,
															 ts_cache_memory_ctx(hcache));

do_return:
	retval = chunk_column_stats_disable_datum(fcinfo, ht->fd.id, &colname, delete_count > 0);
	ts_cache_release(hcache);

	PG_RETURN_DATUM(retval);
}

/*
 * Dimension range entries are similar to OPEN DIMENSION entries. So, most of
 * the default fields are similar to them.
 */
Dimension *
ts_chunk_column_stats_fill_dummy_dimension(FormData_chunk_column_stats *r, Oid main_table_relid)
{
	Dimension *d = palloc0(sizeof(Dimension));

	d->fd.id = r->id;
	d->fd.hypertable_id = r->hypertable_id;
	d->fd.aligned = true;
	namestrcpy(&d->fd.column_name, NameStr(r->column_name));
	d->fd.interval_length = 1; /* a dummy interval length for the dummy dimension */

	/* similar to open dimensions except that we don't participate in partitioning */
	d->type = DIMENSION_TYPE_STATS;
	d->column_attno = get_attnum(main_table_relid, NameStr(d->fd.column_name));
	d->main_table_relid = main_table_relid;

	/* rest of the fields are zeroed out */
	return d;
}

static ScanTupleResult
chunk_column_stats_tuple_found(TupleInfo *ti, void *data)
{
	ChunkRangeSpace *rs = data;
	Form_chunk_column_stats d = &rs->range_cols[rs->num_range_cols++];
	Form_chunk_column_stats fd;
	bool should_free;
	HeapTuple tuple = ts_scanner_fetch_heap_tuple(ti, false, &should_free);

	Assert(rs->num_range_cols <= rs->capacity);

	fd = (Form_chunk_column_stats) GETSTRUCT(tuple);
	memcpy(d, fd, sizeof(*fd));

	if (should_free)
		heap_freetuple(tuple);

	return SCAN_CONTINUE;
}

ChunkRangeSpace *
ts_chunk_column_stats_range_space_scan(int32 hypertable_id, Oid ht_reloid, MemoryContext mctx)
{
	/* We won't have more entries than the number of columns in the HT */
	int num_range_cols = ts_get_relnatts(ht_reloid);
	ChunkRangeSpace *range_space =
		MemoryContextAllocZero(mctx, CHUNKRANGESPACE_SIZE(num_range_cols));
	ScanKeyData scankey[2];

	range_space->hypertable_id = hypertable_id;
	range_space->capacity = num_range_cols;
	range_space->num_range_cols = 0;

	/* Perform an index scan on hypertable_id, invalid chunk_id. */
	ScanKeyInit(
		&scankey[0],
		Anum_chunk_column_stats_ht_id_chunk_id_column_name_range_start_range_end_idx_hypertable_id,
		BTEqualStrategyNumber,
		F_INT4EQ,
		Int32GetDatum(hypertable_id));
	ScanKeyInit(
		&scankey[1],
		Anum_chunk_column_stats_ht_id_chunk_id_column_name_range_start_range_end_idx_chunk_id,
		BTEqualStrategyNumber,
		F_INT4EQ,
		Int32GetDatum(INVALID_CHUNK_ID));

	chunk_column_stats_scan_internal(scankey,
									 2,
									 chunk_column_stats_tuple_found,
									 range_space,
									 0,
									 CHUNK_COLUMN_STATS_HT_ID_CHUNK_ID_COLUMN_NAME_IDX,
									 AccessShareLock,
									 mctx);

	if (range_space->num_range_cols == 0)
	{
		pfree(range_space);
		return NULL;
	}

	return range_space;
}

static ScanTupleResult
form_range_tuple_found(TupleInfo *ti, void *data)
{
	Form_chunk_column_stats rg = data;
	bool should_free;
	HeapTuple tuple = ts_scanner_fetch_heap_tuple(ti, false, &should_free);

	memcpy(rg, GETSTRUCT(tuple), sizeof(FormData_chunk_column_stats));

	if (should_free)
		heap_freetuple(tuple);

	return SCAN_DONE;
}

Form_chunk_column_stats
ts_chunk_column_stats_lookup(int32 hypertable_id, int32 chunk_id, const char *col_name)
{
	ScanKeyData scankey[3];
	Form_chunk_column_stats form_range = palloc0(sizeof(FormData_chunk_column_stats));
	form_range->chunk_id = INVALID_CHUNK_ID; /* for clarity */

	/* Perform an index scan on hypertable_id, chunk_id, col_name. */
	ScanKeyInit(
		&scankey[0],
		Anum_chunk_column_stats_ht_id_chunk_id_column_name_range_start_range_end_idx_hypertable_id,
		BTEqualStrategyNumber,
		F_INT4EQ,
		Int32GetDatum(hypertable_id));
	ScanKeyInit(
		&scankey[1],
		Anum_chunk_column_stats_ht_id_chunk_id_column_name_range_start_range_end_idx_chunk_id,
		BTEqualStrategyNumber,
		F_INT4EQ,
		Int32GetDatum(chunk_id));
	ScanKeyInit(
		&scankey[2],
		Anum_chunk_column_stats_ht_id_chunk_id_column_name_range_start_range_end_idx_column_name,
		BTEqualStrategyNumber,
		F_NAMEEQ,
		CStringGetDatum(col_name));

	chunk_column_stats_scan_internal(scankey,
									 3,
									 form_range_tuple_found,
									 form_range,
									 1,
									 CHUNK_COLUMN_STATS_HT_ID_CHUNK_ID_COLUMN_NAME_IDX,
									 AccessShareLock,
									 CurrentMemoryContext);

	if (strlen(NameStr(form_range->column_name)) == 0)
	{
		pfree(form_range);
		return NULL;
	}

	return form_range;
}

/*
 * Update column dimension ranges in the catalog for the
 * provided chunk (it's assumed that the chunk is locked
 * appropriately).
 *
 * Calculate actual ranges for the given chunk for the columns
 * insert these entries. This allows for the
 * chunk to be picked up when queries use these columns in
 * WHERE clauses with these ranges.
 *
 * Returns the number of column entries that have been added or
 * updated.
 */
int
ts_chunk_column_stats_calculate(const Hypertable *ht, const Chunk *chunk)
{
	Size i = 0;
	ChunkRangeSpace *rs = ht->range_space;
	MemoryContext work_mcxt, orig_mcxt;

	/* Quick check. Bail out early if none */
	if (rs == NULL)
		return i;

	work_mcxt =
		AllocSetContextCreate(CurrentMemoryContext, "dimension-range-work", ALLOCSET_DEFAULT_SIZES);
	orig_mcxt = MemoryContextSwitchTo(work_mcxt);

	for (int range_index = 0; range_index < rs->num_range_cols; range_index++)
	{
		Datum minmax[2];
		AttrNumber attno;
		char *col_name = NameStr(rs->range_cols[range_index].column_name);
		Oid col_type;

		/* Get the attribute number in the HT for this column, and map to the chunk */
		attno = get_attnum(ht->main_table_relid, col_name);
		attno = ts_map_attno(ht->main_table_relid, chunk->table_id, attno);
		col_type = get_atttype(ht->main_table_relid, attno);

		/* calculate the min/max range for this column on this chunk */
		if (ts_chunk_get_minmax(chunk->table_id, col_type, attno, "column range", minmax))
		{
			Form_chunk_column_stats range;
			int64 min = ts_time_value_to_internal(minmax[0], col_type);
			int64 max = ts_time_value_to_internal(minmax[1], col_type);

			/* The end value is exclusive to the range, so incr by 1 */
			if (max != DIMENSION_SLICE_MAXVALUE)
			{
				max++;
				/* Again, check overflow */
				max = REMAP_LAST_COORDINATE(max);
			}

			/*
			 * Check if an entry exists for this ht, chunk_id, colname combo. If it exists
			 * and it's not -inf/+inf then it's probably a case of re-computation of the
			 * ranges. In such a case, we compare the stored range_start and range_end entries
			 * and compare with the min/max calculated.
			 *
			 * if min < range_start, then new_range_start = min
			 * if max > range_end, then new_range_end = max
			 *
			 * We need to update the existing entry with changes in the range.
			 * Also, in case of updates, the entry might be marked "invalid" so it needs to be
			 * made "valid" again as well.
			 */
			range = ts_chunk_column_stats_lookup(ht->fd.id, chunk->fd.id, col_name);

			/* Add a new entry if none exists */
			if (range == NULL)
			{
				FormData_chunk_column_stats fd = { 0 };
				fd.hypertable_id = ht->fd.id;
				fd.chunk_id = chunk->fd.id;
				namestrcpy(&fd.column_name, col_name);
				fd.range_start = min;
				fd.range_end = max;
				fd.valid = true;

				chunk_column_stats_insert(&fd);
				i++;
			}
			/* update case */
			else if (range->range_start != min || range->range_end != max || !range->valid)
			{
				range->range_start = min;
				range->range_end = max;
				range->valid = true;

				ts_chunk_column_stats_update_by_id(range->id, range);
				i++;
			}
		}
		else
			ereport(WARNING, errmsg("unable to calculate min/max values for column ranges"));
	}

	MemoryContextSwitchTo(orig_mcxt);
	MemoryContextDelete(work_mcxt);

	return i;
}

/*
 * Insert column dimension ranges in the catalog for the
 * provided chunk (it's assumed that the chunk is locked
 * appropriately).
 *
 * We insert -inf/+inf entries for the given chunk which means
 * default selection till the actual ranges get calculated later.
 *
 * Returns the number of column entries that have been inserted.
 */
int
ts_chunk_column_stats_insert(const Hypertable *ht, const Chunk *chunk)
{
	Size range_index = 0;
	ChunkRangeSpace *rs = ht->range_space;
	MemoryContext work_mcxt, orig_mcxt;

	/* Quick check. Bail out early if none */
	if (rs == NULL)
		return range_index;

	work_mcxt =
		AllocSetContextCreate(CurrentMemoryContext, "dimension-range-work", ALLOCSET_DEFAULT_SIZES);
	orig_mcxt = MemoryContextSwitchTo(work_mcxt);

	for (range_index = 0; range_index < rs->num_range_cols; range_index++)
	{
		AttrNumber attno;
		char *col_name = NameStr(rs->range_cols[range_index].column_name);
		FormData_chunk_column_stats fd = { 0 };

		/* Get the attribute number in the HT for this column, and map to the chunk */
		attno = get_attnum(ht->main_table_relid, col_name);
		attno = ts_map_attno(ht->main_table_relid, chunk->table_id, attno);

		/* insert an entry for this ht_id, chunk_id for this col_name with -inf/+inf range */
		fd.hypertable_id = ht->fd.id;
		fd.chunk_id = chunk->fd.id;
		namestrcpy(&fd.column_name, col_name);
		fd.range_start = PG_INT64_MIN;
		fd.range_end = PG_INT64_MAX;
		fd.valid = true;

		chunk_column_stats_insert(&fd);
	}

	MemoryContextSwitchTo(orig_mcxt);
	MemoryContextDelete(work_mcxt);

	return range_index;
}

/*
 * Check if there is a min/max range tracking on this column which is being dropped.
 * Need to delete the entries from _timescaledb_catalog.chunk_column_stats table in that case.
 */
void
ts_chunk_column_stats_drop(const Hypertable *ht, const char *col_name, bool *dropped)
{
	/* delete all entries belonging to this HT and pointing to this column */
	*dropped = (ts_chunk_column_stats_delete_by_ht_colname(ht->fd.id, col_name) > 0);
}

static ScanTupleResult
chunk_column_stats_tuple_delete(TupleInfo *ti, void *data)
{
	CatalogSecurityContext sec_ctx;
	int *count = data;

	/* delete catalog entry */
	ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);
	ts_catalog_delete_tid(ti->scanrel, ts_scanner_get_tuple_tid(ti));
	ts_catalog_restore_user(&sec_ctx);
	*count = *count + 1;

	return SCAN_CONTINUE;
}

int
ts_chunk_column_stats_delete_by_ht_colname(int32 hypertable_id, const char *col_name)
{
	ScanKeyData scankey[2];
	int count = 0;

	/* Perform an index scan on hypertable_id, col_name. */
	ScanKeyInit(
		&scankey[0],
		Anum_chunk_column_stats_ht_id_chunk_id_column_name_range_start_range_end_idx_hypertable_id,
		BTEqualStrategyNumber,
		F_INT4EQ,
		Int32GetDatum(hypertable_id));
	ScanKeyInit(
		&scankey[1],
		Anum_chunk_column_stats_ht_id_chunk_id_column_name_range_start_range_end_idx_column_name,
		BTEqualStrategyNumber,
		F_NAMEEQ,
		CStringGetDatum((col_name)));

	chunk_column_stats_scan_internal(scankey,
									 2,
									 chunk_column_stats_tuple_delete,
									 &count,
									 0,
									 CHUNK_COLUMN_STATS_HT_ID_CHUNK_ID_COLUMN_NAME_IDX,
									 RowExclusiveLock,
									 CurrentMemoryContext);

	return count;
}

int
ts_chunk_column_stats_delete_by_chunk_id(int32 chunk_id)
{
	ScanKeyData scankey[1];
	int count = 0;

	/* Perform an index scan on chunk_id. */
	ScanKeyInit(
		&scankey[0],
		Anum_chunk_column_stats_ht_id_chunk_id_column_name_range_start_range_end_idx_chunk_id,
		BTEqualStrategyNumber,
		F_INT4EQ,
		Int32GetDatum(chunk_id));

	chunk_column_stats_scan_internal(scankey,
									 1,
									 chunk_column_stats_tuple_delete,
									 &count,
									 0,
									 CHUNK_COLUMN_STATS_HT_ID_CHUNK_ID_COLUMN_NAME_IDX,
									 RowExclusiveLock,
									 CurrentMemoryContext);

	return count;
}

int
ts_chunk_column_stats_reset_by_chunk_id(int32 chunk_id)
{
	ScanKeyData scankey[1];
	FormData_chunk_column_stats fd = { 0 };

	/* reset the range to min and max for all entries belonging to this chunk */
	fd.range_start = PG_INT64_MIN;
	fd.range_end = PG_INT64_MAX;
	fd.valid = true;

	/* Perform an index scan on chunk_id. */
	ScanKeyInit(
		&scankey[0],
		Anum_chunk_column_stats_ht_id_chunk_id_column_name_range_start_range_end_idx_chunk_id,
		BTEqualStrategyNumber,
		F_INT4EQ,
		Int32GetDatum(chunk_id));

	return chunk_column_stats_scan_internal(scankey,
											1,
											chunk_column_stats_tuple_update,
											&fd,
											0,
											CHUNK_COLUMN_STATS_HT_ID_CHUNK_ID_COLUMN_NAME_IDX,
											RowExclusiveLock,
											CurrentMemoryContext);
}

int
ts_chunk_column_stats_delete_by_hypertable_id(int32 hypertable_id)
{
	ScanKeyData scankey[1];
	int count = 0;

	/* Perform an index scan on hypertable_id. */
	ScanKeyInit(
		&scankey[0],
		Anum_chunk_column_stats_ht_id_chunk_id_column_name_range_start_range_end_idx_hypertable_id,
		BTEqualStrategyNumber,
		F_INT4EQ,
		Int32GetDatum(hypertable_id));

	chunk_column_stats_scan_internal(scankey,
									 1,
									 chunk_column_stats_tuple_delete,
									 &count,
									 0,
									 CHUNK_COLUMN_STATS_HT_ID_CHUNK_ID_COLUMN_NAME_IDX,
									 RowExclusiveLock,
									 CurrentMemoryContext);

	return count;
}

/*
 * For min/max ranges we are interested in the occurrence of a value which
 * possibly lies in multiple entries from _timescaledb_catalog.chunk_column_stats.
 *
 * The check for enclosure needs to be run as a FILTER on top of all the matching
 * entries for the hypertable, column combo. So, we only use ht_id, col_name for
 * the scan below.
 */
static int
chunk_column_stats_scan_iterator_set(ScanIterator *it, int32 hypertable_id, const char *col_name)
{
	Catalog *catalog = ts_catalog_get();

	it->ctx.index = catalog_get_index(catalog,
									  CHUNK_COLUMN_STATS,
									  CHUNK_COLUMN_STATS_HT_ID_CHUNK_ID_COLUMN_NAME_IDX);
	ts_scan_iterator_scan_key_reset(it);
	ts_scan_iterator_scan_key_init(
		it,
		Anum_chunk_column_stats_ht_id_chunk_id_column_name_range_start_range_end_idx_hypertable_id,
		BTEqualStrategyNumber,
		F_INT4EQ,
		Int32GetDatum(hypertable_id));

	ts_scan_iterator_scan_key_init(
		it,
		Anum_chunk_column_stats_ht_id_chunk_id_column_name_range_start_range_end_idx_column_name,
		BTEqualStrategyNumber,
		F_NAMEEQ,
		CStringGetDatum((col_name)));

	it->ctx.scandirection = ForwardScanDirection;

	return it->ctx.nkeys;
}

/*
 * We need to get all chunks matching the hypertable ID and the column name.
 * For each chunk obtained, we need to run a FILTER using the strategies
 * and the lower/upper bound values provided.
 *
 * The EXPLAIN plan is basically like below:
 *
 * Index Scan using chunk_column_stats_ht_id_chunk_id_colname_range_start_end_key
 *												on _timescaledb_catalog.chunk_column_stats
 * Output: chunk_id
 * Index Cond: ((chunk_column_stats.hypertable_id = :ht_id) AND
 *								(chunk_column_stats.column_name = ':colname'))
 * Filter: ((chunk_column_stats.range_end BTREE_OP lower_bound/upper_bound) OR
 *					(chunk_column_stats.range_start BTREE_OP lower_bound/upper_bound))
 *
 * The strategies and lower_bound/upper_bound values get assigned in
 * dimension_restrict_info_range_add function.
 *
 * We need to run the "Filter" above ourselves because there's no other PG mechanism for OR
 * types of checks like these.
 */
List *
ts_chunk_column_stats_get_chunk_ids_by_scan(DimensionRestrictInfo *dri)
{
	ScanIterator it;
	List *chunkids = NIL;
	DimensionRestrictInfoOpen *open;

	Assert(dri && dri->dimension->type == DIMENSION_TYPE_STATS);

	/* setup the scanner */
	it = ts_scan_iterator_create(CHUNK_COLUMN_STATS, AccessShareLock, CurrentMemoryContext);
	it.ctx.flags |= SCANNER_F_NOEND_AND_NOCLOSE;
	it.ctx.tuplock = NULL;

	open = (DimensionRestrictInfoOpen *) dri;
	/*
	 * We need to get all chunks matching the hypertable ID and the column name.
	 */
	chunk_column_stats_scan_iterator_set(&it,
										 dri->dimension->fd.hypertable_id,
										 NameStr(dri->dimension->fd.column_name));
	/*
	 * For each chunk obtained, we need to run a FILTER using the strategies
	 * and the lower/upper bound values provided.
	 */
	ts_scan_iterator_start_or_restart_scan(&it);
	ts_scanner_foreach(&it)
	{
		Form_chunk_column_stats fd;
		bool should_free, matched = false;
		HeapTuple tuple = ts_scanner_fetch_heap_tuple(it.tinfo, false, &should_free);

		fd = (Form_chunk_column_stats) GETSTRUCT(tuple);

		/*
		 * We have an entry with INVALID_CHUNK_ID which will match all cases due to
		 * -INF/+INF range entries for it. Ignore that.
		 */
		if (fd->chunk_id == INVALID_CHUNK_ID)
			goto done;

		/*
		 * If an entry is marked "invalid" then it means that the ranges cannot be relied
		 * on. So, we assume the worse case and include this chunk for the scan.
		 *
		 * (Entry is typically marked "invalid" when a compressed chunk becomes partial
		 * due to DML in it.)
		 */
		if (!fd->valid)
		{
			matched = true;
			goto done;
		}

		/*
		 * All data is in int8 format so we do regular comparisons. Also, it's an OR
		 * check so prepare to short circuit if one evaluates to true.
		 *
		 * No real way to know if checking range_start or range_end first will be more
		 * effective. So let's start with range_end checks first.
		 */
		switch (open->upper_strategy)
		{
			case BTLessEqualStrategyNumber:
			case BTLessStrategyNumber: /* e.g: id <= 90 */
			{
				/* range_end is exclusive, so check accordingly */
				if ((fd->range_end - 1) <= open->upper_bound)
					matched = true;
			}
			break;
			case BTGreaterEqualStrategyNumber:
			case BTGreaterStrategyNumber: /* e.g: id >= 9 */
			{
				/* range_end is exclusive, so check accordingly */
				if ((fd->range_end - 1) >= open->upper_bound)
					matched = true;
			}
			break;
			case BTEqualStrategyNumber:
			default:
				/* unsupported strategy */
				break;
		}

		if (matched)
			goto done;

		/* range_end checks didn't match, check for range_start now */
		switch (open->lower_strategy)
		{
			case BTLessEqualStrategyNumber:
			case BTLessStrategyNumber:
			{
				if (fd->range_start <= open->lower_bound)
					matched = true;
			}
			break;
			case BTGreaterEqualStrategyNumber:
			case BTGreaterStrategyNumber:
			{
				if (fd->range_start >= open->lower_bound)
					matched = true;
			}
			break;
			case BTEqualStrategyNumber:
			default:
				/* unsupported strategy */
				break;
		}

	done:
		if (matched)
			chunkids = lappend_int(chunkids, fd->chunk_id);

		if (should_free)
			heap_freetuple(tuple);
	}
	ts_scan_iterator_close(&it);

	return chunkids;
}

/*
 * Update all entries for this ht_id, old_colname to point to the new_colname
 */
int
ts_chunk_column_stats_set_name(FormData_chunk_column_stats *in_fd, char *new_colname)
{
	ScanIterator it;
	NameData new_column_name;
	int count = 0;

	namestrcpy(&new_column_name, new_colname);
	/* setup the scanner */
	it = ts_scan_iterator_create(CHUNK_COLUMN_STATS, AccessShareLock, CurrentMemoryContext);
	it.ctx.flags |= SCANNER_F_NOEND_AND_NOCLOSE;
	it.ctx.tuplock = NULL;

	/*
	 * We need to get all chunks matching the hypertable ID and the column name.
	 */
	chunk_column_stats_scan_iterator_set(&it, in_fd->hypertable_id, NameStr(in_fd->column_name));
	/*
	 * For each entry obtained, we need to update the column_name to point to the
	 * new_colname
	 */
	ts_scan_iterator_start_or_restart_scan(&it);
	ts_scanner_foreach(&it)
	{
		Datum values[Natts_chunk_column_stats] = { 0 };
		bool isnull[Natts_chunk_column_stats] = { 0 };
		bool doReplace[Natts_chunk_column_stats] = { 0 };
		bool should_free;
		TupleInfo *ti = ts_scan_iterator_tuple_info(&it);

		HeapTuple tuple = ts_scanner_fetch_heap_tuple(ti, false, &should_free);

		values[AttrNumberGetAttrOffset(Anum_chunk_column_stats_column_name)] =
			NameGetDatum(&new_column_name);
		doReplace[AttrNumberGetAttrOffset(Anum_chunk_column_stats_column_name)] = true;

		HeapTuple new_tuple =
			heap_modify_tuple(tuple, ts_scanner_get_tupledesc(ti), values, isnull, doReplace);

		ts_catalog_update(ti->scanrel, new_tuple);

		heap_freetuple(new_tuple);
		if (should_free)
			heap_freetuple(tuple);

		count++;
	}

	ts_scan_iterator_close(&it);
	return count;
}

static ScanTupleResult
invalidate_range_tuple_found(TupleInfo *ti, void *data)
{
	bool should_free;
	HeapTuple tuple = ts_scanner_fetch_heap_tuple(ti, false, &should_free);
	bool valid = false;

	Datum values[Natts_chunk_column_stats] = { 0 };
	bool isnull[Natts_chunk_column_stats] = { 0 };
	bool doReplace[Natts_chunk_column_stats] = { 0 };

	values[AttrNumberGetAttrOffset(Anum_chunk_column_stats_valid)] = BoolGetDatum(valid);
	doReplace[AttrNumberGetAttrOffset(Anum_chunk_column_stats_valid)] = true;

	HeapTuple new_tuple =
		heap_modify_tuple(tuple, ts_scanner_get_tupledesc(ti), values, isnull, doReplace);

	ts_catalog_update(ti->scanrel, new_tuple);

	heap_freetuple(new_tuple);
	if (should_free)
		heap_freetuple(tuple);

	return SCAN_CONTINUE;
}

/*
 * Mark all entries for a given chunk_id as "invalid"
 */
void
ts_chunk_column_stats_set_invalid(int32 hypertable_id, int32 chunk_id)
{
	ScanKeyData scankey[2];

	/* Perform an index scan on hypertable_id, chunk_id. */
	ScanKeyInit(
		&scankey[0],
		Anum_chunk_column_stats_ht_id_chunk_id_column_name_range_start_range_end_idx_hypertable_id,
		BTEqualStrategyNumber,
		F_INT4EQ,
		Int32GetDatum(hypertable_id));
	ScanKeyInit(
		&scankey[1],
		Anum_chunk_column_stats_ht_id_chunk_id_column_name_range_start_range_end_idx_chunk_id,
		BTEqualStrategyNumber,
		F_INT4EQ,
		Int32GetDatum(chunk_id));

	chunk_column_stats_scan_internal(scankey,
									 2,
									 invalidate_range_tuple_found,
									 NULL,
									 0,
									 CHUNK_COLUMN_STATS_HT_ID_CHUNK_ID_COLUMN_NAME_IDX,
									 RowExclusiveLock,
									 CurrentMemoryContext);
}
