/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <executor/spi.h>
#include <fmgr.h>
#include <lib/stringinfo.h>
#include <utils/builtins.h>
#include <utils/rel.h>
#include <utils/relcache.h>
#include <utils/date.h>
#include <utils/snapmgr.h>

#include <scanner.h>
#include <compat/compat.h>
#include <scan_iterator.h>
#include "ts_catalog/continuous_agg.h"
#include <time_utils.h>

#include "materialize.h"
#include "invalidation.h"

static bool ranges_overlap(InternalTimeRange invalidation_range,
						   InternalTimeRange new_materialization_range);
static TimeRange internal_time_range_to_time_range(InternalTimeRange internal);
static int64 range_length(const InternalTimeRange range);
static Datum internal_to_time_value_or_infinite(int64 internal, Oid time_type,
												bool *is_infinite_out);

/***************************
 * materialization support *
 ***************************/

static void spi_update_materializations(SchemaAndName partial_view,
										SchemaAndName materialization_table,
										const NameData *time_column_name,
										TimeRange invalidation_range, const int32 chunk_id);
static void spi_delete_materializations(SchemaAndName materialization_table,
										const NameData *time_column_name,
										TimeRange invalidation_range,
										const char *const chunk_condition);
static void spi_insert_materializations(SchemaAndName partial_view,
										SchemaAndName materialization_table,
										const NameData *time_column_name,
										TimeRange materialization_range,
										const char *const chunk_condition);

void
continuous_agg_update_materialization(SchemaAndName partial_view,
									  SchemaAndName materialization_table,
									  const NameData *time_column_name,
									  InternalTimeRange new_materialization_range,
									  InternalTimeRange invalidation_range, int32 chunk_id)
{
	InternalTimeRange combined_materialization_range = new_materialization_range;
	bool materialize_invalidations_separately = range_length(invalidation_range) > 0;
	int res = SPI_connect();
	if (res != SPI_OK_CONNECT)
		elog(ERROR, "could not connect to SPI in materializer");

	/* pin the start of new_materialization to the end of new_materialization,
	 * we are not allowed to materialize beyond that point
	 */
	if (new_materialization_range.start > new_materialization_range.end)
		new_materialization_range.start = new_materialization_range.end;

	if (range_length(invalidation_range) > 0)
	{
		Assert(invalidation_range.start <= invalidation_range.end);

		/* we never materialize beyond the new materialization range */
		if (invalidation_range.start >= new_materialization_range.end ||
			invalidation_range.end > new_materialization_range.end)
			elog(ERROR, "internal error: invalidation range ahead of new materialization range");

		/* If the invalidation and new materialization ranges overlap, materialize in one go */
		materialize_invalidations_separately =
			!ranges_overlap(invalidation_range, new_materialization_range);

		combined_materialization_range.start =
			int64_min(invalidation_range.start, new_materialization_range.start);
	}

	/* Then insert the materializations.
	 * We insert them in two groups:
	 * [lowest_invalidated, greatest_invalidated] and
	 * [start_of_new_materialization, end_of_new_materialization]
	 * eventually, we may want more precise deletions and insertions for the invalidated ranges.
	 * if greatest_invalidated == end_of_new_materialization then we only perform 1 insertion.
	 * to prevent values from being inserted multiple times.
	 */
	if (range_length(invalidation_range) == 0 || !materialize_invalidations_separately)
	{
		spi_update_materializations(partial_view,
									materialization_table,
									time_column_name,
									internal_time_range_to_time_range(
										combined_materialization_range),
									chunk_id);
	}
	else
	{
		spi_update_materializations(partial_view,
									materialization_table,
									time_column_name,
									internal_time_range_to_time_range(invalidation_range),
									chunk_id);

		spi_update_materializations(partial_view,
									materialization_table,
									time_column_name,
									internal_time_range_to_time_range(new_materialization_range),
									chunk_id);
	}

	if ((res = SPI_finish()) != SPI_OK_FINISH)
		elog(ERROR, "SPI_finish failed: %s", SPI_result_code_string(res));
}

static bool
ranges_overlap(InternalTimeRange invalidation_range, InternalTimeRange new_materialization_range)
{
	Assert(invalidation_range.start <= invalidation_range.end);
	Assert(new_materialization_range.start <= new_materialization_range.end);
	return !(invalidation_range.end < new_materialization_range.start ||
			 new_materialization_range.end < invalidation_range.start);
}

static int64
range_length(const InternalTimeRange range)
{
	Assert(range.end >= range.start);

	return int64_saturating_sub(range.end, range.start);
}

static Datum
time_range_internal_to_min_time_value(Oid type)
{
	switch (type)
	{
		case TIMESTAMPOID:
			return TimestampGetDatum(DT_NOBEGIN);
		case TIMESTAMPTZOID:
			return TimestampTzGetDatum(DT_NOBEGIN);
		case DATEOID:
			return DateADTGetDatum(DATEVAL_NOBEGIN);
		default:
			return ts_internal_to_time_value(PG_INT64_MIN, type);
	}
}

static Datum
time_range_internal_to_max_time_value(Oid type)
{
	switch (type)
	{
		case TIMESTAMPOID:
			return TimestampGetDatum(DT_NOEND);
		case TIMESTAMPTZOID:
			return TimestampTzGetDatum(DT_NOEND);
		case DATEOID:
			return DateADTGetDatum(DATEVAL_NOEND);
			break;
		default:
			return ts_internal_to_time_value(PG_INT64_MAX, type);
	}
}

static Datum
internal_to_time_value_or_infinite(int64 internal, Oid time_type, bool *is_infinite_out)
{
	/* MIN and MAX can occur due to NULL thresholds, or due to a lack of invalidations. Since our
	 * regular conversion function errors in those cases, and we want to use those as markers for an
	 * open threshold in one direction, we special case this here*/
	if (internal == PG_INT64_MIN)
	{
		if (is_infinite_out != NULL)
			*is_infinite_out = true;
		return time_range_internal_to_min_time_value(time_type);
	}
	else if (internal == PG_INT64_MAX)
	{
		if (is_infinite_out != NULL)
			*is_infinite_out = true;
		return time_range_internal_to_max_time_value(time_type);
	}
	else
	{
		if (is_infinite_out != NULL)
			*is_infinite_out = false;
		return ts_internal_to_time_value(internal, time_type);
	}
}

static TimeRange
internal_time_range_to_time_range(InternalTimeRange internal)
{
	TimeRange range;
	range.type = internal.type;

	range.start = internal_to_time_value_or_infinite(internal.start, internal.type, NULL);
	range.end = internal_to_time_value_or_infinite(internal.end, internal.type, NULL);

	return range;
}

static void
spi_update_materializations(SchemaAndName partial_view, SchemaAndName materialization_table,
							const NameData *time_column_name, TimeRange invalidation_range,
							const int32 chunk_id)
{
	StringInfo chunk_condition = makeStringInfo();

	/*
	 * chunk_id is valid if the materializaion update should be done only on the given chunk.
	 * This is used currently for refresh on chunk drop only. In other cases, manual
	 * call to refresh_continuous_aggregate or call from a refresh policy, chunk_id is
	 * not provided, i.e., invalid.
	 */
	if (chunk_id != INVALID_CHUNK_ID)
		appendStringInfo(chunk_condition, "AND chunk_id = %d", chunk_id);

	spi_delete_materializations(materialization_table,
								time_column_name,
								invalidation_range,
								chunk_condition->data);
	spi_insert_materializations(partial_view,
								materialization_table,
								time_column_name,
								invalidation_range,
								chunk_condition->data);
}

static void
spi_delete_materializations(SchemaAndName materialization_table, const NameData *time_column_name,
							TimeRange invalidation_range, const char *const chunk_condition)
{
	int res;
	StringInfo command = makeStringInfo();
	Oid out_fn;
	bool type_is_varlena;
	char *invalidation_start;
	char *invalidation_end;

	getTypeOutputInfo(invalidation_range.type, &out_fn, &type_is_varlena);
	invalidation_start = OidOutputFunctionCall(out_fn, invalidation_range.start);
	invalidation_end = OidOutputFunctionCall(out_fn, invalidation_range.end);

	appendStringInfo(command,
					 "DELETE FROM %s.%s AS D WHERE "
					 "D.%s >= %s AND D.%s < %s %s;",
					 quote_identifier(NameStr(*materialization_table.schema)),
					 quote_identifier(NameStr(*materialization_table.name)),
					 quote_identifier(NameStr(*time_column_name)),
					 quote_literal_cstr(invalidation_start),
					 quote_identifier(NameStr(*time_column_name)),
					 quote_literal_cstr(invalidation_end),
					 chunk_condition);

	res = SPI_execute_with_args(command->data,
								0 /*=nargs*/,
								NULL,
								NULL,
								NULL /*=Nulls*/,
								false /*=read_only*/,
								0 /*count*/);
	if (res < 0)
		elog(ERROR, "could not delete old values from materialization table");
}

static void
spi_insert_materializations(SchemaAndName partial_view, SchemaAndName materialization_table,
							const NameData *time_column_name, TimeRange materialization_range,
							const char *const chunk_condition)
{
	int res;
	StringInfo command = makeStringInfo();
	Oid out_fn;
	bool type_is_varlena;
	char *materialization_start;
	char *materialization_end;

	getTypeOutputInfo(materialization_range.type, &out_fn, &type_is_varlena);
	materialization_start = OidOutputFunctionCall(out_fn, materialization_range.start);
	materialization_end = OidOutputFunctionCall(out_fn, materialization_range.end);

	appendStringInfo(command,
					 "INSERT INTO %s.%s SELECT * FROM %s.%s AS I "
					 "WHERE I.%s >= %s AND I.%s < %s %s;",
					 quote_identifier(NameStr(*materialization_table.schema)),
					 quote_identifier(NameStr(*materialization_table.name)),
					 quote_identifier(NameStr(*partial_view.schema)),
					 quote_identifier(NameStr(*partial_view.name)),
					 quote_identifier(NameStr(*time_column_name)),
					 quote_literal_cstr(materialization_start),
					 quote_identifier(NameStr(*time_column_name)),
					 quote_literal_cstr(materialization_end),
					 chunk_condition);

	res = SPI_execute_with_args(command->data,
								0 /*=nargs*/,
								NULL /*=argtypes*/,
								NULL /*=Values*/,
								NULL /*=Nulls*/,
								false /*=read_only*/,
								0 /*count*/);

	if (res < 0)
		elog(ERROR, "could not materialize values into the materialization table");
}

/*
 * Initialize MatTableColumnInfo
 */
void
mattablecolumninfo_init(MatTableColumnInfo *matcolinfo, List *grouplist)
{
	matcolinfo->matcollist = NIL;
	matcolinfo->partial_seltlist = NIL;
	matcolinfo->partial_grouplist = grouplist;
	matcolinfo->mat_groupcolname_list = NIL;
	matcolinfo->matpartcolno = -1;
	matcolinfo->matpartcolname = NULL;
}

/*
 * Add internal columns for the materialization table
 */
void
mattablecolumninfo_addinternal(MatTableColumnInfo *matcolinfo, RangeTblEntry *usertbl_rte,
							   int32 usertbl_htid)
{
	Index maxRef;
	int colno = list_length(matcolinfo->partial_seltlist) + 1;
	ColumnDef *col;
	Var *chunkfn_arg1;
	FuncExpr *chunk_fnexpr;
	Oid chunkfnoid;
	Oid argtype[] = { OIDOID };
	Oid rettype = INT4OID;
	TargetEntry *chunk_te;
	Oid sortop, eqop;
	bool hashable;
	ListCell *lc;
	SortGroupClause *grpcl;

	/* add a chunk_id column for materialization table */
	Node *vexpr = (Node *) makeVar(1, colno, INT4OID, -1, InvalidOid, 0);
	col = makeColumnDef(CONTINUOUS_AGG_CHUNK_ID_COL_NAME,
						exprType(vexpr),
						exprTypmod(vexpr),
						exprCollation(vexpr));
	matcolinfo->matcollist = lappend(matcolinfo->matcollist, col);

	/* need to add an entry to the target list for computing chunk_id column
	: chunk_for_tuple( htid, table.*)
	*/
	chunkfnoid =
		LookupFuncName(list_make2(makeString(INTERNAL_SCHEMA_NAME), makeString(CHUNKIDFROMRELID)),
					   sizeof(argtype) / sizeof(argtype[0]),
					   argtype,
					   false);
	chunkfn_arg1 = makeVar(1, TableOidAttributeNumber, OIDOID, -1, 0, 0);

	chunk_fnexpr = makeFuncExpr(chunkfnoid,
								rettype,
								list_make1(chunkfn_arg1),
								InvalidOid,
								InvalidOid,
								COERCE_EXPLICIT_CALL);
	chunk_te = makeTargetEntry((Expr *) chunk_fnexpr,
							   colno,
							   pstrdup(CONTINUOUS_AGG_CHUNK_ID_COL_NAME),
							   false);
	matcolinfo->partial_seltlist = lappend(matcolinfo->partial_seltlist, chunk_te);
	/*any internal column needs to be added to the group-by clause as well */
	maxRef = 0;
	foreach (lc, matcolinfo->partial_seltlist)
	{
		Index ref = ((TargetEntry *) lfirst(lc))->ressortgroupref;

		if (ref > maxRef)
			maxRef = ref;
	}
	chunk_te->ressortgroupref =
		maxRef + 1; /* used by sortgroupclause to identify the targetentry */
	grpcl = makeNode(SortGroupClause);
	get_sort_group_operators(exprType((Node *) chunk_te->expr),
							 false,
							 true,
							 false,
							 &sortop,
							 &eqop,
							 NULL,
							 &hashable);
	grpcl->tleSortGroupRef = chunk_te->ressortgroupref;
	grpcl->eqop = eqop;
	grpcl->sortop = sortop;
	grpcl->nulls_first = false;
	grpcl->hashable = hashable;

	matcolinfo->partial_grouplist = lappend(matcolinfo->partial_grouplist, grpcl);
}

/* add additional indexes to materialization table for the columns derived from
 * the group-by column list of the partial select query
 * if partial select query has:
 * GROUP BY timebucket_expr, <grpcol1, grpcol2, grpcol3 ...>
 * index on mattable is <grpcol1, timebucketcol>, <grpcol2, timebucketcol> ... and so on.
 * i.e. #indexes =(  #grp-cols - 1)
 */
static void
mattablecolumninfo_add_mattable_index(MatTableColumnInfo *matcolinfo, Hypertable *ht)
{
	IndexStmt stmt = {
		.type = T_IndexStmt,
		.accessMethod = DEFAULT_INDEX_TYPE,
		.idxname = NULL,
		.relation = makeRangeVar(NameStr(ht->fd.schema_name), NameStr(ht->fd.table_name), 0),
		.tableSpace = get_tablespace_name(get_rel_tablespace(ht->main_table_relid)),
	};
	IndexElem timeelem = { .type = T_IndexElem,
						   .name = matcolinfo->matpartcolname,
						   .ordering = SORTBY_DESC };
	ListCell *le = NULL;
	foreach (le, matcolinfo->mat_groupcolname_list)
	{
		NameData indxname;
		ObjectAddress indxaddr;
		HeapTuple indxtuple;
		char *grpcolname = (char *) lfirst(le);
		IndexElem grpelem = { .type = T_IndexElem, .name = grpcolname };
		stmt.indexParams = list_make2(&grpelem, &timeelem);
		indxaddr = DefineIndex(ht->main_table_relid,
							   &stmt,
							   InvalidOid, /* indexRelationId */
							   InvalidOid, /* parentIndexId */
							   InvalidOid, /* parentConstraintId */
							   false,	  /* is_alter_table */
							   false,	  /* check_rights */
							   false,	  /* check_not_in_use */
							   false,	  /* skip_build */
							   false);	 /* quiet */
		indxtuple = SearchSysCache1(RELOID, ObjectIdGetDatum(indxaddr.objectId));

		if (!HeapTupleIsValid(indxtuple))
			elog(ERROR, "cache lookup failed for index relid %u", indxaddr.objectId);
		indxname = ((Form_pg_class) GETSTRUCT(indxtuple))->relname;
		elog(DEBUG1,
			 "adding index %s ON %s.%s USING BTREE(%s, %s)",
			 NameStr(indxname),
			 NameStr(ht->fd.schema_name),
			 NameStr(ht->fd.table_name),
			 grpcolname,
			 matcolinfo->matpartcolname);
		ReleaseSysCache(indxtuple);
	}
}

/* create hypertable for the table referred by mat_tbloid
 * matpartcolname - partition column for hypertable
 * timecol_interval - is the partitioning column's interval for hypertable partition
 */
static void
cagg_create_hypertable(int32 hypertable_id, Oid mat_tbloid, const char *matpartcolname,
					   int64 mat_tbltimecol_interval)
{
	bool created;
	int flags = 0;
	NameData mat_tbltimecol;
	DimensionInfo *time_dim_info;
	ChunkSizingInfo *chunk_sizing_info;
	namestrcpy(&mat_tbltimecol, matpartcolname);
	time_dim_info = ts_dimension_info_create_open(mat_tbloid,
												  &mat_tbltimecol,
												  Int64GetDatum(mat_tbltimecol_interval),
												  INT8OID,
												  InvalidOid);
	/* Ideally would like to change/expand the API so setting the column name manually is
	 * unnecessary, but not high priority */
	chunk_sizing_info = ts_chunk_sizing_info_get_default_disabled(mat_tbloid);
	chunk_sizing_info->colname = matpartcolname;
	created = ts_hypertable_create_from_info(mat_tbloid,
											 hypertable_id,
											 flags,
											 time_dim_info,
											 NULL,
											 NULL,
											 NULL,
											 chunk_sizing_info,
											 HYPERTABLE_REGULAR,
											 NULL);
	if (!created)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("could not create materialization hypertable")));
}

/*
 * Create the materialization hypertable root by faking up a
 * CREATE TABLE parsetree and passing it to DefineRelation.
 * Reuse the information from ViewStmt:
 *   Remove the options on the into clause that we will not honour
 *   Modify the relname to ts_internal_<name>
 *
 *  Parameters:
 *    mat_rel: relation information for the materialization table
 *    origquery_tblinfo: - user query's tbale information. used for setting up
 *        thr partitioning of the hypertable.
 *    tablespace_name: Name of the tablespace for the materialization table.
 *    table_access_method: Name of the table access method to use for the
 *        materialization table.
 *    mataddress: return the ObjectAddress RETURNS: hypertable id of the
 *        materialization table
 */
int32
mattablecolumninfo_create_materialization_table(MatTableColumnInfo *matcolinfo, int32 hypertable_id,
												RangeVar *mat_rel,
												CAggTimebucketInfo *origquery_tblinfo,
												bool create_addl_index, char *const tablespacename,
												char *const table_access_method,
												ObjectAddress *mataddress)
{
	Oid uid, saved_uid;
	int sec_ctx;
	char *matpartcolname = matcolinfo->matpartcolname;
	CreateStmt *create;
	Datum toast_options;
	int64 matpartcol_interval;
	static char *validnsps[] = HEAP_RELOPT_NAMESPACES;
	int32 mat_htid;
	Oid mat_relid;
	Cache *hcache;
	Hypertable *mat_ht = NULL, *orig_ht = NULL;
	Oid owner = GetUserId();

	create = makeNode(CreateStmt);
	create->relation = mat_rel;
	create->tableElts = matcolinfo->matcollist;
	create->inhRelations = NIL;
	create->ofTypename = NULL;
	create->constraints = NIL;
	create->options = NULL;
	create->oncommit = ONCOMMIT_NOOP;
	create->tablespacename = tablespacename;
	create->accessMethod = table_access_method;
	create->if_not_exists = false;

	/*  Create the materialization table.  */
	SWITCH_TO_TS_USER(mat_rel->schemaname, uid, saved_uid, sec_ctx);
	*mataddress = DefineRelation(create, RELKIND_RELATION, owner, NULL, NULL);
	CommandCounterIncrement();
	mat_relid = mataddress->objectId;

	/* NewRelationCreateToastTable calls CommandCounterIncrement */
	toast_options =
		transformRelOptions((Datum) 0, create->options, "toast", validnsps, true, false);
	(void) heap_reloptions(RELKIND_TOASTVALUE, toast_options, true);
	NewRelationCreateToastTable(mat_relid, toast_options);
	RESTORE_USER(uid, saved_uid, sec_ctx);

	/*convert the mat. table to a hypertable */
	matpartcol_interval = MATPARTCOL_INTERVAL_FACTOR * (origquery_tblinfo->htpartcol_interval_len);
	cagg_create_hypertable(hypertable_id, mat_relid, matpartcolname, matpartcol_interval);

	/* retrieve the hypertable id from the cache */
	mat_ht = ts_hypertable_cache_get_cache_and_entry(mat_relid, CACHE_FLAG_NONE, &hcache);
	mat_htid = mat_ht->fd.id;

	/* create additional index on the group-by columns for the materialization table */
	if (create_addl_index)
		mattablecolumninfo_add_mattable_index(matcolinfo, mat_ht);

	/* Initialize the invalidation log for the cagg. Initially, everything is
	 * invalid. Add an infinite invalidation for the continuous
	 * aggregate. This is the initial state of the aggregate before any
	 * refreshes. */
	orig_ht = ts_hypertable_cache_get_entry(hcache, origquery_tblinfo->htoid, CACHE_FLAG_NONE);
	continuous_agg_invalidate_mat_ht(orig_ht, mat_ht, TS_TIME_NOBEGIN, TS_TIME_NOEND);
	ts_cache_release(hcache);
	return mat_htid;
}

/* Use the userview query to create the partial query to populate
 * the materialization columns and remove HAVING clause and ORDER BY
 */
Query *
mattablecolumninfo_get_partial_select_query(MatTableColumnInfo *mattblinfo, Query *userview_query,
											bool finalized)
{
	Query *partial_selquery;

	CAGG_MAKEQUERY(partial_selquery, userview_query);
	partial_selquery->rtable = copyObject(userview_query->rtable);
	partial_selquery->jointree = copyObject(userview_query->jointree);

	partial_selquery->targetList = mattblinfo->partial_seltlist;
	partial_selquery->groupClause = mattblinfo->partial_grouplist;

	if (finalized)
	{
		partial_selquery->havingQual = copyObject(userview_query->havingQual);
		partial_selquery->sortClause = copyObject(userview_query->sortClause);
	}
	else
	{
		partial_selquery->havingQual = NULL;
		partial_selquery->sortClause = NULL;
	}

	return partial_selquery;
}
