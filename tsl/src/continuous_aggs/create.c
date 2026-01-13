/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

/*
 * This file contains the code for processing continuous aggregate
 * DDL statements which are of the form:
 *
 * CREATE MATERIALIZED VIEW <name> WITH (ts_continuous = [option] )
 * AS  <select query>
 * The entry point for the code is
 * tsl_process_continuous_agg_viewstmt
 * The bulk of the code that creates the underlying tables/views etc. is in
 * cagg_create.
 */
#include <postgres.h>

#include <access/reloptions.h>
#include <access/sysattr.h>
#include <access/xact.h>
#include <access/xlogutils.h>
#include <catalog/index.h>
#include <catalog/indexing.h>
#include <catalog/pg_namespace.h>
#include <catalog/pg_type.h>
#include <catalog/toasting.h>
#include <commands/defrem.h>
#include <commands/tablecmds.h>
#include <commands/tablespace.h>
#include <commands/view.h>
#include <executor/spi.h>
#include <fmgr.h>
#include <miscadmin.h>
#include <nodes/makefuncs.h>
#include <nodes/nodeFuncs.h>
#include <nodes/nodes.h>
#include <nodes/parsenodes.h>
#include <nodes/pg_list.h>
#include <optimizer/clauses.h>
#include <optimizer/optimizer.h>
#include <optimizer/prep.h>
#include <optimizer/tlist.h>
#include <parser/analyze.h>
#include <parser/parse_func.h>
#include <parser/parse_oper.h>
#include <parser/parse_relation.h>
#include <parser/parse_type.h>
#include <parser/parsetree.h>
#include <replication/logical.h>
#include <replication/slot.h>
#include <storage/lwlocknames.h>
#include <utils/acl.h>
#include <utils/builtins.h>
#include <utils/catcache.h>
#include <utils/elog.h>
#include <utils/pg_lsn.h>
#include <utils/rel.h>
#include <utils/resowner.h>
#include <utils/ruleutils.h>
#include <utils/snapshot.h>
#include <utils/syscache.h>
#include <utils/typcache.h>

#include "common.h"
#include "config.h"
#include "create.h"
#include "finalize.h"
#include "invalidation_threshold.h"

#include "debug_assert.h"
#include "dimension.h"
#include "extension_constants.h"
#include "guc.h"
#include "hypertable.h"
#include "hypertable_cache.h"
#include "invalidation.h"
#include "refresh.h"
#include "time_utils.h"
#include "ts_catalog/catalog.h"
#include "ts_catalog/continuous_agg.h"
#include "ts_catalog/continuous_aggs_watermark.h"
#include "with_clause/create_materialized_view_with_clause.h"

static void create_cagg_catalog_entry(int32 matht_id, int32 rawht_id, const char *user_schema,
									  const char *user_view, const char *partial_schema,
									  const char *partial_view, bool materialized_only,
									  const char *direct_schema, const char *direct_view,
									  const int32 parent_mat_hypertable_id);
static void create_bucket_function_catalog_entry(int32 matht_id, Oid bucket_function,
												 const char *bucket_width, const char *origin,
												 const char *offset, const char *timezone,
												 const bool bucket_fixed_width);
static void cagg_create_hypertable(int32 hypertable_id, Oid mat_tbloid, const char *matpartcolname,
								   int64 mat_tbltimecol_interval);
static void mattablecolumninfo_add_mattable_index(MaterializationHypertableColumnInfo *matcolinfo,
												  Hypertable *ht);
static ObjectAddress create_view_for_query(Query *selquery, RangeVar *viewrel);
static void fixup_userview_query_tlist(Query *userquery, List *tlist_aliases);
static void cagg_create(const CreateTableAsStmt *create_stmt, ViewStmt *stmt, Query *panquery,
						ContinuousAggTimeBucketInfo *bucket_info,
						WithClauseResult *with_clause_options);

#define MATPARTCOL_INTERVAL_FACTOR 10

static void
makeMaterializedTableName(char *buf, const char *prefix, int hypertable_id)
{
	int ret = snprintf(buf, NAMEDATALEN, prefix, hypertable_id);
	if (ret < 0 || ret > NAMEDATALEN)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR), errmsg("bad materialization internal name")));
	}
}

/* STATIC functions defined on the structs above. */
static int32 mattablecolumninfo_create_materialization_table(
	MaterializationHypertableColumnInfo *matcolinfo, int32 hypertable_id, RangeVar *mat_rel,
	ContinuousAggTimeBucketInfo *bucket_info, bool create_addl_index, char *tablespacename,
	char *table_access_method, int64 matpartcol_interval, ObjectAddress *mataddress);
static Query *
mattablecolumninfo_get_partial_select_query(MaterializationHypertableColumnInfo *mattblinfo,
											Query *userview_query);

/*
 * Create a entry for the materialization table in table CONTINUOUS_AGGS.
 */
static void
create_cagg_catalog_entry(int32 matht_id, int32 rawht_id, const char *user_schema,
						  const char *user_view, const char *partial_schema,
						  const char *partial_view, bool materialized_only,
						  const char *direct_schema, const char *direct_view,
						  const int32 parent_mat_hypertable_id)
{
	Catalog *catalog = ts_catalog_get();
	Relation rel;
	TupleDesc desc;
	NameData user_schnm, user_viewnm, partial_schnm, partial_viewnm, direct_schnm, direct_viewnm;
	Datum values[Natts_continuous_agg];
	bool nulls[Natts_continuous_agg] = { false };
	CatalogSecurityContext sec_ctx;

	namestrcpy(&user_schnm, user_schema);
	namestrcpy(&user_viewnm, user_view);
	namestrcpy(&partial_schnm, partial_schema);
	namestrcpy(&partial_viewnm, partial_view);
	namestrcpy(&direct_schnm, direct_schema);
	namestrcpy(&direct_viewnm, direct_view);
	rel = table_open(catalog_get_table_id(catalog, CONTINUOUS_AGG), RowExclusiveLock);
	desc = RelationGetDescr(rel);

	memset(values, 0, sizeof(values));
	values[AttrNumberGetAttrOffset(Anum_continuous_agg_mat_hypertable_id)] = matht_id;
	values[AttrNumberGetAttrOffset(Anum_continuous_agg_raw_hypertable_id)] = rawht_id;

	if (parent_mat_hypertable_id == INVALID_HYPERTABLE_ID)
		nulls[AttrNumberGetAttrOffset(Anum_continuous_agg_parent_mat_hypertable_id)] = true;
	else
	{
		values[AttrNumberGetAttrOffset(Anum_continuous_agg_parent_mat_hypertable_id)] =
			parent_mat_hypertable_id;
	}

	values[AttrNumberGetAttrOffset(Anum_continuous_agg_user_view_schema)] =
		NameGetDatum(&user_schnm);
	values[AttrNumberGetAttrOffset(Anum_continuous_agg_user_view_name)] =
		NameGetDatum(&user_viewnm);
	values[AttrNumberGetAttrOffset(Anum_continuous_agg_partial_view_schema)] =
		NameGetDatum(&partial_schnm);
	values[AttrNumberGetAttrOffset(Anum_continuous_agg_partial_view_name)] =
		NameGetDatum(&partial_viewnm);
	values[AttrNumberGetAttrOffset(Anum_continuous_agg_direct_view_schema)] =
		NameGetDatum(&direct_schnm);
	values[AttrNumberGetAttrOffset(Anum_continuous_agg_direct_view_name)] =
		NameGetDatum(&direct_viewnm);
	values[AttrNumberGetAttrOffset(Anum_continuous_agg_materialize_only)] =
		BoolGetDatum(materialized_only);

	ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);
	ts_catalog_insert_values(rel, desc, values, nulls);
	ts_catalog_restore_user(&sec_ctx);
	table_close(rel, RowExclusiveLock);
}

/*
 * Create a entry for the materialization table in table
 * CONTINUOUS_AGGS_BUCKET_FUNCTION.
 */
static void
create_bucket_function_catalog_entry(int32 matht_id, Oid bucket_function, const char *bucket_width,
									 const char *bucket_origin, const char *bucket_offset,
									 const char *bucket_timezone, const bool bucket_fixed_width)
{
	Catalog *catalog = ts_catalog_get();
	Relation rel;
	TupleDesc desc;
	Datum values[Natts_continuous_aggs_bucket_function];
	bool nulls[Natts_continuous_aggs_bucket_function] = { false };
	CatalogSecurityContext sec_ctx;

	Assert(OidIsValid(bucket_function));
	Assert(bucket_width != NULL);

	rel = table_open(catalog_get_table_id(catalog, CONTINUOUS_AGGS_BUCKET_FUNCTION),
					 RowExclusiveLock);
	desc = RelationGetDescr(rel);

	memset(values, 0, sizeof(values));

	/* Hypertable ID */
	values[AttrNumberGetAttrOffset(Anum_continuous_aggs_bucket_function_mat_hypertable_id)] =
		matht_id;

	/* Bucket function */
	values[AttrNumberGetAttrOffset(Anum_continuous_aggs_bucket_function_function)] =
		CStringGetTextDatum(format_procedure_qualified(bucket_function));

	/* Bucket width */
	values[AttrNumberGetAttrOffset(Anum_continuous_aggs_bucket_function_bucket_width)] =
		CStringGetTextDatum(bucket_width);

	/* Bucket origin */
	if (bucket_origin != NULL)
	{
		values[AttrNumberGetAttrOffset(Anum_continuous_aggs_bucket_function_bucket_origin)] =
			CStringGetTextDatum(bucket_origin);
	}
	else
	{
		nulls[AttrNumberGetAttrOffset(Anum_continuous_aggs_bucket_function_bucket_origin)] = true;
	}

	/* Bucket offset */
	if (bucket_offset != NULL)
	{
		values[AttrNumberGetAttrOffset(Anum_continuous_aggs_bucket_function_bucket_offset)] =
			CStringGetTextDatum(bucket_offset);
	}
	else
	{
		nulls[AttrNumberGetAttrOffset(Anum_continuous_aggs_bucket_function_bucket_offset)] = true;
	}

	/* Bucket timezone */
	if (bucket_timezone != NULL)
	{
		values[AttrNumberGetAttrOffset(Anum_continuous_aggs_bucket_function_bucket_timezone)] =
			CStringGetTextDatum(bucket_timezone);
	}
	else
	{
		nulls[AttrNumberGetAttrOffset(Anum_continuous_aggs_bucket_function_bucket_timezone)] = true;
	}

	/* Bucket fixed width */
	values[AttrNumberGetAttrOffset(Anum_continuous_aggs_bucket_function_bucket_fixed_width)] =
		BoolGetDatum(bucket_fixed_width);

	ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);
	ts_catalog_insert_values(rel, desc, values, nulls);
	ts_catalog_restore_user(&sec_ctx);
	table_close(rel, RowExclusiveLock);
}

/*
 * Create hypertable for the table referred by mat_tbloid
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
												  InvalidOid,
												  (Datum) 0,  /* origin */
												  InvalidOid, /* origin_type */
												  false);	  /* has_origin */
	/*
	 * Ideally would like to change/expand the API so setting the column name manually is
	 * unnecessary, but not high priority.
	 */
	chunk_sizing_info = ts_chunk_sizing_info_get_default_disabled(mat_tbloid);
	chunk_sizing_info->colname = matpartcolname;
	created = ts_hypertable_create_from_info(mat_tbloid,
											 hypertable_id,
											 flags,
											 time_dim_info,
											 NULL,
											 NULL,
											 NULL,
											 chunk_sizing_info);
	if (!created)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("could not create materialization hypertable")));
}

/*
 * Add additional indexes to materialization table for the columns derived from
 * the group-by column list of the partial select query.
 * If partial select query has:
 * GROUP BY timebucket_expr, <grpcol1, grpcol2, grpcol3 ...>
 * index on mattable is <grpcol1, timebucketcol>, <grpcol2, timebucketcol> ... and so on.
 * i.e. #indexes =(  #grp-cols - 1)
 */
static void
mattablecolumninfo_add_mattable_index(MaterializationHypertableColumnInfo *matcolinfo,
									  Hypertable *ht)
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
		indxaddr = DefineIndexCompat(ht->main_table_relid,
									 &stmt,
									 InvalidOid, /* indexRelationId */
									 InvalidOid, /* parentIndexId */
									 InvalidOid, /* parentConstraintId */
									 -1,		 /* total_parts */
									 false,		 /* is_alter_table */
									 false,		 /* check_rights */
									 false,		 /* check_not_in_use */
									 false,		 /* skip_build */
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

/*
 * Create the materialization hypertable root by faking up a
 * CREATE TABLE parsetree and passing it to DefineRelation.
 * Reuse the information from ViewStmt:
 *   Remove the options on the into clause that we will not honour
 *   Modify the relname to ts_internal_<name>
 *
 *  Parameters:
 *    mat_rel: relation information for the materialization table
 *    bucket_info: bucket information used for setting up the
 *                 hypertable partitioning (`chunk_interval_size`).
 *    tablespace_name: Name of the tablespace for the materialization table.
 *    table_access_method: Name of the table access method to use for the
 *        materialization table.
 *    mataddress: return the ObjectAddress RETURNS: hypertable id of the
 *        materialization table
 */
static int32
mattablecolumninfo_create_materialization_table(
	MaterializationHypertableColumnInfo *matcolinfo, int32 hypertable_id, RangeVar *mat_rel,
	ContinuousAggTimeBucketInfo *bucket_info, bool create_addl_index, char *const tablespacename,
	char *const table_access_method, int64 matpartcol_interval, ObjectAddress *mataddress)
{
	Oid uid, saved_uid;
	int sec_ctx;
	char *matpartcolname = matcolinfo->matpartcolname;
	CreateStmt *create;
	Datum toast_options;
#if PG18_LT
	char *validnsps[] = HEAP_RELOPT_NAMESPACES;
#else
	const char *const validnsps[] = HEAP_RELOPT_NAMESPACES;
#endif
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

	/* NewRelationCreateToastTable calls CommandCounterIncrement. */
	toast_options =
		transformRelOptions(UnassignedDatum, create->options, "toast", validnsps, true, false);
	(void) heap_reloptions(RELKIND_TOASTVALUE, toast_options, true);
	NewRelationCreateToastTable(mat_relid, toast_options);
	RESTORE_USER(uid, saved_uid, sec_ctx);

	cagg_create_hypertable(hypertable_id, mat_relid, matpartcolname, matpartcol_interval);

	/* Retrieve the hypertable id from the cache. */
	mat_ht = ts_hypertable_cache_get_cache_and_entry(mat_relid, CACHE_FLAG_NONE, &hcache);
	mat_htid = mat_ht->fd.id;

	/* Create additional index on the group-by columns for the materialization table. */
	if (create_addl_index)
		mattablecolumninfo_add_mattable_index(matcolinfo, mat_ht);

	/*
	 * Initialize the invalidation log for the cagg. Initially, everything is
	 * invalid. Add an infinite invalidation for the continuous
	 * aggregate. This is the initial state of the aggregate before any
	 * refreshes.
	 */
	orig_ht = ts_hypertable_cache_get_entry(hcache, bucket_info->htoid, CACHE_FLAG_NONE);
	continuous_agg_invalidate_mat_ht(orig_ht, mat_ht, TS_TIME_NOBEGIN, TS_TIME_NOEND);
	ts_cache_release(&hcache);
	return mat_htid;
}

/*
 * Use the userview query to create the partial query to populate
 * the materialization columns and remove HAVING clause and ORDER BY.
 */
static Query *
mattablecolumninfo_get_partial_select_query(MaterializationHypertableColumnInfo *mattblinfo,
											Query *userview_query)
{
	Query *partial_selquery = NULL;

	partial_selquery = copyObject(userview_query);
	/* Partial view should always include the time dimension column */
	partial_selquery->targetList = mattblinfo->partial_seltlist;
	partial_selquery->groupClause = mattblinfo->partial_grouplist;

	return partial_selquery;
}

/*
 * Create a view for the query using the SELECt stmt sqlquery
 * and view name from RangeVar viewrel.
 */
static ObjectAddress
create_view_for_query(Query *selquery, RangeVar *viewrel)
{
	Oid uid, saved_uid;
	int sec_ctx;
	ObjectAddress address;
	CreateStmt *create;
	List *selcollist = NIL;
	Oid owner = GetUserId();
	ListCell *lc;
	foreach (lc, selquery->targetList)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(lc);
		if (!tle->resjunk)
		{
			ColumnDef *col = makeColumnDef(tle->resname,
										   exprType((Node *) tle->expr),
										   exprTypmod((Node *) tle->expr),
										   exprCollation((Node *) tle->expr));
			selcollist = lappend(selcollist, col);
		}
	}

	create = makeNode(CreateStmt);
	create->relation = viewrel;
	create->tableElts = selcollist;
	create->inhRelations = NIL;
	create->ofTypename = NULL;
	create->constraints = NIL;
	create->options = NULL;
	create->oncommit = ONCOMMIT_NOOP;
	create->tablespacename = NULL;
	create->if_not_exists = false;

	/*
	 * Create the view. Viewname is in viewrel.
	 */
	SWITCH_TO_TS_USER(viewrel->schemaname, uid, saved_uid, sec_ctx);
	address = DefineRelation(create, RELKIND_VIEW, owner, NULL, NULL);
	CommandCounterIncrement();
	StoreViewQuery(address.objectId, selquery, false);
	CommandCounterIncrement();
	RESTORE_USER(uid, saved_uid, sec_ctx);
	return address;
}

/*
 * Assign aliases to the targetlist in the query according to the
 * column_names provided in the CREATE VIEW statement.
 */
static void
fixup_userview_query_tlist(Query *userquery, List *tlist_aliases)
{
	if (tlist_aliases != NIL)
	{
		ListCell *lc;
		ListCell *alist_item = list_head(tlist_aliases);
		foreach (lc, userquery->targetList)
		{
			TargetEntry *tle = (TargetEntry *) lfirst(lc);

			/* Junk columns don't get aliases. */
			if (tle->resjunk)
				continue;
			tle->resname = pstrdup(strVal(lfirst(alist_item)));
			alist_item = lnext(tlist_aliases, alist_item);
			if (alist_item == NULL)
				break; /* done assigning aliases */
		}

		if (alist_item != NULL)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR), errmsg("too many column names specified")));
	}
}

/*
 * Modifies the passed in ViewStmt to do the following
 * a) Create a hypertable for the continuous agg materialization.
 * b) create a view that references the underlying
 * materialization table instead of the original table used in
 * the CREATE VIEW stmt.
 * Example:
 * CREATE VIEW mcagg ...
 * AS  select a, min(b)+max(d) from foo group by a,timebucket(a);
 *
 * Step 1. create a materialiation table which stores the partials for the
 * aggregates and the grouping columns + internal columns.
 * So we have a table like _materialization_hypertable
 * with columns:
 *( a, col1, col2, col3, internal-columns)
 * where col1 =  partialize(min(b)), col2= partialize(max(d)),
 * col3= timebucket(a))
 *
 * Step 2: Create a view with modified select query
 * CREATE VIEW mcagg
 * as
 * select a, finalize( col1) + finalize(col2))
 * from _materialization_hypertable
 * group by a, col3
 *
 * Step 3: Create a view to populate the materialization table
 * create view ts_internal_mcagg_view
 * as
 * select a, partialize(min(b)), partialize(max(d)), timebucket(a), <internal-columns>
 * from foo
 * group by <internal-columns> , a , timebucket(a);
 *
 * Notes: ViewStmt->query is the raw parse tree
 * panquery is the output of running parse_anlayze( ViewStmt->query)

 * Since 1.7, we support real time aggregation.
 * If real time aggregation is off i.e. materialized only, the mcagg view is as described in Step 2.
 * If it is turned on
 * we build a union query that selects from the internal mat view and the raw hypertable
 *     (see build_union_query for details)
 * CREATE VIEW mcagg
 * as
 * SELECT * from
 *        ( SELECT a, finalize(col1) + finalize(col2) from ts_internal_mcagg_view
 *                 ---> query from Step 2 with additional where clause
 *          WHERE timecol < materialization threshold
 *          group by <internal-columns> , a , timebucket(a);
 *          UNION ALL
 *          SELECT a, min(b)+max(d) from foo ---> original view stmt
 *                              ----> with additional where clause
 *          WHERE timecol >= materialization threshold
 *          GROUP BY a, time_bucket(a)
 *        )
 */
static void
cagg_create(const CreateTableAsStmt *create_stmt, ViewStmt *stmt, Query *panquery,
			ContinuousAggTimeBucketInfo *bucket_info, WithClauseResult *with_clause_options)
{
	ObjectAddress mataddress;
	char relnamebuf[NAMEDATALEN];
	MaterializationHypertableColumnInfo mattblinfo;
	FinalizeQueryInfo finalqinfo;
	CatalogSecurityContext sec_ctx;
	bool is_create_mattbl_index;

	Query *final_selquery;
	Query *partial_selquery;	/* query to populate the mattable*/
	Query *orig_userview_query; /* copy of the original user query for dummy view */
	Oid nspid;
	RangeVar *part_rel = NULL, *mat_rel = NULL, *dum_rel = NULL;
	int32 materialize_hypertable_id;
	bool materialized_only =
		DatumGetBool(with_clause_options[CreateMaterializedViewFlagMaterializedOnly].parsed);

	int64 matpartcol_interval = 0;
	if (!with_clause_options[CreateMaterializedViewFlagChunkTimeInterval].is_default)
	{
		matpartcol_interval = interval_to_usec(DatumGetIntervalP(
			with_clause_options[CreateMaterializedViewFlagChunkTimeInterval].parsed));
	}
	else
	{
		matpartcol_interval = bucket_info->htpartcol_interval_len;

		/* Apply the factor just for non-Hierachical CAggs */
		if (bucket_info->parent_mat_hypertable_id == INVALID_HYPERTABLE_ID)
			matpartcol_interval *= MATPARTCOL_INTERVAL_FACTOR;
	}

	/*
	 * Assign the column_name aliases in CREATE VIEW to the query.
	 * No other modifications to panquery.
	 */
	fixup_userview_query_tlist(panquery, stmt->aliases);
	mattablecolumninfo_init(&mattblinfo, copyObject(panquery->groupClause));
	finalizequery_init(&finalqinfo, panquery, &mattblinfo);

	/*
	 * Invalidate all options on the stmt before using it
	 * The options are valid only for internal use (ts_continuous).
	 */
	stmt->options = NULL;

	/*
	 * Old format caggs are not supported anymore, there is no need to add
	 * an internal chunk id column for materialized hypertable.
	 *
	 * Step 1: create the materialization table.
	 */
	ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);
	materialize_hypertable_id = ts_catalog_table_next_seq_id(ts_catalog_get(), HYPERTABLE);
	ts_catalog_restore_user(&sec_ctx);
	makeMaterializedTableName(relnamebuf, "_materialized_hypertable_%d", materialize_hypertable_id);
	mat_rel = makeRangeVar(pstrdup(INTERNAL_SCHEMA_NAME), pstrdup(relnamebuf), -1);
	is_create_mattbl_index =
		DatumGetBool(with_clause_options[CreateMaterializedViewFlagCreateGroupIndexes].parsed);
	mattablecolumninfo_create_materialization_table(&mattblinfo,
													materialize_hypertable_id,
													mat_rel,
													bucket_info,
													is_create_mattbl_index,
													create_stmt->into->tableSpaceName,
													create_stmt->into->accessMethod,
													matpartcol_interval,
													&mataddress);

	/*
	 * Step 2: Create view with select finalize from materialization table.
	 */
	final_selquery = finalizequery_get_select_query(&finalqinfo,
													mattblinfo.matcollist,
													&mataddress,
													mat_rel->relname);

	if (!materialized_only)
		final_selquery = build_union_query(bucket_info,
										   mattblinfo.matpartcolno,
										   final_selquery,
										   panquery,
										   materialize_hypertable_id);

	/* Copy view acl to materialization hypertable. */
	ObjectAddress view_address = create_view_for_query(final_selquery, stmt->view);
	ts_copy_relation_acl(view_address.objectId, mataddress.objectId, GetUserId());

	/*
	 * Step 3: create the internal view with select partialize(..).
	 */
	partial_selquery = mattablecolumninfo_get_partial_select_query(&mattblinfo, panquery);

	makeMaterializedTableName(relnamebuf, "_partial_view_%d", materialize_hypertable_id);
	part_rel = makeRangeVar(pstrdup(INTERNAL_SCHEMA_NAME), pstrdup(relnamebuf), -1);
	create_view_for_query(partial_selquery, part_rel);

	/*
	 * Additional miscellaneous steps.
	 */

	/*
	 * Create a dummy view to store the user supplied view query.
	 * This is to get PG to display the view correctly without
	 * having to replicate the PG source code for make_viewdef.
	 */
	orig_userview_query = copyObject(panquery);
	makeMaterializedTableName(relnamebuf, "_direct_view_%d", materialize_hypertable_id);
	dum_rel = makeRangeVar(pstrdup(INTERNAL_SCHEMA_NAME), pstrdup(relnamebuf), -1);
	create_view_for_query(orig_userview_query, dum_rel);

	/* Step 4: Add catalog table entry for the objects we just created. */
	nspid = RangeVarGetCreationNamespace(stmt->view);

	create_cagg_catalog_entry(materialize_hypertable_id,
							  bucket_info->htid,
							  get_namespace_name(nspid), /*schema name for user view */
							  stmt->view->relname,
							  part_rel->schemaname,
							  part_rel->relname,
							  materialized_only,
							  dum_rel->schemaname,
							  dum_rel->relname,
							  bucket_info->parent_mat_hypertable_id);

	char *bucket_origin = NULL;
	char *bucket_offset = NULL;
	char *bucket_width = NULL;

	if (IS_TIME_BUCKET_INFO_TIME_BASED(bucket_info->bf))
	{
		/* Bucketing on time */
		Assert(bucket_info->bf->bucket_time_width != NULL);
		bucket_width = DatumGetCString(
			DirectFunctionCall1(interval_out,
								IntervalPGetDatum(bucket_info->bf->bucket_time_width)));

		if (!TIMESTAMP_NOT_FINITE(bucket_info->bf->bucket_time_origin))
		{
			bucket_origin = DatumGetCString(
				DirectFunctionCall1(timestamptz_out,
									TimestampTzGetDatum(bucket_info->bf->bucket_time_origin)));
		}

		if (bucket_info->bf->bucket_time_offset != NULL)
		{
			bucket_offset = DatumGetCString(
				DirectFunctionCall1(interval_out,
									IntervalPGetDatum(bucket_info->bf->bucket_time_offset)));
		}
	}
	else
	{
		/* Bucketing on integers */
		bucket_width = palloc0(MAXINT8LEN + 1);
		pg_lltoa(bucket_info->bf->bucket_integer_width, bucket_width);

		/* Integer buckets with origin are not supported, so noting to do. */
		Assert(bucket_origin == NULL);

		if (bucket_info->bf->bucket_integer_offset != 0)
		{
			bucket_offset = palloc0(MAXINT8LEN + 1);
			pg_lltoa(bucket_info->bf->bucket_integer_offset, bucket_offset);
		}
	}

	create_bucket_function_catalog_entry(materialize_hypertable_id,
										 bucket_info->bf->bucket_function,
										 bucket_width,
										 bucket_origin,
										 bucket_offset,
										 bucket_info->bf->bucket_time_timezone,
										 bucket_info->bf->bucket_fixed_interval);
}

DDLResult
tsl_process_continuous_agg_viewstmt(Node *node, const char *query_string, void *pstmt,
									WithClauseResult *with_clause_options)
{
	const CreateTableAsStmt *stmt = castNode(CreateTableAsStmt, node);
	ContinuousAggTimeBucketInfo timebucket_exprinfo;
	Oid nspid;
	ViewStmt viewstmt = {
		.type = T_ViewStmt,
		.view = stmt->into->rel,
		.query = (Node *) stmt->into->viewQuery,
		.options = stmt->into->options,
		.aliases = stmt->into->colNames,
	};
	ContinuousAgg *cagg;
	Hypertable *mat_ht;
	Oid relid;
	char *schema_name;

	ts_feature_flag_check(FEATURE_CAGG);

	nspid = RangeVarGetCreationNamespace(stmt->into->rel);
	relid = get_relname_relid(stmt->into->rel->relname, nspid);

	if (OidIsValid(relid))
	{
		if (stmt->if_not_exists)
		{
			ereport(NOTICE,
					(errcode(ERRCODE_DUPLICATE_TABLE),
					 errmsg("continuous aggregate \"%s\" already exists, skipping",
							stmt->into->rel->relname)));
			return DDL_DONE;
		}
		else
		{
			ereport(ERROR,
					(errcode(ERRCODE_DUPLICATE_TABLE),
					 errmsg("continuous aggregate \"%s\" already exists", stmt->into->rel->relname),
					 errhint("Drop or rename the existing continuous aggregate first or use "
							 "another name.")));
		}
	}

	if (!with_clause_options[CreateMaterializedViewFlagColumnstore].is_default)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot enable compression while creating a continuous aggregate"),
				 errhint("Use ALTER MATERIALIZED VIEW to enable compression.")));
	}

	schema_name = get_namespace_name(nspid);
	timebucket_exprinfo = cagg_validate_query((Query *) stmt->into->viewQuery,
											  schema_name,
											  stmt->into->rel->relname,
											  true);
	cagg_create(stmt, &viewstmt, (Query *) stmt->query, &timebucket_exprinfo, with_clause_options);

	/* Insert the MIN of the time dimension type for the new watermark */
	CommandCounterIncrement();

	relid = get_relname_relid(stmt->into->rel->relname, nspid);
	Ensure(OidIsValid(relid),
		   "relation \"%s\".\"%s\" not found",
		   schema_name,
		   stmt->into->rel->relname);

	cagg = ts_continuous_agg_find_by_relid(relid);
	Ensure(NULL != cagg,
		   "continuous aggregate \"%s\".\"%s\" not found",
		   schema_name,
		   stmt->into->rel->relname);

	mat_ht = ts_hypertable_get_by_id(cagg->data.mat_hypertable_id);
	Ensure(NULL != mat_ht, "materialization hypertable %d not found", cagg->data.mat_hypertable_id);
	ts_cagg_watermark_insert(mat_ht, 0, true);

	invalidation_threshold_initialize(cagg);

	if (!stmt->into->skipData)
	{
		InternalTimeRange refresh_window = {
			.type = InvalidOid,
		};

		/*
		 * We are creating a refresh window here in a similar way to how it's
		 * done in continuous_agg_refresh. We do not call the PG function
		 * directly since we want to be able to suppress the output in that
		 * function and adding a 'verbose' parameter to is not useful for a
		 * user.
		 */
		refresh_window.type = cagg->partition_type;

		/*
		 * To determine inscribed/circumscribed refresh window for variable-sized
		 * buckets we should be able to calculate time_bucket(window.begin) and
		 * time_bucket(window.end). This, however, is not possible in general case.
		 * As an example, the minimum date is 4714-11-24 BC, which is before any
		 * reasonable default `origin` value. Thus for variable-sized buckets
		 * instead of minimum date we use -infinity since time_bucket(-infinity)
		 * is well-defined as -infinity.
		 *
		 * For more details see:
		 * - ts_compute_inscribed_bucketed_refresh_window_variable()
		 * - ts_compute_circumscribed_bucketed_refresh_window_variable()
		 */
		refresh_window.start = cagg_get_time_min(cagg);
		refresh_window.end = ts_time_get_noend_or_max(refresh_window.type);

		ContinuousAggRefreshContext context = { .callctx = CAGG_REFRESH_CREATION };
		continuous_agg_refresh_internal(cagg,
										&refresh_window,
										context,
										true,  /* start_isnull */
										true,  /* end_isnull */
										true,  /* bucketing_refresh_window */
										false, /* force */
										true,  /* process_hypertable_invalidations */
										false /*extend_last_bucket*/);
	}

	return DDL_DONE;
}

/*
 * Flip the view definition of an existing continuous aggregate from
 * real-time to materialized-only or vice versa depending on the current state.
 */
void
cagg_flip_realtime_view_definition(ContinuousAgg *agg, Hypertable *mat_ht)
{
	int sec_ctx;
	Oid uid, saved_uid;
	Query *result_view_query;

	/* User view query of the user defined CAGG. */
	Oid user_view_oid = ts_get_relation_relid(NameStr(agg->data.user_view_schema),
											  NameStr(agg->data.user_view_name),
											  false);
	Relation user_view_rel = relation_open(user_view_oid, AccessShareLock);
	Query *user_query = copyObject(get_view_query(user_view_rel));
	/* Keep lock until end of transaction. */
	relation_close(user_view_rel, NoLock);
	RemoveRangeTableEntries(user_query);

	/* Direct view query of the original user view definition at CAGG creation. */
	Oid direct_view_oid = ts_get_relation_relid(NameStr(agg->data.direct_view_schema),
												NameStr(agg->data.direct_view_name),
												false);
	Relation direct_view_rel = relation_open(direct_view_oid, AccessShareLock);
	Query *direct_query = copyObject(get_view_query(direct_view_rel));
	/* Keep lock until end of transaction. */
	relation_close(direct_view_rel, NoLock);
	RemoveRangeTableEntries(direct_query);

	ContinuousAggTimeBucketInfo timebucket_exprinfo =
		cagg_validate_query(direct_query,
							NameStr(agg->data.user_view_schema),
							NameStr(agg->data.user_view_name),
							false);

	/* Flip */
	agg->data.materialized_only = !agg->data.materialized_only;
	if (agg->data.materialized_only)
	{
		result_view_query = destroy_union_query(user_query);
	}
	else
	{
		/* Get primary partitioning column information of time bucketing. */
		const Dimension *mat_part_dimension = hyperspace_get_open_dimension(mat_ht->space, 0);
		result_view_query = build_union_query(&timebucket_exprinfo,
											  mat_part_dimension->column_attno,
											  user_query,
											  direct_query,
											  mat_ht->fd.id);
	}
	SWITCH_TO_TS_USER(NameStr(agg->data.user_view_schema), uid, saved_uid, sec_ctx);
	StoreViewQuery(user_view_oid, result_view_query, true);
	CommandCounterIncrement();
	RESTORE_USER(uid, saved_uid, sec_ctx);
}

void
cagg_rename_view_columns(ContinuousAgg *agg)
{
	ListCell *lc;
	int sec_ctx;
	Oid uid, saved_uid;

	/* User view query of the user defined CAGG. */
	Oid user_view_oid = ts_get_relation_relid(NameStr(agg->data.user_view_schema),
											  NameStr(agg->data.user_view_name),
											  false);
	Relation user_view_rel = relation_open(user_view_oid, AccessShareLock);
	Query *user_query = copyObject(get_view_query(user_view_rel));
	RemoveRangeTableEntries(user_query);

	/*
	 * When calling StoreViewQuery the target list names of the query have to
	 * match the view's tuple descriptor attribute names. But if a column of the
	 * continuous aggregate has been renamed, the query tree will not have the correct
	 * names in the target list, which will error out when calling StoreViewQuery.
	 * For that reason, we fetch the name from the user view relation and update the
	 * resource name in the query target list to match the name in the user view.
	 */
	TupleDesc desc = RelationGetDescr(user_view_rel);
	int i = 0;
	foreach (lc, user_query->targetList)
	{
		TargetEntry *user_tle;
		FormData_pg_attribute *attr = TupleDescAttr(desc, i);
		user_tle = lfirst_node(TargetEntry, lc);
		if (user_tle->resjunk)
			break;

		user_tle->resname = NameStr(attr->attname);
		++i;
	}

	SWITCH_TO_TS_USER(NameStr(agg->data.user_view_schema), uid, saved_uid, sec_ctx);
	StoreViewQuery(user_view_oid, user_query, true);
	CommandCounterIncrement();
	RESTORE_USER(uid, saved_uid, sec_ctx);

	/*
	 * Keep locks until end of transaction and do not close the relation
	 * before the call to StoreViewQuery since it can otherwise release the
	 * memory for attr->attname, causing a segfault.
	 */
	relation_close(user_view_rel, NoLock);
}
