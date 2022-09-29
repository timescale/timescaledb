/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

/* This file contains the code for processing continuous aggregate
 * DDL statements which are of the form:
 *
 * CREATE MATERIALIZED VIEW <name> WITH (ts_continuous = [option] )
 * AS  <select query>
 * The entry point for the code is
 * tsl_process_continuous_agg_viewstmt
 * The bulk of the code that creates the underlying tables/views etc. is in
 * cagg_create.
 *
 */
#include <postgres.h>
#include <access/reloptions.h>
#include <access/sysattr.h>
#include <access/xact.h>
#include <catalog/index.h>
#include <catalog/indexing.h>
#include <catalog/pg_namespace.h>
#include <catalog/pg_trigger.h>
#include <catalog/pg_type.h>
#include <catalog/toasting.h>
#include <commands/defrem.h>
#include <commands/tablecmds.h>
#include <commands/tablespace.h>
#include <commands/trigger.h>
#include <commands/view.h>
#include <nodes/makefuncs.h>
#include <nodes/nodeFuncs.h>
#include <nodes/nodes.h>
#include <nodes/parsenodes.h>
#include <nodes/pg_list.h>
#include <optimizer/clauses.h>
#include <optimizer/optimizer.h>
#include <optimizer/tlist.h>
#include <parser/analyze.h>
#include <parser/parse_func.h>
#include <parser/parse_oper.h>
#include <parser/parse_relation.h>
#include <parser/parse_type.h>
#include <utils/acl.h>
#include <utils/rel.h>
#include <utils/builtins.h>
#include <utils/catcache.h>
#include <utils/ruleutils.h>
#include <utils/syscache.h>
#include <utils/typcache.h>

#include "finalize.h"
#include "common.h"
#include "create.h"

#include "dimension.h"
#include "extension_constants.h"
#include "func_cache.h"
#include "hypertable_cache.h"
#include "hypertable.h"
#include "invalidation.h"
#include "dimension.h"
#include "ts_catalog/continuous_agg.h"
#include "options.h"
#include "time_utils.h"
#include "utils.h"
#include "errors.h"
#include "refresh.h"
#include "remote/dist_commands.h"
#include "ts_catalog/hypertable_data_node.h"
#include "deparse.h"
#include "timezones.h"

#define PRINT_MATINTERNAL_NAME(buf, prefix, hypertable_id)                                         \
	do                                                                                             \
	{                                                                                              \
		int ret = snprintf(buf, NAMEDATALEN, prefix, hypertable_id);                               \
		if (ret < 0 || ret > NAMEDATALEN)                                                          \
		{                                                                                          \
			ereport(ERROR,                                                                         \
					(errcode(ERRCODE_INTERNAL_ERROR),                                              \
					 errmsg("bad materialization internal name")));                                \
		}                                                                                          \
	} while (0);

/* create a entry for the materialization table in table CONTINUOUS_AGGS */
static void
create_cagg_catalog_entry(int32 matht_id, int32 rawht_id, const char *user_schema,
						  const char *user_view, const char *partial_schema,
						  const char *partial_view, int64 bucket_width, bool materialized_only,
						  const char *direct_schema, const char *direct_view, const bool finalized)
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
	values[AttrNumberGetAttrOffset(Anum_continuous_agg_user_view_schema)] =
		NameGetDatum(&user_schnm);
	values[AttrNumberGetAttrOffset(Anum_continuous_agg_user_view_name)] =
		NameGetDatum(&user_viewnm);
	values[AttrNumberGetAttrOffset(Anum_continuous_agg_partial_view_schema)] =
		NameGetDatum(&partial_schnm);
	values[AttrNumberGetAttrOffset(Anum_continuous_agg_partial_view_name)] =
		NameGetDatum(&partial_viewnm);
	values[AttrNumberGetAttrOffset(Anum_continuous_agg_bucket_width)] = Int64GetDatum(bucket_width);
	values[AttrNumberGetAttrOffset(Anum_continuous_agg_direct_view_schema)] =
		NameGetDatum(&direct_schnm);
	values[AttrNumberGetAttrOffset(Anum_continuous_agg_direct_view_name)] =
		NameGetDatum(&direct_viewnm);
	values[AttrNumberGetAttrOffset(Anum_continuous_agg_materialize_only)] =
		BoolGetDatum(materialized_only);
	values[AttrNumberGetAttrOffset(Anum_continuous_agg_finalized)] = BoolGetDatum(finalized);

	ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);
	ts_catalog_insert_values(rel, desc, values, nulls);
	ts_catalog_restore_user(&sec_ctx);
	table_close(rel, RowExclusiveLock);
}

/* create a entry for the materialization table in table CONTINUOUS_AGGS_BUCKET_FUNCTION */
static void
create_bucket_function_catalog_entry(int32 matht_id, bool experimental, const char *name,
									 const char *bucket_width, const char *origin,
									 const char *timezone)
{
	Catalog *catalog = ts_catalog_get();
	Relation rel;
	TupleDesc desc;
	Datum values[Natts_continuous_aggs_bucket_function];
	bool nulls[Natts_continuous_aggs_bucket_function] = { false };
	CatalogSecurityContext sec_ctx;

	rel = table_open(catalog_get_table_id(catalog, CONTINUOUS_AGGS_BUCKET_FUNCTION),
					 RowExclusiveLock);
	desc = RelationGetDescr(rel);

	memset(values, 0, sizeof(values));
	values[AttrNumberGetAttrOffset(Anum_continuous_agg_mat_hypertable_id)] = matht_id;
	values[AttrNumberGetAttrOffset(Anum_continuous_aggs_bucket_function_experimental)] =
		BoolGetDatum(experimental);
	values[AttrNumberGetAttrOffset(Anum_continuous_aggs_bucket_function_name)] =
		CStringGetTextDatum(name);
	values[AttrNumberGetAttrOffset(Anum_continuous_aggs_bucket_function_bucket_width)] =
		CStringGetTextDatum(bucket_width);
	values[AttrNumberGetAttrOffset(Anum_continuous_aggs_bucket_function_origin)] =
		CStringGetTextDatum(origin);
	values[AttrNumberGetAttrOffset(Anum_continuous_aggs_bucket_function_timezone)] =
		CStringGetTextDatum(timezone ? timezone : "");

	ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);
	ts_catalog_insert_values(rel, desc, values, nulls);
	ts_catalog_restore_user(&sec_ctx);
	table_close(rel, RowExclusiveLock);
}

static bool
check_trigger_exists_hypertable(Oid relid, char *trigname)
{
	Relation tgrel;
	ScanKeyData skey[1];
	SysScanDesc tgscan;
	HeapTuple tuple;
	bool trg_found = false;

	tgrel = table_open(TriggerRelationId, AccessShareLock);
	ScanKeyInit(&skey[0],
				Anum_pg_trigger_tgrelid,
				BTEqualStrategyNumber,
				F_OIDEQ,
				ObjectIdGetDatum(relid));

	tgscan = systable_beginscan(tgrel, TriggerRelidNameIndexId, true, NULL, 1, skey);

	while (HeapTupleIsValid(tuple = systable_getnext(tgscan)))
	{
		Form_pg_trigger trig = (Form_pg_trigger) GETSTRUCT(tuple);
		if (namestrcmp(&(trig->tgname), trigname) == 0)
		{
			trg_found = true;
			break;
		}
	}
	systable_endscan(tgscan);
	table_close(tgrel, AccessShareLock);
	return trg_found;
}

/* add continuous agg invalidation trigger to hypertable
 * relid - oid of hypertable
 * hypertableid - argument to pass to trigger (the hypertable id from timescaledb catalog)
 */
static void
cagg_add_trigger_hypertable(Oid relid, int32 hypertable_id)
{
	char hypertable_id_str[12];
	ObjectAddress objaddr;
	char *relname = get_rel_name(relid);
	Oid schemaid = get_rel_namespace(relid);
	char *schema = get_namespace_name(schemaid);
	Cache *hcache;
	Hypertable *ht;

	CreateTrigStmt stmt_template = {
		.type = T_CreateTrigStmt,
		.row = true,
		.timing = TRIGGER_TYPE_AFTER,
		.trigname = CAGGINVAL_TRIGGER_NAME,
		.relation = makeRangeVar(schema, relname, -1),
		.funcname =
			list_make2(makeString(INTERNAL_SCHEMA_NAME), makeString(CAGG_INVALIDATION_TRIGGER)),
		.args = NIL, /* to be filled in later */
		.events = TRIGGER_TYPE_INSERT | TRIGGER_TYPE_UPDATE | TRIGGER_TYPE_DELETE,
	};
	if (check_trigger_exists_hypertable(relid, CAGGINVAL_TRIGGER_NAME))
		return;
	ht = ts_hypertable_cache_get_cache_and_entry(relid, CACHE_FLAG_NONE, &hcache);
	if (hypertable_is_distributed(ht))
	{
		DistCmdResult *result;
		List *data_node_list = ts_hypertable_get_data_node_name_list(ht);
		List *cmd_descriptors = NIL; /* same order as ht->data_nodes */
		DistCmdDescr *cmd_descr_data = NULL;
		ListCell *cell;

		unsigned i = 0;
		cmd_descr_data = palloc(list_length(data_node_list) * sizeof(*cmd_descr_data));
		foreach (cell, ht->data_nodes)
		{
			HypertableDataNode *node = lfirst(cell);
			char node_hypertable_id_str[12];
			CreateTrigStmt remote_stmt = stmt_template;

			pg_ltoa(node->fd.node_hypertable_id, node_hypertable_id_str);
			pg_ltoa(node->fd.hypertable_id, hypertable_id_str);

			remote_stmt.args =
				list_make2(makeString(node_hypertable_id_str), makeString(hypertable_id_str));
			cmd_descr_data[i].sql = deparse_create_trigger(&remote_stmt);
			cmd_descr_data[i].params = NULL;
			cmd_descriptors = lappend(cmd_descriptors, &cmd_descr_data[i++]);
		}

		result =
			ts_dist_multi_cmds_params_invoke_on_data_nodes(cmd_descriptors, data_node_list, true);
		if (result)
			ts_dist_cmd_close_response(result);
		/*
		 * FALL-THROUGH
		 * We let the Access Node create a trigger as well, even though it is not used for data
		 * modifications. We use the Access Node trigger as a check for existence of the remote
		 * triggers.
		 */
	}
	CreateTrigStmt local_stmt = stmt_template;
	pg_ltoa(hypertable_id, hypertable_id_str);
	local_stmt.args = list_make1(makeString(hypertable_id_str));
	objaddr = ts_hypertable_create_trigger(ht, &local_stmt, NULL);
	if (!OidIsValid(objaddr.objectId))
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("could not create continuous aggregate trigger")));
	ts_cache_release(hcache);
}

/* create a  view for the query using the SELECt stmt sqlquery
 * and view name from RangeVar viewrel
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

	/*  Create the view. viewname is in viewrel.
	 */
	SWITCH_TO_TS_USER(viewrel->schemaname, uid, saved_uid, sec_ctx);
	address = DefineRelation(create, RELKIND_VIEW, owner, NULL, NULL);
	CommandCounterIncrement();
	StoreViewQuery(address.objectId, selquery, false);
	CommandCounterIncrement();
	RESTORE_USER(uid, saved_uid, sec_ctx);
	return address;
}

/* Assign aliases to the targetlist in the query according to the column_names provided
 * in the CREATE VIEW statement.
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

			/* junk columns don't get aliases */
			if (tle->resjunk)
				continue;
			tle->resname = pstrdup(strVal(lfirst(alist_item)));
			alist_item = lnext_compat(tlist_aliases, alist_item);
			if (alist_item == NULL)
				break; /* done assigning aliases */
		}

		if (alist_item != NULL)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR), errmsg("too many column names specified")));
	}
}

/* Modifies the passed in ViewStmt to do the following
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
 *               so don't recreate invalidation trigger.

 * Since 1.7, we support real time aggregation.
 * If real time aggregation is off i.e. materialized only, the mcagg vew is as described in Step 2.
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
			CAggTimebucketInfo *origquery_ht, WithClauseResult *with_clause_options)
{
	ObjectAddress mataddress;
	char relnamebuf[NAMEDATALEN];
	MatTableColumnInfo mattblinfo;
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
		DatumGetBool(with_clause_options[ContinuousViewOptionMaterializedOnly].parsed);
	bool finalized = DatumGetBool(with_clause_options[ContinuousViewOptionFinalized].parsed);

	finalqinfo.finalized = finalized;

	/*
	 * Assign the column_name aliases in CREATE VIEW to the query. No other modifications to
	 * panquery
	 */
	fixup_userview_query_tlist(panquery, stmt->aliases);
	mattablecolumninfo_init(&mattblinfo, copyObject(panquery->groupClause));
	finalize_query_init(&finalqinfo, panquery, &mattblinfo);

	/*
	 * Invalidate all options on the stmt before using it
	 * The options are valid only for internal use (ts_continuous)
	 */
	stmt->options = NULL;

	/*
	 * Step 0: add any internal columns needed for materialization based
	 *         on the user query's table
	 */
	if (!finalized)
	{
		RangeTblEntry *usertbl_rte = list_nth(panquery->rtable, 0);
		mattablecolumninfo_addinternal(&mattblinfo, usertbl_rte, origquery_ht->htid);
	}

	/* Step 1: create the materialization table */
	ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);
	materialize_hypertable_id = ts_catalog_table_next_seq_id(ts_catalog_get(), HYPERTABLE);
	ts_catalog_restore_user(&sec_ctx);
	PRINT_MATINTERNAL_NAME(relnamebuf, "_materialized_hypertable_%d", materialize_hypertable_id);
	mat_rel = makeRangeVar(pstrdup(INTERNAL_SCHEMA_NAME), pstrdup(relnamebuf), -1);
	is_create_mattbl_index =
		DatumGetBool(with_clause_options[ContinuousViewOptionCreateGroupIndex].parsed);
	mattablecolumninfo_create_materialization_table(&mattblinfo,
													materialize_hypertable_id,
													mat_rel,
													origquery_ht,
													is_create_mattbl_index,
													create_stmt->into->tableSpaceName,
													create_stmt->into->accessMethod,
													&mataddress);
	/* Step 2: create view with select finalize from materialization
	 * table
	 */
	final_selquery =
		finalize_query_get_select_query(&finalqinfo, mattblinfo.matcollist, &mataddress);

	if (!materialized_only)
		final_selquery = build_union_query(origquery_ht,
										   mattblinfo.matpartcolno,
										   final_selquery,
										   panquery,
										   materialize_hypertable_id);

	/* copy view acl to materialization hypertable */
	ObjectAddress view_address = create_view_for_query(final_selquery, stmt->view);
	ts_copy_relation_acl(view_address.objectId, mataddress.objectId, GetUserId());

	/* Step 3: create the internal view with select partialize(..)
	 */
	partial_selquery =
		mattablecolumninfo_get_partial_select_query(&mattblinfo, panquery, finalqinfo.finalized);

	PRINT_MATINTERNAL_NAME(relnamebuf, "_partial_view_%d", materialize_hypertable_id);
	part_rel = makeRangeVar(pstrdup(INTERNAL_SCHEMA_NAME), pstrdup(relnamebuf), -1);
	create_view_for_query(partial_selquery, part_rel);

	/* Additional miscellaneous steps */
	/* create a dummy view to store the user supplied view query. This is to get PG
	 * to display the view correctly without having to replicate the PG source code for make_viewdef
	 */
	orig_userview_query = copyObject(panquery);
	PRINT_MATINTERNAL_NAME(relnamebuf, "_direct_view_%d", materialize_hypertable_id);
	dum_rel = makeRangeVar(pstrdup(INTERNAL_SCHEMA_NAME), pstrdup(relnamebuf), -1);
	create_view_for_query(orig_userview_query, dum_rel);
	/* Step 4 add catalog table entry for the objects we just created */
	nspid = RangeVarGetCreationNamespace(stmt->view);

	create_cagg_catalog_entry(materialize_hypertable_id,
							  origquery_ht->htid,
							  get_namespace_name(nspid), /*schema name for user view */
							  stmt->view->relname,
							  part_rel->schemaname,
							  part_rel->relname,
							  origquery_ht->bucket_width,
							  materialized_only,
							  dum_rel->schemaname,
							  dum_rel->relname,
							  finalized);

	if (origquery_ht->bucket_width == BUCKET_WIDTH_VARIABLE)
	{
		const char *bucket_width;
		const char *origin = "";

		/*
		 * Variable-sized buckets work only with intervals.
		 */
		Assert(origquery_ht->interval != NULL);
		bucket_width = DatumGetCString(
			DirectFunctionCall1(interval_out, IntervalPGetDatum(origquery_ht->interval)));

		if (!TIMESTAMP_NOT_FINITE(origquery_ht->origin))
		{
			origin = DatumGetCString(
				DirectFunctionCall1(timestamp_out, TimestampGetDatum(origquery_ht->origin)));
		}

		/*
		 * These values are not used for
		 * anything except Assert's yet for the same reasons. Once the design
		 * of variable-sized buckets is finalized we will have a better idea
		 * of what schema is needed exactly. Until then the choice was made
		 * in favor of the most generic schema that can be optimized later.
		 */
		create_bucket_function_catalog_entry(materialize_hypertable_id,
											 get_func_namespace(
												 origquery_ht->bucket_func->funcid) !=
												 PG_PUBLIC_NAMESPACE,
											 get_func_name(origquery_ht->bucket_func->funcid),
											 bucket_width,
											 origin,
											 origquery_ht->timezone);
	}

	/* Step 5 create trigger on raw hypertable -specified in the user view query*/
	cagg_add_trigger_hypertable(origquery_ht->htoid, origquery_ht->htid);
}

DDLResult
tsl_process_continuous_agg_viewstmt(Node *node, const char *query_string, void *pstmt,
									WithClauseResult *with_clause_options)
{
	const CreateTableAsStmt *stmt = castNode(CreateTableAsStmt, node);
	CAggTimebucketInfo timebucket_exprinfo;
	Oid nspid;
	bool finalized = with_clause_options[ContinuousViewOptionFinalized].parsed;
	ViewStmt viewstmt = {
		.type = T_ViewStmt,
		.view = stmt->into->rel,
		.query = stmt->into->viewQuery,
		.options = stmt->into->options,
		.aliases = stmt->into->colNames,
	};

	nspid = RangeVarGetCreationNamespace(stmt->into->rel);
	if (get_relname_relid(stmt->into->rel->relname, nspid))
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
					 errhint("Drop or rename the existing continuous aggregate"
							 " first or use another name.")));
		}
	}
	if (!with_clause_options[ContinuousViewOptionCompress].is_default)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot enable compression while creating a continuous aggregate"),
				 errhint("Use ALTER MATERIALIZED VIEW to enable compression.")));
	}

	timebucket_exprinfo = cagg_validate_query((Query *) stmt->into->viewQuery, finalized);
	cagg_create(stmt, &viewstmt, (Query *) stmt->query, &timebucket_exprinfo, with_clause_options);

	if (!stmt->into->skipData)
	{
		Oid relid;
		ContinuousAgg *cagg;
		InternalTimeRange refresh_window = {
			.type = InvalidOid,
		};

		CommandCounterIncrement();

		/* We are creating a refresh window here in a similar way to how it's
		 * done in continuous_agg_refresh. We do not call the PG function
		 * directly since we want to be able to suppress the output in that
		 * function and adding a 'verbose' parameter to is not useful for a
		 * user. */
		relid = get_relname_relid(stmt->into->rel->relname, nspid);
		cagg = ts_continuous_agg_find_by_relid(relid);
		Assert(cagg != NULL);
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
		refresh_window.start = ts_continuous_agg_bucket_width_variable(cagg) ?
								   ts_time_get_nobegin(refresh_window.type) :
								   ts_time_get_min(refresh_window.type);
		refresh_window.end = ts_time_get_noend_or_max(refresh_window.type);

		continuous_agg_refresh_internal(cagg, &refresh_window, CAGG_REFRESH_CREATION);
	}
	return DDL_DONE;
}

/*
 * Flip the view definition of an existing continuous aggregate from real-time to materialized-only
 * or vice versa depending on the current state.
 */
void
cagg_flip_realtime_view_definition(ContinuousAgg *agg, Hypertable *mat_ht)
{
	int sec_ctx;
	Oid uid, saved_uid;
	Query *result_view_query;

	/* user view query of the user defined CAGG */
	Oid user_view_oid = relation_oid(agg->data.user_view_schema, agg->data.user_view_name);
	Relation user_view_rel = relation_open(user_view_oid, AccessShareLock);
	Query *user_query = copyObject(get_view_query(user_view_rel));
	/* keep lock until end of transaction */
	relation_close(user_view_rel, NoLock);
	remove_old_and_new_rte_from_query(user_query);

	/* direct view query of the original user view definition at CAGG creation */
	Oid direct_view_oid = relation_oid(agg->data.direct_view_schema, agg->data.direct_view_name);
	Relation direct_view_rel = relation_open(direct_view_oid, AccessShareLock);
	Query *direct_query = copyObject(get_view_query(direct_view_rel));
	/* keep lock until end of transaction */
	relation_close(direct_view_rel, NoLock);
	remove_old_and_new_rte_from_query(direct_query);

	CAggTimebucketInfo timebucket_exprinfo = cagg_validate_query(direct_query, agg->data.finalized);

	/* flip */
	agg->data.materialized_only = !agg->data.materialized_only;
	if (agg->data.materialized_only)
	{
		result_view_query = destroy_union_query(user_query);
	}
	else
	{
		/* get primary partitioning column information of time bucketing */
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

	/* user view query of the user defined CAGG */
	Oid user_view_oid = relation_oid(agg->data.user_view_schema, agg->data.user_view_name);
	Relation user_view_rel = relation_open(user_view_oid, AccessShareLock);
	Query *user_query = copyObject(get_view_query(user_view_rel));
	remove_old_and_new_rte_from_query(user_query);

	/*
	 * When calling StoreViewQuery the target list names of the query have to
	 * match the view's tuple descriptor attribute names. But if a column of the continuous
	 * aggregate has been renamed, the query tree will not have the correct
	 * names in the target list, which will error out when calling
	 * StoreViewQuery. For that reason, we fetch the name from the user view
	 * relation and update the resource name in the query target list to match
	 * the name in the user view.
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

	/* Keep locks until end of transaction and do not close the relation
	 * before the call to StoreViewQuery since it can otherwise release the
	 * memory for attr->attname, causing a segfault. */
	relation_close(user_view_rel, NoLock);
}
