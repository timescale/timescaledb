/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include "repair.h"

#include <funcapi.h>
#include <utils/snapmgr.h>

#include "func_cache.h"

static void cagg_rebuild_view_definition(ContinuousAgg *agg, Hypertable *mat_ht,
										 bool force_rebuild);

/*
 * Test the view definition of an existing continuous aggregate
 * for errors and attempt to rebuild it if required.
 */
static void
cagg_rebuild_view_definition(ContinuousAgg *agg, Hypertable *mat_ht, bool force_rebuild)
{
	bool test_failed = false;
	char *relname = NameStr(agg->data.user_view_name);
	char *schema = NameStr(agg->data.user_view_schema);
	ListCell *lc1, *lc2;
	int sec_ctx;
	Oid uid, saved_uid;
	bool finalized = ContinuousAggIsFinalized(agg);

	if (!finalized)
	{
		ereport(WARNING,
				(errmsg("repairing Continuous Aggregates with partials are not supported anymore."),
				 errdetail("Migrate the Continuous Aggregates to finalized form to rebuild."),
				 errhint("Run \"CALL cagg_migrate('%s.%s');\" to migrate to the new "
						 "format.",
						 schema,
						 relname)));
		return;
	}

	/* Cagg view created by the user. */
	Oid user_view_oid = relation_oid(&agg->data.user_view_schema, &agg->data.user_view_name);
	Relation user_view_rel = relation_open(user_view_oid, AccessShareLock);
	Query *user_query = get_view_query(user_view_rel);

	bool rebuild_cagg_with_joins = false;

	/* Extract final query from user view query. */
	Query *final_query = copyObject(user_query);
	RemoveRangeTableEntries(final_query);

	if (finalized && !force_rebuild)
	{
		/* This continuous aggregate does not have partials, do not check for defects. */
		elog(DEBUG1,
			 "[cagg_rebuild_view_definition] %s.%s does not have partials, do not check for "
			 "defects!",
			 NameStr(agg->data.user_view_schema),
			 NameStr(agg->data.user_view_name)

		);
		relation_close(user_view_rel, NoLock);
		return;
	}

	if (!agg->data.materialized_only)
	{
		final_query = destroy_union_query(final_query);
	}
	FinalizeQueryInfo fqi;
	MatTableColumnInfo mattblinfo;
	ObjectAddress mataddress = {
		.classId = RelationRelationId,
		.objectId = mat_ht->main_table_relid,
	};

	Oid direct_view_oid = relation_oid(&agg->data.direct_view_schema, &agg->data.direct_view_name);
	Relation direct_view_rel = relation_open(direct_view_oid, AccessShareLock);
	Query *direct_query = copyObject(get_view_query(direct_view_rel));
	RemoveRangeTableEntries(direct_query);

	/*
	 * If there is a join in CAggs then rebuild it definitley,
	 * because v2.10.0 has created the definition with missing structs.
	 *
	 * Removed the check for direct_query->jointree != NULL because
	 * we don't allow queries without FROM clause in Continuous Aggregate
	 * definition.
	 *
	 * Per coverityscan:
	 * https://scan4.scan.coverity.com/reports.htm#v54116/p12995/fileInstanceId=131745632&defectInstanceId=14569562&mergedDefectId=384045
	 *
	 */
	if (force_rebuild)
	{
		ListCell *l;
		foreach (l, direct_query->jointree->fromlist)
		{
			Node *jtnode = (Node *) lfirst(l);
			if (IsA(jtnode, JoinExpr))
				rebuild_cagg_with_joins = true;
		}
	}

	if (!rebuild_cagg_with_joins && finalized)
	{
		/* There's nothing to fix, so no need to rebuild */
		elog(DEBUG1,
			 "[cagg_rebuild_view_definition] %s.%s does not have JOINS, so no need to rebuild the "
			 "definition!",
			 NameStr(agg->data.user_view_schema),
			 NameStr(agg->data.user_view_name)

		);
		relation_close(user_view_rel, NoLock);
		relation_close(direct_view_rel, NoLock);
		return;
	}
	else
		elog(DEBUG1,
			 "[cagg_rebuild_view_definition] %s.%s has been rebuilt!",
			 NameStr(agg->data.user_view_schema),
			 NameStr(agg->data.user_view_name));

	CAggTimebucketInfo timebucket_exprinfo =
		cagg_validate_query(direct_query,
							finalized,
							NameStr(agg->data.user_view_schema),
							NameStr(agg->data.user_view_name),
							true);

	mattablecolumninfo_init(&mattblinfo, copyObject(direct_query->groupClause));
	fqi.finalized = finalized;
	finalizequery_init(&fqi, direct_query, &mattblinfo);

	Query *view_query = finalizequery_get_select_query(&fqi,
													   mattblinfo.matcollist,
													   &mataddress,
													   NameStr(mat_ht->fd.table_name));

	if (!agg->data.materialized_only)
	{
		view_query = build_union_query(&timebucket_exprinfo,
									   mattblinfo.matpartcolno,
									   view_query,
									   direct_query,
									   mat_ht->fd.id);
	}

	if (list_length(mattblinfo.matcollist) != ts_get_relnatts(mat_ht->main_table_relid))
		/*
		 * There is a mismatch of columns between the current version's finalization view
		 * building logic and the existing schema of the materialization table. As of version
		 * 2.7.0 this only happens due to buggy view generation in previous versions. Do not
		 * rebuild those views since the materialization table can not be queried correctly.
		 */
		test_failed = true;
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
	forboth (lc1, view_query->targetList, lc2, user_query->targetList)
	{
		TargetEntry *view_tle, *user_tle;
		FormData_pg_attribute *attr = TupleDescAttr(desc, i);
		view_tle = lfirst_node(TargetEntry, lc1);
		user_tle = lfirst_node(TargetEntry, lc2);
		if (view_tle->resjunk && user_tle->resjunk)
			break;
		else if (view_tle->resjunk || user_tle->resjunk)
		{
			/*
			 * This should never happen but if it ever does it's safer to
			 * error here instead of creating broken view definitions.
			 */
			test_failed = true;
			break;
		}
		view_tle->resname = user_tle->resname = NameStr(attr->attname);
		++i;
	}

	if (test_failed)
	{
		ereport(WARNING,
				(errmsg("Inconsistent view definitions for continuous aggregate view "
						"\"%s.%s\"",
						schema,
						relname),
				 errdetail("Continuous aggregate data possibly corrupted."),
				 errhint("You may need to recreate the continuous aggregate with CREATE "
						 "MATERIALIZED VIEW.")));
	}
	else
	{
		SWITCH_TO_TS_USER(NameStr(agg->data.user_view_schema), uid, saved_uid, sec_ctx);
		StoreViewQuery(user_view_oid, view_query, true);
		CommandCounterIncrement();
		RESTORE_USER(uid, saved_uid, sec_ctx);
	}
	/*
	 * Keep locks until end of transaction and do not close the relation
	 * before the call to StoreViewQuery since it can otherwise release the
	 * memory for attr->attname, causing a segfault.
	 */
	relation_close(direct_view_rel, NoLock);
	relation_close(user_view_rel, NoLock);
}

Datum
tsl_cagg_try_repair(PG_FUNCTION_ARGS)
{
	Oid relid = PG_ARGISNULL(0) ? InvalidOid : PG_GETARG_OID(0);
	char relkind = get_rel_relkind(relid);
	bool force_rebuild = PG_ARGISNULL(0) ? false : PG_GETARG_BOOL(1);
	ContinuousAgg *cagg = NULL;

	if (RELKIND_VIEW == relkind)
		cagg = ts_continuous_agg_find_by_relid(relid);

	if (RELKIND_VIEW != relkind || !cagg)
	{
		ereport(WARNING,
				(errmsg("invalid OID \"%u\" for continuous aggregate view", relid),
				 errdetail("Check for database corruption.")));
		PG_RETURN_VOID();
	}

	Cache *hcache = ts_hypertable_cache_pin();

	Hypertable *mat_ht = ts_hypertable_cache_get_entry_by_id(hcache, cagg->data.mat_hypertable_id);
	Assert(mat_ht != NULL);
	cagg_rebuild_view_definition(cagg, mat_ht, force_rebuild);

	ts_cache_release(hcache);

	PG_RETURN_VOID();
}

typedef struct
{
	/* Input parameter */
	int32 mat_hypertable_id;

	/* Output parameter */
	Oid bucket_fuction;
} CaggQueryWalkerContext;

/* Process the CAgg query and find all used (usually one) time_bucket functions. It returns
 * InvalidOid is no or more than one bucket function is found. */
static bool
cagg_query_walker(Node *node, CaggQueryWalkerContext *context)
{
	if (node == NULL)
		return false;

	if (IsA(node, FuncExpr))
	{
		FuncExpr *func_expr = castNode(FuncExpr, node);

		/* Is the used function a bucket function?
		 * We can not call ts_func_cache_get_bucketing_func at this point, since
		 */
		FuncInfo *func_info = ts_func_cache_get_bucketing_func(func_expr->funcid);
		if (func_info != NULL)
		{
			/* First bucket function found */
			if (!OidIsValid(context->bucket_fuction))
			{
				context->bucket_fuction = func_expr->funcid;
			}
			else
			{
				/* Got multiple bucket functions. Should never happen because this is checked during
				 * CAgg query validation.
				 */
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("found multiple time_bucket functions in CAgg definition for "
								"mat_ht_id: %d",
								context->mat_hypertable_id)));
				pg_unreachable();
			}
		}
	}
	else if (IsA(node, Query))
	{
		Query *query = castNode(Query, node);
		return query_tree_walker(query, cagg_query_walker, context, 0);
	}

	return expression_tree_walker(node, cagg_query_walker, context);
}

/* Get the Oid of the direct view of the CAgg. We cannot use the TimescaleDB internal
 * functions such as ts_continuous_agg_find_by_mat_hypertable_id() at this point since this
 * function can be called during an extension upgrade and ts_catalog_get() does not work.
 */
static Oid
get_direct_view_oid(int32 mat_hypertable_id)
{
	RangeVar *ts_cagg = makeRangeVar(CATALOG_SCHEMA_NAME,
									 CONTINUOUS_AGG_TABLE_NAME,
									 -1 /* taken location unknown */);
	Relation cagg_rel = relation_openrv_extended(ts_cagg, AccessShareLock, /* missing ok */ true);

	RangeVar *ts_cagg_idx =
		makeRangeVar(CATALOG_SCHEMA_NAME, TS_CAGG_CATALOG_IDX, -1 /* taken location unknown */);
	Relation cagg_idx_rel =
		relation_openrv_extended(ts_cagg_idx, AccessShareLock, /* missing ok */ true);

	/* Prepare relation scan */
	TupleTableSlot *slot = table_slot_create(cagg_rel, NULL);
	ScanKeyData scankeys[1];
	ScanKeyEntryInitialize(&scankeys[0],
						   0,
						   1,
						   BTEqualStrategyNumber,
						   InvalidOid,
						   InvalidOid,
						   F_INT4EQ,
						   Int32GetDatum(mat_hypertable_id));

	/* Prepare index scan */
	IndexScanDesc indexscan =
		index_beginscan(cagg_rel, cagg_idx_rel, GetTransactionSnapshot(), 1, 0);
	index_rescan(indexscan, scankeys, 1, NULL, 0);

	/* Read tuple from relation */
	bool got_next_slot = index_getnext_slot(indexscan, ForwardScanDirection, slot);
	Ensure(got_next_slot, "unable to find CAgg definition for mat_ht %d", mat_hypertable_id);

	bool is_null = false;

	/* We use the view_schema and view_name to get data from the system catalog. Even char pointers
	 * are passed to the catalog, it calls namehashfast() internally, which assumes that a char of
	 * NAMEDATALEN is allocated. */
	NameData view_schema_name;
	NameData view_name_name;

	/* We need to get the attribute names dynamically since this function can be called during an
	 * upgrade and the fixed attribution numbers (i.e., Anum_continuous_agg_direct_view_schema) can
	 * be different. */
	AttrNumber direct_view_schema_attr = get_attnum(cagg_rel->rd_id, "direct_view_schema");
	Ensure(direct_view_schema_attr != InvalidAttrNumber,
		   "unable to get attribute number for direct_view_schema");

	AttrNumber direct_view_name_attr = get_attnum(cagg_rel->rd_id, "direct_view_name");
	Ensure(direct_view_name_attr != InvalidAttrNumber,
		   "unable to get attribute number for direct_view_name");

	char *view_schema = DatumGetCString(slot_getattr(slot, direct_view_schema_attr, &is_null));
	Ensure(!is_null, "unable to get view schema for oid %d", mat_hypertable_id);
	Assert(view_schema != NULL);
	namestrcpy(&view_schema_name, view_schema);

	char *view_name = DatumGetCString(slot_getattr(slot, direct_view_name_attr, &is_null));
	Ensure(!is_null, "unable to get view name for oid %d", mat_hypertable_id);
	Assert(view_name != NULL);
	namestrcpy(&view_name_name, view_name);

	got_next_slot = index_getnext_slot(indexscan, ForwardScanDirection, slot);
	Ensure(!got_next_slot, "found duplicate definitions for CAgg mat_ht %d", mat_hypertable_id);

	/* End relation scan */
	index_endscan(indexscan);
	ExecDropSingleTupleTableSlot(slot);
	relation_close(cagg_rel, AccessShareLock);
	relation_close(cagg_idx_rel, AccessShareLock);

	/* Get Oid of user view */
	Oid direct_view_oid =
		ts_get_relation_relid(NameStr(view_schema_name), NameStr(view_name_name), false);
	Assert(OidIsValid(direct_view_oid));
	return direct_view_oid;
}

/*
 * In older TimescaleDB versions, information about the Timezone and the Oid of the time_bucket
 * function are not stored in the catalog table. This function gets the data from the used
 * view definition and return it.
 */
Datum
continuous_agg_get_bucket_function(PG_FUNCTION_ARGS)
{
	const int32 mat_hypertable_id = PG_GETARG_INT32(0);

	/* Get the user view query of the user defined CAGG.  */
	Oid direct_view_oid = get_direct_view_oid(mat_hypertable_id);
	Assert(OidIsValid(direct_view_oid));

	Relation direct_view_rel = relation_open(direct_view_oid, AccessShareLock);
	Query *direct_query = copyObject(get_view_query(direct_view_rel));
	relation_close(direct_view_rel, NoLock);

	Assert(direct_query != NULL);
	Assert(direct_query->commandType == CMD_SELECT);

	/* Process the query and collect function information */
	CaggQueryWalkerContext context = { 0 };
	context.mat_hypertable_id = mat_hypertable_id;
	context.bucket_fuction = InvalidOid;

	cagg_query_walker((Node *) direct_query, &context);

	PG_RETURN_DATUM(ObjectIdGetDatum(context.bucket_fuction));
}
