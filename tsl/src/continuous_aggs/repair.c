/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include "repair.h"

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
	char *relname = agg->data.user_view_name.data;
	char *schema = agg->data.user_view_schema.data;
	ListCell *lc1, *lc2;
	int sec_ctx;
	Oid uid, saved_uid;

	/* Cagg view created by the user. */
	Oid user_view_oid = relation_oid(&agg->data.user_view_schema, &agg->data.user_view_name);
	Relation user_view_rel = relation_open(user_view_oid, AccessShareLock);
	Query *user_query = get_view_query(user_view_rel);

	bool finalized = ContinuousAggIsFinalized(agg);
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
							NameStr(agg->data.user_view_name));

	mattablecolumninfo_init(&mattblinfo, copyObject(direct_query->groupClause));
	fqi.finalized = finalized;
	finalizequery_init(&fqi, direct_query, &mattblinfo);

	/*
	 * Add any internal columns needed for materialization based
	 * on the user query's table.
	 */
	if (!finalized)
		mattablecolumninfo_addinternal(&mattblinfo);

	Query *view_query = NULL;
	if (rebuild_cagg_with_joins)
	{
		view_query = finalizequery_get_select_query(&fqi,
													mattblinfo.matcollist,
													&mataddress,
													NameStr(mat_ht->fd.table_name));
	}
	else
		view_query =
			finalizequery_get_select_query(&fqi, mattblinfo.matcollist, &mataddress, relname);

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
