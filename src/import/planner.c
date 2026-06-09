/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */

/*
 * This file contains source code that was copied and/or modified from
 * the PostgreSQL database, which is licensed under the open-source
 * PostgreSQL License. Please see the NOTICE at the top level
 * directory for a copy of the PostgreSQL License.
 *
 * These function were copied from the PostgreSQL core planner, since
 * they were declared static in the core planner, but we need them for
 * our manipulations.
 */
#include <postgres.h>
#include <access/htup_details.h>
#include <catalog/pg_collation.h>
#include <catalog/pg_statistic.h>
#include <executor/nodeAgg.h>
#include <nodes/makefuncs.h>
#include <nodes/nodeFuncs.h>
#include <optimizer/clauses.h>
#include <optimizer/cost.h>
#include <optimizer/optimizer.h>
#include <optimizer/paramassign.h>
#include <optimizer/paths.h>
#include <optimizer/placeholder.h>
#include <optimizer/planmain.h>
#include <optimizer/planner.h>
#include <optimizer/tlist.h>
#include <parser/parsetree.h>
#include <utils/datum.h>
#include <utils/lsyscache.h>
#include <utils/rel.h>
#include <utils/syscache.h>

#include "compat/compat.h"
#include "planner.h"

/* copied verbatim from optimizer/util/appendinfo.c at REL_17_6 */
void
ts_make_inh_translation_list(Relation oldrelation, Relation newrelation, Index newvarno,
							 AppendRelInfo *appinfo)
{
	List *vars = NIL;
	AttrNumber *pcolnos;
	TupleDesc old_tupdesc = RelationGetDescr(oldrelation);
	TupleDesc new_tupdesc = RelationGetDescr(newrelation);
	Oid new_relid = RelationGetRelid(newrelation);
	int oldnatts = old_tupdesc->natts;
	int newnatts = new_tupdesc->natts;
	int old_attno;
	int new_attno = 0;

	/* Initialize reverse-translation array with all entries zero */
	appinfo->num_child_cols = newnatts;
	appinfo->parent_colnos = pcolnos = (AttrNumber *) palloc0(newnatts * sizeof(AttrNumber));

	for (old_attno = 0; old_attno < oldnatts; old_attno++)
	{
		Form_pg_attribute att;
		char *attname;
		Oid atttypid;
		int32 atttypmod;
		Oid attcollation;

		att = TupleDescAttr(old_tupdesc, old_attno);
		if (att->attisdropped)
		{
			/* Just put NULL into this list entry */
			vars = lappend(vars, NULL);
			continue;
		}
		attname = NameStr(att->attname);
		atttypid = att->atttypid;
		atttypmod = att->atttypmod;
		attcollation = att->attcollation;

		/*
		 * When we are generating the "translation list" for the parent table
		 * of an inheritance set, no need to search for matches.
		 */
		if (oldrelation == newrelation)
		{
			vars = lappend(vars,
						   makeVar(newvarno,
								   (AttrNumber) (old_attno + 1),
								   atttypid,
								   atttypmod,
								   attcollation,
								   0));
			pcolnos[old_attno] = old_attno + 1;
			continue;
		}

		/*
		 * Otherwise we have to search for the matching column by name.
		 * There's no guarantee it'll have the same column position, because
		 * of cases like ALTER TABLE ADD COLUMN and multiple inheritance.
		 * However, in simple cases, the relative order of columns is mostly
		 * the same in both relations, so try the column of newrelation that
		 * follows immediately after the one that we just found, and if that
		 * fails, let syscache handle it.
		 */
		if (new_attno >= newnatts || (att = TupleDescAttr(new_tupdesc, new_attno))->attisdropped ||
			strcmp(attname, NameStr(att->attname)) != 0)
		{
			HeapTuple newtup;

			newtup = SearchSysCacheAttName(new_relid, attname);
			if (!HeapTupleIsValid(newtup))
				elog(ERROR,
					 "could not find inherited attribute \"%s\" of relation \"%s\"",
					 attname,
					 RelationGetRelationName(newrelation));
			new_attno = ((Form_pg_attribute) GETSTRUCT(newtup))->attnum - 1;
			Assert(new_attno >= 0 && new_attno < newnatts);
			ReleaseSysCache(newtup);

			att = TupleDescAttr(new_tupdesc, new_attno);
		}

		/* Found it, check type and collation match */
		if (atttypid != att->atttypid || atttypmod != att->atttypmod)
			elog(ERROR,
				 "attribute \"%s\" of relation \"%s\" does not match parent's type",
				 attname,
				 RelationGetRelationName(newrelation));
		if (attcollation != att->attcollation)
			elog(ERROR,
				 "attribute \"%s\" of relation \"%s\" does not match parent's collation",
				 attname,
				 RelationGetRelationName(newrelation));

		vars = lappend(vars,
					   makeVar(newvarno,
							   (AttrNumber) (new_attno + 1),
							   atttypid,
							   atttypmod,
							   attcollation,
							   0));
		pcolnos[new_attno] = old_attno + 1;
		new_attno++;
	}

	appinfo->translated_vars = vars;
}

/* copied verbatim from planner.c */
struct PathTarget *
ts_make_partial_grouping_target(struct PlannerInfo *root, PathTarget *grouping_target)
{
	struct Query *parse = root->parse;
	PathTarget *partial_target;
	struct List *non_group_cols;
	struct List *non_group_exprs;
	int i;
	ListCell *lc;

	partial_target = create_empty_pathtarget();
	non_group_cols = NIL;

	i = 0;
	foreach (lc, grouping_target->exprs)
	{
		struct Expr *expr = (struct Expr *) lfirst(lc);
		unsigned int sgref = get_pathtarget_sortgroupref(grouping_target, i);

		if (sgref && parse->groupClause &&
			get_sortgroupref_clause_noerr(sgref, parse->groupClause) != NULL)
		{
			/*
			 * It's a grouping column, so add it to the partial_target as-is.
			 * (This allows the upper agg step to repeat the grouping calcs.)
			 */
			add_column_to_pathtarget(partial_target, expr, sgref);
		}
		else
		{
			/*
			 * Non-grouping column, so just remember the expression for later
			 * call to pull_var_clause.
			 */
			non_group_cols = lappend(non_group_cols, expr);
		}

		i++;
	}

	/*
	 * If there's a HAVING clause, we'll need the Vars/Aggrefs it uses, too.
	 */
	if (parse->havingQual)
		non_group_cols = lappend(non_group_cols, parse->havingQual);

	/*
	 * Pull out all the Vars, PlaceHolderVars, and Aggrefs mentioned in
	 * non-group cols (plus HAVING), and add them to the partial_target if not
	 * already present.  (An expression used directly as a GROUP BY item will
	 * be present already.)  Note this includes Vars used in resjunk items, so
	 * we are covering the needs of ORDER BY and window specifications.
	 */
	non_group_exprs = pull_var_clause((struct Node *) non_group_cols,
									  PVC_INCLUDE_AGGREGATES | PVC_RECURSE_WINDOWFUNCS |
										  PVC_INCLUDE_PLACEHOLDERS);

	add_new_columns_to_pathtarget(partial_target, non_group_exprs);

	/*
	 * Adjust Aggrefs to put them in partial mode.  At this point all Aggrefs
	 * are at the top level of the target list, so we can just scan the list
	 * rather than recursing through the expression trees.
	 */
	foreach (lc, partial_target->exprs)
	{
		struct Aggref *aggref = (struct Aggref *) lfirst(lc);

		if (IsA(aggref, Aggref))
		{
			struct Aggref *newaggref;

			/*
			 * We shouldn't need to copy the substructure of the Aggref node,
			 * but flat-copy the node itself to avoid damaging other trees.
			 */
			newaggref = makeNode(Aggref);
			memcpy(newaggref, aggref, sizeof(struct Aggref));

			/* For now, assume serialization is required */
			mark_partial_aggref(newaggref, AGGSPLIT_INITIAL_SERIAL);

			lfirst(lc) = newaggref;
		}
	}

	/* clean up cruft */
	list_free(non_group_exprs);
	list_free(non_group_cols);

	/* XXX this causes some redundant cost calculation ... */
	return set_pathtarget_cost_width(root, partial_target);
}

/* copied verbatim from selfuncs.c */
bool
ts_get_variable_range(PlannerInfo *root, VariableStatData *vardata, Oid sortop, Datum *min,
					  Datum *max)
{
	Datum tmin = 0;
	Datum tmax = 0;
	bool have_data = false;
	int16 typLen;
	bool typByVal;
	Oid opfuncoid;
	AttStatsSlot sslot;
	int i;

	/*
	 * XXX It's very tempting to try to use the actual column min and max, if
	 * we can get them relatively-cheaply with an index probe.  However, since
	 * this function is called many times during join planning, that could
	 * have unpleasant effects on planning speed.  Need more investigation
	 * before enabling this.
	 */
#ifdef NOT_USED
	if (get_actual_variable_range(root, vardata, sortop, min, max))
		return true;
#endif

	if (!HeapTupleIsValid(vardata->statsTuple))
	{
		/* no stats available, so default result */
		return false;
	}

	/*
	 * If we can't apply the sortop to the stats data, just fail.  In
	 * principle, if there's a histogram and no MCVs, we could return the
	 * histogram endpoints without ever applying the sortop ... but it's
	 * probably not worth trying, because whatever the caller wants to do with
	 * the endpoints would likely fail the security check too.
	 */
	opfuncoid = get_opcode(sortop);
	if (!statistic_proc_security_check(vardata, opfuncoid))
		return false;

	get_typlenbyval(vardata->atttype, &typLen, &typByVal);

	/*
	 * If there is a histogram, grab the first and last values.
	 *
	 * If there is a histogram that is sorted with some other operator than
	 * the one we want, fail --- this suggests that there is data we can't
	 * use.
	 */
	if (get_attstatsslot(&sslot,
						 vardata->statsTuple,
						 STATISTIC_KIND_HISTOGRAM,
						 sortop,
						 ATTSTATSSLOT_VALUES))
	{
		if (sslot.nvalues > 0)
		{
			tmin = datumCopy(sslot.values[0], typByVal, typLen);
			tmax = datumCopy(sslot.values[sslot.nvalues - 1], typByVal, typLen);
			have_data = true;
		}
		free_attstatsslot(&sslot);
	}
	else if (get_attstatsslot(&sslot, vardata->statsTuple, STATISTIC_KIND_HISTOGRAM, InvalidOid, 0))
	{
		free_attstatsslot(&sslot);
		return false;
	}

	/*
	 * If we have most-common-values info, look for extreme MCVs.  This is
	 * needed even if we also have a histogram, since the histogram excludes
	 * the MCVs.  However, usually the MCVs will not be the extreme values, so
	 * avoid unnecessary data copying.
	 */
	if (get_attstatsslot(&sslot,
						 vardata->statsTuple,
						 STATISTIC_KIND_MCV,
						 InvalidOid,
						 ATTSTATSSLOT_VALUES))
	{
		bool tmin_is_mcv = false;
		bool tmax_is_mcv = false;
		FmgrInfo opproc;

		fmgr_info(opfuncoid, &opproc);

		for (i = 0; i < sslot.nvalues; i++)
		{
			if (!have_data)
			{
				tmin = tmax = sslot.values[i];
				tmin_is_mcv = tmax_is_mcv = have_data = true;
				continue;
			}
			if (DatumGetBool(
					FunctionCall2Coll(&opproc, DEFAULT_COLLATION_OID, sslot.values[i], tmin)))
			{
				tmin = sslot.values[i];
				tmin_is_mcv = true;
			}
			if (DatumGetBool(
					FunctionCall2Coll(&opproc, DEFAULT_COLLATION_OID, tmax, sslot.values[i])))
			{
				tmax = sslot.values[i];
				tmax_is_mcv = true;
			}
		}
		if (tmin_is_mcv)
			tmin = datumCopy(tmin, typByVal, typLen);
		if (tmax_is_mcv)
			tmax = datumCopy(tmax, typByVal, typLen);
		free_attstatsslot(&sslot);
	}

	*min = tmin;
	*max = tmax;
	return have_data;
}


/* In PG18, child ems are not added to ec_members
 * but need to be maintained in separate Lists.
 *
 * https://github.com/postgres/postgres/commit/d69d45a5
 */
/* copied from add_child_eq_member */
#if PG18_GE
void
ts_add_child_eq_member(PlannerInfo *root, EquivalenceClass *ec, EquivalenceMember *em,
					   int child_relid)
{
	/*
	 * Allocate the array to store child members; an array of Lists indexed by
	 * relid, or expand the existing one, if necessary.
	 */
	if (unlikely(ec->ec_childmembers_size < root->simple_rel_array_size))
	{
		ec->ec_relids = bms_add_members(ec->ec_relids, em->em_relids);
		if (ec->ec_childmembers == NULL)
			ec->ec_childmembers = palloc0_array(List *, root->simple_rel_array_size);
		else
			ec->ec_childmembers = repalloc0_array(ec->ec_childmembers,
												  List *,
												  ec->ec_childmembers_size,
												  root->simple_rel_array_size);
		ec->ec_childmembers_size = root->simple_rel_array_size;
	}
	/* add member to the ec_childmembers List for the given child_relid */
	ec->ec_childmembers[child_relid] = lappend(ec->ec_childmembers[child_relid], em);
}
#endif
