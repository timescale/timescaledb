/*
 * This file contains functions copied verbatim from the PG core planner.
 * These function had to be copied since they were declared static in the core planner, but we need them for our
 * manipulations.
 */
#include <postgres.h>
#include <utils/rel.h>
#include <nodes/makefuncs.h>
#include <optimizer/clauses.h>
#include <utils/lsyscache.h>
#include <catalog/pg_statistic.h>
#include <catalog/pg_collation.h>
#include <utils/datum.h>
#include <executor/nodeAgg.h>
#include <optimizer/tlist.h>
#include <optimizer/var.h>
#include <optimizer/planner.h>
#include <optimizer/cost.h>
#include <access/htup_details.h>
#include <utils/selfuncs.h>

#include "planner_import.h"
#include "compat.h"


/* copied verbatim from prepunion.c */
void
make_inh_translation_list(Relation oldrelation, Relation newrelation,
						  Index newvarno,
						  List **translated_vars)
{
	List	   *vars = NIL;
	TupleDesc	old_tupdesc = RelationGetDescr(oldrelation);
	TupleDesc	new_tupdesc = RelationGetDescr(newrelation);
	int			oldnatts = old_tupdesc->natts;
	int			newnatts = new_tupdesc->natts;
	int			old_attno;

	for (old_attno = 0; old_attno < oldnatts; old_attno++)
	{
		Form_pg_attribute att;
		char	   *attname;
		Oid			atttypid;
		int32		atttypmod;
		Oid			attcollation;
		int			new_attno;

		att = old_tupdesc->attrs[old_attno];
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
			vars = lappend(vars, makeVar(newvarno,
										 (AttrNumber) (old_attno + 1),
										 atttypid,
										 atttypmod,
										 attcollation,
										 0));
			continue;
		}

		/*
		 * Otherwise we have to search for the matching column by name.
		 * There's no guarantee it'll have the same column position, because
		 * of cases like ALTER TABLE ADD COLUMN and multiple inheritance.
		 * However, in simple cases it will be the same column number, so try
		 * that before we go groveling through all the columns.
		 *
		 * Note: the test for (att = ...) != NULL cannot fail, it's just a
		 * notational device to include the assignment into the if-clause.
		 */
		if (old_attno < newnatts &&
			(att = new_tupdesc->attrs[old_attno]) != NULL &&
			!att->attisdropped &&
			strcmp(attname, NameStr(att->attname)) == 0)
			new_attno = old_attno;
		else
		{
			for (new_attno = 0; new_attno < newnatts; new_attno++)
			{
				att = new_tupdesc->attrs[new_attno];
				if (!att->attisdropped &&
					strcmp(attname, NameStr(att->attname)) == 0)
					break;
			}
			if (new_attno >= newnatts)
				elog(ERROR, "could not find inherited attribute \"%s\" of relation \"%s\"",
					 attname, RelationGetRelationName(newrelation));
		}

		/* Found it, check type and collation match */
		if (atttypid != att->atttypid || atttypmod != att->atttypmod)
			elog(ERROR, "attribute \"%s\" of relation \"%s\" does not match parent's type",
				 attname, RelationGetRelationName(newrelation));
		if (attcollation != att->attcollation)
			elog(ERROR, "attribute \"%s\" of relation \"%s\" does not match parent's collation",
				 attname, RelationGetRelationName(newrelation));

		vars = lappend(vars, makeVar(newvarno,
									 (AttrNumber) (new_attno + 1),
									 atttypid,
									 atttypmod,
									 attcollation,
									 0));
	}

	*translated_vars = vars;
}

/* copied exactly from planner.c */
size_t
estimate_hashagg_tablesize(struct Path *path,
						   const struct AggClauseCosts *agg_costs,
						   double dNumGroups)
{
	size_t		hashentrysize;

	/* Estimate per-hash-entry space at tuple width... */
	hashentrysize = MAXALIGN(path->pathtarget->width) +
		MAXALIGN(SizeofMinimalTupleHeader);

	/* plus space for pass-by-ref transition values... */
	hashentrysize += agg_costs->transitionSpace;
	/* plus the per-hash-entry overhead */
	hashentrysize += hash_agg_entry_size(agg_costs->numAggs);

	/*
	 * Note that this disregards the effect of fill-factor and growth policy
	 * of the hash-table. That's probably ok, given default the default
	 * fill-factor is relatively high. It'd be hard to meaningfully factor in
	 * "double-in-size" growth policies here.
	 */
	return hashentrysize * dNumGroups;
}

/* copied verbatim from planner.c */
struct PathTarget *
make_partial_grouping_target(struct PlannerInfo *root, PathTarget *grouping_target)
{
	struct Query *parse = root->parse;
	PathTarget *partial_target;
	struct List *non_group_cols;
	struct List *non_group_exprs;
	int			i;
	struct ListCell *lc;

	partial_target = create_empty_pathtarget();
	non_group_cols = NIL;

	i = 0;
	foreach(lc, grouping_target->exprs)
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
									  PVC_INCLUDE_AGGREGATES |
									  PVC_RECURSE_WINDOWFUNCS |
									  PVC_INCLUDE_PLACEHOLDERS);

	add_new_columns_to_pathtarget(partial_target, non_group_exprs);

	/*
	 * Adjust Aggrefs to put them in partial mode.  At this point all Aggrefs
	 * are at the top level of the target list, so we can just scan the list
	 * rather than recursing through the expression trees.
	 */
	foreach(lc, partial_target->exprs)
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

#if PG96
/* copied verbatim from selfuncs.c */
bool
get_variable_range(PlannerInfo *root, VariableStatData *vardata, Oid sortop,
				   Datum *min, Datum *max)
{
	uintptr_t	tmin = 0;
	uintptr_t	tmax = 0;
	char		have_data = false;
	short		typLen;
	char		typByVal;
	unsigned int opfuncoid;
	uintptr_t  *values;
	int			nvalues;
	int			i;

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

#if (PG_VERSION_NUM >= 90603)	/* statistic_proc_security_check was added in
								 * 9.6.3 */

	/*
	 * If we can't apply the sortop to the stats data, just fail.  In
	 * principle, if there's a histogram and no MCVs, we could return the
	 * histogram endpoints without ever applying the sortop ... but it's
	 * probably not worth trying, because whatever the caller wants to do with
	 * the endpoints would likely fail the security check too.
	 */
	if (!statistic_proc_security_check(vardata,
									   (opfuncoid = get_opcode(sortop))))
		return false;
#else
	opfuncoid = get_opcode(sortop);
#endif

	get_typlenbyval(vardata->atttype, &typLen, &typByVal);

	/*
	 * If there is a histogram, grab the first and last values.
	 *
	 * If there is a histogram that is sorted with some other operator than
	 * the one we want, fail --- this suggests that there is data we can't
	 * use.
	 */
	if (get_attstatsslot(vardata->statsTuple,
						 vardata->atttype, vardata->atttypmod,
						 STATISTIC_KIND_HISTOGRAM, sortop,
						 NULL,
						 &values, &nvalues,
						 NULL, NULL))
	{
		if (nvalues > 0)
		{
			tmin = datumCopy(values[0], typByVal, typLen);
			tmax = datumCopy(values[nvalues - 1], typByVal, typLen);
			have_data = true;
		}
		free_attstatsslot(vardata->atttype, values, nvalues, NULL, 0);
	}
	else if (get_attstatsslot(vardata->statsTuple,
							  vardata->atttype, vardata->atttypmod,
							  STATISTIC_KIND_HISTOGRAM, InvalidOid,
							  NULL,
							  &values, &nvalues,
							  NULL, NULL))
	{
		free_attstatsslot(vardata->atttype, values, nvalues, NULL, 0);
		return false;
	}

	/*
	 * If we have most-common-values info, look for extreme MCVs.  This is
	 * needed even if we also have a histogram, since the histogram excludes
	 * the MCVs.  However, usually the MCVs will not be the extreme values, so
	 * avoid unnecessary data copying.
	 */
	if (get_attstatsslot(vardata->statsTuple,
						 vardata->atttype, vardata->atttypmod,
						 STATISTIC_KIND_MCV, InvalidOid,
						 NULL,
						 &values, &nvalues,
						 NULL, NULL))
	{
		char		tmin_is_mcv = false;
		char		tmax_is_mcv = false;
		struct FmgrInfo opproc;

		fmgr_info(opfuncoid, &opproc);

		for (i = 0; i < nvalues; i++)
		{
			if (!have_data)
			{
				tmin = tmax = values[i];
				tmin_is_mcv = tmax_is_mcv = have_data = true;
				continue;
			}
			if (DatumGetBool(FunctionCall2Coll(&opproc,
											   DEFAULT_COLLATION_OID,
											   values[i], tmin)))
			{
				tmin = values[i];
				tmin_is_mcv = true;
			}
			if (DatumGetBool(FunctionCall2Coll(&opproc,
											   DEFAULT_COLLATION_OID,
											   tmax, values[i])))
			{
				tmax = values[i];
				tmax_is_mcv = true;
			}
		}
		if (tmin_is_mcv)
			tmin = datumCopy(tmin, typByVal, typLen);
		if (tmax_is_mcv)
			tmax = datumCopy(tmax, typByVal, typLen);
		free_attstatsslot(vardata->atttype, values, nvalues, NULL, 0);
	}

	*min = tmin;
	*max = tmax;
	return have_data;
}
#else
/* copied verbatim from selfuncs.c */
bool
get_variable_range(PlannerInfo *root, VariableStatData *vardata, Oid sortop,
				   Datum *min, Datum *max)
{
	Datum		tmin = 0;
	Datum		tmax = 0;
	bool		have_data = false;
	int16		typLen;
	bool		typByVal;
	Oid			opfuncoid;
	AttStatsSlot sslot;
	int			i;

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
	if (!statistic_proc_security_check(vardata,
									   (opfuncoid = get_opcode(sortop))))
		return false;

	get_typlenbyval(vardata->atttype, &typLen, &typByVal);

	/*
	 * If there is a histogram, grab the first and last values.
	 *
	 * If there is a histogram that is sorted with some other operator than
	 * the one we want, fail --- this suggests that there is data we can't
	 * use.
	 */
	if (get_attstatsslot(&sslot, vardata->statsTuple,
						 STATISTIC_KIND_HISTOGRAM, sortop,
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
	else if (get_attstatsslot(&sslot, vardata->statsTuple,
							  STATISTIC_KIND_HISTOGRAM, InvalidOid,
							  0))
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
	if (get_attstatsslot(&sslot, vardata->statsTuple,
						 STATISTIC_KIND_MCV, InvalidOid,
						 ATTSTATSSLOT_VALUES))
	{
		bool		tmin_is_mcv = false;
		bool		tmax_is_mcv = false;
		FmgrInfo	opproc;

		fmgr_info(opfuncoid, &opproc);

		for (i = 0; i < sslot.nvalues; i++)
		{
			if (!have_data)
			{
				tmin = tmax = sslot.values[i];
				tmin_is_mcv = tmax_is_mcv = have_data = true;
				continue;
			}
			if (DatumGetBool(FunctionCall2Coll(&opproc,
											   DEFAULT_COLLATION_OID,
											   sslot.values[i], tmin)))
			{
				tmin = sslot.values[i];
				tmin_is_mcv = true;
			}
			if (DatumGetBool(FunctionCall2Coll(&opproc,
											   DEFAULT_COLLATION_OID,
											   tmax, sslot.values[i])))
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
#endif
