/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

/*
 * This file contains the code related to the *NOT* finalized version of
 * Continuous Aggregates (with partials)
 */
#include "finalize.h"

#include <parser/parse_relation.h>

#include "create.h"
#include "common.h"
#include <partialize_finalize.h>

/* Static function prototypes */
static Var *mattablecolumninfo_addentry(MatTableColumnInfo *out, Node *input,
										int original_query_resno, bool finalized,
										bool *skip_adding);
static inline void makeMaterializeColumnName(char *colbuf, const char *type,
											 int original_query_resno, int colno);

static inline void
makeMaterializeColumnName(char *colbuf, const char *type, int original_query_resno, int colno)
{
	int ret = snprintf(colbuf, NAMEDATALEN, "%s_%d_%d", type, original_query_resno, colno);
	if (ret < 0 || ret >= NAMEDATALEN)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR), errmsg("bad materialization table column name")));
}

/*
 * Init the finalize query data structure.
 * Parameters:
 * orig_query - the original query from user view that is being used as template for the finalize
 * query tlist_aliases - aliases for the view select list materialization table columns are created
 * . This will be returned in  the mattblinfo
 *
 * DO NOT modify orig_query. Make a copy if needed.
 * SIDE_EFFECT: the data structure in mattblinfo is modified as a side effect by adding new
 * materialize table columns and partialize exprs.
 */
void
finalizequery_init(FinalizeQueryInfo *inp, Query *orig_query, MatTableColumnInfo *mattblinfo)
{
	ListCell *lc;
	int resno = 1;

	inp->final_userquery = copyObject(orig_query);
	inp->final_seltlist = NIL;
	inp->final_havingqual = NULL;

	/*
	 * We want all the entries in the targetlist (resjunk or not)
	 * in the materialization  table definition so we include group-by/having clause etc.
	 * We have to do 3 things here:
	 * 1) create a column for mat table
	 * 2) partialize_expr to populate it, and
	 * 3) modify the target entry to be a finalize_expr
	 *    that selects from the materialization table.
	 */
	foreach (lc, orig_query->targetList)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(lc);
		TargetEntry *modte = copyObject(tle);

		/*
		 * We need columns for non-aggregate targets.
		 * If it is not a resjunk OR appears in the grouping clause.
		 */
		if (tle->resjunk == false || tle->ressortgroupref > 0)
		{
			Var *var;
			bool skip_adding = false;
			var = mattablecolumninfo_addentry(mattblinfo,
											  (Node *) tle,
											  resno,
											  inp->finalized,
											  &skip_adding);

			/* Skip adding this column for finalized form. */
			if (skip_adding)
			{
				continue;
			}

			/* Fix the expression for the target entry. */
			modte->expr = (Expr *) var;
		}
		/*
		 * Construct the targetlist for the query on the
		 * materialization table. The TL maps 1-1 with the original query:
		 * e.g select a, min(b)+max(d) from foo group by a,timebucket(a);
		 * becomes
		 * select <a-col>,
		 * ts_internal_cagg_final(..b-col ) + ts_internal_cagg_final(..d-col)
		 * from mattbl
		 * group by a-col, timebucket(a-col)
		 */

		/*
		 * We copy the modte target entries, resnos should be the same for
		 * final_selquery and origquery. So tleSortGroupReffor the targetentry
		 * can be reused, only table info needs to be modified.
		 */
		Assert(inp->finalized && modte->resno >= resno);
		resno++;
		if (IsA(modte->expr, Var))
		{
			modte->resorigcol = ((Var *) modte->expr)->varattno;
		}
		inp->final_seltlist = lappend(inp->final_seltlist, modte);
	}
}

/*
 * Create select query with the finalize aggregates
 * for the materialization table.
 * matcollist - column list for mat table
 * mattbladdress - materialization table ObjectAddress
 * This is the function responsible for creating the final
 * structures for selecting from the materialized hypertable
 * created for the Cagg which is
 * select * from _timescaldeb_internal._materialized_hypertable_<xxx>
 */
Query *
finalizequery_get_select_query(FinalizeQueryInfo *inp, List *matcollist,
							   ObjectAddress *mattbladdress, char *relname)
{
	Query *final_selquery = NULL;
	ListCell *lc;
	FromExpr *fromexpr;
	RangeTblEntry *rte;
#if PG16_GE
	RTEPermissionInfo *perminfo;
#endif

	CAGG_MAKEQUERY(final_selquery, inp->final_userquery);
	final_selquery->hasAggs = !inp->finalized;

	/*
	 * For initial cagg creation rtable will have only 1 entry,
	 * for alter table rtable will have multiple entries with our
	 * RangeTblEntry as last member.
	 * For cagg with joins, we need to create a new RTE and jointree
	 * which contains the information of the materialised hypertable
	 * that is created for this cagg.
	 */
	if (list_length(inp->final_userquery->jointree->fromlist) >=
			CONTINUOUS_AGG_MAX_JOIN_RELATIONS ||
		!IsA(linitial(inp->final_userquery->jointree->fromlist), RangeTblRef))
	{
		rte = makeNode(RangeTblEntry);
		rte->alias = makeAlias(relname, NIL);
		rte->inFromCl = true;
		rte->inh = true;
		rte->rellockmode = 1;
		rte->eref = copyObject(rte->alias);
		rte->relid = mattbladdress->objectId;
#if PG16_GE
		perminfo = addRTEPermissionInfo(&final_selquery->rteperminfos, rte);
		perminfo->selectedCols = NULL;
#endif
		ListCell *l;
		foreach (l, inp->final_userquery->jointree->fromlist)
		{
			/*
			 * In case of joins, update the rte with all the join related struct.
			 */
			Node *jtnode = (Node *) lfirst(l);
			JoinExpr *join = NULL;
			if (IsA(jtnode, JoinExpr))
			{
				join = castNode(JoinExpr, jtnode);
				RangeTblEntry *jrte = rt_fetch(join->rtindex, inp->final_userquery->rtable);
				rte->joinaliasvars = jrte->joinaliasvars;
				rte->jointype = jrte->jointype;
				rte->joinleftcols = jrte->joinleftcols;
				rte->joinrightcols = jrte->joinrightcols;
				rte->joinmergedcols = jrte->joinmergedcols;
#if PG14_GE
				rte->join_using_alias = jrte->join_using_alias;
#endif
#if PG16_LT
				rte->selectedCols = jrte->selectedCols;
#else
				if (jrte->perminfoindex > 0)
				{
					RTEPermissionInfo *jperminfo =
						getRTEPermissionInfo(inp->final_userquery->rteperminfos, jrte);
					perminfo->selectedCols = jperminfo->selectedCols;
				}
#endif
			}
		}
	}
	else
	{
		rte = llast_node(RangeTblEntry, inp->final_userquery->rtable);
		rte->eref->colnames = NIL;
#if PG16_LT
		rte->selectedCols = NULL;
#else
		perminfo = getRTEPermissionInfo(inp->final_userquery->rteperminfos, rte);
		perminfo->selectedCols = NULL;
#endif
	}
	if (rte->eref->colnames == NIL)
	{
		/*
		 * We only need to do this for the case when there is no Join node in the query.
		 * In the case of join, rte->eref is already populated by jrte->eref and hence the
		 * relevant info, so need not to do this.
		 */

		/* Aliases for column names for the materialization table. */
		foreach (lc, matcollist)
		{
			ColumnDef *cdef = lfirst_node(ColumnDef, lc);
			rte->eref->colnames = lappend(rte->eref->colnames, makeString(cdef->colname));
			int attno = list_length(rte->eref->colnames) - FirstLowInvalidHeapAttributeNumber;
#if PG16_LT
			rte->selectedCols = bms_add_member(rte->selectedCols, attno);
#else
			perminfo->selectedCols = bms_add_member(perminfo->selectedCols, attno);
#endif
		}
	}
	rte->relid = mattbladdress->objectId;
	rte->rtekind = RTE_RELATION;
	rte->relkind = RELKIND_RELATION;
	rte->tablesample = NULL;
#if PG16_LT
	rte->requiredPerms |= ACL_SELECT;
	rte->insertedCols = NULL;
	rte->updatedCols = NULL;
#else
	perminfo->relid = mattbladdress->objectId;
	perminfo->requiredPerms |= ACL_SELECT;
	perminfo->insertedCols = NULL;
	perminfo->updatedCols = NULL;
#endif

	/* 2. Fixup targetlist with the correct rel information. */
	foreach (lc, inp->final_seltlist)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(lc);
		/*
		 * In case when this is a cagg with joins, the Var from the normal table
		 * already has resorigtbl populated and we need to use that to resolve
		 * the Var. Hence only modify the tle when resorigtbl is unset
		 * which means it is Var of the Hypertable
		 */
		if (IsA(tle->expr, Var) && !OidIsValid(tle->resorigtbl))
		{
			tle->resorigtbl = rte->relid;
			tle->resorigcol = ((Var *) tle->expr)->varattno;
		}
	}

	if (list_length(inp->final_userquery->jointree->fromlist) >=
			CONTINUOUS_AGG_MAX_JOIN_RELATIONS ||
		!IsA(linitial(inp->final_userquery->jointree->fromlist), RangeTblRef))
	{
		RangeTblRef *rtr;
		final_selquery->rtable = list_make1(rte);
#if PG16_GE
		/* perminfo has been set already in the previous if/else */
		Assert(list_length(final_selquery->rteperminfos) == 1);
#endif
		rtr = makeNode(RangeTblRef);
		rtr->rtindex = 1;
		fromexpr = makeFromExpr(list_make1(rtr), NULL);
	}
	else
	{
		final_selquery->rtable = inp->final_userquery->rtable;
#if PG16_GE
		final_selquery->rteperminfos = inp->final_userquery->rteperminfos;
#endif
		fromexpr = inp->final_userquery->jointree;
		fromexpr->quals = NULL;
	}

	/*
	 * Fixup from list. No quals on original table should be
	 * present here - they should be on the query that populates
	 * the mattable (partial_selquery). For the Cagg with join,
	 * we can not copy the fromlist from inp->final_userquery as
	 * it has two tables in this case.
	 */
	Assert(list_length(inp->final_userquery->jointree->fromlist) <=
		   CONTINUOUS_AGG_MAX_JOIN_RELATIONS);

	final_selquery->jointree = fromexpr;
	final_selquery->targetList = inp->final_seltlist;
	final_selquery->sortClause = inp->final_userquery->sortClause;

	/* Already finalized query no need to copy group by or having clause. */

	return final_selquery;
}

/*
 * Add Information required to create and populate the materialization table columns
 * a) create a columndef for the materialization table
 * b) create the corresponding expr to populate the column of the materialization table (e..g for a
 *    column that is an aggref, we create a partialize_agg expr to populate the column Returns: the
 *    Var corresponding to the newly created column of the materialization table
 *
 * Notes: make sure the materialization table columns do not save
 *        values computed by mutable function.
 *
 * Notes on TargetEntry fields:
 * - (resname != NULL) means it's projected in our case
 * - (ressortgroupref > 0) means part of GROUP BY, which can be projected or not, depending of the
 *                         value of the resjunk
 * - (resjunk == true) applies for GROUP BY columns that are not projected
 *
 */
static Var *
mattablecolumninfo_addentry(MatTableColumnInfo *out, Node *input, int original_query_resno,
							bool finalized, bool *skip_adding)
{
	int matcolno = list_length(out->matcollist) + 1;
	char colbuf[NAMEDATALEN];
	char *colname;
	TargetEntry *part_te = NULL;
	ColumnDef *col;
	Var *var;
	Oid coltype = InvalidOid, colcollation = InvalidOid;
	int32 coltypmod;

	*skip_adding = false;

	if (contain_mutable_functions(input))
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("only immutable functions supported in continuous aggregate view"),
				 errhint("Make sure all functions in the continuous aggregate definition"
						 " have IMMUTABLE volatility. Note that functions or expressions"
						 " may be IMMUTABLE for one data type, but STABLE or VOLATILE for"
						 " another.")));
	}

	switch (nodeTag(input))
	{
		case T_TargetEntry:
		{
			TargetEntry *tle = (TargetEntry *) input;
			bool timebkt_chk = false;

			if (IsA(tle->expr, FuncExpr))
				timebkt_chk = function_allowed_in_cagg_definition(((FuncExpr *) tle->expr)->funcid);

			if (tle->resname)
				colname = pstrdup(tle->resname);
			else
			{
				if (timebkt_chk)
					colname = DEFAULT_MATPARTCOLUMN_NAME;
				else
				{
					makeMaterializeColumnName(colbuf, "grp", original_query_resno, matcolno);
					colname = colbuf;

					/* For finalized form we skip adding extra group by columns. */
					*skip_adding = finalized;
				}
			}

			if (timebkt_chk)
			{
				tle->resname = pstrdup(colname);
				out->matpartcolno = matcolno;
				out->matpartcolname = pstrdup(colname);
			}
			else
			{
				/*
				 * Add indexes only for columns that are part of the GROUP BY clause
				 * and for finals form.
				 * We skip adding it because we'll not add the extra group by columns
				 * to the materialization hypertable anymore.
				 */
				if (!*skip_adding && tle->ressortgroupref > 0)
					out->mat_groupcolname_list =
						lappend(out->mat_groupcolname_list, pstrdup(colname));
			}

			coltype = exprType((Node *) tle->expr);
			coltypmod = exprTypmod((Node *) tle->expr);
			colcollation = exprCollation((Node *) tle->expr);
			col = makeColumnDef(colname, coltype, coltypmod, colcollation);
			part_te = (TargetEntry *) copyObject(input);

			/* Keep original resjunk if not time bucket. */
			if (timebkt_chk)
			{
				/*
				 * Need to project all the partial entries so that
				 * materialization table is filled.
				 */
				part_te->resjunk = false;
			}

			part_te->resno = matcolno;

			if (timebkt_chk)
			{
				col->is_not_null = true;
			}

			if (part_te->resname == NULL)
			{
				part_te->resname = pstrdup(colname);
			}
		}
		break;

		case T_Var:
		{
			makeMaterializeColumnName(colbuf, "var", original_query_resno, matcolno);
			colname = colbuf;

			coltype = exprType(input);
			coltypmod = exprTypmod(input);
			colcollation = exprCollation(input);
			col = makeColumnDef(colname, coltype, coltypmod, colcollation);
			part_te = makeTargetEntry((Expr *) input, matcolno, pstrdup(colname), false);

			/* Need to project all the partial entries so that materialization table is filled. */
			part_te->resjunk = false;
			part_te->resno = matcolno;
		}
		break;

		default:
			elog(ERROR, "invalid node type %d", nodeTag(input));
			break;
	}
	Assert(finalized && list_length(out->matcollist) <= list_length(out->partial_seltlist));
	Assert(col != NULL);
	Assert(part_te != NULL);

	if (!*skip_adding)
	{
		out->matcollist = lappend(out->matcollist, col);
	}

	out->partial_seltlist = lappend(out->partial_seltlist, part_te);

	var = makeVar(1, matcolno, coltype, coltypmod, colcollation, 0);
	return var;
}
