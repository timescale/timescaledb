/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include "planner/continuous_agg_rewrites.h"

#include <optimizer/tlist.h>
#include <parser/parse_relation.h>
#include <utils/acl.h>
#include <utils/date.h>
#include <utils/timestamp.h>

#include "guc.h"
#include "planner.h"

/* Wrappers for common methods checking Cagg view query validity */
void
check_query_for_cagg_rewrites(CaggRewriteContext *cagg_rewrite_ctx, Query *query)
{
	bool finalized = true;
	bool for_rewrites = true;
	cagg_rewrite_ctx->eligible =
		ts_cagg_query_supported(query, cagg_rewrite_ctx->msg, NULL, finalized, for_rewrites);
}

void
check_query_rtes_for_cagg_rewrites(CaggRewriteContext *cagg_rewrite_ctx, RangeTblEntry *rte)
{
	bool for_rewrites = true;
	cagg_rewrite_ctx->eligible = ts_cagg_query_rtes_supported(rte,
															  &(cagg_rewrite_ctx->ht_rte),
															  cagg_rewrite_ctx->msg,
															  for_rewrites);
}

const Dimension *
check_hypertable_dim_for_cagg_rewrites(CaggRewriteContext *cagg_rewrite_ctx)
{
	bool for_rewrites = true;
	const Dimension *ret = ts_cagg_hypertable_dim_supported(cagg_rewrite_ctx->ht_rte,
															cagg_rewrite_ctx->ht,
															cagg_rewrite_ctx->msg,
															NULL,
															NULL,
															for_rewrites);
	cagg_rewrite_ctx->eligible = (ret != NULL);
	return ret;
}

void
check_timebucket_for_cagg_rewrites(CaggRewriteContext *cagg_rewrite_ctx, Query *query)
{
	Assert(cagg_rewrite_ctx->ht);
	const Dimension *part_dimension = check_hypertable_dim_for_cagg_rewrites(cagg_rewrite_ctx);
	Assert(part_dimension || !cagg_rewrite_ctx->eligible);

	if (part_dimension)
	{
		int32 parent_mat_hypertable_id = INVALID_HYPERTABLE_ID;
		ts_caggtimebucketinfo_init(&cagg_rewrite_ctx->tbinfo,
								   cagg_rewrite_ctx->ht->fd.id,
								   cagg_rewrite_ctx->ht->main_table_relid,
								   part_dimension->column_attno,
								   part_dimension->fd.column_type,
								   part_dimension->fd.interval_length,
								   parent_mat_hypertable_id);
		bool is_cagg_create = false;
		bool for_rewrites = true;
		cagg_rewrite_ctx->eligible =
			ts_caggtimebucket_validate(cagg_rewrite_ctx->tbinfo.bf,
									   query->groupClause,
									   query->targetList,
									   query->rtable,
									   cagg_rewrite_ctx->tbinfo.htpartcolno,
									   cagg_rewrite_ctx->msg,
									   is_cagg_create,
									   for_rewrites);
	}
}

/* Methods to match Vars in queries with same tables joined in different but equivalent order
 */
typedef struct
{
	int sublevels_up;		  /* (current) nesting depth */
	const AttrMap *varno_map; /* map array for user attnos */
} map_varnos_context;

static Node *
map_varnos_mutator(Node *node, map_varnos_context *context)
{
	if (node == NULL)
		return NULL;
	if (IsA(node, Var))
	{
		Var *var = (Var *) node;
		if (var->varlevelsup == (Index) context->sublevels_up &&
			var->varno <= context->varno_map->maplen)
		{
			/* Found a variable, make the substitution */
			Var *newvar = (Var *) palloc(sizeof(Var));
			*newvar = *var; /* initially copy all fields of the Var */

			int new_varno = context->varno_map->attnums[var->varno - 1] + 1;
			newvar->varno = new_varno;
			/* If the syntactic referent is same RTE, fix it too */
			if (newvar->varnosyn == (Index) var->varno)
				newvar->varnosyn = newvar->varno;

			return (Node *) newvar;
		}
	}
	else if (IsA(node, Query))
	{
		/* Recurse into RTE subquery or not-yet-planned sublink subquery */
		Query *newnode;

		context->sublevels_up++;
		newnode = query_tree_mutator((Query *) node, map_varnos_mutator, context, 0);
		context->sublevels_up--;
		return (Node *) newnode;
	}
	return expression_tree_mutator(node, map_varnos_mutator, context);
}

static Node *
map_varnos(Node *node, const AttrMap *varno_map)
{
	map_varnos_context context;

	context.sublevels_up = 0;
	context.varno_map = varno_map;

	/*
	 * Must be prepared to start with a Query or a bare expression tree; if
	 * it's a Query, we don't want to increment sublevels_up.
	 */
	return query_or_expression_tree_mutator(node, map_varnos_mutator, &context, 0);
}

/* Collect flattened quals for comparison */
typedef struct CollectQualsContext
{
	List *quals;

} CollectQualsContext;

static void
flatten_qual(Node *node, CollectQualsContext *ctx)
{
	Node *qual = (Node *) canonicalize_qual((Expr *) node, false);
	qual = (Node *) make_ands_implicit((Expr *) qual);
	if (qual && IsA(qual, List))
		ctx->quals = list_concat(ctx->quals, (List *) qual);
	else if (qual)
		ctx->quals = lappend(ctx->quals, qual);
}

static bool
collect_quals_walker(Node *node, CollectQualsContext *context)
{
	if (node == NULL)
		return false;
	if (IsA(node, FromExpr))
		flatten_qual(((FromExpr *) node)->quals, context);
	else if (IsA(node, JoinExpr))
		flatten_qual(((JoinExpr *) node)->quals, context);

	return expression_tree_walker(node, collect_quals_walker, context);
}

/* Either collect and flatten quals from Query::jointree
 * or flatten a standalone qual like Query::havingQual */
static List *
collect_flattened_quals(Node *node)
{
	CollectQualsContext ctx = { .quals = NULL };
	if (IsA(node, FromExpr))
		collect_quals_walker(node, &ctx);
	else
		flatten_qual(node, &ctx);

	return ctx.quals;
}

/* Match expressions, check equivalencies if possible */
static bool
match_expr(Node *expr1, Node *expr2)
{
	if (IsA(expr1, RangeTblEntry) && IsA(expr2, RangeTblEntry))
	{
		RangeTblEntry *rte1 = castNode(RangeTblEntry, expr1);
		RangeTblEntry *rte2 = castNode(RangeTblEntry, expr2);
		if (rte1->rtekind != rte2->rtekind || rte1->relid != rte2->relid)
			return false;
		if (rte1->rtekind == RTE_RELATION)
			return true;
		else if (rte1->rtekind == RTE_SUBQUERY)
		{
			return (rte1->lateral == rte2->lateral && rte1->inFromCl == rte2->inFromCl &&
					equal(rte1->subquery, rte2->subquery));
		}
	}
	/* Time buckets are matched separately */
	else if (is_time_bucket_function((Expr *) expr1))
		return true;
	/* TBD: check for equivalent OpExprs like "a=5" and "5=a" */
	else if (equal(expr1, expr2))
		return true;

	return false;
}

/* Match every item in src to an item in tgt
 * if "exact=true" also match every item in tgt to an item in src
 */
static bool
match_lists(List *src, List *tgt)
{
	ListCell *ls;
	ListCell *lt;

	foreach (ls, src)
	{
		bool match = false;
		Node *src_item = (Node *) lfirst(ls);
		/* We want to use custom "match_expr" instead of "equal", so can't use "list_member" */
		foreach (lt, tgt)
		{
			Node *tgt_item = (Node *) lfirst(lt);
			if (match_expr(src_item, tgt_item))
			{
				match = true;
				break;
			}
		}
		if (!match)
			return false;
	}
	return true;
}

/* Methods for efficient rewrite of query expressions with subquery targets,
 * borrowed from PostgreSQL and simplified for our use
 * as we call these methods before planning is done.
 */
typedef struct
{
	int varno;			 /* RT index of Var */
	AttrNumber varattno; /* attr number of Var */
	AttrNumber resno;	 /* TLE position of Var */
} ts_tlist_vinfo;

typedef struct
{
	List *tlist;								/* underlying target list */
	int num_vars;								/* number of plain Var tlist entries */
	bool has_non_vars;							/* are there other entries? */
	ts_tlist_vinfo vars[FLEXIBLE_ARRAY_MEMBER]; /* has num_vars entries */
} ts_indexed_tlist;

typedef struct
{
	ts_indexed_tlist *subplan_itlist;
	int newvarno;
	int rtoffset;
	bool match;
} replace_tlist_expr_context;

static ts_indexed_tlist *
ts_build_tlist_index(List *tlist)
{
	ts_indexed_tlist *itlist;
	ts_tlist_vinfo *vinfo;
	ListCell *l;

	/* Create data structure with enough slots for all tlist entries */
	itlist = (ts_indexed_tlist *) palloc(offsetof(ts_indexed_tlist, vars) +
										 list_length(tlist) * sizeof(ts_tlist_vinfo));

	itlist->tlist = tlist;
	itlist->has_non_vars = false;

	/* Find the Vars and fill in the index array */
	vinfo = itlist->vars;
	foreach (l, tlist)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(l);

		if (tle->expr && IsA(tle->expr, Var))
		{
			Var *var = (Var *) tle->expr;

			vinfo->varno = var->varno;
			vinfo->varattno = var->varattno;
			vinfo->resno = tle->resno;
			vinfo++;
		}
		else
			itlist->has_non_vars = true;
	}

	itlist->num_vars = (vinfo - itlist->vars);

	return itlist;
}

static Var *
ts_search_indexed_tlist_for_var(Var *var, ts_indexed_tlist *itlist, int newvarno, int rtoffset)
{
	int varno = var->varno;
	AttrNumber varattno = var->varattno;
	ts_tlist_vinfo *vinfo;
	int i;

	vinfo = itlist->vars;
	i = itlist->num_vars;
	while (i-- > 0)
	{
		if (vinfo->varno == varno && vinfo->varattno == varattno)
		{
			/* Found a match */
			Var *newvar = (Var *) copyObject(var);
			newvar->varno = newvarno;
			newvar->varattno = vinfo->resno;
			if (newvar->varnosyn > 0)
				newvar->varnosyn += rtoffset;
			return newvar;
		}
		vinfo++;
	}
	return NULL; /* no match */
}

static Var *
ts_search_indexed_tlist_for_non_var(Expr *node, ts_indexed_tlist *itlist, int newvarno)
{
	TargetEntry *tle;

	/*
	 * If it's a simple Const, replacing it with a Var is silly, even if there
	 * happens to be an identical Const below; a Var is more expensive to
	 * execute than a Const.  What's more, replacing it could confuse some
	 * places in the executor that expect to see simple Consts for, eg,
	 * dropped columns.
	 */
	if (IsA(node, Const))
		return NULL;

	tle = tlist_member(node, itlist->tlist);
	if (tle)
	{
		/* Found a matching subplan output expression */
		Var *newvar;

		newvar = makeVarFromTargetEntry(newvarno, tle);
		newvar->varnosyn = 0; /* wasn't ever a plain Var */
		newvar->varattnosyn = 0;
		return newvar;
	}
	return NULL; /* no match */
}

static Node *
replace_tlist_expr_mutator(Node *node, replace_tlist_expr_context *context)
{
	Var *newvar;

	if (node == NULL)
		return NULL;
	if (IsA(node, Var))
	{
		Var *var = (Var *) node;

		newvar = ts_search_indexed_tlist_for_var(var,
												 context->subplan_itlist,
												 context->newvarno,
												 context->rtoffset);
		if (!newvar)
		{
			context->match = false;
			return node;
		}
		return (Node *) newvar;
	}
	/* Try matching more complex expressions too, if tlist has any */
	if (context->subplan_itlist->has_non_vars)
	{
		newvar = ts_search_indexed_tlist_for_non_var((Expr *) node,
													 context->subplan_itlist,
													 context->newvarno);
		if (newvar)
			return (Node *) newvar;
	}
	return expression_tree_mutator(node, replace_tlist_expr_mutator, context);
}

/* Will try to rewrite query expression with subquery targets,
 * returns NULL if can't fully rewrite the expression.
 */
static Node *
replace_tlist_expr(Node *node, ts_indexed_tlist *subplan_itlist, int newvarno, int rtoffset)
{
	replace_tlist_expr_context context;

	context.subplan_itlist = subplan_itlist;
	context.newvarno = newvarno;
	context.rtoffset = rtoffset;
	context.match = true;
	Node *result = replace_tlist_expr_mutator(node, &context);
	if (!context.match)
		return NULL;
	else
		return result;
}

void
match_query_to_cagg(CaggRewriteContext *cagg_rewrite_ctx, Query *query, bool do_rewrite)
{
	if (!ts_hypertable_has_continuous_aggregates(cagg_rewrite_ctx->ht->fd.id))
	{
		cagg_rewrite_ctx->eligible = false;
		if (ts_guc_cagg_rewrites_debug_info)
			appendStringInfo(cagg_rewrite_ctx->msg,
							 "no continuous aggregates defined on \"%s.%s\"",
							 NameStr(cagg_rewrite_ctx->ht->fd.schema_name),
							 NameStr(cagg_rewrite_ctx->ht->fd.table_name));
		return;
	}

	List *caggs = ts_continuous_aggs_find_by_raw_table_id(cagg_rewrite_ctx->ht->fd.id);
	ListCell *l;
	foreach (l, caggs)
	{
		ContinuousAgg *cagg = lfirst(l);
		/* Can only rewrite with finalized Caggs with matching bucket */
		if (!cagg->data.finalized ||
			!caggtimebucket_equal(cagg->bucket_function, cagg_rewrite_ctx->tbinfo.bf)
			/* TEMP: only consider real-time Caggs */
			|| cagg->data.materialized_only)
			continue;

		Query *view_query = ts_continuous_agg_get_query(cagg);

		/* joins need to be matched exactly, group clauses also as we don't allow reaggregation */
		if (list_length(query->rtable) != list_length(view_query->rtable) ||
			list_length(query->groupClause) != list_length(view_query->groupClause)
			/* If Cagg has quals, they'll need to be exactly matched in the query */
			|| (view_query->jointree->quals && !query->jointree->quals) ||
			(!view_query->jointree->quals && query->jointree->quals)
			/* original query HAVING clause derived from Cagg targets is allowed */
			|| (view_query->havingQual && !query->havingQual))
			continue;

		bool ht_only = false;
		AttrMap *varno_map = NULL;
#if PG18_GE
		/* hypertable RTE + group RTE */
		ht_only = (list_length(query->rtable) == 2);
#else
		ht_only = (list_length(query->rtable) == 1);
#endif
		ListCell *lq;
		ListCell *lv;
		bool same_join_order = true;
		/* have to match a join
		 * "FROM t1, t2" can be matched to "FROM t2, t1"
		 * so we'll record if this is the same join but with different order
		 */
		if (!ht_only)
		{
			int varno_maplen = list_length(query->rtable);
#if PG18_GE
			--varno_maplen;
#endif
			varno_map = make_attrmap(varno_maplen);
			int qrti = 0;
			bool match = true;
			foreach (lq, query->rtable)
			{
				RangeTblEntry *qrte = lfirst_node(RangeTblEntry, lq);
#if PG18_GE
				if (qrte->rtekind == RTE_GROUP)
					break;
#endif
				bool found = false;
				int vrti = 0;
				foreach (lv, view_query->rtable)
				{
					RangeTblEntry *vrte = lfirst_node(RangeTblEntry, lv);
					if (match_expr((Node *) qrte, (Node *) vrte))
					{
						found = true;
						varno_map->attnums[qrti] = vrti;
						if (qrti != vrti)
							same_join_order = false;
						break;
					}
					++vrti;
				}
				if (!found)
				{
					match = false;
					break;
				}
				++qrti;
			}
			if (!match)
			{
				free_attrmap(varno_map);
				varno_map = NULL;
				continue;
			}
			if (same_join_order)
			{
				free_attrmap(varno_map);
				varno_map = NULL;
			}
		}

		Query *newquery = (Query *) copyObject(query);
		if (varno_map)
			newquery = (Query *) map_varnos((Node *) newquery, varno_map);

		bool timebucket_only = (list_length(newquery->groupClause) == 1);
		/* have to match group expressions */
		if (!timebucket_only)
		{
			List *query_group_exprs;
			List *view_group_exprs;
#if PG18_LT
			query_group_exprs =
				get_sortgrouplist_exprs(newquery->groupClause, newquery->targetList);
			view_group_exprs =
				get_sortgrouplist_exprs(view_query->groupClause, view_query->targetList);
#endif
#if PG18_GE
			RangeTblEntry *qrte = llast_node(RangeTblEntry, newquery->rtable);
			Assert(qrte->rtekind == RTE_GROUP);
			query_group_exprs = qrte->groupexprs;

			RangeTblEntry *vrte = llast_node(RangeTblEntry, view_query->rtable);
			Assert(vrte->rtekind == RTE_GROUP);
			view_group_exprs = vrte->groupexprs;
#endif
			/* TBD: exclude time buckets from the group exprs to be matched */
			if (!match_lists(view_group_exprs, query_group_exprs))
				continue;
		}

		/* have to match aggregate expressions */
		List *qaggrefs = ts_find_aggrefs((Node *) newquery->targetList);
		List *vaggrefs = ts_find_aggrefs((Node *) view_query->targetList);
		/* every query aggregate should match Cagg aggregate */
		if (!match_lists(qaggrefs, vaggrefs))
			continue;

		/* have to match view HAVING quals, if any */
		if (view_query->havingQual)
		{
			List *query_having = collect_flattened_quals(newquery->havingQual);
			List *view_having = collect_flattened_quals(view_query->havingQual);
			/* Every Cagg havingQual needs to be matched in the query */
			if (!match_lists(view_having, query_having))
				continue;
		}

		/* have to exactly match join and where quals, if any
		 * we flatten quals as we collect them
		 */
		List *query_quals = collect_flattened_quals((Node *) newquery->jointree);
		List *view_quals = collect_flattened_quals((Node *) view_query->jointree);
		/* Cannot trust same length lists as can have duplicate quals */
		if (!match_lists(view_quals, query_quals))
			continue;
		if (!match_lists(query_quals, view_quals))
			continue;

		/* Have to derive query TLEs and query HAVING from view TLEs
		 * i.e. (query.bucket + 1)*2 from (view.bucket + 1)
		 */
		ts_indexed_tlist *itlist = ts_build_tlist_index(view_query->targetList);
		Node *new_tlist = replace_tlist_expr((Node *) newquery->targetList, itlist, 1, 0);
		if (new_tlist)
			newquery->targetList = (List *) new_tlist;
		else
			continue;

		if (newquery->havingQual)
		{
			Node *new_having = replace_tlist_expr(newquery->havingQual, itlist, 1, 0);
			if (new_having)
				newquery->havingQual = new_having;
			else
				continue;
		}

		/* Found matching CAgg: rewrite query with it if asked */
		cagg_rewrite_ctx->cagg = cagg;
		if (!do_rewrite)
			return;

		/* rewrite the query with Cagg:
		 * TLEs and HAVING are already rewritten,
		 * also need to drop quals, group exprs, joins, turn Having into Where,
		 * keep order by and limit.
		 */
		Query *cagg_finalized_query = ts_continuous_agg_get_finalized_query(cagg);
		Assert(cagg_finalized_query);

		query->targetList = newquery->targetList;
		query->groupClause = NULL;
		query->hasAggs = false;
#if PG18_GE
		query->hasGroupRTE = false;
#endif
		RangeTblEntry *newrte = makeNode(RangeTblEntry);
		newrte->rtekind = RTE_SUBQUERY;
		newrte->relkind = RELKIND_VIEW;
		newrte->relid = cagg->relid;
		newrte->inh = false;
		newrte->alias = NULL;
		newrte->eref = makeAlias(cagg->data.user_view_name.data, NIL);
		newrte->subquery = copyObject(cagg_finalized_query);
		newrte->inFromCl = true;
		query->rtable = list_make1(newrte);

#if PG16_LT
		newrte->requiredPerms = 0;
		newrte->requiredPerms |= ACL_SELECT;
		newrte->checkAsUser = InvalidOid; /* not set-uid by default, either */
		newrte->selectedCols = NULL;
		newrte->insertedCols = NULL;
		newrte->updatedCols = NULL;
#else
		query->rteperminfos = NULL;
		/* Add empty perminfo for the new RTE to make build_simple_rel happy. */
		RTEPermissionInfo *perminfo = addRTEPermissionInfo(&query->rteperminfos, newrte);
		perminfo->requiredPerms |= ACL_SELECT;
		/* why? */
		perminfo->inh = true;
#endif
		int attno = 0;
		foreach (lv, cagg_finalized_query->targetList)
		{
			TargetEntry *tle = lfirst_node(TargetEntry, lv);
			if (tle->resjunk)
				continue;
			newrte->eref->colnames = lappend(newrte->eref->colnames, makeString(tle->resname));
			attno = list_length(newrte->eref->colnames) - FirstLowInvalidHeapAttributeNumber;
#if PG16_LT
			newrte->selectedCols = bms_add_member(newrte->selectedCols, attno);
#else
			perminfo->selectedCols = bms_add_member(perminfo->selectedCols, attno);
#endif
		}
		foreach (lq, query->targetList)
		{
			TargetEntry *tle = lfirst_node(TargetEntry, lq);
			tle->resorigtbl = newrte->relid;
			if (IsA(tle->expr, Var))
				tle->resorigcol = castNode(Var, tle->expr)->varattno;
		}

		RangeTblRef *rtr = makeNode(RangeTblRef);
		rtr->rtindex = 1;
		query->jointree = makeFromExpr(list_make1(rtr), NULL);
		query->jointree->quals = newquery->havingQual;

		/* TBD: call QueryRewrite on Cagg view subquery as it may have nested views
		 *  we need to acquire locks on the view subquery tree as it's been newly added
		 */
		AcquireRewriteLocks(newrte->subquery, true, false);

		break;
	}

	if (!cagg_rewrite_ctx->cagg)
	{
		cagg_rewrite_ctx->eligible = false;
		if (ts_guc_cagg_rewrites_debug_info)
			appendStringInfo(cagg_rewrite_ctx->msg,
							 "none of continuous aggregates defined on \"%s.%s\" are matching the "
							 "query",
							 NameStr(cagg_rewrite_ctx->ht->fd.schema_name),
							 NameStr(cagg_rewrite_ctx->ht->fd.table_name));
	}
}
