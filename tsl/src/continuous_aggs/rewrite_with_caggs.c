/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include "rewrite_with_caggs.h"

#include <optimizer/tlist.h>
#include <parser/parse_relation.h>
#include <rewrite/rewriteDefine.h>
#include <utils/acl.h>
#include <utils/date.h>
#include <utils/timestamp.h>

#include "guc.h"
#include "import/setrefs.h"
#include "invalidation.h"
#include "utils.h"

#if PG16_GE

static bool match_lists(List *src, List *tgt);

/* Wrappers for common methods checking Cagg view query validity */
static void
check_query_for_cagg_rewrites(CaggRewriteContext *cagg_rewrite_ctx, Query *query)
{
	bool for_rewrites = true;
	cagg_rewrite_ctx->eligible =
		cagg_query_supported(query, &(cagg_rewrite_ctx->msg), NULL, for_rewrites);
}

static void
check_query_rtes_for_cagg_rewrites(CaggRewriteContext *cagg_rewrite_ctx, RangeTblEntry *rte)
{
	bool for_rewrites = true;
	cagg_rewrite_ctx->eligible = cagg_query_rtes_supported(rte,
														   &(cagg_rewrite_ctx->ht_rte),
														   &(cagg_rewrite_ctx->msg),
														   for_rewrites);
}

static const Dimension *
check_hypertable_dim_for_cagg_rewrites(CaggRewriteContext *cagg_rewrite_ctx)
{
	bool for_rewrites = true;
	const Dimension *ret = cagg_hypertable_dim_supported(cagg_rewrite_ctx->ht_rte,
														 cagg_rewrite_ctx->ht,
														 &(cagg_rewrite_ctx->msg),
														 NULL,
														 NULL,
														 for_rewrites);
	cagg_rewrite_ctx->eligible = (ret != NULL);
	return ret;
}

static void
check_timebucket_for_cagg_rewrites(CaggRewriteContext *cagg_rewrite_ctx, Query *query)
{
	Assert(cagg_rewrite_ctx->ht);
	const Dimension *part_dimension = check_hypertable_dim_for_cagg_rewrites(cagg_rewrite_ctx);
	Assert(part_dimension || !cagg_rewrite_ctx->eligible);

	if (part_dimension)
	{
		int32 parent_mat_hypertable_id = INVALID_HYPERTABLE_ID;
		caggtimebucketinfo_init(&cagg_rewrite_ctx->tbinfo,
								cagg_rewrite_ctx->ht->fd.id,
								cagg_rewrite_ctx->ht->main_table_relid,
								part_dimension->column_attno,
								part_dimension->fd.column_type,
								part_dimension->fd.interval_length,
								parent_mat_hypertable_id);
		bool is_cagg_create = false;
		bool for_rewrites = true;
		cagg_rewrite_ctx->eligible =
			caggtimebucket_validate_common(cagg_rewrite_ctx->tbinfo.bf,
										   query->groupClause,
										   query->targetList,
										   query->rtable,
										   cagg_rewrite_ctx->tbinfo.htpartcolno,
										   &(cagg_rewrite_ctx->msg),
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
	Node *qual = eval_const_expressions(NULL, node);
	qual = (Node *) canonicalize_qual((Expr *) qual, false);
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
		if (rte1->relkind == rte2->relkind && rte1->relkind == RELKIND_VIEW &&
			rte1->relid == rte2->relid)
			return true;
		if (rte1->rtekind != rte2->rtekind || rte1->relid != rte2->relid)
			return false;
		if (rte1->rtekind == RTE_RELATION)
			return true;
		else if (rte1->rtekind == RTE_SUBQUERY)
		{
			return (rte1->lateral == rte2->lateral && rte1->inFromCl == rte2->inFromCl &&
					equal(rte1->subquery, rte2->subquery));
		}
		else
			return equal(rte1, rte2);
	}
	/* Time buckets are matched separately */
	else if (ts_is_time_bucket_function((Expr *) expr1))
		return true;
	else if (equal(expr1, expr2))
		return true;
	/* Check for equivalent OpExprs like "a=5" and "5=a" or "a<b" and "b>a" */
	else if (IsA(expr1, OpExpr) && IsA(expr2, OpExpr))
	{
		OpExpr *op1 = castNode(OpExpr, expr1);
		OpExpr *op2 = castNode(OpExpr, expr2);
		if (list_length(op1->args) == 2 && list_length(op2->args) == 2 &&
			match_expr((Node *) linitial(op1->args), (Node *) lsecond(op2->args)) &&
			match_expr((Node *) linitial(op2->args), (Node *) lsecond(op1->args)))
		{
			Oid pred_op = get_commutator(op1->opno);
			if (OidIsValid(pred_op) && pred_op == op2->opno)
				return true;
		}
	}

	return false;
}

/* Match every item in src to an item in tgt
 */
bool
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

static inline void
add_optional_debug_info(ContinuousAgg *cagg, StringInfo *str, char *init_str)
{
	if (ts_guc_cagg_rewrites_debug_info)
	{
		if (*str == NULL)
		{
			*str = makeStringInfo();
			appendStringInfoString(*str, init_str);
		}
		appendStringInfo(*str,
						 " \"%s.%s\"",
						 NameStr(cagg->data.user_view_schema),
						 NameStr(cagg->data.user_view_name));
	}
}

/* Tries to match aggregation query over a hypertable to Caggs defined on this hypertable */
static void
match_query_to_cagg(CaggRewriteContext *cagg_rewrite_ctx, Query *query, bool do_rewrite)
{
	if (!ts_hypertable_has_continuous_aggregates(cagg_rewrite_ctx->ht->fd.id))
	{
		cagg_rewrite_ctx->eligible = false;
		if (ts_guc_cagg_rewrites_debug_info)
			appendStringInfo(&(cagg_rewrite_ctx->msg),
							 "no continuous aggregates defined on \"%s.%s\"",
							 (cagg_rewrite_ctx->cagg_parent ?
								  NameStr(cagg_rewrite_ctx->cagg_parent->data.user_view_schema) :
								  NameStr(cagg_rewrite_ctx->ht->fd.schema_name)),
							 (cagg_rewrite_ctx->cagg_parent ?
								  NameStr(cagg_rewrite_ctx->cagg_parent->data.user_view_name) :
								  NameStr(cagg_rewrite_ctx->ht->fd.table_name)));
		return;
	}

	if (invalidation_hypertable_has_invalidations(cagg_rewrite_ctx->ht->fd.id))
	{
		cagg_rewrite_ctx->eligible = false;
		if (ts_guc_cagg_rewrites_debug_info)
			appendStringInfo(&(cagg_rewrite_ctx->msg),
							 "invalidated ranges are present in hypertable \"%s.%s\"",
							 (cagg_rewrite_ctx->cagg_parent ?
								  NameStr(cagg_rewrite_ctx->cagg_parent->data.user_view_schema) :
								  NameStr(cagg_rewrite_ctx->ht->fd.schema_name)),
							 (cagg_rewrite_ctx->cagg_parent ?
								  NameStr(cagg_rewrite_ctx->cagg_parent->data.user_view_name) :
								  NameStr(cagg_rewrite_ctx->ht->fd.table_name)));
		return;
	}

	/* Diagnostics on matching steps  */
	StringInfo not_realtime = NULL;
	StringInfo invalidated = NULL;
	StringInfo nonmatching_buckets = NULL;
	StringInfo nonmatching_joins = NULL;
	StringInfo nonmatching_groupby = NULL;
	StringInfo nonmatching_aggrefs = NULL;
	StringInfo nonmatching_quals = NULL;
	StringInfo nonmatching_having_quals = NULL;
	StringInfo nonmatching_targets = NULL;

	List *caggs = ts_continuous_aggs_find_by_raw_table_id(cagg_rewrite_ctx->ht->fd.id);
	ListCell *l;
	foreach (l, caggs)
	{
		ContinuousAgg *cagg = lfirst(l);
		/* Can only rewrite with Caggs with matching bucket */
		if (!caggtimebucket_equal(cagg->bucket_function, cagg_rewrite_ctx->tbinfo.bf))
		{
			add_optional_debug_info(cagg, &nonmatching_buckets, "Buckets do not match:");
			continue;
		}
		/* TEMP: only consider real-time Caggs */
		if (cagg->data.materialized_only)
		{
			add_optional_debug_info(cagg, &not_realtime, "Caggs are not real-time:");
			continue;
		}
		/* TEMP: only consider Caggs with no invalidations */
		if (invalidation_cagg_has_invalidations(cagg))
		{
			add_optional_debug_info(cagg, &invalidated, "Invalidated caggs:");
			continue;
		}

		Query *view_query = ts_continuous_agg_get_query(cagg);

		/* joins need to be matched exactly */
		if (list_length(query->rtable) != list_length(view_query->rtable))
		{
			add_optional_debug_info(cagg, &nonmatching_joins, "Joins do not match:");
			continue;
		}
		/* group clauses also need to be matched as we don't allow reaggregation (yet) */
		if (list_length(query->groupClause) != list_length(view_query->groupClause))
		{
			add_optional_debug_info(cagg, &nonmatching_groupby, "Grouping clauses do not match:");
			continue;
		}
		/* If Cagg has quals, they'll need to be exactly matched in the query */
		if ((view_query->jointree->quals && !query->jointree->quals) ||
			(!view_query->jointree->quals && query->jointree->quals))
		{
			add_optional_debug_info(cagg, &nonmatching_quals, "WHERE/ON quals do not match:");
			continue;
		}
		if ((view_query->havingQual && !query->havingQual) ||
			(!view_query->havingQual && query->havingQual))
		{
			add_optional_debug_info(cagg, &nonmatching_having_quals, "HAVING quals do not match:");
			continue;
		}

		/* Adjust user permissions in view query to those of input query so that we can match those
		 * queries */
		Assert(query->rteperminfos);
		Oid userOid = ((RTEPermissionInfo *) linitial(query->rteperminfos))->checkAsUser;
		setRuleCheckAsUser((Node *) view_query, userOid);

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
				add_optional_debug_info(cagg, &nonmatching_joins, "Joins do not match:");
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
#else
			RangeTblEntry *qrte = llast_node(RangeTblEntry, newquery->rtable);
			Assert(qrte->rtekind == RTE_GROUP);
			query_group_exprs = qrte->groupexprs;

			RangeTblEntry *vrte = llast_node(RangeTblEntry, view_query->rtable);
			Assert(vrte->rtekind == RTE_GROUP);
			view_group_exprs = vrte->groupexprs;
#endif
			/* TBD: exclude time buckets from the group exprs to be matched */
			if (!match_lists(view_group_exprs, query_group_exprs))
			{
				add_optional_debug_info(cagg,
										&nonmatching_groupby,
										"Grouping clauses do not match:");
				continue;
			}
		}

		/* have to match aggregate expressions */
		List *qaggrefs = ts_find_aggrefs((Node *) newquery->targetList);
		List *vaggrefs = ts_find_aggrefs((Node *) view_query->targetList);
		/* every query aggregate should match Cagg aggregate */
		if (!match_lists(qaggrefs, vaggrefs))
		{
			add_optional_debug_info(cagg, &nonmatching_aggrefs, "Query aggregates do not match:");
			continue;
		}
		/* have to match view HAVING quals, if any */
		if (view_query->havingQual)
		{
			List *query_having = collect_flattened_quals(newquery->havingQual);
			List *view_having = collect_flattened_quals(view_query->havingQual);
			/* Every Cagg havingQual needs to be matched in the query and vice versa */
			if (!match_lists(view_having, query_having))
			{
				add_optional_debug_info(cagg,
										&nonmatching_having_quals,
										"HAVING quals do not match:");
				continue;
			}
			if (!match_lists(query_having, view_having))
			{
				add_optional_debug_info(cagg,
										&nonmatching_having_quals,
										"HAVING quals do not match:");
				continue;
			}
		}

		/* have to exactly match join and where quals, if any
		 * we flatten quals as we collect them
		 */
		List *query_quals = collect_flattened_quals((Node *) newquery->jointree);
		List *view_quals = collect_flattened_quals((Node *) view_query->jointree);
		/* Cannot trust same length lists as can have duplicate quals */
		if (!match_lists(view_quals, query_quals))
		{
			add_optional_debug_info(cagg, &nonmatching_quals, "WHERE/JOIN quals do not match:");
			continue;
		}
		if (!match_lists(query_quals, view_quals))
		{
			add_optional_debug_info(cagg, &nonmatching_quals, "WHERE/JOIN quals do not match:");
			continue;
		}

		/* Have to derive query TLEs and query HAVING from view TLEs
		 * i.e. (query.bucket + 1)*2 from (view.bucket + 1)
		 */
		ts_indexed_tlist *itlist = ts_build_tlist_index(view_query->targetList);

		List *new_tlist = NULL;
		ListCell *lsrc;
		foreach (lsrc, newquery->targetList)
		{
			TargetEntry *tle = (TargetEntry *) lfirst(lsrc);
			if (tle->resjunk)
				continue;
			Node *new_entry = ts_replace_tlist_expr((Node *) tle, itlist, 1, 0);
			if (!new_entry)
			{
				new_tlist = NULL;
				break;
			}
			else
				new_tlist = lappend(new_tlist, new_entry);
		}

		if (new_tlist)
			newquery->targetList = new_tlist;
		else
		{
			add_optional_debug_info(cagg,
									&nonmatching_targets,
									"Query targets cannot be derived from Cagg targets:");
			continue;
		}

		/* Found matching CAgg: rewrite query with it if asked */
		cagg_rewrite_ctx->cagg = cagg;
		if (!do_rewrite)
			return;

		/* rewrite the query with Cagg:
		 * TLEs are already rewritten,
		 * also need to drop quals, group exprs, joins,
		 * keep order by and limit.
		 */
		Query *cagg_finalized_query = ts_continuous_agg_get_finalized_query(cagg);
		Assert(cagg_finalized_query);

		query->targetList = newquery->targetList;
		query->groupClause = NULL;
		query->hasAggs = false;
		query->havingQual = NULL;
#if PG18_GE
		query->hasGroupRTE = false;
#endif
		RangeTblEntry *newrte = makeNode(RangeTblEntry);
		newrte->rtekind = RTE_RELATION;
		newrte->relkind = RELKIND_VIEW;
		newrte->relid = cagg->relid;
		newrte->inh = false;
		newrte->alias = NULL;
		newrte->eref = makeAlias(cagg->data.user_view_name.data, NIL);
		newrte->subquery = NULL;
		newrte->inFromCl = true;
		query->rtable = list_make1(newrte);

		query->rteperminfos = NULL;
		/* Add empty perminfo for the new RTE to make build_simple_rel happy. */
		RTEPermissionInfo *perminfo = addRTEPermissionInfo(&query->rteperminfos, newrte);
		perminfo->requiredPerms |= ACL_SELECT;
		/* why? */
		perminfo->inh = true;
		int attno = 0;
		foreach (lv, cagg_finalized_query->targetList)
		{
			TargetEntry *tle = lfirst_node(TargetEntry, lv);
			if (tle->resjunk)
				continue;
			newrte->eref->colnames = lappend(newrte->eref->colnames, makeString(tle->resname));
			attno = list_length(newrte->eref->colnames) - FirstLowInvalidHeapAttributeNumber;
			perminfo->selectedCols = bms_add_member(perminfo->selectedCols, attno);
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

		break;
	}

	if (!cagg_rewrite_ctx->cagg)
	{
		cagg_rewrite_ctx->eligible = false;
		if (ts_guc_cagg_rewrites_debug_info)
		{
			appendStringInfo(&(cagg_rewrite_ctx->msg),
							 "none of continuous aggregates defined on \"%s.%s\" are matching the "
							 "query:\n",
							 (cagg_rewrite_ctx->cagg_parent ?
								  NameStr(cagg_rewrite_ctx->cagg_parent->data.user_view_schema) :
								  NameStr(cagg_rewrite_ctx->ht->fd.schema_name)),
							 (cagg_rewrite_ctx->cagg_parent ?
								  NameStr(cagg_rewrite_ctx->cagg_parent->data.user_view_name) :
								  NameStr(cagg_rewrite_ctx->ht->fd.table_name)));
			if (not_realtime)
			{
				appendStringInfo(&(cagg_rewrite_ctx->msg), "%s\n", not_realtime->data);
				pfree(not_realtime);
			}
			if (invalidated)
			{
				appendStringInfo(&(cagg_rewrite_ctx->msg), "%s\n", invalidated->data);
				pfree(invalidated);
			}
			if (nonmatching_buckets)
			{
				appendStringInfo(&(cagg_rewrite_ctx->msg), "%s\n", nonmatching_buckets->data);
				pfree(nonmatching_buckets);
			}
			if (nonmatching_joins)
			{
				appendStringInfo(&(cagg_rewrite_ctx->msg), "%s\n", nonmatching_joins->data);
				pfree(nonmatching_joins);
			}
			if (nonmatching_groupby)
			{
				appendStringInfo(&(cagg_rewrite_ctx->msg), "%s\n", nonmatching_groupby->data);
				pfree(nonmatching_groupby);
			}
			if (nonmatching_aggrefs)
			{
				appendStringInfo(&(cagg_rewrite_ctx->msg), "%s\n", nonmatching_aggrefs->data);
				pfree(nonmatching_aggrefs);
			}
			if (nonmatching_quals)
			{
				appendStringInfo(&(cagg_rewrite_ctx->msg), "%s\n", nonmatching_quals->data);
				pfree(nonmatching_quals);
			}
			if (nonmatching_having_quals)
			{
				appendStringInfo(&(cagg_rewrite_ctx->msg), "%s\n", nonmatching_having_quals->data);
				pfree(nonmatching_having_quals);
			}
			if (nonmatching_targets)
			{
				appendStringInfo(&(cagg_rewrite_ctx->msg), "%s\n", nonmatching_targets->data);
				pfree(nonmatching_targets);
			}
		}
	}
}

typedef struct
{
	Oid cagg_relid;
	bool is_root_query;
	char *subquery_alias;
	bool rewritten_with_caggs;
} RewriteWithCaggsContext;

static bool
rewrite_query_with_caggs(Node *node, RewriteWithCaggsContext *context)
{
	if (node == NULL)
		return false;

	/* FROM subquery can be a Cagg (user or internal) view: don't rewrite it with Caggs in this case
	 */
	if (IsA(node, RangeTblEntry))
	{
		RangeTblEntry *rte = castNode(RangeTblEntry, node);
		if (rte->rtekind == RTE_SUBQUERY &&
			(ts_guc_enable_cagg_rewrites || ts_guc_cagg_rewrites_debug_info))
		{
			context->subquery_alias = rte->eref->aliasname;
			if (rte->relkind == RELKIND_VIEW)
			{
				const char *view_name = get_rel_name(rte->relid);
				const char *view_schema = get_namespace_name(get_rel_namespace(rte->relid));
				ContinuousAgg *cagg = ts_continuous_agg_find_by_view_name(view_schema,
																		  view_name,
																		  ContinuousAggAnyView);
				if (cagg)
				{
					/* Enter Cagg view query*/
					if (!OidIsValid(context->cagg_relid))
						context->cagg_relid = cagg->relid;
					/* Leave the same Cagg view query */
					else if (context->cagg_relid == cagg->relid)
						context->cagg_relid = InvalidOid;
				}
			}
		}
		return false;
	}
	else if (IsA(node, CommonTableExpr))
	{
		CommonTableExpr *cte = (CommonTableExpr *) node;
		context->subquery_alias = cte->ctename;
	}
	else if (IsA(node, Query))
	{
		Query *query = castNode(Query, node);
		ListCell *lc;
		bool ret;

		CaggRewriteContext cagg_rewrite_ctx = { 0 };
		cagg_rewrite_ctx.eligible = (ts_guc_enable_cagg_rewrites || ts_guc_cagg_rewrites_debug_info)
									/* Not inside Cagg query */
									&& !OidIsValid(context->cagg_relid);
		if (!OidIsValid(context->cagg_relid) && ts_guc_cagg_rewrites_debug_info)
			initStringInfo(&cagg_rewrite_ctx.msg);
		if (cagg_rewrite_ctx.eligible)
			check_query_for_cagg_rewrites(&cagg_rewrite_ctx, query);

		Cache *hcache = ts_hypertable_cache_pin();
		foreach (lc, query->rtable)
		{
			RangeTblEntry *rte = lfirst_node(RangeTblEntry, lc);
			Hypertable *ht;

			if (cagg_rewrite_ctx.eligible)
				check_query_rtes_for_cagg_rewrites(&cagg_rewrite_ctx, rte);

			if (cagg_rewrite_ctx.eligible && rte->rtekind == RTE_RELATION)
			{
				ht = ts_hypertable_cache_get_entry(hcache, rte->relid, CACHE_FLAG_MISSING_OK);

				/* Record hypertable in case query will be eligible for Cagg rewrites */
				if (ht)
					cagg_rewrite_ctx.ht = ht;
			}
		}
		ts_cache_release(&hcache);

		if (cagg_rewrite_ctx.eligible && !cagg_rewrite_ctx.ht_rte)
		{
			cagg_rewrite_ctx.eligible = false;
			if (ts_guc_cagg_rewrites_debug_info)
				appendStringInfoString(&cagg_rewrite_ctx.msg,
									   "at least one hypertable should be used in the query");
		}

		/* Found a hypertable, check that time bucket is valid and on hypertable time dimension */
		if (cagg_rewrite_ctx.eligible)
		{
			/* "ht_rte" must be a Cagg, obtain its hypertable */
			if (!cagg_rewrite_ctx.ht)
			{
				cagg_rewrite_ctx.cagg_parent =
					ts_continuous_agg_find_by_relid(cagg_rewrite_ctx.ht_rte->relid);
				Assert(cagg_rewrite_ctx.cagg_parent);
				Cache *hcache = ts_hypertable_cache_pin();
				cagg_rewrite_ctx.ht =
					ts_hypertable_cache_get_entry_by_id(hcache,
														cagg_rewrite_ctx.cagg_parent->data
															.mat_hypertable_id);
				ts_cache_release(&hcache);
			}
			check_timebucket_for_cagg_rewrites(&cagg_rewrite_ctx, query);
		}

		/* Validated all we could before retrieving CAggs for the hypertable,
		 * now we'll try to match available CAggs to the query.
		 * If CAgg rewrites are disabled we will do match without rewriting, for debug info.
		 */
		if (cagg_rewrite_ctx.eligible)
			match_query_to_cagg(&cagg_rewrite_ctx, query, ts_guc_enable_cagg_rewrites);

		if (cagg_rewrite_ctx.cagg && ts_guc_enable_cagg_rewrites)
			context->rewritten_with_caggs = true;

		if (!OidIsValid(context->cagg_relid) && ts_guc_cagg_rewrites_debug_info)
		{
			StringInfoData query_label;
			initStringInfo(&query_label);
			if (context->is_root_query)
				appendStringInfoString(&query_label, "Query");
			else if (context->subquery_alias)
				appendStringInfo(&query_label, "Subquery \"%s\"", context->subquery_alias);
			else
				appendStringInfo(&query_label, "Sublink");

			if (cagg_rewrite_ctx.cagg)
			{
				if (ts_guc_enable_cagg_rewrites)
					elog(INFO,
						 "%s was rewritten with Cagg \"%s.%s\"",
						 query_label.data,
						 cagg_rewrite_ctx.cagg->data.user_view_schema.data,
						 cagg_rewrite_ctx.cagg->data.user_view_name.data);
				else
					elog(INFO,
						 "%s can be rewritten with Cagg \"%s.%s\"",
						 query_label.data,
						 cagg_rewrite_ctx.cagg->data.user_view_schema.data,
						 cagg_rewrite_ctx.cagg->data.user_view_name.data);
			}
			else
				elog(INFO,
					 "%s cannot be rewritten with CAggs: %s",
					 query_label.data,
					 cagg_rewrite_ctx.msg.data);
		}

		if (context->is_root_query)
			context->is_root_query = false;
		context->subquery_alias = NULL;

		ret = query_tree_walker(query,
								rewrite_query_with_caggs,
								context,
								QTW_EXAMINE_RTES_BEFORE | QTW_EXAMINE_RTES_AFTER);
		return ret;
	}

	return expression_tree_walker(node, rewrite_query_with_caggs, context);
}

Query *
continuous_agg_apply_rewrites(Query *parse)
{
	Query *result = parse;
	RewriteWithCaggsContext rewrite_with_caggs_context = { 0 };
	rewrite_with_caggs_context.is_root_query = true;
	rewrite_with_caggs_context.subquery_alias = NULL;
	rewrite_query_with_caggs((Node *) parse, &rewrite_with_caggs_context);
	/*
	 *  we need to rewrite the query to fire Cagg view rules
	 *  if query was rewritten with Caggs
	 */
	if (rewrite_with_caggs_context.rewritten_with_caggs)
	{
		Query *copied_query = copyObject(parse);
		AcquireRewriteLocks(copied_query, true, false);
		List *rewritten = QueryRewrite(copied_query);
		Assert(list_length(rewritten) == 1);
		result = linitial_node(Query, rewritten);
	}
	return result;
}

#endif
