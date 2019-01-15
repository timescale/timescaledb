/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_TSL_FDW_DEPARSE_H
#define TIMESCALEDB_TSL_FDW_DEPARSE_H

#include <postgres.h>

extern void deparseInsertSql(StringInfo buf, RangeTblEntry *rte, Index rtindex, Relation rel,
							 List *targetAttrs, bool doNothing, List *returningList,
							 List **retrieved_attrs);

extern void deparseUpdateSql(StringInfo buf, RangeTblEntry *rte, Index rtindex, Relation rel,
							 List *targetAttrs, List *returningList, List **retrieved_attrs);

extern void deparseDeleteSql(StringInfo buf, RangeTblEntry *rte, Index rtindex, Relation rel,
							 List *returningList, List **retrieved_attrs);

extern bool is_foreign_expr(PlannerInfo *root, RelOptInfo *baserel, Expr *expr);

extern void classifyConditions(PlannerInfo *root, RelOptInfo *baserel, List *input_conds,
							   List **remote_conds, List **local_conds);

extern List *build_tlist_to_deparse(RelOptInfo *foreignrel);

extern void deparseSelectStmtForRel(StringInfo buf, PlannerInfo *root, RelOptInfo *rel, List *tlist,
									List *remote_conds, List *pathkeys, bool is_subquery,
									List **retrieved_attrs, List **params_list);

extern const char *get_jointype_name(JoinType jointype);
extern void deparseStringLiteral(StringInfo buf, const char *val);
extern void deparseAnalyzeSizeSql(StringInfo buf, Relation rel);
extern void deparseAnalyzeSql(StringInfo buf, Relation rel, List **retrieved_attrs);

#endif /* TIMESCALEDB_TSL_FDW_DEPARSE_H */
