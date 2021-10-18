/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_TSL_FDW_DEPARSE_H
#define TIMESCALEDB_TSL_FDW_DEPARSE_H

#include <postgres.h>
#include "data_node_chunk_assignment.h"

typedef struct DeparsedInsertStmt
{
	const char *target; /* INSERT INTO (...) */
	unsigned int num_target_attrs;
	const char *target_attrs;
	bool do_nothing;
	const char *returning;
	List *retrieved_attrs;
} DeparsedInsertStmt;

extern void deparse_insert_stmt(DeparsedInsertStmt *stmt, RangeTblEntry *rte, Index rtindex,
								Relation rel, List *target_attrs, bool do_nothing,
								List *returning_list);

extern List *deparsed_insert_stmt_to_list(DeparsedInsertStmt *stmt);
extern void deparsed_insert_stmt_from_list(DeparsedInsertStmt *stmt, List *list_stmt);

extern const char *deparsed_insert_stmt_get_sql(DeparsedInsertStmt *stmt, int64 num_rows);
extern const char *deparsed_insert_stmt_get_sql_explain(DeparsedInsertStmt *stmt, int64 num_rows);

extern void deparseInsertSql(StringInfo buf, RangeTblEntry *rte, Index rtindex, Relation rel,
							 List *targetAttrs, int64 num_rows, bool doNothing, List *returningList,
							 List **retrieved_attrs);

extern void deparseUpdateSql(StringInfo buf, RangeTblEntry *rte, Index rtindex, Relation rel,
							 List *targetAttrs, List *returningList, List **retrieved_attrs);

extern void deparseDeleteSql(StringInfo buf, RangeTblEntry *rte, Index rtindex, Relation rel,
							 List *returningList, List **retrieved_attrs);

extern bool is_foreign_expr(PlannerInfo *root, RelOptInfo *baserel, Expr *expr);

extern void classify_conditions(PlannerInfo *root, RelOptInfo *baserel, List *input_conds,
								List **remote_conds, List **local_conds);

extern List *build_tlist_to_deparse(RelOptInfo *foreignrel);

extern void deparseSelectStmtForRel(StringInfo buf, PlannerInfo *root, RelOptInfo *rel, List *tlist,
									List *remote_where, List *remote_having, List *pathkeys,
									bool is_subquery, List **retrieved_attrs, List **params_list,
									DataNodeChunkAssignment *swa);

extern const char *get_jointype_name(JoinType jointype);
extern void deparseStringLiteral(StringInfo buf, const char *val);
extern void deparseAnalyzeSizeSql(StringInfo buf, Relation rel);
extern void deparseAnalyzeSql(StringInfo buf, Relation rel, List **retrieved_attrs);

#endif /* TIMESCALEDB_TSL_FDW_DEPARSE_H */
