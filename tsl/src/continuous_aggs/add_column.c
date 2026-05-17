/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <postgres.h>

#include <access/heapam.h>
#include <commands/tablecmds.h>
#include <commands/view.h>
#include <fmgr.h>
#include <miscadmin.h>
#include <nodes/makefuncs.h>
#include <nodes/parsenodes.h>
#include <storage/lmgr.h>
#include <storage/lockdefs.h>
#include <tcop/tcopprot.h>
#include <utils/acl.h>
#include <utils/builtins.h>
#include <utils/lsyscache.h>
#include <utils/rel.h>
#include <utils/snapmgr.h>

#include "compression/create.h"
#include "continuous_aggs/create.h"
#include "debug_point.h"
#include "hypertable.h"
#include "ts_catalog/continuous_agg.h"

#include "add_column.h"

/*
 * Pull the CONSTR_GENERATED STORED constraint out of a ColumnDef's
 * constraint list. Returns NULL if there is no such constraint.
 */
static Constraint *
get_stored_generated_constraint(ColumnDef *coldef)
{
	ListCell *lc;
	foreach (lc, coldef->constraints)
	{
		Constraint *c = lfirst_node(Constraint, lc);
		if (c->contype == CONSTR_GENERATED && c->generated_kind == ATTRIBUTE_GENERATED_STORED)
			return c;
	}
	return NULL;
}

/*
 * Validate one ADD COLUMN ... GENERATED ALWAYS AS (agg_expr) STORED.
 *
 * We do only the two checks here:
 *   1. The cmd must carry a CONSTR_GENERATED STORED clause (rejects VIRTUAL
 *      and the no-GENERATED-at-all case).
 *   2. No other column-level constraints (NOT NULL / COLLATE / STORAGE /
 *      COMPRESSION) are allowed -- the mat-HT ALTER strips them silently
 *      and the SELECT we feed DefineView wouldn't carry them either, so
 *      they'd be ignored. Reject explicitly.
 *
 * Everything else (must-be-aggregate, columns-must-exist, type-matches,
 * unique-column-name, JOIN-source resolution) falls out of PG's CREATE OR
 * REPLACE VIEW analysis when we re-define each view.
 */
static void
validate_one_aggregation_cmd(ColumnDef *coldef)
{
	Constraint *gen = get_stored_generated_constraint(coldef);
	if (gen == NULL || gen->raw_expr == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("ADD COLUMN on a continuous aggregate must use GENERATED ALWAYS AS "
						"(<aggregate>) STORED")));

	if (list_length(coldef->constraints) > 1)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("only the GENERATED ALWAYS AS (...) STORED clause is supported on "
						"this ADD COLUMN; column \"%s\" has additional constraints",
						coldef->colname),
				 errhint("Drop the additional constraints (NOT NULL / COLLATE / STORAGE / "
						 "COMPRESSION / etc.) on this column.")));
}

/*
 * Run ALTER TABLE _materialized_hypertable_<N> ADD COLUMN <name> <type>
 * for each cmd in `stmt->cmds`. The new ColumnDefs strip the GENERATED
 * constraint so PG sees a plain ADD COLUMN with no DEFAULT.
 */
static void
add_columns_to_mat_hypertable(Oid mat_ht_oid, AlterTableStmt *stmt)
{
	List *cmds = NIL;
	List *stripped_defs = NIL;
	ListCell *lc;
	foreach (lc, stmt->cmds)
	{
		AlterTableCmd *src = lfirst_node(AlterTableCmd, lc);
		ColumnDef *src_def = castNode(ColumnDef, src->def);

		ColumnDef *dst_def = copyObject(src_def);
		dst_def->constraints = NIL;
		dst_def->raw_default = NULL;
		dst_def->cooked_default = NULL;
		dst_def->generated = '\0';

		AlterTableCmd *dst = makeNode(AlterTableCmd);
		dst->subtype = AT_AddColumn;
		dst->name = NULL;
		dst->def = (Node *) dst_def;
		dst->missing_ok = false;

		cmds = lappend(cmds, dst);
		stripped_defs = lappend(stripped_defs, dst_def);
	}

	AlterTableInternal(mat_ht_oid, cmds, true /* recurse */);
	CommandCounterIncrement();

	Hypertable *mat_ht = ts_hypertable_get_by_id(ts_hypertable_relid_to_id(mat_ht_oid));
	if (mat_ht != NULL && TS_HYPERTABLE_HAS_COMPRESSION_TABLE(mat_ht))
	{
		ListCell *def_cell;
		foreach (def_cell, stripped_defs)
		{
			ColumnDef *def = (ColumnDef *) lfirst(def_cell);
			tsl_process_compress_table_add_column(mat_ht, def);
		}
		CommandCounterIncrement();
	}
}

/*
 * Rebuild a CAgg-owned view (partial/direct/user) by appending
 * `new_targets` to its SELECT list and feeding the result back into DefineView.
 */
static void
rebuild_cagg_view(NameData schema, NameData name, List *new_targets)
{
	Oid view_oid =
		ts_get_relation_relid(NameStr(schema), NameStr(name), /* return_invalid */ false);

	/* pg_get_viewdef caches an SPI plan internally and reads pg_rewrite under
	 * its own snapshot, which doesn't see catalog updates we did earlier in the
	 * same statement (e.g. rewrite of view rules). */
	PushActiveSnapshot(GetTransactionSnapshot());
	Datum def_datum = DirectFunctionCall1(pg_get_viewdef, ObjectIdGetDatum(view_oid));
	PopActiveSnapshot();
	char *view_def = TextDatumGetCString(def_datum);

	List *parsetrees = pg_parse_query(view_def);
	Assert(list_length(parsetrees) == 1);
	RawStmt *raw = (RawStmt *) linitial(parsetrees);
	Assert(IsA(raw->stmt, SelectStmt));

	SelectStmt *select = (SelectStmt *) raw->stmt;

	select->targetList = list_concat(select->targetList, new_targets);

	ViewStmt *vstmt = makeNode(ViewStmt);
	vstmt->view = makeRangeVar(pstrdup(NameStr(schema)), pstrdup(NameStr(name)), -1);
	vstmt->aliases = NIL;
	vstmt->query = (Node *) select;
	vstmt->replace = true;
	vstmt->options = NIL;
	vstmt->withCheckOption = NO_CHECK_OPTION;

	/* Partial and direct views live in `_timescaledb_internal`, which the
	 * invoking user generally doesn't have CREATE rights on. Switch to the
	 * catalog owner for the duration of DefineView. */
	Oid uid = InvalidOid;
	Oid saved_uid = InvalidOid;
	int sec_ctx = 0;
	SWITCH_TO_TS_USER(NameStr(schema), uid, saved_uid, sec_ctx);
	DefineView(vstmt, view_def, 0, strlen(view_def));
	RESTORE_USER(uid, saved_uid, sec_ctx);
	CommandCounterIncrement();
}

static ResTarget *
make_expr_target(ColumnDef *coldef, Node *raw_expr)
{
	ResTarget *rt = makeNode(ResTarget);
	rt->name = pstrdup(coldef->colname);
	rt->indirection = NIL;
	rt->val = copyObject(raw_expr);
	rt->location = -1;
	return rt;
}

static ResTarget *
make_colref_target(ColumnDef *coldef)
{
	ColumnRef *cref = makeNode(ColumnRef);
	cref->fields = list_make1(makeString(pstrdup(coldef->colname)));
	cref->location = -1;

	ResTarget *rt = makeNode(ResTarget);
	rt->name = pstrdup(coldef->colname);
	rt->indirection = NIL;
	rt->val = (Node *) cref;
	rt->location = -1;
	return rt;
}

void
continuous_agg_add_column(ContinuousAgg *cagg, AlterTableStmt *stmt)
{
	if (!object_ownercheck(RelationRelationId, cagg->relid, GetUserId()))
		aclcheck_error(ACLCHECK_NOT_OWNER,
					   get_relkind_objtype(get_rel_relkind(cagg->relid)),
					   get_rel_name(cagg->relid));

	/* Lock all the relations we'll touch, in the same order as
	 * DROP so concurrent DROP and ADD COLUMN cannot deadlock. */
	Oid cagg_relid = cagg->relid;
	char *cagg_relname = get_rel_name(cagg_relid);
	DEBUG_WAITPOINT("cagg_add_column_before_uv_lock");
	LockRelationOid(cagg_relid, AccessExclusiveLock);

	/* A concurrent DROP could have committed between the cagg lookup and user-view lock
	 * here. */
	cagg = ts_continuous_agg_find_by_relid(cagg_relid);
	if (cagg == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("continuous aggregate %s does not exist", cagg_relname)));

	Oid mat_ht_oid = ts_hypertable_id_to_relid(cagg->data.mat_hypertable_id, false);
	Oid partial_view_oid = ts_get_relation_relid(NameStr(cagg->data.partial_view_schema),
												 NameStr(cagg->data.partial_view_name),
												 false);
	Oid direct_view_oid = ts_get_relation_relid(NameStr(cagg->data.direct_view_schema),
												NameStr(cagg->data.direct_view_name),
												false);
	DEBUG_WAITPOINT("cagg_add_column_before_ht_lock");
	LockRelationOid(mat_ht_oid, AccessExclusiveLock);
	LockRelationOid(partial_view_oid, AccessExclusiveLock);
	LockRelationOid(direct_view_oid, AccessExclusiveLock);
	DEBUG_WAITPOINT("cagg_add_column_after_locks");

	ListCell *lc;
	foreach (lc, stmt->cmds)
	{
		AlterTableCmd *cmd = lfirst_node(AlterTableCmd, lc);
		ColumnDef *coldef = castNode(ColumnDef, cmd->def);
		validate_one_aggregation_cmd(coldef);
	}

	/* Build the per-view ResTarget lists. Partial / direct views get
	 * the user's aggregate expression; the user view gets a column
	 * reference to the freshly-added mat-HT column. */
	List *expr_targets = NIL;
	List *colref_targets = NIL;
	foreach (lc, stmt->cmds)
	{
		AlterTableCmd *cmd = lfirst_node(AlterTableCmd, lc);
		ColumnDef *coldef = castNode(ColumnDef, cmd->def);
		Constraint *gen = get_stored_generated_constraint(coldef);
		expr_targets = lappend(expr_targets, make_expr_target(coldef, gen->raw_expr));
		colref_targets = lappend(colref_targets, make_colref_target(coldef));
	}

	rebuild_cagg_view(cagg->data.partial_view_schema,
					  cagg->data.partial_view_name,
					  list_copy_deep(expr_targets));

	rebuild_cagg_view(cagg->data.direct_view_schema,
					  cagg->data.direct_view_name,
					  list_copy_deep(expr_targets));

	add_columns_to_mat_hypertable(mat_ht_oid, stmt);

	/* If the CAgg is real-time (materialized_only=false), the user view is
	 * a UNION ALL that our rebuild_cagg_view doesn't handle. Flip to
	 * materialized-only so the user view becomes a plain SELECT from the mat
	 * HT, do all our rebuilds against that single-SELECT shape, then flip back
	 * at the end so the final user view is a UNION ALL again. */
	bool was_realtime = !cagg->data.materialized_only;
	if (was_realtime)
		cagg_flip_realtime_view_definition(cagg, ts_hypertable_get_by_id(cagg->data.mat_hypertable_id));
	rebuild_cagg_view(cagg->data.user_view_schema,
					  cagg->data.user_view_name,
					  colref_targets);
	if (was_realtime)
		cagg_flip_realtime_view_definition(cagg, ts_hypertable_get_by_id(cagg->data.mat_hypertable_id));
}
