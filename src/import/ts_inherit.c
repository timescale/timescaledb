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
 * These functions were copied from the PostgreSQL core planner, since
 * they were declared static in the core planner, but we need them for
 * our manipulations.
 */
#include <postgres.h>

#include <access/sysattr.h>
#include <catalog/pg_type.h>
#include <nodes/makefuncs.h>
#include <optimizer/appendinfo.h>
#include <optimizer/planner.h>
#include <parser/parsetree.h>
#include <utils/rel.h>

#include "ts_inherit.h"

/* copied verbatim from optimizer/util/inherit.c at REL_18_3 */
static void
expand_single_inheritance_child(PlannerInfo *root, RangeTblEntry *parentrte,
								Index parentRTindex, Relation parentrel,
								PlanRowMark *top_parentrc, Relation childrel,
								RangeTblEntry **childrte_p,
								Index *childRTindex_p)
{
	Query	   *parse = root->parse;
	Oid			parentOID PG_USED_FOR_ASSERTS_ONLY =
		RelationGetRelid(parentrel);
	Oid			childOID = RelationGetRelid(childrel);
	RangeTblEntry *childrte;
	Index		childRTindex;
	AppendRelInfo *appinfo;
	TupleDesc	child_tupdesc;
	List	   *parent_colnames;
	List	   *child_colnames;

	/*
	 * Build an RTE for the child, and attach to query's rangetable list. We
	 * copy most scalar fields of the parent's RTE, but replace relation OID,
	 * relkind, and inh for the child.  Set the child's securityQuals to
	 * empty, because we only want to apply the parent's RLS conditions
	 * regardless of what RLS properties individual children may have. (This
	 * is an intentional choice to make inherited RLS work like regular
	 * permissions checks.) The parent securityQuals will be propagated to
	 * children along with other base restriction clauses, so we don't need to
	 * do it here.  Other infrastructure of the parent RTE has to be
	 * translated to match the child table's column ordering, which we do
	 * below, so a "flat" copy is sufficient to start with.
	 */
	childrte = makeNode(RangeTblEntry);
	memcpy(childrte, parentrte, sizeof(RangeTblEntry));
	Assert(parentrte->rtekind == RTE_RELATION); /* else this is dubious */
	childrte->relid = childOID;
	childrte->relkind = childrel->rd_rel->relkind;
	/* A partitioned child will need to be expanded further. */
	if (childrte->relkind == RELKIND_PARTITIONED_TABLE)
	{
		Assert(childOID != parentOID);
		childrte->inh = true;
	}
	else
		childrte->inh = false;
	childrte->securityQuals = NIL;

	/* No permission checking for child RTEs. */
	childrte->perminfoindex = 0;

	/* Link not-yet-fully-filled child RTE into data structures */
	parse->rtable = lappend(parse->rtable, childrte);
	childRTindex = list_length(parse->rtable);
	*childrte_p = childrte;
	*childRTindex_p = childRTindex;

	/*
	 * Build an AppendRelInfo struct for each parent/child pair.
	 */
	appinfo = make_append_rel_info(parentrel, childrel,
								   parentRTindex, childRTindex);
	root->append_rel_list = lappend(root->append_rel_list, appinfo);

	/* tablesample is probably null, but copy it */
	childrte->tablesample = copyObject(parentrte->tablesample);

	/*
	 * Construct an alias clause for the child, which we can also use as eref.
	 * This is important so that EXPLAIN will print the right column aliases
	 * for child-table columns.  (Since ruleutils.c doesn't have any easy way
	 * to reassociate parent and child columns, we must get the child column
	 * aliases right to start with.  Note that setting childrte->alias forces
	 * ruleutils.c to use these column names, which it otherwise would not.)
	 */
	child_tupdesc = RelationGetDescr(childrel);
	parent_colnames = parentrte->eref->colnames;
	child_colnames = NIL;
	for (int cattno = 0; cattno < child_tupdesc->natts; cattno++)
	{
		Form_pg_attribute att = TupleDescAttr(child_tupdesc, cattno);
		const char *attname;

		if (att->attisdropped)
		{
			/* Always insert an empty string for a dropped column */
			attname = "";
		}
		else if (appinfo->parent_colnos[cattno] > 0 &&
				 appinfo->parent_colnos[cattno] <= list_length(parent_colnames))
		{
			/* Duplicate the query-assigned name for the parent column */
			attname = strVal(list_nth(parent_colnames,
									  appinfo->parent_colnos[cattno] - 1));
		}
		else
		{
			/* New column, just use its real name */
			attname = NameStr(att->attname);
		}
		child_colnames = lappend(child_colnames, makeString(pstrdup(attname)));
	}

	/*
	 * We just duplicate the parent's table alias name for each child.  If the
	 * plan gets printed, ruleutils.c has to sort out unique table aliases to
	 * use, which it can handle.
	 */
	childrte->alias = childrte->eref = makeAlias(parentrte->eref->aliasname,
												 child_colnames);

	/*
	 * Store the RTE and appinfo in the respective PlannerInfo arrays, which
	 * the caller must already have allocated space for.
	 */
	Assert(childRTindex < (Index) root->simple_rel_array_size);
	Assert(root->simple_rte_array[childRTindex] == NULL);
	root->simple_rte_array[childRTindex] = childrte;
	Assert(root->append_rel_array[childRTindex] == NULL);
	root->append_rel_array[childRTindex] = appinfo;

	/*
	 * Build a PlanRowMark if parent is marked FOR UPDATE/SHARE.
	 */
	if (top_parentrc)
	{
		PlanRowMark *childrc = makeNode(PlanRowMark);

		childrc->rti = childRTindex;
		childrc->prti = top_parentrc->rti;
		childrc->rowmarkId = top_parentrc->rowmarkId;
		/* Reselect rowmark type, because relkind might not match parent */
		childrc->markType = select_rowmark_type(childrte,
												top_parentrc->strength);
		childrc->allMarkTypes = (1 << childrc->markType);
		childrc->strength = top_parentrc->strength;
		childrc->waitPolicy = top_parentrc->waitPolicy;

		/*
		 * We mark RowMarks for partitioned child tables as parent RowMarks so
		 * that the executor ignores them (except their existence means that
		 * the child tables will be locked using the appropriate mode).
		 */
		childrc->isParent = (childrte->relkind == RELKIND_PARTITIONED_TABLE);

		/* Include child's rowmark type in top parent's allMarkTypes */
		top_parentrc->allMarkTypes |= childrc->allMarkTypes;

		root->rowMarks = lappend(root->rowMarks, childrc);
	}

	/*
	 * If we are creating a child of the query target relation (only possible
	 * in UPDATE/DELETE/MERGE), add it to all_result_relids, as well as
	 * leaf_result_relids if appropriate, and make sure that we generate
	 * required row-identity data.
	 */
	if (bms_is_member(parentRTindex, root->all_result_relids))
	{
		/* OK, record the child as a result rel too. */
		root->all_result_relids = bms_add_member(root->all_result_relids,
												 childRTindex);

		/* Non-leaf partitions don't need any row identity info. */
		if (childrte->relkind != RELKIND_PARTITIONED_TABLE)
		{
			Var		   *rrvar;

			root->leaf_result_relids = bms_add_member(root->leaf_result_relids,
													  childRTindex);

			/*
			 * If we have any child target relations, assume they all need to
			 * generate a junk "tableoid" column.  (If only one child survives
			 * pruning, we wouldn't really need this, but it's not worth
			 * thrashing about to avoid it.)
			 */
			rrvar = makeVar(childRTindex,
							TableOidAttributeNumber,
							OIDOID,
							-1,
							InvalidOid,
							0);
			add_row_identity_var(root, rrvar, childRTindex, "tableoid");

			/* Register any row-identity columns needed by this child. */
			add_row_identity_columns(root, childRTindex,
									 childrte, childrel);
		}
	}
}

void
ts_expand_single_inheritance_child(PlannerInfo *root, RangeTblEntry *parentrte,
													Index parentRTindex, Relation parentrel,
													PlanRowMark *top_parentrc, Relation childrel,
													RangeTblEntry **childrte_p,
													Index *childRTindex_p)
{
	expand_single_inheritance_child(root,
								 parentrte,
								 parentRTindex,
								 parentrel,
								 top_parentrc,
								 childrel,
								 childrte_p,
								 childRTindex_p);
}
