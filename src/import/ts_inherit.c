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
 * The function was copied from optimizer/util/inherit.c, since it is
 * declared static in the core code, but we need it for our hypertable
 * chunk expansion.
 */
#include <postgres.h>

#include <catalog/pg_type.h>
#include <nodes/makefuncs.h>
#include <optimizer/appendinfo.h>
#include <parser/parsetree.h>
#include <utils/rel.h>

#include "compat/compat.h"
#include "ts_inherit.h"

/*
 * Copied from expand_single_inheritance_child (optimizer/util/inherit.c)
 * with the following adaptations:
 * - Uses copyObject() instead of memcpy for deep copy of RTE
 * - Drops PlanRowMark handling (caller passes NULL for top_parentrc)
 * - Adds PG16 compat for requiredPerms vs perminfoindex
 */
void
ts_expand_single_inheritance_child(PlannerInfo *root, RangeTblEntry *parentrte, Index parentRTindex,
								   Relation parentrel, PlanRowMark *top_parentrc, Relation childrel,
								   RangeTblEntry **childrte_p, Index *childRTindex_p)
{
	Query *parse = root->parse;
	Oid parentOID PG_USED_FOR_ASSERTS_ONLY = RelationGetRelid(parentrel);
	Oid childOID = RelationGetRelid(childrel);
	RangeTblEntry *childrte;
	Index childRTindex;
	AppendRelInfo *appinfo;
	TupleDesc child_tupdesc;
	List *parent_colnames;
	List *child_colnames;

	/*
	 * Build an RTE for the child, and attach to query's rangetable list.
	 * We use copyObject for a deep copy to correctly handle nested pointers,
	 * unlike PG's memcpy flat copy.
	 */
	childrte = copyObject(parentrte);
	Assert(parentrte->rtekind == RTE_RELATION);
	childrte->relid = childOID;
	childrte->relkind = childrel->rd_rel->relkind;
	if (childrte->relkind == RELKIND_PARTITIONED_TABLE)
	{
		Assert(childOID != parentOID);
		childrte->inh = true;
	}
	else
		childrte->inh = false;
	childrte->securityQuals = NIL;

#if PG16_LT
	childrte->requiredPerms = 0;
#else
	childrte->perminfoindex = 0;
#endif

	/* Link not-yet-fully-filled child RTE into data structures */
	parse->rtable = lappend(parse->rtable, childrte);
	childRTindex = list_length(parse->rtable);
	*childrte_p = childrte;
	*childRTindex_p = childRTindex;

	appinfo = make_append_rel_info(parentrel, childrel, parentRTindex, childRTindex);
	root->append_rel_list = lappend(root->append_rel_list, appinfo);

	/* tablesample is probably null, but copy it */
	childrte->tablesample = copyObject(parentrte->tablesample);

	/*
	 * Construct an alias clause for the child, which we can also use as eref.
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
			attname = "";
		}
		else if (appinfo->parent_colnos[cattno] > 0 &&
				 appinfo->parent_colnos[cattno] <= list_length(parent_colnames))
		{
			attname = strVal(list_nth(parent_colnames, appinfo->parent_colnos[cattno] - 1));
		}
		else
		{
			attname = NameStr(att->attname);
		}
		child_colnames = lappend(child_colnames, makeString(pstrdup(attname)));
	}

	childrte->alias = childrte->eref = makeAlias(parentrte->eref->aliasname, child_colnames);

	/*
	 * Store the RTE and appinfo in the respective PlannerInfo arrays, which
	 * the caller must already have allocated space for.
	 */
	Assert((int) childRTindex < root->simple_rel_array_size);
	Assert(root->simple_rte_array[childRTindex] == NULL);
	root->simple_rte_array[childRTindex] = childrte;
	Assert(root->append_rel_array[childRTindex] == NULL);
	root->append_rel_array[childRTindex] = appinfo;

	/*
	 * PlanRowMark handling is skipped -- our caller always passes NULL
	 * for top_parentrc since rowMarks are handled separately.
	 */

	/*
	 * If we are creating a child of the query target relation (only possible
	 * in UPDATE/DELETE/MERGE), add it to all_result_relids, as well as
	 * leaf_result_relids if appropriate, and make sure that we generate
	 * required row-identity data.
	 */
	if (bms_is_member(parentRTindex, root->all_result_relids))
	{
		root->all_result_relids = bms_add_member(root->all_result_relids, childRTindex);

		if (childrte->relkind != RELKIND_PARTITIONED_TABLE)
		{
			Var *rrvar;

			root->leaf_result_relids = bms_add_member(root->leaf_result_relids, childRTindex);

			rrvar = makeVar(childRTindex, TableOidAttributeNumber, OIDOID, -1, InvalidOid, 0);
			add_row_identity_var(root, rrvar, childRTindex, "tableoid");

			add_row_identity_columns(root, childRTindex, childrte, childrel);
		}
	}
}
