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
 */
#include <postgres.h>
#include <access/amapi.h>
#include <access/genam.h>
#include <access/htup_details.h>
#include <access/table.h>
#include <access/tableam.h>
#include <access/transam.h>
#include <access/xact.h>
#include <catalog/heap.h>
#include <catalog/index.h>
#include <catalog/pg_am.h>
#include <catalog/pg_attribute.h>
#include <nodes/makefuncs.h>
#include <nodes/nodeFuncs.h>
#include <nodes/pathnodes.h>
#include <optimizer/optimizer.h>
#include <optimizer/plancat.h>
#include <parser/parsetree.h>
#include <rewrite/rewriteManip.h>
#include <storage/bufmgr.h>
#include <utils/lsyscache.h>
#include <utils/rel.h>
#include <utils/snapmgr.h>
#include <utils/syscache.h>

#include "compat/compat.h"
#include "plancat.h"

/*
 * Copied verbatim from build_index_tlist() in
 * src/backend/optimizer/util/plancat.c.
 */
static List *
build_index_tlist(PlannerInfo *root, IndexOptInfo *index, Relation heapRelation)
{
	List *tlist = NIL;
	Index varno = index->rel->relid;
	ListCell *indexpr_item;
	int i;

	indexpr_item = list_head(index->indexprs);
	for (i = 0; i < index->ncolumns; i++)
	{
		int indexkey = index->indexkeys[i];
		Expr *indexvar;

		if (indexkey != 0)
		{
			/* simple column */
			const FormData_pg_attribute *att_tup;

			if (indexkey < 0)
				att_tup = SystemAttributeDefinition(indexkey);
			else
				att_tup = TupleDescAttr(heapRelation->rd_att, indexkey - 1);

			indexvar = (Expr *) makeVar(varno,
									   indexkey,
									   att_tup->atttypid,
									   att_tup->atttypmod,
									   att_tup->attcollation,
									   0);
		}
		else
		{
			/* expression column */
			if (indexpr_item == NULL)
				elog(ERROR, "wrong number of index expressions");
			indexvar = (Expr *) lfirst(indexpr_item);
			indexpr_item = lnext(index->indexprs, indexpr_item);
		}

		tlist = lappend(tlist, makeTargetEntry(indexvar, i + 1, NULL, false));
	}
	if (indexpr_item != NULL)
		elog(ERROR, "wrong number of index expressions");

	return tlist;
}

/*
 * Copied and trimmed down from get_relation_info() in
 * src/backend/optimizer/util/plancat.c. Postgres itself skips building
 * rel->indexlist for a relation whose RTE it considers an inheritance parent.
 *
 * We call this to build the indexlist for hypertables ourselves because
 * some planner optimizations that run before our own hypertable expansion
 * (e.g. useless-join and self-join elimination) need a populated indexlist to
 * prove the relation unique.
 *
 * Unlike get_relation_info(), this does not populate rel->tuples/pages (that
 * stays 0 for a hypertable parent, to be filled in later from its chunks),
 * so per-index tuple estimates copied from rel->tuples are unreliable here.
 */
void
ts_build_indexlist(PlannerInfo *root, RelOptInfo *rel)
{
	Index varno = rel->relid;
	RangeTblEntry *rte = planner_rt_fetch(varno, root);
	Relation relation;
	List *indexinfos = NIL;
	List *indexoidlist;
	LOCKMODE lmode;
	ListCell *l;

	relation = table_open(rte->relid, NoLock);

	if (!relation->rd_rel->relhasindex)
	{
		table_close(relation, NoLock);
		return;
	}

	indexoidlist = RelationGetIndexList(relation);
	lmode = rte->rellockmode;

	foreach (l, indexoidlist)
	{
		Oid indexoid = lfirst_oid(l);
		Relation indexRelation;
		Form_pg_index index;
		const IndexAmRoutine *amroutine = NULL;
		IndexOptInfo *info;
		int ncolumns, nkeycolumns;
		int i;

		indexRelation = index_open(indexoid, lmode);
		index = indexRelation->rd_index;

		if (!index->indisvalid)
		{
			index_close(indexRelation, NoLock);
			continue;
		}

		if (index->indcheckxmin &&
			!TransactionIdPrecedes(HeapTupleHeaderGetXmin(indexRelation->rd_indextuple->t_data),
									TransactionXmin))
		{
			root->glob->transientPlan = true;
			index_close(indexRelation, NoLock);
			continue;
		}

		info = makeNode(IndexOptInfo);

		info->indexoid = index->indexrelid;
		info->reltablespace = RelationGetForm(indexRelation)->reltablespace;
		info->rel = rel;
		info->ncolumns = ncolumns = index->indnatts;
		info->nkeycolumns = nkeycolumns = index->indnkeyatts;

		info->indexkeys = palloc_array(int, ncolumns);
		info->indexcollations = palloc_array(Oid, nkeycolumns);
		info->opfamily = palloc_array(Oid, nkeycolumns);
		info->opcintype = palloc_array(Oid, nkeycolumns);
		info->canreturn = palloc_array(bool, ncolumns);

		for (i = 0; i < ncolumns; i++)
		{
			info->indexkeys[i] = index->indkey.values[i];
			info->canreturn[i] = index_can_return(indexRelation, i + 1);
		}

		for (i = 0; i < nkeycolumns; i++)
		{
			info->opfamily[i] = indexRelation->rd_opfamily[i];
			info->opcintype[i] = indexRelation->rd_opcintype[i];
			info->indexcollations[i] = indexRelation->rd_indcollation[i];
		}

		info->relam = indexRelation->rd_rel->relam;

		if (indexRelation->rd_rel->relkind != RELKIND_PARTITIONED_INDEX)
		{
			amroutine = indexRelation->rd_indam;
			info->amcanorderbyop = amroutine->amcanorderbyop;
			info->amoptionalkey = amroutine->amoptionalkey;
			info->amsearcharray = amroutine->amsearcharray;
			info->amsearchnulls = amroutine->amsearchnulls;
			info->amcanparallel = amroutine->amcanparallel;
			info->amhasgettuple = (amroutine->amgettuple != NULL);
			info->amhasgetbitmap =
				amroutine->amgetbitmap != NULL && relation->rd_tableam->scan_bitmap_next_tuple != NULL;
			info->amcanmarkpos =
				(amroutine->ammarkpos != NULL && amroutine->amrestrpos != NULL);
			info->amcostestimate = amroutine->amcostestimate;
			Assert(info->amcostestimate != NULL);

			info->opclassoptions = RelationGetIndexAttOptions(indexRelation, true);

			if (info->relam == BTREE_AM_OID)
			{
				Assert(amroutine->amcanorder);

				info->sortopfamily = info->opfamily;
				info->reverse_sort = palloc_array(bool, nkeycolumns);
				info->nulls_first = palloc_array(bool, nkeycolumns);

				for (i = 0; i < nkeycolumns; i++)
				{
					int16 opt = indexRelation->rd_indoption[i];

					info->reverse_sort[i] = (opt & INDOPTION_DESC) != 0;
					info->nulls_first[i] = (opt & INDOPTION_NULLS_FIRST) != 0;
				}
			}
			else if (amroutine->amcanorder)
			{
				info->sortopfamily = palloc_array(Oid, nkeycolumns);
				info->reverse_sort = palloc_array(bool, nkeycolumns);
				info->nulls_first = palloc_array(bool, nkeycolumns);

				for (i = 0; i < nkeycolumns; i++)
				{
					int16 opt = indexRelation->rd_indoption[i];
					Oid ltopr;
					Oid opfamily;
					Oid opcintype;
					CompareType cmptype;

					info->reverse_sort[i] = (opt & INDOPTION_DESC) != 0;
					info->nulls_first[i] = (opt & INDOPTION_NULLS_FIRST) != 0;

					ltopr = get_opfamily_member_for_cmptype(info->opfamily[i],
															info->opcintype[i],
															info->opcintype[i],
															COMPARE_LT);
					if (OidIsValid(ltopr) &&
						get_ordering_op_properties(ltopr, &opfamily, &opcintype, &cmptype) &&
						opcintype == info->opcintype[i] && cmptype == COMPARE_LT)
					{
						info->sortopfamily[i] = opfamily;
					}
					else
					{
						info->sortopfamily = NULL;
						info->reverse_sort = NULL;
						info->nulls_first = NULL;
						break;
					}
				}
			}
			else
			{
				info->sortopfamily = NULL;
				info->reverse_sort = NULL;
				info->nulls_first = NULL;
			}
		}
		else
		{
			info->amcanorderbyop = false;
			info->amoptionalkey = false;
			info->amsearcharray = false;
			info->amsearchnulls = false;
			info->amcanparallel = false;
			info->amhasgettuple = false;
			info->amhasgetbitmap = false;
			info->amcanmarkpos = false;
			info->amcostestimate = NULL;

			info->sortopfamily = NULL;
			info->reverse_sort = NULL;
			info->nulls_first = NULL;
		}

		info->indexprs = RelationGetIndexExpressions(indexRelation);
		info->indpred = RelationGetIndexPredicate(indexRelation);
		if (info->indexprs && varno != 1)
			ChangeVarNodes((Node *) info->indexprs, 1, varno, 0);
		if (info->indpred && varno != 1)
			ChangeVarNodes((Node *) info->indpred, 1, varno, 0);

		info->indextlist = build_index_tlist(root, info, relation);

		info->indrestrictinfo = NIL;
		info->predOK = false;
		info->unique = index->indisunique;
#if PG18_GE
		info->nullsnotdistinct = index->indnullsnotdistinct;
#endif
		info->immediate = index->indimmediate;
		info->hypothetical = false;

		if (indexRelation->rd_rel->relkind != RELKIND_PARTITIONED_INDEX)
		{
			if (info->indpred == NIL)
			{
				info->pages = RelationGetNumberOfBlocks(indexRelation);
				info->tuples = rel->tuples;
			}
			else
			{
				double allvisfrac; /* dummy */

				estimate_rel_size(indexRelation, NULL, &info->pages, &info->tuples, &allvisfrac);
				if (info->tuples > rel->tuples)
					info->tuples = rel->tuples;
			}

#if PG18_GE
			if (amroutine->amgettreeheight)
				info->tree_height = amroutine->amgettreeheight(indexRelation);
			else
#endif
				info->tree_height = -1;
		}
		else
		{
			info->pages = 0;
			info->tuples = 0.0;
			info->tree_height = -1;
		}

		index_close(indexRelation, NoLock);

		indexinfos = lcons(info, indexinfos);
	}

	list_free(indexoidlist);
	table_close(relation, NoLock);

	rel->indexlist = indexinfos;
}
