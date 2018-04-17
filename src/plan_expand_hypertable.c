#include <postgres.h>
#include <nodes/relation.h>
#include <parser/parsetree.h>
#include <optimizer/var.h>
#include <optimizer/restrictinfo.h>
#include <catalog/pg_inherits_fn.h>
#include <nodes/plannodes.h>
#include <optimizer/prep.h>
#include <nodes/nodeFuncs.h>

#include "plan_expand_hypertable.h"
#include "hypertable.h"
#include "hypertable_restrict_info.h"
#include "planner_import.h"
#include "compat.h"



typedef struct CollectQualCtx
{
	PlannerInfo *root;
	RelOptInfo *rel;
	List	   *result;
} CollectQualCtx;


static bool
collect_quals_walker(Node *node, CollectQualCtx *ctx)
{
	if (node == NULL)
		return false;

	if (IsA(node, FromExpr))
	{
		FromExpr   *f = (FromExpr *) node;
		ListCell   *lc;

		foreach(lc, (List *) f->quals)
		{
			Node	   *qual = (Node *) lfirst(lc);
			RestrictInfo *restrictinfo;

			Relids		relids = pull_varnos(qual);

			if (bms_num_members(relids) != 1 || !bms_is_member(ctx->rel->relid, relids))
				continue;
#if PG96
			restrictinfo = make_restrictinfo((Expr *) qual,
											 true,
											 false,
											 false,
											 relids,
											 NULL,
											 NULL);
#else
			restrictinfo = make_restrictinfo((Expr *) qual,
											 true,
											 false,
											 false,
											 ctx->root->qual_security_level,
											 relids,
											 NULL,
											 NULL);
#endif
			ctx->result = lappend(ctx->result, restrictinfo);
		}
	}

	return expression_tree_walker(node, collect_quals_walker, ctx);
}

/* Since baserestrictinfo is not yet set by the planner, we have to derive
 * it ourselves. It's safe for us to miss some restrict info clauses (this
 * will just result in more chunks being included) so this does not need
 * to be as comprehensive as the PG native derivation. This is inspired
 * by the derivation in `deconstruct_recurse` in PG */

static List *
get_restrict_info(PlannerInfo *root, RelOptInfo *rel)
{
	CollectQualCtx ctx = {
		.root = root,
		.rel = rel,
		.result = NIL,
	};

	collect_quals_walker((Node *) root->parse->jointree, &ctx);

	return ctx.result;
}

static List *
find_children_oids(HypertableRestrictInfo *hri, Hypertable *ht, LOCKMODE lockmode)
{
	List	   *result;

	/*
	 * Using the HRI only makes sense if we are not using all the chunks,
	 * otherwise using the cached inheritance hierarchy is faster.
	 */
	if (!hypertable_restrict_info_has_restrictions(hri))
		return find_all_inheritors(ht->main_table_relid, lockmode, NULL);;

	/* always include parent again, just as find_all_inheritors does */
	result = list_make1_oid(ht->main_table_relid);

	/* add chunks */
	result = list_concat(result,
						 hypertable_restrict_info_get_chunk_oids(hri,
																 ht,
																 lockmode));
	return result;
}

bool
plan_expand_hypertable_valid_hypertable(Hypertable *ht, Query *parse, Index rti, RangeTblEntry *rte)
{
	if (ht == NULL ||
	/* inheritance enabled */
		rte->inh == false ||
	/* row locks not necessary */
		parse->rowMarks != NIL ||
	/* not update and/or delete */
		0 != parse->resultRelation)
		return false;

	return true;
}

/* Inspired by expand_inherited_rtentry but expands
 * a hypertable chunks into an append rekationship */
void
plan_expand_hypertable_chunks(Hypertable *ht,
							  PlannerInfo *root,
							  Oid parent_oid,
							  bool inhparent,
							  RelOptInfo *rel)
{
	RangeTblEntry *rte = rt_fetch(rel->relid, root->parse->rtable);
	List	   *inh_oids;
	ListCell   *l;
	Relation	oldrelation = heap_open(parent_oid, NoLock);
	Query	   *parse = root->parse;
	Index		rti = rel->relid;
	List	   *appinfos = NIL;
	HypertableRestrictInfo *hri;
	PlanRowMark *oldrc;
	List	   *restrictinfo;

	/* double check our permissions are valid */
	Assert(rti != parse->resultRelation);
	oldrc = get_plan_rowmark(root->rowMarks, rti);
	if (oldrc && RowMarkRequiresRowShareLock(oldrc->markType))
		elog(ERROR, "unexpected permissions requested");


	/* mark the parent as an append relation */
	rte->inh = true;

	/*
	 * rel->baserestrictinfo is not yet set at this point in the planner. So
	 * do a simple version of that deduction here.
	 */
	restrictinfo = get_restrict_info(root, rel);

	/*
	 * This is where the magic happens: use our HypertableRestrictInfo
	 * infrastructure to deduce the appropriate chunks using our range
	 * exclusion
	 */
	hri = hypertable_restrict_info_create(rel, ht);
	hypertable_restrict_info_add(hri, root, restrictinfo);
	inh_oids = find_children_oids(hri, ht, AccessShareLock);

	/*
	 * the simple_*_array structures have already been set, we need to add the
	 * children to them
	 */
	root->simple_rel_array_size += list_length(inh_oids);
	root->simple_rel_array = repalloc(root->simple_rel_array, root->simple_rel_array_size * sizeof(RelOptInfo *));
	root->simple_rte_array = repalloc(root->simple_rte_array, root->simple_rel_array_size * sizeof(RangeTblEntry *));


	foreach(l, inh_oids)
	{
		Oid			child_oid = lfirst_oid(l);
		Relation	newrelation;
		RangeTblEntry *childrte;
		Index		child_rtindex;
		AppendRelInfo *appinfo;

		/* Open rel if needed; we already have required locks */
		if (child_oid != parent_oid)
			newrelation = heap_open(child_oid, NoLock);
		else
			newrelation = oldrelation;

		/* chunks cannot be temp tables */
		Assert(!RELATION_IS_OTHER_TEMP(newrelation));

		/*
		 * Build an RTE for the child, and attach to query's rangetable list.
		 * We copy most fields of the parent's RTE, but replace relation OID
		 * and relkind, and set inh = false.  Also, set requiredPerms to zero
		 * since all required permissions checks are done on the original RTE.
		 * Likewise, set the child's securityQuals to empty, because we only
		 * want to apply the parent's RLS conditions regardless of what RLS
		 * properties individual children may have.  (This is an intentional
		 * choice to make inherited RLS work like regular permissions checks.)
		 * The parent securityQuals will be propagated to children along with
		 * other base restriction clauses, so we don't need to do it here.
		 */
		childrte = copyObject(rte);
		childrte->relid = child_oid;
		childrte->relkind = newrelation->rd_rel->relkind;
		childrte->inh = false;
		/* clear the magic bit */
		childrte->ctename = NULL;
		childrte->requiredPerms = 0;
		childrte->securityQuals = NIL;
		parse->rtable = lappend(parse->rtable, childrte);
		child_rtindex = list_length(parse->rtable);
		root->simple_rte_array[child_rtindex] = childrte;
		root->simple_rel_array[child_rtindex] = NULL;

#if !PG96
		Assert(childrte->relkind != RELKIND_PARTITIONED_TABLE);
#endif

		appinfo = makeNode(AppendRelInfo);
		appinfo->parent_relid = rti;
		appinfo->child_relid = child_rtindex;
		appinfo->parent_reltype = oldrelation->rd_rel->reltype;
		appinfo->child_reltype = newrelation->rd_rel->reltype;
		make_inh_translation_list(oldrelation, newrelation, child_rtindex,
								  &appinfo->translated_vars);
		appinfo->parent_reloid = parent_oid;
		appinfos = lappend(appinfos, appinfo);


		/* Close child relations, but keep locks */
		if (child_oid != parent_oid)
			heap_close(newrelation, NoLock);
	}

	heap_close(oldrelation, NoLock);

	root->append_rel_list = list_concat(root->append_rel_list, appinfos);
}
