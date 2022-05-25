/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */

#include <postgres.h>
#include <nodes/pathnodes.h>

#include "planner.h"

/*
 * This code deals with removing the intermediate constraints
 * we added before planning to improve chunk exclusion.
 */

static bool
restrictinfo_is_marked(RestrictInfo *ri)
{
	switch (nodeTag(ri->clause))
	{
		case T_OpExpr:
			return castNode(OpExpr, ri->clause)->location == PLANNER_LOCATION_MAGIC;
		case T_ScalarArrayOpExpr:
			return castNode(ScalarArrayOpExpr, ri->clause)->location == PLANNER_LOCATION_MAGIC;
		default:
			break;
	}
	return false;
}

/*
 * Remove marked constraints from RestrictInfo clause.
 */
static List *
restrictinfo_cleanup(List *restrictinfos, bool *pfiltered)
{
	List *filtered_ri = NIL;
	ListCell *lc;
	bool filtered = false;
	if (!restrictinfos)
		return NULL;

	foreach (lc, restrictinfos)
	{
		RestrictInfo *ri = (RestrictInfo *) lfirst(lc);
		if (restrictinfo_is_marked(ri))
		{
			filtered = true;
			continue;
		}
		filtered_ri = lappend(filtered_ri, ri);
	}

	if (pfiltered)
		*pfiltered = filtered;

	return filtered ? filtered_ri : restrictinfos;
}

/*
 * Remove marked constraints from IndexPath.
 */
static void
indexpath_cleanup(IndexPath *path)
{
	ListCell *lc;
	List *filtered_ic = NIL;
	path->indexinfo->indrestrictinfo = restrictinfo_cleanup(path->indexinfo->indrestrictinfo, NULL);

	foreach (lc, path->indexclauses)
	{
		IndexClause *iclause = lfirst_node(IndexClause, lc);
		if (restrictinfo_is_marked(iclause->rinfo))
			continue;

		filtered_ic = lappend(filtered_ic, iclause);
	}
	path->indexclauses = filtered_ic;
}

void
ts_planner_constraint_cleanup(PlannerInfo *root, RelOptInfo *rel)
{
	ListCell *lc;
	bool filtered = false;
	if (rel->baserestrictinfo)
		rel->baserestrictinfo = restrictinfo_cleanup(rel->baserestrictinfo, &filtered);

	/*
	 * If we added constraints those will be present in baserestrictinfo.
	 * If we did not remove anything from baserestrictinfo in the step
	 * above we can skip looking in the paths.
	 */
	if (filtered)
	{
		/*
		 * For seqscan cleaning up baserestrictinfo is enough but for
		 * BitmapHeapPath and IndexPath we need some extra steps.
		 */
		foreach (lc, rel->pathlist)
		{
			switch (nodeTag(lfirst(lc)))
			{
				case T_BitmapHeapPath:
				{
					BitmapHeapPath *path = lfirst_node(BitmapHeapPath, lc);
					if (IsA(path->bitmapqual, IndexPath))
						indexpath_cleanup(castNode(IndexPath, path->bitmapqual));

					break;
				}
				case T_IndexPath:
				{
					indexpath_cleanup(castNode(IndexPath, lfirst(lc)));
					break;
				}
				default:
					break;
			}
		}
	}
}
