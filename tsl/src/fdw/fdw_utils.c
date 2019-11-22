/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <optimizer/paths.h>
#include <optimizer/pathnode.h>
#include <nodes/extensible.h>

#include "fdw_utils.h"
#include "fdw/relinfo.h"

#ifdef TS_DEBUG
/*
 * This is shallow Path copy that includes deep copy of few fields of interest for printing debug
 * information. Doing deep copy of Path seems to be a lot of work and we only need few fields
 * anyhow. Casting or doing any work with incomplete Path copy is unsafe and should be avoided as
 * there will be memory errors.
 *
 * Copied path is intended to only be used in debug.c.
 *
 * PostgreSQL function copyObject does not support copying Path(s) so we have our own copy function.
 */
static Path *
copy_path(Path *in)
{
	Path *path_copy;
	RelOptInfo *parent = palloc(sizeof(RelOptInfo));

	*parent = *in->parent;

	switch (nodeTag(in))
	{
		case T_CustomPath:
		{
			CustomPath *cp_copy = palloc(sizeof(CustomPath));
			CustomPath *cp = castNode(CustomPath, in);
			ListCell *lc;
			cp_copy->custom_paths = NIL;

			*cp_copy = *cp;
			foreach (lc, cp->custom_paths)
			{
				Path *p = lfirst_node(Path, lc);
				Path *c = copy_path(p);
				cp_copy->custom_paths = lappend(cp_copy->custom_paths, c);
			}
			path_copy = (Path *) cp_copy;
			break;
		}

		default:
		{
			path_copy = palloc(sizeof(Path));
			*path_copy = *in;
		}
	}
	path_copy->parent = parent;

	return path_copy;
}
#endif /* TS_DEBUG */

void
fdw_utils_add_path(RelOptInfo *rel, Path *new_path)
{
#ifdef TS_DEBUG
	TsFdwRelInfo *fdw_info = fdw_relinfo_get(rel);
	Path *path_copy;

	/* Since add_path will deallocate thrown paths we need to create a copy here so we can print it
	 * later on */
	path_copy = copy_path(new_path);
	fdw_info->considered_paths = lappend(fdw_info->considered_paths, path_copy);
#endif
	add_path(rel, new_path);
}

/*
 * Deallocate path copy
 */
void
fdw_utils_free_path(Path *path)
{
	pfree(path->parent);
	if (nodeTag(path) == T_CustomPath)
	{
		CustomPath *cp = (CustomPath *) path;
		ListCell *lc;

		foreach (lc, cp->custom_paths)
		{
			Path *p = lfirst(lc);
			cp->custom_paths = list_delete_ptr(cp->custom_paths, p);
			fdw_utils_free_path(p);
		}
	}
	pfree(path);
}
