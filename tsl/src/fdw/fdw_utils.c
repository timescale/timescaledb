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
 * Copy a path.
 *
 * The returned path is a shallow copy that includes deep copies of a few
 * fields of interest when printing debug information. Doing a deep copy of a
 * Path is a lot of work so we only copy the fields we need.
 *
 * The copied path is intended to be used only in debug.c.
 *
 * Note that PostgreSQL's copyObject does not support copying Path(s) so we
 * have our own copy function.
 */
static Path *
copy_path(Path *in)
{
	Path *path;
	RelOptInfo *parent = makeNode(RelOptInfo);

	*parent = *in->parent;

	switch (nodeTag(in))
	{
		case T_CustomPath:
		{
			CustomPath *cp_copy = makeNode(CustomPath);
			CustomPath *cp = castNode(CustomPath, in);
			ListCell *lc;

			*cp_copy = *cp;
			cp_copy->custom_paths = NIL;

			foreach (lc, cp->custom_paths)
			{
				Path *p = copy_path(lfirst_node(Path, lc));
				cp_copy->custom_paths = lappend(cp_copy->custom_paths, p);
			}
			path = &cp_copy->path;
			break;
		}
		case T_ForeignPath:
		{
			ForeignPath *fp = makeNode(ForeignPath);
			*fp = *castNode(ForeignPath, in);
			path = &fp->path;
			break;
		}
		default:
			/* Not supported */
			Assert(false);
			pg_unreachable();
			return in;
	}

	path->parent = parent;

	return path;
}

static ConsideredPath *
create_considered_path(Path *path)
{
	ConsideredPath *cp = palloc(sizeof(ConsideredPath));

	cp->path = copy_path(path);
	cp->origin = (uintptr_t) path;

	return cp;
}

void
fdw_utils_add_path(RelOptInfo *rel, Path *new_path)
{
	TsFdwRelInfo *fdw_info = fdw_relinfo_get(rel);
	ConsideredPath *cp = create_considered_path(new_path);

	/* Since add_path will deallocate thrown paths we need to create a copy here so we can print it
	 * later on */
	fdw_info->considered_paths = lappend(fdw_info->considered_paths, cp);
	add_path(rel, new_path);
}

static void
free_path(Path *path)
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
			free_path(p);
		}
	}
	pfree(path);
}

/*
 * Deallocate path copy
 */
void
fdw_utils_free_path(ConsideredPath *cpath)
{
	free_path(cpath->path);
	pfree(cpath);
}

#endif /* TS_DEBUG */
