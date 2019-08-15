/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <postgres.h>
#include <nodes/bitmapset.h>
#include <nodes/makefuncs.h>
#include <nodes/nodeFuncs.h>
#include <optimizer/clauses.h>
#include <optimizer/cost.h>
#include <optimizer/pathnode.h>
#include <optimizer/paths.h>
#include <optimizer/restrictinfo.h>
#include <optimizer/tlist.h>
#include <optimizer/var.h>
#include <parser/parsetree.h>
#include <utils/builtins.h>
#include <utils/typcache.h>

#include "compat.h"
#include "chunk.h"
#include "hypertable.h"
#include "hypertable_compression.h"
#include "decompress_chunk/decompress_chunk.h"
#include "decompress_chunk/planner.h"
#include "decompress_chunk/qual_pushdown.h"

#define DECOMPRESS_CHUNK_CPU_TUPLE_COST 0.01
#define DECOMPRESS_CHUNK_BATCH_SIZE 1000

static CustomPathMethods decompress_chunk_path_methods = {
	.CustomName = "DecompressChunk",
	.PlanCustomPath = decompress_chunk_plan_create,
};

static RangeTblEntry *decompress_chunk_make_rte(PlannerInfo *root, Oid compressed_relid,
												LOCKMODE lockmode);
static void create_compressed_scan_paths(PlannerInfo *root, DecompressChunkPath *path);

/*
 * calculate cost for DecompressChunkPath
 *
 * since we have to read whole batch before producing tuple
 * we put cost of 1 tuple of compressed_scan as startup cost
 */
static void
cost_decompress_chunk(Path *path, Path *compressed_path)
{
	/* startup_cost is cost before fetching first tuple */
	if (compressed_path->rows > 0)
		path->startup_cost = compressed_path->total_cost / compressed_path->rows;

	/* total_cost is cost for fetching all tuples */
	path->total_cost = compressed_path->total_cost + path->rows * DECOMPRESS_CHUNK_CPU_TUPLE_COST;
}

Path *
ts_decompress_chunk_path_create(PlannerInfo *root, RelOptInfo *rel, Hypertable *ht, Chunk *chunk,
								Path *subpath)
{
	DecompressChunkPath *path;
	Index compressed_index = root->simple_rel_array_size;
	Index chunk_index = rel->relid;
	Chunk *compressed_chunk = ts_chunk_get_by_id(chunk->fd.compressed_chunk_id, 0, true);
	Oid compressed_relid = compressed_chunk->table_id;

	path = (DecompressChunkPath *) newNode(sizeof(DecompressChunkPath), T_CustomPath);

	path->chunk_rel = rel;
	path->chunk_rte = planner_rt_fetch(chunk_index, root);
	path->hypertable_id = ht->fd.id;
	path->compression_info = get_hypertablecompression_info(ht->fd.id);

	path->cpath.path.pathtype = T_CustomScan;
	path->cpath.path.parent = rel;
	path->cpath.path.pathtarget = rel->reltarget;
	path->cpath.path.param_info = subpath->param_info;

	path->cpath.path.parallel_aware = false;
	path->cpath.path.parallel_safe = subpath->parallel_safe;
	path->cpath.path.parallel_workers = subpath->parallel_workers;

	path->cpath.flags = CUSTOMPATH_SUPPORT_BACKWARD_SCAN;
	path->cpath.methods = &decompress_chunk_path_methods;

	/*
	 * create RangeTblEntry and RelOptInfo for the compressed chunk
	 * and add it to PlannerInfo
	 */
	root->simple_rel_array_size++;
	root->simple_rel_array =
		repalloc(root->simple_rel_array, root->simple_rel_array_size * sizeof(RelOptInfo *));
	root->simple_rte_array =
		repalloc(root->simple_rte_array, root->simple_rel_array_size * sizeof(RangeTblEntry *));
#if PG11_GE
	root->append_rel_array =
		repalloc(root->append_rel_array, root->simple_rel_array_size * sizeof(AppendRelInfo *));
	root->append_rel_array[compressed_index] = NULL;
#endif

	path->compressed_rte = decompress_chunk_make_rte(root, compressed_relid, AccessShareLock);

	root->simple_rte_array[compressed_index] = path->compressed_rte;
	root->parse->rtable = lappend(root->parse->rtable, path->compressed_rte);

	root->simple_rel_array[compressed_index] = NULL;
#if PG96
	path->compressed_rel = build_simple_rel(root, compressed_index, RELOPT_BASEREL);
#else
	path->compressed_rel = build_simple_rel(root, compressed_index, NULL);
#endif
	root->simple_rel_array[compressed_index] = path->compressed_rel;

	pushdown_quals(root, path);
	set_baserel_size_estimates(root, path->compressed_rel);
	path->cpath.path.rows = path->compressed_rel->rows * DECOMPRESS_CHUNK_BATCH_SIZE;

	create_compressed_scan_paths(root, path);
	path->cpath.custom_paths = list_make1(path->compressed_rel->cheapest_total_path);

	cost_decompress_chunk(&path->cpath.path, path->compressed_rel->cheapest_total_path);

	return &path->cpath.path;
}

static void
create_compressed_scan_paths(PlannerInfo *root, DecompressChunkPath *path)
{
	List *pathlist = NIL;
	Path *compressed_path;

	compressed_path = create_seqscan_path(root, path->compressed_rel, NULL, 0);
	pathlist = lappend(pathlist, compressed_path);

	path->compressed_rel->pathlist = pathlist;

	set_cheapest(path->compressed_rel);
}

/*
 * create RangeTblEntry for compressed chunk
 */
static RangeTblEntry *
decompress_chunk_make_rte(PlannerInfo *root, Oid compressed_relid, LOCKMODE lockmode)
{
	RangeTblEntry *rte = makeNode(RangeTblEntry);
	Relation r = heap_open(compressed_relid, lockmode);
	int varattno;

	rte->rtekind = RTE_RELATION;
	rte->relid = compressed_relid;
	rte->relkind = r->rd_rel->relkind;
	rte->eref = makeAlias(RelationGetRelationName(r), NULL);

	/*
	 * inlined from buildRelationAliases()
	 * alias handling has been stripped because we won't
	 * need alias handling at this level
	 */
	for (varattno = 0; varattno < r->rd_att->natts; varattno++)
	{
		Form_pg_attribute attr = TupleDescAttr(r->rd_att, varattno);
		Value *attrname;

		if (attr->attisdropped)
			/* Always insert an empty string for a dropped column */
			attrname = makeString(pstrdup(""));
		else
			attrname = makeString(pstrdup(NameStr(attr->attname)));

		rte->eref->colnames = lappend(rte->eref->colnames, attrname);
	}

	/*
	 * Drop the rel refcount, but keep the access lock till end of transaction
	 * so that the table can't be deleted or have its schema modified
	 * underneath us.
	 */
	heap_close(r, NoLock);

	/*
	 * Set flags and access permissions.
	 *
	 * The initial default on access checks is always check-for-READ-access,
	 * which is the right thing for all except target tables.
	 */
	rte->lateral = false;
	rte->inh = false;
	rte->inFromCl = false;

	rte->requiredPerms = ACL_SELECT;
	rte->checkAsUser = InvalidOid; /* not set-uid by default, either */
	rte->selectedCols = NULL;
	rte->insertedCols = NULL;
	rte->updatedCols = NULL;

	return rte;
}

FormData_hypertable_compression *
get_column_compressioninfo(List *hypertable_compression_info, char *column_name)
{
	ListCell *lc;

	foreach (lc, hypertable_compression_info)
	{
		FormData_hypertable_compression *fd = lfirst(lc);
		if (namestrcmp(&fd->attname, column_name) == 0)
			return fd;
	}

	elog(ERROR, "No compression information for column \"%s\" found.", column_name);

	pg_unreachable();
}

/*
 * find matching column attno for compressed chunk
 *
 * since we dont want aliasing to interfere we lookup directly in catalog
 * instead of using RangeTblEntry
 */
AttrNumber
get_compressed_attno(DecompressChunkPath *dcpath, AttrNumber chunk_attno)
{
	AttrNumber compressed_attno;
	char *chunk_col = get_attname_compat(dcpath->chunk_rte->relid, chunk_attno, false);
	compressed_attno = get_attnum(dcpath->compressed_rte->relid, chunk_col);

	if (compressed_attno == InvalidAttrNumber)
		elog(ERROR, "No matching column in compressed chunk found.");

	return compressed_attno;
}
