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
#include "utils.h"

#define DECOMPRESS_CHUNK_CPU_TUPLE_COST 0.01
#define DECOMPRESS_CHUNK_BATCH_SIZE 1000

static CustomPathMethods decompress_chunk_path_methods = {
	.CustomName = "DecompressChunk",
	.PlanCustomPath = decompress_chunk_plan_create,
};

static RangeTblEntry *decompress_chunk_make_rte(Oid compressed_relid, LOCKMODE lockmode);
static void create_compressed_scan_paths(PlannerInfo *root, RelOptInfo *compressed_rel,
										 int parallel_workers);

static Path *decompress_chunk_path_create(PlannerInfo *root, RelOptInfo *chunk_rel,
										  RelOptInfo *compressed_rel, Hypertable *ht,
										  List *compression_info, int parallel_workers);

static Index decompress_chunk_add_plannerinfo(PlannerInfo *root, Chunk *chunk);

void
ts_decompress_chunk_generate_paths(PlannerInfo *root, RelOptInfo *chunk_rel, Hypertable *ht,
								   Chunk *chunk)
{
	Index compressed_index;
	RelOptInfo *compressed_rel;
	Path *path;
	List *compression_info = ts_hypertable_compression_get(ht->fd.id);
	/*
	 * since we rely on parallel coordination from the scan below
	 * this node it is probably not beneficial to have more
	 * than a single worker per chunk
	 */
	int parallel_workers = 1;

	Assert(chunk->fd.compressed_chunk_id > 0);

	chunk_rel->pathlist = NIL;
	chunk_rel->partial_pathlist = NIL;

	/* add RangeTblEntry and RelOptInfo for compressed chunk */
	compressed_index = decompress_chunk_add_plannerinfo(root, chunk);
	compressed_rel = root->simple_rel_array[compressed_index];

	compressed_rel->consider_parallel = chunk_rel->consider_parallel;

	pushdown_quals(root, chunk_rel, compressed_rel, compression_info);
	set_baserel_size_estimates(root, compressed_rel);
	chunk_rel->rows = compressed_rel->rows * DECOMPRESS_CHUNK_BATCH_SIZE;
	create_compressed_scan_paths(root,
								 compressed_rel,
								 compressed_rel->consider_parallel ? parallel_workers : 0);

	/* create non-parallel path */
	path = decompress_chunk_path_create(root, chunk_rel, compressed_rel, ht, compression_info, 0);
	add_path(chunk_rel, path);

	/* create parallel path */
	if (compressed_rel->consider_parallel && list_length(compressed_rel->partial_pathlist) > 0)
	{
		path = decompress_chunk_path_create(root,
											chunk_rel,
											compressed_rel,
											ht,
											compression_info,
											parallel_workers);
		add_partial_path(chunk_rel, path);
	}
}

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

/*
 * create RangeTblEntry and RelOptInfo for the compressed chunk
 * and add it to PlannerInfo
 */
static Index
decompress_chunk_add_plannerinfo(PlannerInfo *root, Chunk *chunk)
{
	Index compressed_index = root->simple_rel_array_size;
	Chunk *compressed_chunk = ts_chunk_get_by_id(chunk->fd.compressed_chunk_id, 0, true);
	Oid compressed_relid = compressed_chunk->table_id;
	RangeTblEntry *compressed_rte;

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

	compressed_rte = decompress_chunk_make_rte(compressed_relid, AccessShareLock);
	root->simple_rte_array[compressed_index] = compressed_rte;

	root->parse->rtable = lappend(root->parse->rtable, compressed_rte);

	root->simple_rel_array[compressed_index] = NULL;
#if PG96
	root->simple_rel_array[compressed_index] =
		build_simple_rel(root, compressed_index, RELOPT_BASEREL);
#else
	root->simple_rel_array[compressed_index] = build_simple_rel(root, compressed_index, NULL);
#endif

	return compressed_index;
}

static Path *
decompress_chunk_path_create(PlannerInfo *root, RelOptInfo *chunk_rel, RelOptInfo *compressed_rel,
							 Hypertable *ht, List *compression_info, int parallel_workers)
{
	DecompressChunkPath *path;
	AppendRelInfo *appinfo;
	bool parallel_safe = false;

	path = (DecompressChunkPath *) newNode(sizeof(DecompressChunkPath), T_CustomPath);

	path->chunk_rel = chunk_rel;
	path->chunk_rte = planner_rt_fetch(chunk_rel->relid, root);
	path->compressed_rel = compressed_rel;
	path->compressed_rte = planner_rt_fetch(compressed_rel->relid, root);
	path->hypertable_id = ht->fd.id;
	path->compression_info = compression_info;

	appinfo = ts_get_appendrelinfo(root, chunk_rel->relid);
	path->ht_rte = planner_rt_fetch(appinfo->parent_relid, root);

	path->cpath.path.pathtype = T_CustomScan;
	path->cpath.path.parent = chunk_rel;
	path->cpath.path.pathtarget = chunk_rel->reltarget;

	path->cpath.flags = CUSTOMPATH_SUPPORT_BACKWARD_SCAN;
	path->cpath.methods = &decompress_chunk_path_methods;

	path->cpath.path.rows = path->compressed_rel->rows * DECOMPRESS_CHUNK_BATCH_SIZE;

	if (parallel_workers > 0 && list_length(compressed_rel->partial_pathlist) > 0)
		parallel_safe = true;

	path->cpath.path.parallel_aware = false;
	path->cpath.path.parallel_safe = parallel_safe;
	path->cpath.path.parallel_workers = parallel_workers;

	if (parallel_safe)
		path->cpath.custom_paths = list_make1(linitial(compressed_rel->partial_pathlist));
	else
		path->cpath.custom_paths = list_make1(compressed_rel->cheapest_total_path);

	cost_decompress_chunk(&path->cpath.path, path->compressed_rel->cheapest_total_path);

	return &path->cpath.path;
}

static void
create_compressed_scan_paths(PlannerInfo *root, RelOptInfo *compressed_rel, int parallel_workers)
{
	Path *compressed_path;

	/* create non parallel scan path */
	compressed_path = create_seqscan_path(root, compressed_rel, NULL, 0);
	add_path(compressed_rel, compressed_path);

	/* create parallel scan path */
	if (compressed_rel->consider_parallel && parallel_workers > 0)
	{
		compressed_path = create_seqscan_path(root, compressed_rel, NULL, parallel_workers);
		Assert(compressed_path->parallel_aware);
		add_partial_path(compressed_rel, compressed_path);
	}

	set_cheapest(compressed_rel);
}

/*
 * create RangeTblEntry for compressed chunk
 */
static RangeTblEntry *
decompress_chunk_make_rte(Oid compressed_relid, LOCKMODE lockmode)
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
 * find matching column attno for compressed chunk based on hypertable attno
 *
 * since we dont want aliasing to interfere we lookup directly in catalog
 * instead of using RangeTblEntry
 */
AttrNumber
get_compressed_attno(DecompressChunkPath *dcpath, AttrNumber ht_attno)
{
	AttrNumber compressed_attno;
	char *chunk_col = get_attname_compat(dcpath->ht_rte->relid, ht_attno, false);
	compressed_attno = get_attnum(dcpath->compressed_rte->relid, chunk_col);

	if (compressed_attno == InvalidAttrNumber)
		elog(ERROR, "No matching column in compressed chunk found.");

	return compressed_attno;
}
