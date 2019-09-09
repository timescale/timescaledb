/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <postgres.h>
#include <catalog/pg_operator.h>
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
#include <utils/lsyscache.h>
#include <utils/typcache.h>

#include "compat.h"
#include "chunk.h"
#include "hypertable.h"
#include "hypertable_compression.h"
#include "compression/create.h"
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

static Path *decompress_chunk_path_create(PlannerInfo *root, CompressionInfo *info,
										  int parallel_workers);

static void decompress_chunk_add_plannerinfo(PlannerInfo *root, CompressionInfo *info,
											 Chunk *chunk);

static bool can_order_by_pathkeys(CompressionInfo *info, List *pathkeys, bool *needs_sequence_num);

static DecompressChunkPath *
copy_decompress_chunk_path(DecompressChunkPath *src)
{
	DecompressChunkPath *dst = palloc(sizeof(DecompressChunkPath));
	memcpy(dst, src, sizeof(DecompressChunkPath));

	return dst;
}

static CompressionInfo *
build_compressioninfo(PlannerInfo *root, Hypertable *ht, RelOptInfo *chunk_rel)
{
	ListCell *lc;
	AppendRelInfo *appinfo;

	CompressionInfo *info = palloc0(sizeof(CompressionInfo));

	info->chunk_rel = chunk_rel;
	info->chunk_rte = planner_rt_fetch(chunk_rel->relid, root);

	appinfo = ts_get_appendrelinfo(root, chunk_rel->relid);
	info->ht_rte = planner_rt_fetch(appinfo->parent_relid, root);
	info->hypertable_id = ht->fd.id;

	info->hypertable_compression_info = ts_hypertable_compression_get(ht->fd.id);

	foreach (lc, info->hypertable_compression_info)
	{
		FormData_hypertable_compression *fd = lfirst(lc);
		if (fd->orderby_column_index > 0)
			info->num_orderby_columns++;
		if (fd->segmentby_column_index > 0)
			info->num_segmentby_columns++;
	}

	return info;
}

void
ts_decompress_chunk_generate_paths(PlannerInfo *root, RelOptInfo *chunk_rel, Hypertable *ht,
								   Chunk *chunk)
{
	RelOptInfo *compressed_rel;
	Path *path;
	bool needs_sequence_num = false;

	CompressionInfo *info = build_compressioninfo(root, ht, chunk_rel);

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
	decompress_chunk_add_plannerinfo(root, info, chunk);
	compressed_rel = info->compressed_rel;

	compressed_rel->consider_parallel = chunk_rel->consider_parallel;

	pushdown_quals(root, chunk_rel, compressed_rel, info->hypertable_compression_info);
	set_baserel_size_estimates(root, compressed_rel);
	chunk_rel->rows = compressed_rel->rows * DECOMPRESS_CHUNK_BATCH_SIZE;
	create_compressed_scan_paths(root,
								 compressed_rel,
								 compressed_rel->consider_parallel ? parallel_workers : 0);

	/* create non-parallel path */
	path = decompress_chunk_path_create(root, info, 0);
	add_path(chunk_rel, path);

	/* create ordered path if compressed order is compatible with query order */
	if (root->query_pathkeys &&
		can_order_by_pathkeys(info, root->query_pathkeys, &needs_sequence_num))
	{
		DecompressChunkPath *dcpath = copy_decompress_chunk_path((DecompressChunkPath *) path);
		dcpath->needs_sequence_num = needs_sequence_num;
		dcpath->cpath.path.pathkeys = root->query_pathkeys;
		add_path(chunk_rel, (Path *) dcpath);
	}

	/* create parallel path */
	if (compressed_rel->consider_parallel && list_length(compressed_rel->partial_pathlist) > 0)
	{
		path = decompress_chunk_path_create(root, info, parallel_workers);
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
static void
decompress_chunk_add_plannerinfo(PlannerInfo *root, CompressionInfo *info, Chunk *chunk)
{
	Index compressed_index = root->simple_rel_array_size;
	Chunk *compressed_chunk = ts_chunk_get_by_id(chunk->fd.compressed_chunk_id, 0, true);
	Oid compressed_relid = compressed_chunk->table_id;

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

	info->compressed_rte = decompress_chunk_make_rte(compressed_relid, AccessShareLock);
	root->simple_rte_array[compressed_index] = info->compressed_rte;

	root->parse->rtable = lappend(root->parse->rtable, info->compressed_rte);

	root->simple_rel_array[compressed_index] = NULL;
#if PG96
	root->simple_rel_array[compressed_index] =
		build_simple_rel(root, compressed_index, RELOPT_BASEREL);
#else
	root->simple_rel_array[compressed_index] = build_simple_rel(root, compressed_index, NULL);
#endif

	info->compressed_rel = root->simple_rel_array[compressed_index];
}

static Path *
decompress_chunk_path_create(PlannerInfo *root, CompressionInfo *info, int parallel_workers)
{
	DecompressChunkPath *path;
	bool parallel_safe = false;

	path = (DecompressChunkPath *) newNode(sizeof(DecompressChunkPath), T_CustomPath);

	path->info = info;

	path->cpath.path.pathtype = T_CustomScan;
	path->cpath.path.parent = info->chunk_rel;
	path->cpath.path.pathtarget = info->chunk_rel->reltarget;

	path->cpath.flags = 0;
	path->cpath.methods = &decompress_chunk_path_methods;

	path->cpath.path.rows = info->compressed_rel->rows * DECOMPRESS_CHUNK_BATCH_SIZE;

	if (parallel_workers > 0 && list_length(info->compressed_rel->partial_pathlist) > 0)
		parallel_safe = true;

	path->cpath.path.parallel_aware = false;
	path->cpath.path.parallel_safe = parallel_safe;
	path->cpath.path.parallel_workers = parallel_workers;

	if (parallel_safe)
		path->cpath.custom_paths = list_make1(linitial(info->compressed_rel->partial_pathlist));
	else
		path->cpath.custom_paths = list_make1(info->compressed_rel->cheapest_total_path);

	cost_decompress_chunk(&path->cpath.path, info->compressed_rel->cheapest_total_path);

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
get_compressed_attno(CompressionInfo *info, AttrNumber ht_attno)
{
	AttrNumber compressed_attno;
	char *chunk_col = get_attname_compat(info->ht_rte->relid, ht_attno, false);
	compressed_attno = get_attnum(info->compressed_rte->relid, chunk_col);

	if (compressed_attno == InvalidAttrNumber)
		elog(ERROR, "No matching column in compressed chunk found.");

	return compressed_attno;
}

static bool
can_order_by_pathkeys(CompressionInfo *info, List *pathkeys, bool *needs_sequence_num)
{
	int pk_index;
	PathKey *pk;
	Var *var;
	Expr *expr;
	char *column_name;
	FormData_hypertable_compression *ci;
	ListCell *lc = list_head(pathkeys);

	/* all segmentby columns need to be prefix of pathkeys */
	if (info->num_segmentby_columns > 0)
	{
		Bitmapset *segmentby_columns = NULL;

		for (; lc != NULL && bms_num_members(segmentby_columns) < info->num_segmentby_columns;
			 lc = lnext(lc))
		{
			pk = lfirst(lc);
			expr = ts_find_em_expr_for_rel(pk->pk_eclass, info->chunk_rel);

			if (expr == NULL || !IsA(expr, Var))
				return false;

			var = castNode(Var, expr);
			column_name = get_attname_compat(info->chunk_rte->relid, var->varattno, false);
			ci = get_column_compressioninfo(info->hypertable_compression_info, column_name);

			if (ci->segmentby_column_index > 0)
			{
				segmentby_columns = bms_add_member(segmentby_columns, var->varattno);
				continue;
			}

			return false;
		}

		if (bms_num_members(segmentby_columns) != info->num_segmentby_columns)
			return false;
	}

	/*
	 * if pathkeys includes columns past segmentby columns
	 * we need sequence_num in the targetlist for ordering
	 */
	if (lc != NULL)
		*needs_sequence_num = true;

	/*
	 * loop over the rest of pathkeys
	 * this needs to exactly match the configured compress_orderby
	 */
	for (pk_index = 1; lc != NULL; lc = lnext(lc), pk_index++)
	{
		pk = lfirst(lc);
		expr = ts_find_em_expr_for_rel(pk->pk_eclass, info->chunk_rel);

		if (expr == NULL || !IsA(expr, Var))
			return false;

		var = castNode(Var, expr);
		column_name = get_attname_compat(info->chunk_rte->relid, var->varattno, false);
		ci = get_column_compressioninfo(info->hypertable_compression_info, column_name);

		if (ci->orderby_column_index != pk_index)
			return false;

		if (ci->orderby_nullsfirst != pk->pk_nulls_first)
			return false;

		/*
		 * pk_strategy is either BTLessStrategyNumber (for ASC) or
		 * BTGreaterStrategyNumber (for DESC)
		 */
		if ((pk->pk_strategy == BTLessStrategyNumber && !ci->orderby_asc) ||
			(pk->pk_strategy == BTGreaterStrategyNumber && ci->orderby_asc))
			return false;
	}

	return true;
}
