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

enum SeqNumOrderBy
{
	SEQNUM_ORDERBY_NOMATCH = 1,
	SEQNUM_ORDERBY_SAME = 2,
	SEQNUM_ORDERBY_REVERSE = 3
};
typedef struct OrderByInfo
{
	enum SeqNumOrderBy sequence_num_orderby_stat;
	List *compressed_pathkeys;
	bool needs_sequence_num;
	bool try_order_by_compressed;
} OrderByInfo;

static RangeTblEntry *decompress_chunk_make_rte(Oid compressed_relid, LOCKMODE lockmode);
static void create_compressed_scan_paths(PlannerInfo *root, RelOptInfo *compressed_rel,
										 int parallel_workers, CompressionInfo *info,
										 OrderByInfo *seqnum_info);

static Path *decompress_chunk_path_create(PlannerInfo *root, CompressionInfo *info,
										  int parallel_workers, Path *compressed_path);

static void decompress_chunk_add_plannerinfo(PlannerInfo *root, CompressionInfo *info, Chunk *chunk,
											 RelOptInfo *chunk_rel, bool needs_sequence_num);

static bool can_order_by_pathkeys(RelOptInfo *chunk_rel, CompressionInfo *info, List *pathkeys,
								  OrderByInfo *ret);

static void
build_compressed_scan_pathkeys(OrderByInfo *seqnum_info, PlannerInfo *root, List *chunk_pathkeys,
							   CompressionInfo *info)
{
	Var *var;
	int varattno;
	List *compressed_pathkeys = NIL;
	PathKey *pk;

	/*
	 * all segmentby columns need to be prefix of pathkeys
	 * except those with equality constraint in baserestrictinfo
	 */
	if (info->num_segmentby_columns > 0)
	{
		Bitmapset *segmentby_columns = bms_copy(info->chunk_segmentby_ri);
		ListCell *lc;
		char *column_name;
		PG_USED_FOR_ASSERTS_ONLY FormData_hypertable_compression *ci;
		Oid sortop;

		for (lc = list_head(chunk_pathkeys);
			 lc != NULL && bms_num_members(segmentby_columns) < info->num_segmentby_columns;
			 lc = lnext(lc))
		{
			PathKey *pk = lfirst(lc);
			var = (Var *) ts_find_em_expr_for_rel(pk->pk_eclass, info->chunk_rel);

			if (var == NULL || !IsA(var, Var))
				/* this should not happen because we validated the pathkeys when creating the path
				 */
				elog(ERROR, "Invalid pathkey for compressed scan");

			column_name = get_attname_compat(info->chunk_rte->relid, var->varattno, false);
			ci = get_column_compressioninfo(info->hypertable_compression_info, column_name);

			Assert(ci->segmentby_column_index > 0);
			segmentby_columns = bms_add_member(segmentby_columns, var->varattno);
			varattno = get_attnum(info->compressed_rte->relid, column_name);
			var = makeVar(info->compressed_rel->relid,
						  varattno,
						  var->vartype,
						  var->vartypmod,
						  var->varcollid,
						  0);

			sortop =
				get_opfamily_member(pk->pk_opfamily, var->vartype, var->vartype, pk->pk_strategy);
			pk = ts_make_pathkey_from_sortop(root,
											 (Expr *) var,
											 NULL,
											 sortop,
											 pk->pk_nulls_first,
											 0,
											 true);
			compressed_pathkeys = lappend(compressed_pathkeys, pk);
		}

		/* we validated this when we created the Path so only asserting here */
		Assert(bms_num_members(segmentby_columns) == info->num_segmentby_columns);
	}

	/*
	 * If pathkeys contains non-segmentby columns the rest of the ordering
	 * requirements will be satisfied by ordering by sequence_num
	 */
	if (list_length(chunk_pathkeys) > list_length(compressed_pathkeys))
	{
		bool nulls_first;
		Oid sortop;
		varattno =
			get_attnum(info->compressed_rte->relid, COMPRESSION_COLUMN_METADATA_SEQUENCE_NUM_NAME);
		var = makeVar(info->compressed_rel->relid, varattno, INT4OID, -1, InvalidOid, 0);
		/*sequence_num is ordered as ASC nulls last by default */
		if (seqnum_info->sequence_num_orderby_stat == SEQNUM_ORDERBY_REVERSE)
		{
			sortop = get_commutator(Int4LessOperator);
			nulls_first = true;
		}
		else // NOMATCH or SAME use the default with which seqnum is stored
		{
			sortop = Int4LessOperator;
			nulls_first = false;
		}
		/* Find Int4LessOperator used to sort seq num */
		pk = ts_make_pathkey_from_sortop(root, (Expr *) var, NULL, sortop, nulls_first, 0, true);

		compressed_pathkeys = lappend(compressed_pathkeys, pk);
	}
	seqnum_info->compressed_pathkeys = compressed_pathkeys;
}

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

	appinfo = ts_get_appendrelinfo(root, chunk_rel->relid, false);
	info->ht_rte = planner_rt_fetch(appinfo->parent_relid, root);
	info->hypertable_id = ht->fd.id;

	info->hypertable_compression_info = ts_hypertable_compression_get(ht->fd.id);

	foreach (lc, info->hypertable_compression_info)
	{
		FormData_hypertable_compression *fd = lfirst(lc);
		if (fd->orderby_column_index > 0)
			info->num_orderby_columns++;
		if (fd->segmentby_column_index > 0)
		{
			AttrNumber chunk_attno = get_attnum(info->chunk_rte->relid, NameStr(fd->attname));
			info->chunk_segmentby_attnos =
				bms_add_member(info->chunk_segmentby_attnos, chunk_attno);
			info->num_segmentby_columns++;
		}
	}

	return info;
}

void
ts_decompress_chunk_generate_paths(PlannerInfo *root, RelOptInfo *chunk_rel, Hypertable *ht,
								   Chunk *chunk)
{
	RelOptInfo *compressed_rel;
	RelOptInfo *hypertable_rel;
	ListCell *lc;
	double new_row_estimate;

	CompressionInfo *info = build_compressioninfo(root, ht, chunk_rel);
	Index ht_index;
	OrderByInfo seqnum_info = { 0 };

	bool try_order_by_compressed =
		root->query_pathkeys &&
		can_order_by_pathkeys(chunk_rel, info, root->query_pathkeys, &seqnum_info);
	/*
	 * since we rely on parallel coordination from the scan below
	 * this node it is probably not beneficial to have more
	 * than a single worker per chunk
	 */
	int parallel_workers = 1;

	AppendRelInfo *chunk_info = ts_get_appendrelinfo(root, chunk_rel->relid, false);
	Assert(chunk_info != NULL);
	Assert(chunk_info->parent_reloid == ht->main_table_relid);
	ht_index = chunk_info->parent_relid;
	hypertable_rel = root->simple_rel_array[ht_index];

	Assert(chunk->fd.compressed_chunk_id > 0);

	chunk_rel->pathlist = NIL;
	chunk_rel->partial_pathlist = NIL;

	/* add RangeTblEntry and RelOptInfo for compressed chunk */
	decompress_chunk_add_plannerinfo(root, info, chunk, chunk_rel, try_order_by_compressed);
	compressed_rel = info->compressed_rel;

	compressed_rel->consider_parallel = chunk_rel->consider_parallel;

	pushdown_quals(root, chunk_rel, compressed_rel, info->hypertable_compression_info);
	set_baserel_size_estimates(root, compressed_rel);
	new_row_estimate = compressed_rel->rows * DECOMPRESS_CHUNK_BATCH_SIZE;
	/* adjust the parent's estimate by the diff of new and old estimate */
	hypertable_rel->rows += (new_row_estimate - chunk_rel->rows);
	chunk_rel->rows = new_row_estimate;
	create_compressed_scan_paths(root,
								 compressed_rel,
								 compressed_rel->consider_parallel ? parallel_workers : 0,
								 info,
								 &seqnum_info);

	/* create non-parallel paths */
	foreach (lc, compressed_rel->pathlist)
	{
		Path *child_path = lfirst(lc);
		Path *path;

		/*
		 * filter out all paths that try to JOIN the compressed chunk on the
		 * hypertable or the uncompressed chunk
		 * TODO ideally we wouldn't create these paths in the first place...
		 */
		if (child_path->param_info != NULL &&
			(bms_is_member(chunk_rel->relid, child_path->param_info->ppi_req_outer) ||
			 bms_is_member(ht_index, child_path->param_info->ppi_req_outer)))
			continue;

		path = decompress_chunk_path_create(root, info, 0, child_path);

		/* create ordered path if compressed order is compatible with query order */
		if (try_order_by_compressed)
		{
			DecompressChunkPath *dcpath = copy_decompress_chunk_path((DecompressChunkPath *) path);
			dcpath->needs_sequence_num = seqnum_info.needs_sequence_num;
			dcpath->reverse =
				(seqnum_info.sequence_num_orderby_stat == SEQNUM_ORDERBY_REVERSE) ? true : false;
			dcpath->compressed_pathkeys = seqnum_info.compressed_pathkeys;
			Assert(dcpath->cpath.path.pathkeys == NIL);
			dcpath->cpath.path.pathkeys = root->query_pathkeys;
			add_path(chunk_rel, (Path *) dcpath);
		}
		/* this has to go after the path is copied for the ordered path since path can get freed in
		 * add_path */
		add_path(chunk_rel, path);
	}
	/* the chunk_rel now owns the paths, remove them from the compressed_rel so they can't be freed
	 * if it's planned */
	compressed_rel->pathlist = NIL;

	/* create parallel paths */
	if (compressed_rel->consider_parallel)
	{
		foreach (lc, compressed_rel->partial_pathlist)
		{
			Path *child_path = lfirst(lc);
			Path *path;
			if (child_path->param_info != NULL &&
				(bms_is_member(chunk_rel->relid, child_path->param_info->ppi_req_outer) ||
				 bms_is_member(ht_index, child_path->param_info->ppi_req_outer)))
				continue;
			path = decompress_chunk_path_create(root, info, parallel_workers, child_path);
			add_partial_path(chunk_rel, path);
		}
		/* the chunk_rel now owns the paths, remove them from the compressed_rel so they can't be
		 * freed if it's planned */
		compressed_rel->partial_pathlist = NIL;
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
	path->rows = compressed_path->rows * DECOMPRESS_CHUNK_BATCH_SIZE;
}

static void
compressed_reltarget_add_whole_row_var(RelOptInfo *compressed_rel)
{
	compressed_rel->reltarget->exprs =
		lappend(compressed_rel->reltarget->exprs,
				makeVar(compressed_rel->relid, 0, get_atttype(compressed_rel->relid, 0), -1, 0, 0));
}

static void
compressed_reltarget_add_var_for_column(RelOptInfo *compressed_rel, Oid compressed_relid,
										const char *column_name)
{
	AttrNumber attnum = get_attnum(compressed_relid, column_name);
	Assert(attnum > 0);
	compressed_rel->reltarget->exprs = lappend(compressed_rel->reltarget->exprs,
											   makeVar(compressed_rel->relid,
													   attnum,
													   get_atttype(compressed_rel->relid, attnum),
													   -1,
													   0,
													   0));
}

/* copy over the vars from the chunk_rel->reltarget to the compressed_rel->reltarget
 * altering the fields that need it
 */
static void
compressed_rel_setup_reltarget(RelOptInfo *compressed_rel, CompressionInfo *info,
							   bool needs_sequence_num)
{
	Oid compressed_relid = info->compressed_rte->relid;
	ListCell *lc;
	foreach (lc, info->chunk_rel->reltarget->exprs)
	{
		ListCell *lc2;
		List *chunk_vars = pull_var_clause(lfirst(lc),
										   PVC_RECURSE_AGGREGATES | PVC_RECURSE_WINDOWFUNCS |
											   PVC_RECURSE_PLACEHOLDERS);
		foreach (lc2, chunk_vars)
		{
			FormData_hypertable_compression *column_info;
			char *column_name;
			Var *chunk_var = castNode(Var, lfirst(lc2));

			/* skip vars that aren't from the uncompressed chunk */
			if (chunk_var->varno != info->chunk_rel->relid)
				continue;

			/* if there's a system column or whole-row reference, add a whole-
			 * row reference, and we're done.
			 */
			if (chunk_var->varattno <= 0)
			{
				compressed_reltarget_add_whole_row_var(compressed_rel);
				return;
			}

			column_name = get_attname_compat(info->chunk_rte->relid, chunk_var->varattno, false);
			column_info =
				get_column_compressioninfo(info->hypertable_compression_info, column_name);

			Assert(column_info != NULL);

			compressed_reltarget_add_var_for_column(compressed_rel, compressed_relid, column_name);

			/* if the column is an orderby, add it's metadata columns too */
			if (column_info->orderby_column_index > 0)
			{
				compressed_reltarget_add_var_for_column(compressed_rel,
														compressed_relid,
														compression_column_segment_min_name(
															column_info));
				compressed_reltarget_add_var_for_column(compressed_rel,
														compressed_relid,
														compression_column_segment_max_name(
															column_info));
			}
		}
	}

	/* always add the count column */
	compressed_reltarget_add_var_for_column(compressed_rel,
											compressed_relid,
											COMPRESSION_COLUMN_METADATA_COUNT_NAME);

	/* add the segment order column if we may try to order by it */
	if (needs_sequence_num)
	{
		compressed_reltarget_add_var_for_column(compressed_rel,
												compressed_relid,
												COMPRESSION_COLUMN_METADATA_SEQUENCE_NUM_NAME);
	}
}

typedef struct EMCreationContext
{
	List *compression_info;
	Oid uncompressed_relid;
	Oid compressed_relid;
	Index uncompressed_relid_idx;
	Index compressed_relid_idx;
	FormData_hypertable_compression *current_col_info;
} EMCreationContext;

/* get the segmentby compression info for an EquivalenceMember (EM) expr,
 * or return NULL if it's not one we can create an EM for
 */
static FormData_hypertable_compression *
segmentby_compression_info_for_em(Node *node, EMCreationContext *context)
{
	/* based on adjust_appendrel_attrs_mutator */
	if (node == NULL)
		return NULL;

	Assert(!IsA(node, Query));

	if (IsA(node, Var))
	{
		FormData_hypertable_compression *col_info;
		char *column_name;
		Var *var = castNode(Var, node);
		if (var->varno != context->uncompressed_relid_idx)
			return NULL;

		/* we can't add an EM for system attributes or whole-row refs */
		if (var->varattno <= 0)
			return NULL;

		column_name = get_attname_compat(context->uncompressed_relid, var->varattno, true);
		if (column_name == NULL)
			return NULL;

		col_info = get_column_compressioninfo(context->compression_info, column_name);

		if (col_info == NULL)
			return NULL;

		/* we can only add EMs for segmentby columns */
		if (col_info->segmentby_column_index <= 0)
			return NULL;

		return col_info;
	}

	/*
	 * we currently ignore non-Var expressions; the EC we care about
	 * (the one relating Hypertable columns to chunk columns)
	 * should not have any
	 */
	return NULL;
}

static Node *
create_var_for_compressed_equivalence_member(Node *node, const EMCreationContext *context)
{
	/* based on adjust_appendrel_attrs_mutator */
	if (node == NULL)
		return NULL;

	Assert(!IsA(node, Query));

	if (IsA(node, Var))
	{
		Var *var = castNode(Var, node);

		Assert(context->current_col_info != NULL);
		Assert(context->current_col_info->segmentby_column_index > 0);
		Assert(var->varno == context->uncompressed_relid_idx);
		Assert(var->varattno > 0);

		var = (Var *) copyObject(node);

		if (var->varlevelsup == 0)
		{
			var->varno = context->compressed_relid_idx;
			var->varnoold = context->compressed_relid_idx;
			var->varattno =
				get_attnum(context->compressed_relid, NameStr(context->current_col_info->attname));

			return (Node *) var;
		}

		return NULL;
	}

	/*
	 * we currently ignore non-Var expressions; the EC we care about
	 * (the one relating Hypertable columns to chunk columns)
	 * should not have any
	 */
	return NULL;
}

static void
add_segmentby_to_equivalence_class(EquivalenceClass *cur_ec, CompressionInfo *info,
								   EMCreationContext *context)
{
	Relids uncompressed_chunk_relids = info->chunk_rel->relids;
	ListCell *lc;
	foreach (lc, cur_ec->ec_members)
	{
		Expr *child_expr;
		Relids new_relids;
		Relids new_nullable_relids;
		EquivalenceMember *cur_em = (EquivalenceMember *) lfirst(lc);
		Assert(!bms_overlap(cur_em->em_relids, info->compressed_rel->relids));

		/* skip EquivalenceMembers that do not reference the uncompressed
		 * chunk
		 */
		if (!bms_overlap(cur_em->em_relids, uncompressed_chunk_relids))
			continue;

		context->current_col_info =
			segmentby_compression_info_for_em((Node *) cur_em->em_expr, context);
		if (context->current_col_info == NULL)
			continue;

		child_expr = (Expr *) create_var_for_compressed_equivalence_member((Node *) cur_em->em_expr,
																		   context);
		if (child_expr == NULL)
			continue;

		/*
		 * Transform em_relids to match.  Note we do *not* do
		 * pull_varnos(child_expr) here, as for example the
		 * transformation might have substituted a constant, but we
		 * don't want the child member to be marked as constant.
		 */
		new_relids = bms_difference(cur_em->em_relids, uncompressed_chunk_relids);
		new_relids = bms_add_members(new_relids, info->compressed_rel->relids);

		/*
		 * And likewise for nullable_relids.  Note this code assumes
		 * parent and child relids are singletons.
		 */
		new_nullable_relids = cur_em->em_nullable_relids;
		if (bms_overlap(new_nullable_relids, uncompressed_chunk_relids))
		{
			new_nullable_relids = bms_difference(new_nullable_relids, uncompressed_chunk_relids);
			new_nullable_relids =
				bms_add_members(new_nullable_relids, info->compressed_rel->relids);
		}

		/* copied from add_eq_member */
		{
			EquivalenceMember *em = makeNode(EquivalenceMember);

			em->em_expr = child_expr;
			em->em_relids = new_relids;
			em->em_nullable_relids = new_nullable_relids;
			em->em_is_const = false;
			em->em_is_child = true;
			em->em_datatype = cur_em->em_datatype;
			cur_ec->ec_relids = bms_add_members(cur_ec->ec_relids, info->compressed_rel->relids);
			cur_ec->ec_members = lappend(cur_ec->ec_members, em);

			return;
		}
	}
}

static void
compressed_rel_setup_equivalence_classes(PlannerInfo *root, CompressionInfo *info)
{
	EMCreationContext context = {
		.compression_info = info->hypertable_compression_info,

		.uncompressed_relid = info->chunk_rte->relid,
		.compressed_relid = info->compressed_rte->relid,

		.uncompressed_relid_idx = info->chunk_rel->relid,
		.compressed_relid_idx = info->compressed_rel->relid,
	};

	ListCell *lc;
	Assert(info->chunk_rte->relid != info->compressed_rel->relid);
	Assert(info->chunk_rel->relid != info->compressed_rel->relid);
	/* based on add_child_rel_equivalences */
	foreach (lc, root->eq_classes)
	{
		EquivalenceClass *cur_ec = (EquivalenceClass *) lfirst(lc);

		/*
		 * If this EC contains a volatile expression, then generating child
		 * EMs would be downright dangerous, so skip it.  We rely on a
		 * volatile EC having only one EM.
		 */
		if (cur_ec->ec_has_volatile)
			continue;

		/* if the compressed rel is already part of this EC,
		 * we don't need to re-add it
		 */
		if (bms_overlap(cur_ec->ec_relids, info->compressed_rel->relids))
			continue;
		add_segmentby_to_equivalence_class(cur_ec, info, &context);
	}
	info->compressed_rel->has_eclass_joins = info->chunk_rel->has_eclass_joins;
}

/*
 * create RangeTblEntry and RelOptInfo for the compressed chunk
 * and add it to PlannerInfo
 */
static void
decompress_chunk_add_plannerinfo(PlannerInfo *root, CompressionInfo *info, Chunk *chunk,
								 RelOptInfo *chunk_rel, bool needs_sequence_num)
{
	Index compressed_index = root->simple_rel_array_size;
	Chunk *compressed_chunk = ts_chunk_get_by_id(chunk->fd.compressed_chunk_id, 0, true);
	Oid compressed_relid = compressed_chunk->table_id;
	RelOptInfo *compressed_rel;

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
	compressed_rel = build_simple_rel(root, compressed_index, RELOPT_BASEREL);
#else
	compressed_rel = build_simple_rel(root, compressed_index, NULL);
#endif

	root->simple_rel_array[compressed_index] = compressed_rel;
	info->compressed_rel = compressed_rel;

	compressed_rel_setup_reltarget(compressed_rel, info, needs_sequence_num);
	compressed_rel_setup_equivalence_classes(root, info);
}

static Path *
decompress_chunk_path_create(PlannerInfo *root, CompressionInfo *info, int parallel_workers,
							 Path *compressed_path)
{
	DecompressChunkPath *path;

	path = (DecompressChunkPath *) newNode(sizeof(DecompressChunkPath), T_CustomPath);

	path->info = info;

	path->cpath.path.pathtype = T_CustomScan;
	path->cpath.path.parent = info->chunk_rel;
	path->cpath.path.pathtarget = info->chunk_rel->reltarget;

	path->cpath.path.param_info = compressed_path->param_info;

	path->cpath.flags = 0;
	path->cpath.methods = &decompress_chunk_path_methods;

	Assert(parallel_workers == 0 || compressed_path->parallel_safe);

	path->cpath.path.parallel_aware = false;
	path->cpath.path.parallel_safe = compressed_path->parallel_safe;
	path->cpath.path.parallel_workers = parallel_workers;

	path->cpath.custom_paths = list_make1(compressed_path);
	path->reverse = false;
	path->compressed_pathkeys = NIL;
	cost_decompress_chunk(&path->cpath.path, compressed_path);

	return &path->cpath.path;
}

static void
create_compressed_scan_paths(PlannerInfo *root, RelOptInfo *compressed_rel, int parallel_workers,
							 CompressionInfo *info, OrderByInfo *seqnum_info)
{
	Path *compressed_path;
	List *orig_pathkeys;

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
	if (seqnum_info && seqnum_info->try_order_by_compressed)
	{
		orig_pathkeys = root->query_pathkeys;
		build_compressed_scan_pathkeys(seqnum_info, root, root->query_pathkeys, info);
		root->query_pathkeys = seqnum_info->compressed_pathkeys;
	}
	check_index_predicates(root, compressed_rel);
	create_index_paths(root, compressed_rel);
	if (seqnum_info && seqnum_info->try_order_by_compressed)
		root->query_pathkeys = orig_pathkeys;
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
can_order_by_pathkeys(RelOptInfo *chunk_rel, CompressionInfo *info, List *pathkeys,
					  OrderByInfo *ret)
{
	int pk_index;
	PathKey *pk;
	Var *var;
	Expr *expr;
	char *column_name;
	FormData_hypertable_compression *ci;
	ListCell *lc = list_head(pathkeys);
	bool found_first_orderby = false;
	enum SeqNumOrderBy sequence_num_orderby_stat = SEQNUM_ORDERBY_NOMATCH;

	ret->try_order_by_compressed = false;
	ret->needs_sequence_num = false;
	ret->sequence_num_orderby_stat = true;

	/* all segmentby columns need to be prefix of pathkeys */
	if (info->num_segmentby_columns > 0)
	{
		Bitmapset *segmentby_columns = NULL;

		/*
		 * if a segmentby column is not prefix of pathkeys we can still
		 * generate ordered output if there is an equality constraint
		 * on the segmentby column
		 */
		if (chunk_rel->baserestrictinfo != NIL)
		{
			ListCell *lc_ri;
			foreach (lc_ri, chunk_rel->baserestrictinfo)
			{
				RestrictInfo *ri = lfirst(lc_ri);

				if (IsA(ri->clause, OpExpr) && list_length(castNode(OpExpr, ri->clause)->args) == 2)
				{
					OpExpr *op = castNode(OpExpr, ri->clause);
					Var *var;
					Expr *other;

					if (op->opretset)
						continue;

					if (IsA(linitial(op->args), Var))
					{
						var = castNode(Var, linitial(op->args));
						other = lsecond(op->args);
					}
					else if (IsA(lsecond(op->args), Var))
					{
						var = castNode(Var, lsecond(op->args));
						other = linitial(op->args);
					}
					else
						continue;

					if (var->varno != chunk_rel->relid || var->varattno <= 0)
						continue;

					if (IsA(other, Const) || IsA(other, Param))
					{
						TypeCacheEntry *tce = lookup_type_cache(var->vartype, TYPECACHE_EQ_OPR);

						if (op->opno != tce->eq_opr)
							continue;

						if (bms_is_member(var->varattno, info->chunk_segmentby_attnos))
						{
							segmentby_columns = bms_add_member(segmentby_columns, var->varattno);
							/*
							 * remember segmentby attnos in baserestrictinfo because we need
							 * them again when generating pathkeys for compressed scan
							 */
							info->chunk_segmentby_ri =
								bms_add_member(info->chunk_segmentby_ri, var->varattno);
						}
					}
				}
			}
		}

		for (; lc != NULL && bms_num_members(segmentby_columns) < info->num_segmentby_columns;
			 lc = lnext(lc))
		{
			pk = lfirst(lc);
			expr = ts_find_em_expr_for_rel(pk->pk_eclass, info->chunk_rel);

			if (expr == NULL || !IsA(expr, Var))
				break;
			var = castNode(Var, expr);

			if (var->varattno <= 0)
				break;

			column_name = get_attname_compat(info->chunk_rte->relid, var->varattno, false);
			ci = get_column_compressioninfo(info->hypertable_compression_info, column_name);

			if (ci->segmentby_column_index <= 0)
				break;
			segmentby_columns = bms_add_member(segmentby_columns, var->varattno);
		}
		/*pathkeys that are not segment by columns are present */
		if (bms_num_members(segmentby_columns) != info->num_segmentby_columns)
		{
			return false;
		}
	}

	/*
	 * if pathkeys includes columns past segmentby columns
	 * we need sequence_num in the targetlist for ordering
	 */
	if (lc != NULL)
		ret->needs_sequence_num = true;

	/*
	 * loop over the rest of pathkeys
	 * this needs to exactly match the configured compress_orderby
	 */
	found_first_orderby = false;
	sequence_num_orderby_stat = SEQNUM_ORDERBY_NOMATCH;
	for (pk_index = 1; lc != NULL; lc = lnext(lc), pk_index++)
	{
		enum SeqNumOrderBy stat;
		pk = lfirst(lc);
		expr = ts_find_em_expr_for_rel(pk->pk_eclass, info->chunk_rel);

		if (expr == NULL || !IsA(expr, Var))
			return false;

		var = castNode(Var, expr);

		if (var->varattno <= 0)
			return false;

		column_name = get_attname_compat(info->chunk_rte->relid, var->varattno, false);
		ci = get_column_compressioninfo(info->hypertable_compression_info, column_name);

		if (ci->orderby_column_index != pk_index)
			return false;

		/*
		 * pk_strategy is either BTLessStrategyNumber (for ASC) or
		 * BTGreaterStrategyNumber (for DESC)
		 */
		stat = SEQNUM_ORDERBY_NOMATCH;
		if (pk->pk_strategy == BTLessStrategyNumber)
		{
			if (ci->orderby_asc && (ci->orderby_nullsfirst == pk->pk_nulls_first))
				stat = SEQNUM_ORDERBY_SAME;
			else if (!ci->orderby_asc && (ci->orderby_nullsfirst != pk->pk_nulls_first))
				stat = SEQNUM_ORDERBY_REVERSE;
		}
		else if (pk->pk_strategy == BTGreaterStrategyNumber)
		{
			if (ci->orderby_asc == false && (ci->orderby_nullsfirst == pk->pk_nulls_first))
				stat = SEQNUM_ORDERBY_SAME;
			else if (ci->orderby_asc && (ci->orderby_nullsfirst != pk->pk_nulls_first))
				stat = SEQNUM_ORDERBY_REVERSE;
		}
		if (stat == SEQNUM_ORDERBY_NOMATCH) /*we cannot match order by requested by pathkey */
			return false;
		if (!found_first_orderby)
		{
			found_first_orderby = true;
			sequence_num_orderby_stat = stat;
		}
		else
		{
			if (sequence_num_orderby_stat != stat)
				return false;
		}
	}
	ret->try_order_by_compressed = true;
	ret->sequence_num_orderby_stat = sequence_num_orderby_stat;
	return true;
}
