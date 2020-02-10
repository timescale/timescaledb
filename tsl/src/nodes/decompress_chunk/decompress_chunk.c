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
#include <optimizer/cost.h>
#include <optimizer/pathnode.h>
#include <optimizer/paths.h>
#include <parser/parsetree.h>
#include <utils/builtins.h>
#include <utils/lsyscache.h>
#include <utils/typcache.h>
#include <miscadmin.h>

#include "compat.h"
#if PG12_LT
#include <optimizer/clauses.h>
#include <optimizer/restrictinfo.h>
#include <optimizer/tlist.h>
#include <optimizer/var.h>
#else
#include <optimizer/optimizer.h>
#endif

#include "hypertable_compression.h"
#include "compression/create.h"
#include "nodes/decompress_chunk/decompress_chunk.h"
#include "nodes/decompress_chunk/planner.h"
#include "nodes/decompress_chunk/qual_pushdown.h"
#include "utils.h"

#define DECOMPRESS_CHUNK_CPU_TUPLE_COST 0.01
#define DECOMPRESS_CHUNK_BATCH_SIZE 1000

static CustomPathMethods decompress_chunk_path_methods = {
	.CustomName = "DecompressChunk",
	.PlanCustomPath = decompress_chunk_plan_create,
};

typedef struct SortInfo
{
	List *compressed_pathkeys;
	bool needs_sequence_num;
	bool can_pushdown_sort; /* sort can be pushed below DecompressChunk */
	bool reverse;
} SortInfo;

static RangeTblEntry *decompress_chunk_make_rte(Oid compressed_relid, LOCKMODE lockmode);
static void create_compressed_scan_paths(PlannerInfo *root, RelOptInfo *compressed_rel,
										 int parallel_workers, CompressionInfo *info,
										 SortInfo *sort_info);

static DecompressChunkPath *decompress_chunk_path_create(PlannerInfo *root, CompressionInfo *info,
														 int parallel_workers,
														 Path *compressed_path);

static void decompress_chunk_add_plannerinfo(PlannerInfo *root, CompressionInfo *info, Chunk *chunk,
											 RelOptInfo *chunk_rel, bool needs_sequence_num);

static SortInfo build_sortinfo(RelOptInfo *chunk_rel, CompressionInfo *info, List *pathkeys);

/*
 * Like ts_make_pathkey_from_sortop but passes down the compressed relid so that existing
 * equivalence members that are marked as childen are properly checked.
 */
static PathKey *
make_pathkey_from_compressed(PlannerInfo *root, Index compressed_relid, Expr *expr, Oid ordering_op,
							 bool nulls_first)
{
	Oid opfamily, opcintype, collation;
	int16 strategy;

	/* Find the operator in pg_amop --- failure shouldn't happen */
	if (!get_ordering_op_properties(ordering_op, &opfamily, &opcintype, &strategy))
		elog(ERROR, "operator %u is not a valid ordering operator", ordering_op);

	/* Because SortGroupClause doesn't carry collation, consult the expr */
	collation = exprCollation((Node *) expr);

	return ts_make_pathkey_from_sortinfo(root,
										 expr,
										 NULL,
										 opfamily,
										 opcintype,
										 collation,
										 (strategy == BTGreaterStrategyNumber),
										 nulls_first,
										 0,
										 bms_make_singleton(compressed_relid),
										 true);
}

static void
prepend_ec_for_seqnum(PlannerInfo *root, CompressionInfo *info, SortInfo *sort_info, Var *var,
					  Oid sortop, bool nulls_first)
{
	MemoryContext oldcontext = MemoryContextSwitchTo(root->planner_cxt);

	Oid opfamily, opcintype, equality_op;
	int16 strategy;
	List *opfamilies;
	EquivalenceClass *newec = makeNode(EquivalenceClass);
	EquivalenceMember *em = makeNode(EquivalenceMember);

	/* Find the operator in pg_amop --- failure shouldn't happen */
	if (!get_ordering_op_properties(sortop, &opfamily, &opcintype, &strategy))
		elog(ERROR, "operator %u is not a valid ordering operator", sortop);

	/*
	 * EquivalenceClasses need to contain opfamily lists based on the family
	 * membership of mergejoinable equality operators, which could belong to
	 * more than one opfamily.  So we have to look up the opfamily's equality
	 * operator and get its membership.
	 */
	equality_op = get_opfamily_member(opfamily, opcintype, opcintype, BTEqualStrategyNumber);
	if (!OidIsValid(equality_op)) /* shouldn't happen */
		elog(ERROR,
			 "missing operator %d(%u,%u) in opfamily %u",
			 BTEqualStrategyNumber,
			 opcintype,
			 opcintype,
			 opfamily);
	opfamilies = get_mergejoin_opfamilies(equality_op);
	if (!opfamilies) /* certainly should find some */
		elog(ERROR, "could not find opfamilies for equality operator %u", equality_op);

	em->em_expr = (Expr *) var;
	em->em_relids = bms_make_singleton(info->compressed_rel->relid);
	em->em_nullable_relids = NULL;
	em->em_is_const = false;
	em->em_is_child = false;
	em->em_datatype = INT4OID;

	newec->ec_opfamilies = list_copy(opfamilies);
	newec->ec_collation = 0;
	newec->ec_members = list_make1(em);
	newec->ec_sources = NIL;
	newec->ec_derives = NIL;
	newec->ec_relids = bms_make_singleton(info->compressed_rel->relid);
	newec->ec_has_const = false;
	newec->ec_has_volatile = false;
	newec->ec_below_outer_join = false;
	newec->ec_broken = false;
	newec->ec_sortref = 0;
#if PG10_GE
	newec->ec_min_security = UINT_MAX;
	newec->ec_max_security = 0;
#endif
	newec->ec_merged = NULL;

	/* Prepend the ec */
	root->eq_classes = lcons(newec, root->eq_classes);

	MemoryContextSwitchTo(oldcontext);
}

static void
build_compressed_scan_pathkeys(SortInfo *sort_info, PlannerInfo *root, List *chunk_pathkeys,
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

			/* not a segmentby column, rest of pathkeys should be handled by compress_orderby */
			if (!bms_is_member(var->varattno, info->chunk_segmentby_attnos))
				break;

			/* skip duplicate references */
			if (bms_is_member(var->varattno, segmentby_columns))
				continue;

			column_name = get_attname_compat(info->chunk_rte->relid, var->varattno, false);
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
			pk = make_pathkey_from_compressed(root,
											  info->compressed_rel->relid,
											  (Expr *) var,
											  sortop,
											  pk->pk_nulls_first);
			compressed_pathkeys = lappend(compressed_pathkeys, pk);
		}

		/* we validated this when we created the Path so only asserting here */
		Assert(bms_num_members(segmentby_columns) == info->num_segmentby_columns ||
			   list_length(compressed_pathkeys) == list_length(chunk_pathkeys));
	}

	/*
	 * If pathkeys contains non-segmentby columns the rest of the ordering
	 * requirements will be satisfied by ordering by sequence_num
	 */
	if (sort_info->needs_sequence_num)
	{
		bool nulls_first;
		Oid sortop;
		varattno =
			get_attnum(info->compressed_rte->relid, COMPRESSION_COLUMN_METADATA_SEQUENCE_NUM_NAME);
		var = makeVar(info->compressed_rel->relid, varattno, INT4OID, -1, InvalidOid, 0);

		if (sort_info->reverse)
		{
			sortop = get_commutator(Int4LessOperator);
			nulls_first = true;
		}
		else
		{
			sortop = Int4LessOperator;
			nulls_first = false;
		}

		/* Prepend the ec class for the sequence number. We are prepending
		 * the ec for efficiency in finding it. We are more likely to look for it
		 * then other ec classes */
		prepend_ec_for_seqnum(root, info, sort_info, var, sortop, nulls_first);

		pk = make_pathkey_from_compressed(root,
										  info->compressed_rte->relid,
										  (Expr *) var,
										  sortop,
										  nulls_first);

		compressed_pathkeys = lappend(compressed_pathkeys, pk);
	}
	sort_info->compressed_pathkeys = compressed_pathkeys;
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

	/*
	 * since we rely on parallel coordination from the scan below
	 * this node it is probably not beneficial to have more
	 * than a single worker per chunk
	 */
	int parallel_workers = 1;
	AppendRelInfo *chunk_info = ts_get_appendrelinfo(root, chunk_rel->relid, false);
	SortInfo sort_info = build_sortinfo(chunk_rel, info, root->query_pathkeys);

	Assert(chunk_info != NULL);
	Assert(chunk_info->parent_reloid == ht->main_table_relid);
	ht_index = chunk_info->parent_relid;
	hypertable_rel = root->simple_rel_array[ht_index];

	Assert(chunk->fd.compressed_chunk_id > 0);

	chunk_rel->pathlist = NIL;
	chunk_rel->partial_pathlist = NIL;

	/* add RangeTblEntry and RelOptInfo for compressed chunk */
	decompress_chunk_add_plannerinfo(root, info, chunk, chunk_rel, sort_info.needs_sequence_num);
	compressed_rel = info->compressed_rel;

	compressed_rel->consider_parallel = chunk_rel->consider_parallel;
	/* translate chunk_rel->baserestrictinfo */
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
								 &sort_info);

	/* create non-parallel paths */
	foreach (lc, compressed_rel->pathlist)
	{
		Path *child_path = lfirst(lc);
		DecompressChunkPath *path;

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

		/* If we can push down the sort below the DecompressChunk node, we set the pathkeys of the
		 * decompress node to the query pathkeys, while remembering the compressed_pathkeys
		 * corresponding to those query_pathkeys. We will determine whether to put a sort between
		 * the decompression node and the scan during plan creation */
		if (sort_info.can_pushdown_sort)
		{
			DecompressChunkPath *dcpath = copy_decompress_chunk_path((DecompressChunkPath *) path);
			dcpath->reverse = sort_info.reverse;
			dcpath->needs_sequence_num = sort_info.needs_sequence_num;
			dcpath->compressed_pathkeys = sort_info.compressed_pathkeys;
			dcpath->cpath.path.pathkeys = root->query_pathkeys;

			/*
			 * Add costing for a sort. The standard Postgres pattern is to add the cost during
			 * path creation, but not add the sort path itself, that's done during plan creation.
			 * Examples of this in: create_merge_append_path & create_merge_append_plan
			 */
			if (!pathkeys_contained_in(dcpath->compressed_pathkeys, child_path->pathkeys))
			{
				Path sort_path; /* dummy for result of cost_sort */

				cost_sort(&sort_path,
						  root,
						  dcpath->compressed_pathkeys,
						  child_path->total_cost,
						  child_path->rows,
						  child_path->pathtarget->width,
						  0.0,
						  work_mem,
						  -1);
				cost_decompress_chunk(&dcpath->cpath.path, &sort_path);
			}
			add_path(chunk_rel, &dcpath->cpath.path);
		}

		/* this has to go after the path is copied for the ordered path since path can get freed in
		 * add_path */
		add_path(chunk_rel, &path->cpath.path);
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
			DecompressChunkPath *path;
			if (child_path->param_info != NULL &&
				(bms_is_member(chunk_rel->relid, child_path->param_info->ppi_req_outer) ||
				 bms_is_member(ht_index, child_path->param_info->ppi_req_outer)))
				continue;
			path = decompress_chunk_path_create(root, info, parallel_workers, child_path);
			add_partial_path(chunk_rel, &path->cpath.path);
		}
		/* the chunk_rel now owns the paths, remove them from the compressed_rel so they can't be
		 * freed if it's planned */
		compressed_rel->partial_pathlist = NIL;
	}
	/* set reloptkind to RELOPT_DEADREL to prevent postgresql from replanning this relation */
	compressed_rel->reloptkind = RELOPT_DEADREL;
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
	Oid typid, collid;
	int32 typmod;
	Assert(attnum > 0);
	get_atttypetypmodcoll(compressed_relid, attnum, &typid, &typmod, &collid);
	compressed_rel->reltarget->exprs =
		lappend(compressed_rel->reltarget->exprs,
				makeVar(compressed_rel->relid, attnum, typid, typmod, collid, 0));
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
		List *chunk_vars = pull_var_clause(lfirst(lc), 0);
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

static Bitmapset *
decompress_chunk_adjust_child_relids(Bitmapset *src, int chunk_relid, int compressed_chunk_relid)
{
	Bitmapset *result = NULL;
	if (src != NULL)
	{
		result = bms_copy(src);
		result = bms_del_member(result, chunk_relid);
		result = bms_add_member(result, compressed_chunk_relid);
	}
	return result;
}

/* based on adjust_appendrel_attrs_mutator handling of RestrictInfo */
static Node *
chunk_joininfo_mutator(Node *node, CompressionInfo *context)
{
	if (node == NULL)
		return NULL;

	if (IsA(node, Var))
	{
		Var *var = castNode(Var, node);
		Var *compress_var = copyObject(var);
		char *column_name;
		AttrNumber compressed_attno;
		FormData_hypertable_compression *compressioninfo;
		if (var->varno != context->chunk_rel->relid)
			return (Node *) var;

		column_name = get_attname_compat(context->chunk_rte->relid, var->varattno, false);
		compressioninfo =
			get_column_compressioninfo(context->hypertable_compression_info, column_name);

		compressed_attno =
			get_attnum(context->compressed_rte->relid, compressioninfo->attname.data);
		compress_var->varno = context->compressed_rel->relid;
		compress_var->varattno = compressed_attno;

		return (Node *) compress_var;
	}
	else if (IsA(node, RestrictInfo))
	{
		RestrictInfo *oldinfo = (RestrictInfo *) node;
		RestrictInfo *newinfo = makeNode(RestrictInfo);

		/* Copy all flat-copiable fields */
		memcpy(newinfo, oldinfo, sizeof(RestrictInfo));

		/* Recursively fix the clause itself */
		newinfo->clause = (Expr *) chunk_joininfo_mutator((Node *) oldinfo->clause, context);

		/* and the modified version, if an OR clause */
		newinfo->orclause = (Expr *) chunk_joininfo_mutator((Node *) oldinfo->orclause, context);

		/* adjust relid sets too */
		newinfo->clause_relids =
			decompress_chunk_adjust_child_relids(oldinfo->clause_relids,
												 context->chunk_rel->relid,
												 context->compressed_rel->relid);
		newinfo->required_relids =
			decompress_chunk_adjust_child_relids(oldinfo->required_relids,
												 context->chunk_rel->relid,
												 context->compressed_rel->relid);
		newinfo->outer_relids =
			decompress_chunk_adjust_child_relids(oldinfo->outer_relids,
												 context->chunk_rel->relid,
												 context->compressed_rel->relid);
		newinfo->nullable_relids =
			decompress_chunk_adjust_child_relids(oldinfo->nullable_relids,
												 context->chunk_rel->relid,
												 context->compressed_rel->relid);
		newinfo->left_relids = decompress_chunk_adjust_child_relids(oldinfo->left_relids,
																	context->chunk_rel->relid,
																	context->compressed_rel->relid);
		newinfo->right_relids =
			decompress_chunk_adjust_child_relids(oldinfo->right_relids,
												 context->chunk_rel->relid,
												 context->compressed_rel->relid);

		newinfo->eval_cost.startup = -1;
		newinfo->norm_selec = -1;
		newinfo->outer_selec = -1;
		newinfo->left_em = NULL;
		newinfo->right_em = NULL;
		newinfo->scansel_cache = NIL;
		newinfo->left_bucketsize = -1;
		newinfo->right_bucketsize = -1;
#if PG11_GE
		newinfo->left_mcvfreq = -1;
		newinfo->right_mcvfreq = -1;
#endif
		return (Node *) newinfo;
	}
	return expression_tree_mutator(node, chunk_joininfo_mutator, context);
}

/* translate chunk_rel->joininfo for compressed_rel
 * this is necessary for create_index_path which gets join clauses from
 * rel->joininfo and sets up paramaterized paths (in rel->ppilist).
 * ppi_clauses is finally used to add any additional filters on the
 * indexpath when creating a plan in create_indexscan_plan.
 * Otherwise we miss additional filters that need to be applied after
 * the index plan is executed (github issue 1558)
 */
static void
compressed_rel_setup_joininfo(RelOptInfo *compressed_rel, CompressionInfo *info)
{
	RelOptInfo *chunk_rel = info->chunk_rel;
	ListCell *lc;
	List *compress_joininfo = NIL;
	foreach (lc, chunk_rel->joininfo)
	{
		RestrictInfo *compress_ri;
		RestrictInfo *ri = (RestrictInfo *) lfirst(lc);
		Node *result = chunk_joininfo_mutator((Node *) ri, info);
		Assert(IsA(result, RestrictInfo));
		compress_ri = (RestrictInfo *) result;
		compress_joininfo = lappend(compress_joininfo, compress_ri);
	}
	compressed_rel->joininfo = compress_joininfo;
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
create_var_for_compressed_equivalence_member(Var *var, const EMCreationContext *context)
{
	/* based on adjust_appendrel_attrs_mutator */
	Assert(context->current_col_info != NULL);
	Assert(context->current_col_info->segmentby_column_index > 0);
	Assert(var->varno == context->uncompressed_relid_idx);
	Assert(var->varattno > 0);

	var = (Var *) copyObject(var);

	if (var->varlevelsup == 0)
	{
		var->varno = context->compressed_relid_idx;
		var->varnoold = context->compressed_relid_idx;
		var->varattno =
			get_attnum(context->compressed_relid, NameStr(context->current_col_info->attname));

		var->varoattno = var->varattno;

		return (Node *) var;
	}

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
		Var *var;
		Assert(!bms_overlap(cur_em->em_relids, info->compressed_rel->relids));

		/* only consider EquivalenceMembers that are vars of the uncompressed chunk */
		if (!IsA(cur_em->em_expr, Var))
			continue;

		var = castNode(Var, cur_em->em_expr);

		if (var->varno != info->chunk_rel->relid)
			continue;

		/* given that the em is a var of the uncompressed chunk, the relid of the chunk should
		 * be set on the em */
		Assert(bms_overlap(cur_em->em_relids, uncompressed_chunk_relids));

		context->current_col_info =
			segmentby_compression_info_for_em((Node *) cur_em->em_expr, context);
		if (context->current_col_info == NULL)
			continue;

		child_expr = (Expr *) create_var_for_compressed_equivalence_member(var, context);
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
			/* Prepend the ec member because it's likely to be accessed soon */
			cur_ec->ec_members = lcons(em, cur_ec->ec_members);

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
	ListCell *lc;
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
	/* github issue :1558
	 * set up top_parent_relids for this rel as the same as the
	 * original hypertable, otherwise eq classes are not computed correctly
	 * in generate_join_implied_equalities (called by
	 * get_baserel_parampathinfo <- create_index_paths)
	 */
#if !PG96
	Assert(chunk_rel->top_parent_relids != NULL);
	compressed_rel->top_parent_relids = bms_copy(chunk_rel->top_parent_relids);
#endif
	root->simple_rel_array[compressed_index] = compressed_rel;
	info->compressed_rel = compressed_rel;
	foreach (lc, info->hypertable_compression_info)
	{
		FormData_hypertable_compression *fd = lfirst(lc);
		if (fd->segmentby_column_index <= 0)
		{
			/* store attnos for the compressed chunk here */
			AttrNumber compressed_chunk_attno =
				get_attnum(info->compressed_rte->relid, NameStr(fd->attname));
			info->compressed_chunk_compressed_attnos =
				bms_add_member(info->compressed_chunk_compressed_attnos, compressed_chunk_attno);
		}
	}
	compressed_rel_setup_reltarget(compressed_rel, info, needs_sequence_num);
	compressed_rel_setup_equivalence_classes(root, info);
	/* translate chunk_rel->joininfo for compressed_rel */
	compressed_rel_setup_joininfo(compressed_rel, info);
}

static DecompressChunkPath *
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

	return path;
}

/* NOTE: this needs to be called strictly after all restrictinfos have been added
 *       to the compressed rel
 */

static void
create_compressed_scan_paths(PlannerInfo *root, RelOptInfo *compressed_rel, int parallel_workers,
							 CompressionInfo *info, SortInfo *sort_info)
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

	if (sort_info->can_pushdown_sort)
	{
		/*
		 * If we can push down sort below decompression we temporarily switch
		 * out root->query_pathkeys to allow matching to pathkeys produces by
		 * decompression
		 */
		List *orig_pathkeys = root->query_pathkeys;
		build_compressed_scan_pathkeys(sort_info, root, root->query_pathkeys, info);
		root->query_pathkeys = sort_info->compressed_pathkeys;
		check_index_predicates(root, compressed_rel);
		create_index_paths(root, compressed_rel);
		root->query_pathkeys = orig_pathkeys;
	}
	else
	{
		check_index_predicates(root, compressed_rel);
		create_index_paths(root, compressed_rel);
	}
}

/*
 * create RangeTblEntry for compressed chunk
 */
static RangeTblEntry *
decompress_chunk_make_rte(Oid compressed_relid, LOCKMODE lockmode)
{
	RangeTblEntry *rte = makeNode(RangeTblEntry);
	Relation r = table_open(compressed_relid, lockmode);
	int varattno;

	rte->rtekind = RTE_RELATION;
	rte->relid = compressed_relid;
	rte->relkind = r->rd_rel->relkind;
#if PG12_GE
	rte->rellockmode = lockmode;
#endif
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
	table_close(r, NoLock);

	/*
	 * Set flags and access permissions.
	 *
	 * The initial default on access checks is always check-for-READ-access,
	 * which is the right thing for all except target tables.
	 */
	rte->lateral = false;
	rte->inh = false;
	rte->inFromCl = false;

	rte->requiredPerms = 0;
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

/*
 * Find toplevel equality constraints of segmentby columns in baserestrictinfo
 *
 * This will detect Var = Const and Var = Param and set the corresponding bit
 * in CompressionInfo->chunk_segmentby_ri
 */
static void
find_restrictinfo_equality(RelOptInfo *chunk_rel, CompressionInfo *info)
{
	Bitmapset *segmentby_columns = NULL;

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
						segmentby_columns = bms_add_member(segmentby_columns, var->varattno);
				}
			}
		}
	}
	info->chunk_segmentby_ri = segmentby_columns;
}

/*
 * Check if we can push down the sort below the DecompressChunk node and fill
 * SortInfo accordingly
 *
 * The following conditions need to be true for pushdown:
 *  - all segmentby columns need to be prefix of pathkeys or have equality constraint
 *  - the rest of pathkeys needs to match compress_orderby
 *
 * If query pathkeys is shorter than segmentby + compress_orderby pushdown can still be done
 */
static SortInfo
build_sortinfo(RelOptInfo *chunk_rel, CompressionInfo *info, List *pathkeys)
{
	int pk_index;
	PathKey *pk;
	Var *var;
	Expr *expr;
	char *column_name;
	FormData_hypertable_compression *ci;
	ListCell *lc = list_head(pathkeys);
	SortInfo sort_info = { .can_pushdown_sort = false, .needs_sequence_num = false };

	if (pathkeys == NIL)
		return sort_info;

	/* all segmentby columns need to be prefix of pathkeys */
	if (info->num_segmentby_columns > 0)
	{
		Bitmapset *segmentby_columns;

		/*
		 * initialize segmentby with equality constraints from baserestrictinfo because
		 * those columns dont need to be prefix of pathkeys
		 */
		find_restrictinfo_equality(chunk_rel, info);
		segmentby_columns = bms_copy(info->chunk_segmentby_ri);

		/*
		 * loop over pathkeys until we find one that is not a segmentby column
		 * we keep looping even if we found all segmentby columns in case a
		 * columns appears both in baserestrictinfo and in ORDER BY clause
		 */
		for (; lc != NULL; lc = lnext(lc))
		{
			Assert(bms_num_members(segmentby_columns) <= info->num_segmentby_columns);
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

		/*
		 * if pathkeys still has items but we didnt find all segmentby columns
		 * we cannot push down sort
		 */
		if (lc != NULL && bms_num_members(segmentby_columns) != info->num_segmentby_columns)
			return sort_info;
	}

	/*
	 * if pathkeys includes columns past segmentby columns
	 * we need sequence_num in the targetlist for ordering
	 */
	if (lc != NULL)
		sort_info.needs_sequence_num = true;

	/*
	 * loop over the rest of pathkeys
	 * this needs to exactly match the configured compress_orderby
	 */
	for (pk_index = 1; lc != NULL; lc = lnext(lc), pk_index++)
	{
		bool reverse = false;
		pk = lfirst(lc);
		expr = ts_find_em_expr_for_rel(pk->pk_eclass, info->chunk_rel);

		if (expr == NULL || !IsA(expr, Var))
			return sort_info;

		var = castNode(Var, expr);

		if (var->varattno <= 0)
			return sort_info;

		column_name = get_attname_compat(info->chunk_rte->relid, var->varattno, false);
		ci = get_column_compressioninfo(info->hypertable_compression_info, column_name);

		if (ci->orderby_column_index != pk_index)
			return sort_info;

		/*
		 * pk_strategy is either BTLessStrategyNumber (for ASC) or
		 * BTGreaterStrategyNumber (for DESC)
		 */
		if (pk->pk_strategy == BTLessStrategyNumber)
		{
			if (ci->orderby_asc && ci->orderby_nullsfirst == pk->pk_nulls_first)
				reverse = false;
			else if (!ci->orderby_asc && ci->orderby_nullsfirst != pk->pk_nulls_first)
				reverse = true;
			else
				return sort_info;
		}
		else if (pk->pk_strategy == BTGreaterStrategyNumber)
		{
			if (!ci->orderby_asc && ci->orderby_nullsfirst == pk->pk_nulls_first)
				reverse = false;
			else if (ci->orderby_asc && ci->orderby_nullsfirst != pk->pk_nulls_first)
				reverse = true;
			else
				return sort_info;
		}

		/*
		 * first pathkey match determines if this is forward or backward scan
		 * any further pathkey items need to have same direction
		 */
		if (pk_index == 1)
			sort_info.reverse = reverse;
		else if (reverse != sort_info.reverse)
			return sort_info;
	}

	/* all pathkeys should be processed */
	Assert(lc == NULL);

	sort_info.can_pushdown_sort = true;
	return sort_info;
}
