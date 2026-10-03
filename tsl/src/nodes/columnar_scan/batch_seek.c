/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

/*
 * Batch seek for lookups on the leading orderby column of a compressed chunk
 * without segmentby.
 *
 * When the chunk is compressed in one pass, the batches are cut from a single
 * stream sorted by the orderby, so batch i ends at or before batch i+1
 * starts. For "col = X", the batches that contain X are the ones that start at
 * X, and at most one batch before them: the last one starting before X.
 *
 * So instead of scanning every batch that starts at or before X, we find the
 * last index entry with batch start <= X, and read the entries backward from
 * there until the first batch that starts before X. The leaf page is read
 * directly, because a backward index scan would first check every remaining
 * entry on the page. This needs one index descent, like the segmentby index
 * lookup. "col = ANY(array)" does the same for each value of the array.
 *
 * Ranges like "col > L AND col < U" use the same order. The batches with
 * values above L are the last one starting before L and all the batches after
 * it, so the start of that batch is a lower bound on the batch start. We find
 * it the same way, and give it to the index scan as an extra condition.
 *
 * Batches added later without sorting (unordered chunk) break this. For those
 * chunks we read every batch that starts at or before the largest value, and a
 * range scan gets no lower bound.
 *
 * ColumnarScan reads its compressed batches from here instead of from the
 * index scan below it. The index scan is still used for unordered chunks, for
 * ranges, and when the leaf page doesn't show where a lookup stops.
 */
#include <postgres.h>
#include <access/genam.h>
#include <access/nbtree.h>
#include <access/stratnum.h>
#include <access/tableam.h>
#include <access/visibilitymap.h>
#include <catalog/pg_am.h>
#include <executor/executor.h>
#include <math.h>
#include <miscadmin.h>
#include <nodes/execnodes.h>
#include <nodes/makefuncs.h>
#include <nodes/nodeFuncs.h>
#include <nodes/pathnodes.h>
#include <optimizer/cost.h>
#include <optimizer/optimizer.h>
#include <optimizer/restrictinfo.h>
#include <utils/array.h>
#include <utils/datum.h>
#include <utils/hsearch.h>
#include <utils/index_selfuncs.h>
#include <utils/lsyscache.h>
#include <utils/memutils.h>
#include <utils/rel.h>
#include <utils/selfuncs.h>

#include "compat/compat.h"
#include "batch_seek.h"
#include "chunk.h"
#include "compression/create.h"
#include "guc.h"
#include "import/commands/explain.h"
#include "import/list.h"

#if PG18_GE
#include <commands/explain_format.h>
#include <commands/explain_state.h>
#endif
#include "ts_catalog/array_utils.h"
#include "utils.h"

/*
 * Before PG 17, the btree search functions took the snapshot of the scan for
 * the old_snapshot_threshold checks.
 */
#if PG17_LT
#define bt_search(rel, heaprel, key, buf, access, snapshot)                                        \
	_bt_search(rel, heaprel, key, buf, access, snapshot)
#define bt_get_endpoint(rel, level, rightmost, snapshot)                                           \
	_bt_get_endpoint(rel, level, rightmost, snapshot)
#else
#define bt_search(rel, heaprel, key, buf, access, snapshot)                                        \
	_bt_search(rel, heaprel, key, buf, access)
#define bt_get_endpoint(rel, level, rightmost, snapshot) _bt_get_endpoint(rel, level, rightmost)
#endif

typedef enum BatchSeekMode
{
	BATCH_SEEK_EQUALITY, /* col = X */
	BATCH_SEEK_VALUES,	 /* col = ANY(array) */
	BATCH_SEEK_RANGE,	 /* col > L, with an optional upper bound */
} BatchSeekMode;

typedef enum BatchSeekPrivate
{
	BSP_Mode,
	BSP_LastIndexCol,
	BSP_Forward, /* return the batches in index order */
	BSP_FirstAttno,
	BSP_Count
} BatchSeekPrivate;

/* Leaf entries copied per lookup, and how many of them may start before X. */
#define BATCH_SEEK_MAX_ENTRIES (2 * MaxTIDsPerBTreePage)
#define BATCH_SEEK_BEFORE_ENTRIES 4

typedef struct BatchSeekEntry
{
	ItemPointerData tid;
	bool before;	  /* the batch starts before X */
	bool ends_before; /* and ends before X too */
	Datum first;	  /* batch start, for range lookups */
} BatchSeekEntry;

struct BatchSeekState
{
	/* The ColumnarScan that reads from us, and the index scan below it. */
	CustomScanState *parent;
	IndexScan *iscan;
	EState *estate;
	int eflags;
	ExprContext *econtext;
	Relation heaprel;
	Oid chunk_relid;

	BatchSeekMode mode;
	bool forward;
	Expr *value_expr;
	ExprState *value;
	ExprState *qual;
	AttrNumber first_attno;
	bool order_checked;
	bool unordered;
	bool done;

	/*
	 * The values to look up, sorted and without duplicates, and the batches
	 * returned for them when there is more than one.
	 */
	MemoryContext seek_context;
	Datum *values;
	int num_values;
	int next_value;
	HTAB *returned;

	/* Reading the leaf page directly. */
	Relation index;
	BTScanInsert key;
	IndexFetchTableData *fetch;
	Buffer vmbuffer;
	int last_indexcol; /* index column of the batch end, 0 if none */
	bool positioned;
	bool page_start_reached;
	bool leftmost;
	BatchSeekEntry *entries;
	int num_entries;

	/*
	 * The batches found for the current value: entries[0 .. first_before - 1]
	 * in backward index order, and the batch where the lookup stops, which
	 * comes before them in index order.
	 */
	int first_before;
	int num_emitted;
	bool stop_pending;
	ItemPointerData stop_tid;
	TupleTableSlot *heap_slot;
	TupleTableSlot *stop_slot;

	/*
	 * Everything below is set up on the first execution, so that the chunks
	 * that are never read, for example after a LIMIT is satisfied, cost almost
	 * nothing.
	 */
	bool initialized;

	/*
	 * Using the index scan for the remaining values, until the first batch
	 * that starts before child_stop. It goes backward, so for forward order
	 * its batches are collected and returned in reverse.
	 */
	bool use_child;
	bool child_done;
	bool has_child_stop;
	Datum child_stop;
	MemoryContext collect_context;
	MinimalTuple *collected;
	int num_collected;
	int max_collected;
	TupleTableSlot *collected_slot;

	/* For EXPLAIN ANALYZE. */
	uint64 leaf_reads;
	uint64 fallbacks;
	uint64 fetches_skipped;
	uint64 range_starts_found;
};

/*
 * Whether the column can use batch seek: the leading ASC orderby column with
 * first and last batch metadata, NOT NULL, in a chunk without segmentby.
 */
bool
batch_seek_column_supported(CompressionSettings *settings, Oid chunk_relid, AttrNumber chunk_attno)
{
	if (settings == NULL || ts_array_length(settings->fd.segmentby) > 0 ||
		ts_array_length(settings->fd.orderby) < 1 ||
		ts_array_get_element_bool(settings->fd.orderby_desc, 1) ||
		orderby_sparse_kind(settings, 1) != ORDERBY_SPARSE_FIRSTLAST)
	{
		return false;
	}

	const char *colname = ts_array_get_element_text(settings->fd.orderby, 1);
	return chunk_attno != InvalidAttrNumber && get_attnum(chunk_relid, colname) == chunk_attno &&
		   ts_get_attnotnull(chunk_relid, chunk_attno);
}

static bool
references_rel(Node *node, void *context)
{
	if (node == NULL)
	{
		return false;
	}
	if (IsA(node, Var))
	{
		return castNode(Var, node)->varlevelsup == 0 &&
			   (Index) castNode(Var, node)->varno == *(Index *) context;
	}
	return expression_tree_walker(node, references_rel, context);
}

/*
 * Match "metadata op value" for the given metadata column and btree strategy,
 * or "metadata op ANY(array)" when array is set. Returns the value or array,
 * which can reference other relations: these are join parameters in the plan.
 */
static Expr *
match_metadata_clause(Node *node, Index relid, AttrNumber attno, Oid opfamily, int strategy,
					  bool array)
{
	Oid opno;
	List *args;
	if (!array && IsA(node, OpExpr))
	{
		opno = castNode(OpExpr, node)->opno;
		args = castNode(OpExpr, node)->args;
	}
	else if (array && IsA(node, ScalarArrayOpExpr) && castNode(ScalarArrayOpExpr, node)->useOr)
	{
		opno = castNode(ScalarArrayOpExpr, node)->opno;
		args = castNode(ScalarArrayOpExpr, node)->args;
	}
	else
	{
		return NULL;
	}

	if (list_length(args) != 2)
	{
		return NULL;
	}

	Node *left = ts_strip_relabel_types(linitial(args));
	Expr *right = lsecond(args);
	Oid right_type = array ? get_element_type(exprType((Node *) right)) : exprType((Node *) right);
	if (!IsA(left, Var) || (Index) castNode(Var, left)->varno != relid ||
		castNode(Var, left)->varattno != attno || right_type != castNode(Var, left)->vartype ||
		references_rel((Node *) right, &relid) ||
		get_op_opfamily_strategy(opno, opfamily) != strategy)
	{
		return NULL;
	}
	return right;
}

/* Position of the first matching clause in the list, or -1. */
static int
find_metadata_clause(List *clauses, Index relid, AttrNumber attno, Oid opfamily, int strategy,
					 bool array, Expr **value)
{
	ListCell *lc;
	foreach (lc, clauses)
	{
		*value = match_metadata_clause(lfirst(lc), relid, attno, opfamily, strategy, array);
		if (*value != NULL)
		{
			return foreach_current_index(lc);
		}
	}
	return -1;
}

/*
 * Find "first <= X" among the index clauses and "last >= X" with the same X
 * among all clauses, or the same with "= ANY(array)". Returns the mode, or -1
 * if there is no such pair.
 */
static int
find_lookup(List *index_clauses, List *all_clauses, Index relid, AttrNumber first_attno,
			AttrNumber last_attno, Oid opfamily, Expr **value, int *seek_index)
{
	for (int array = 0; array <= 1; array++)
	{
		*seek_index = find_metadata_clause(index_clauses,
										   relid,
										   first_attno,
										   opfamily,
										   BTLessEqualStrategyNumber,
										   array,
										   value);
		if (*seek_index < 0)
		{
			continue;
		}

		ListCell *lc;
		foreach (lc, all_clauses)
		{
			Expr *upper = match_metadata_clause(lfirst(lc),
												relid,
												last_attno,
												opfamily,
												BTGreaterEqualStrategyNumber,
												array);
			if (upper != NULL && equal(upper, *value))
			{
				return array ? BATCH_SEEK_VALUES : BATCH_SEEK_EQUALITY;
			}
		}
	}
	return -1;
}

/* Position of "last > L" or "last >= L" among the index clauses, or -1. */
static int
find_range_lower(List *index_clauses, Index relid, AttrNumber last_attno, Oid opfamily,
				 Expr **lower)
{
	int lower_index = find_metadata_clause(index_clauses,
										   relid,
										   last_attno,
										   opfamily,
										   BTGreaterStrategyNumber,
										   false,
										   lower);
	if (lower_index < 0)
	{
		lower_index = find_metadata_clause(index_clauses,
										   relid,
										   last_attno,
										   opfamily,
										   BTGreaterEqualStrategyNumber,
										   false,
										   lower);
	}
	return lower_index;
}

static List *
make_batch_seek_private(BatchSeekMode mode, int last_indexcol, bool forward, AttrNumber first_attno)
{
	List *private = ts_new_list(T_IntList, BSP_Count);
	lfirst_int(list_nth_cell(private, BSP_Mode)) = mode;
	lfirst_int(list_nth_cell(private, BSP_LastIndexCol)) = last_indexcol;
	lfirst_int(list_nth_cell(private, BSP_Forward)) = forward;
	lfirst_int(list_nth_cell(private, BSP_FirstAttno)) = first_attno;
	return private;
}

/*
 * For "first <= X" and "last >= X" with the same X, or the same with
 * "= ANY(array)", look up each value. The index scan keeps only "first <= X",
 * which is used for unordered chunks and when the leaf page doesn't show where
 * a lookup stops. Any other index condition would make the index scan skip the
 * batch before X, and then we would not know where to stop, so the other
 * conditions become a filter on the index scan output.
 */
static List *
batch_seek_lookup_plan_create(IndexScan *iscan, Oid opfamily, AttrNumber first_attno,
							  AttrNumber last_attno, int last_indexcol, Expr **seek_expr)
{
	Index relid = iscan->scan.scanrelid;
	List *all_quals = list_concat_copy(iscan->indexqualorig, iscan->scan.plan.qual);
	Expr *value;
	int seek_index;
	int mode = find_lookup(iscan->indexqualorig,
						   all_quals,
						   relid,
						   first_attno,
						   last_attno,
						   opfamily,
						   &value,
						   &seek_index);
	if (mode < 0)
	{
		return NIL;
	}

	/*
	 * The index is read backward from X, but the batches are returned in the
	 * direction of the original index scan, so the sort order of the path
	 * stays valid.
	 */
	bool forward = iscan->indexorderdir != BackwardScanDirection;

	List *quals = NIL;
	ListCell *lc;
	foreach (lc, iscan->indexqualorig)
	{
		if (foreach_current_index(lc) != seek_index)
		{
			quals = lappend(quals, lfirst(lc));
		}
	}
	iscan->scan.plan.qual = list_concat(quals, iscan->scan.plan.qual);
	iscan->indexqual = list_make1(list_nth(iscan->indexqual, seek_index));
	iscan->indexqualorig = list_make1(list_nth(iscan->indexqualorig, seek_index));
	iscan->indexorderdir = BackwardScanDirection;

	*seek_expr = copyObject(value);
	return make_batch_seek_private(mode, last_indexcol, forward, first_attno);
}

/*
 * For "last > L" or "last >= L", add "first >= start" to the index scan, where
 * the start is found at run time. The condition is added with a NULL value,
 * which is replaced in the scan key before the index scan runs.
 */
static List *
batch_seek_range_plan_create(IndexScan *iscan, Oid opfamily, AttrNumber first_attno,
							 AttrNumber last_attno, int last_indexcol, Expr **seek_expr)
{
	Index relid = iscan->scan.scanrelid;
	Expr *lower;
	int lower_index = find_range_lower(iscan->indexqualorig, relid, last_attno, opfamily, &lower);
	if (lower_index < 0)
	{
		return NIL;
	}

	OpExpr *lower_clause = list_nth(iscan->indexqualorig, lower_index);
	Var *last_var = castNode(Var, ts_strip_relabel_types(linitial(lower_clause->args)));
	Oid ge_opno = get_opfamily_member(opfamily,
									  last_var->vartype,
									  last_var->vartype,
									  BTGreaterEqualStrategyNumber);
	if (!OidIsValid(ge_opno))
	{
		return NIL;
	}

	Const *start = makeNullConst(last_var->vartype, last_var->vartypmod, last_var->varcollid);
	Expr *key = make_opclause(ge_opno,
							  BOOLOID,
							  false,
							  (Expr *) makeVar(INDEX_VAR,
											   1,
											   last_var->vartype,
											   last_var->vartypmod,
											   last_var->varcollid,
											   0),
							  (Expr *) start,
							  InvalidOid,
							  lower_clause->inputcollid);
	Expr *key_orig = make_opclause(ge_opno,
								   BOOLOID,
								   false,
								   (Expr *) makeVar(relid,
													first_attno,
													last_var->vartype,
													last_var->vartypmod,
													last_var->varcollid,
													0),
								   (Expr *) copyObject(start),
								   InvalidOid,
								   lower_clause->inputcollid);
	set_opfuncid(castNode(OpExpr, key));
	set_opfuncid(castNode(OpExpr, key_orig));

	/*
	 * Btree wants the scan keys ordered by index column, so put it first. This
	 * makes it scan key 0 at run time.
	 */
	iscan->indexqual = lcons(key, iscan->indexqual);
	iscan->indexqualorig = lcons(key_orig, iscan->indexqualorig);

	*seek_expr = copyObject(lower);
	return make_batch_seek_private(BATCH_SEEK_RANGE, last_indexcol, true, first_attno);
}

/*
 * Set up batch seek when the compressed index scan is a lookup on the leading
 * orderby column. The index scan is changed for the seek, and the returned
 * list goes to the ColumnarScan plan, with the value to look up in seek_expr.
 * Returns NIL and leaves the scan unchanged otherwise.
 */
List *
batch_seek_plan_create(const ColumnarScanPath *dcpath, Path *compressed_path, Plan *compressed_scan,
					   Expr **seek_expr)
{
	const CompressionInfo *info = dcpath->info;
	CompressionSettings *settings = info->settings;

	*seek_expr = NULL;
	if (!ts_guc_enable_columnar_batch_seek || !IsA(compressed_scan, IndexScan) ||
		!IsA(compressed_path, IndexPath) || compressed_scan->parallel_aware ||
		(info->chunk_status & CHUNK_STATUS_COMPRESSED_UNORDERED) || settings == NULL ||
		ts_array_length(settings->fd.orderby) < 1)
	{
		return NIL;
	}

	Oid chunk_relid = info->chunk_rte->relid;
	AttrNumber chunk_attno =
		get_attnum(chunk_relid, ts_array_get_element_text(settings->fd.orderby, 1));
	if (!batch_seek_column_supported(settings, chunk_relid, chunk_attno))
	{
		return NIL;
	}

	IndexScan *iscan = castNode(IndexScan, compressed_scan);
	IndexOptInfo *index = castNode(IndexPath, compressed_path)->indexinfo;
	if (iscan->indexorderby != NIL || index->relam != BTREE_AM_OID || index->ncolumns < 1 ||
		index->reverse_sort[0])
	{
		return NIL;
	}

	Oid compressed_relid = info->compressed_rte->relid;
	AttrNumber first_attno = compressed_column_metadata_attno(settings,
															  chunk_relid,
															  chunk_attno,
															  compressed_relid,
															  "first");
	AttrNumber last_attno = compressed_column_metadata_attno(settings,
															 chunk_relid,
															 chunk_attno,
															 compressed_relid,
															 "last");
	if (first_attno == InvalidAttrNumber || last_attno == InvalidAttrNumber ||
		index->indexkeys[0] != first_attno)
	{
		return NIL;
	}

	/*
	 * The batches we read from the table are given to ColumnarScan in place of
	 * the index scan output, so that output has to be the compressed chunk
	 * tuple.
	 */
	ListCell *lc;
	foreach (lc, iscan->scan.plan.targetlist)
	{
		TargetEntry *tle = lfirst_node(TargetEntry, lc);
		if (!IsA(tle->expr, Var) || castNode(Var, tle->expr)->varattno != tle->resno)
		{
			return NIL;
		}
	}

	int last_indexcol = index->nkeycolumns > 1 && index->indexkeys[1] == last_attno ? 2 : 0;
	List *private = batch_seek_lookup_plan_create(iscan,
												  index->opfamily[0],
												  first_attno,
												  last_attno,
												  last_indexcol,
												  seek_expr);
	if (private == NIL)
	{
		private = batch_seek_range_plan_create(iscan,
											   index->opfamily[0],
											   first_attno,
											   last_attno,
											   last_indexcol,
											   seek_expr);
	}
	return private;
}

/*
 * The first and the last value of the leading orderby column in the chunk,
 * from the two ends of the batch start index: the start of the first batch,
 * and the end of the last one, or its start when the index has no batch ends.
 */
static void
batch_seek_read_range(IndexOptInfo *index, int last_indexcol, BatchSeekRange *range)
{
	Relation rel = index_open(index->indexoid, AccessShareLock);
	Form_pg_attribute attr = TupleDescAttr(RelationGetDescr(rel), 0);
	for (int rightmost = 0; rightmost <= 1; rightmost++)
	{
		bool found = false;
		Buffer buf = bt_get_endpoint(rel, 0, rightmost, /* snapshot = */ NULL);
		while (BufferIsValid(buf) && !found)
		{
			Page page = BufferGetPage(buf);
			BTPageOpaque opaque = BTPageGetOpaque(page);
			OffsetNumber minoff = P_FIRSTDATAKEY(opaque);
			OffsetNumber maxoff = PageGetMaxOffsetNumber(page);
			if (minoff <= maxoff)
			{
				IndexTuple itup =
					(IndexTuple) PageGetItem(page,
											 PageGetItemId(page, rightmost ? maxoff : minoff));
				int column = rightmost && last_indexcol > 0 ? last_indexcol : 1;
				bool isnull;
				Datum value = index_getattr(itup, column, RelationGetDescr(rel), &isnull);
				if (!isnull)
				{
					value = datumCopy(value, attr->attbyval, attr->attlen);
					if (rightmost)
					{
						range->hi = value;
					}
					else
					{
						range->lo = value;
					}
					found = true;
				}
			}

			/* An end page can be empty after vacuum, move inward. */
			BlockNumber next = rightmost ? opaque->btpo_prev : opaque->btpo_next;
			bool last = rightmost ? P_LEFTMOST(opaque) : P_RIGHTMOST(opaque);
			_bt_relbuf(rel, buf);
			buf = found || last ? InvalidBuffer : _bt_getbuf(rel, next, BT_READ);
		}
		if (!found)
		{
			index_close(rel, AccessShareLock);
			return;
		}
	}
	index_close(rel, AccessShareLock);
	range->valid = true;
}

/*
 * The share of the lookups for "col = value" that find batches in this chunk,
 * when the value comes from another relation. In a join, the values usually
 * spread over many chunks, and in the chunks that don't contain the value,
 * the lookup ends on the leaf page. The share is the selectivity of
 * "value BETWEEN lo AND hi" for the range of the chunk. Without statistics on
 * the value, every lookup is assumed to find batches.
 */
static double
batch_seek_chunk_share(PlannerInfo *root, const CompressionInfo *info, IndexOptInfo *index,
					   int last_indexcol, Expr *value)
{
	Index relid = info->compressed_rel->relid;
	if (!contain_var_clause((Node *) value) || references_rel((Node *) value, &relid))
	{
		return 1.0;
	}

	VariableStatData vardata;
	examine_variable(root, (Node *) value, 0, &vardata);
	bool has_stats = HeapTupleIsValid(vardata.statsTuple);
	ReleaseVariableStats(vardata);
	if (!has_stats)
	{
		return 1.0;
	}

	BatchSeekRange *range = info->batch_seek_range;
	if (!range->checked)
	{
		range->checked = true;
		batch_seek_read_range(index, last_indexcol, range);
	}
	if (!range->valid)
	{
		return 1.0;
	}

	Oid type = exprType((Node *) value);
	Oid collid = exprCollation((Node *) value);
	Oid ge = get_opfamily_member(index->opfamily[0], type, type, BTGreaterEqualStrategyNumber);
	Oid le = get_opfamily_member(index->opfamily[0], type, type, BTLessEqualStrategyNumber);
	if (!OidIsValid(ge) || !OidIsValid(le))
	{
		return 1.0;
	}

	int16 typlen;
	bool typbyval;
	get_typlenbyval(type, &typlen, &typbyval);
	List *clauses = list_make2(make_opclause(ge,
											 BOOLOID,
											 false,
											 copyObject(value),
											 (Expr *) makeConst(type,
																-1,
																collid,
																typlen,
																range->lo,
																false,
																typbyval),
											 InvalidOid,
											 collid),
							   make_opclause(le,
											 BOOLOID,
											 false,
											 copyObject(value),
											 (Expr *) makeConst(type,
																-1,
																collid,
																typlen,
																range->hi,
																false,
																typbyval),
											 InvalidOid,
											 collid));
	Selectivity share = clauselist_selectivity(root, clauses, 0, JOIN_INNER, NULL);
	return Max(Min(share, 1.0), 1e-6);
}

typedef struct BatchSeekEstimate
{
	double batches;	   /* compressed rows the seek returns */
	double leaf_pages; /* index leaf pages it reads */
	Cost startup_cost;
	Cost index_cost; /* the index part: descents and leaf pages */
	Cost total_cost; /* with fetching the batches */
} BatchSeekEstimate;

/*
 * Whether a compressed index path can use batch seek, and what the seek costs.
 * This is the path-time version of the check in batch_seek_plan_create().
 *
 * A lookup costs one index descent and one leaf page per value, plus the
 * batches that contain the value and the one before it. When these entries
 * start on the previous leaf page, the lookup reads that page too. A range
 * costs one lookup, then reads its batches in index order.
 */
static bool
batch_seek_estimate(PlannerInfo *root, const CompressionInfo *info, IndexPath *ipath,
					BatchSeekEstimate *est)
{
	CompressionSettings *settings = info->settings;
	if (!ts_guc_enable_columnar_batch_seek || ipath->path.pathtype != T_IndexScan ||
		(info->chunk_status & CHUNK_STATUS_COMPRESSED_UNORDERED) || settings == NULL ||
		ts_array_length(settings->fd.orderby) < 1)
	{
		return false;
	}

	Oid chunk_relid = info->chunk_rte->relid;
	AttrNumber chunk_attno =
		get_attnum(chunk_relid, ts_array_get_element_text(settings->fd.orderby, 1));
	if (!batch_seek_column_supported(settings, chunk_relid, chunk_attno))
	{
		return false;
	}

	IndexOptInfo *index = ipath->indexinfo;
	if (ipath->indexorderbys != NIL || index->relam != BTREE_AM_OID || index->ncolumns < 1 ||
		index->reverse_sort[0])
	{
		return false;
	}

	Oid compressed_relid = info->compressed_rte->relid;
	AttrNumber first_attno = compressed_column_metadata_attno(settings,
															  chunk_relid,
															  chunk_attno,
															  compressed_relid,
															  "first");
	AttrNumber last_attno = compressed_column_metadata_attno(settings,
															 chunk_relid,
															 chunk_attno,
															 compressed_relid,
															 "last");
	if (first_attno == InvalidAttrNumber || last_attno == InvalidAttrNumber ||
		index->indexkeys[0] != first_attno)
	{
		return false;
	}

	Index relid = info->compressed_rel->relid;
	Oid opfamily = index->opfamily[0];
	List *index_clauses = NIL;
	ListCell *lc;
	foreach (lc, ipath->indexclauses)
	{
		index_clauses = lappend(index_clauses, lfirst_node(IndexClause, lc)->rinfo->clause);
	}
	List *all_clauses =
		list_concat(list_copy(index_clauses),
					extract_actual_clauses(info->compressed_rel->baserestrictinfo, false));
	if (ipath->path.param_info != NULL)
	{
		all_clauses =
			list_concat(all_clauses,
						extract_actual_clauses(ipath->path.param_info->ppi_clauses, false));
	}

	Expr *value;
	int seek_index;
	int mode = find_lookup(index_clauses,
						   all_clauses,
						   relid,
						   first_attno,
						   last_attno,
						   opfamily,
						   &value,
						   &seek_index);
	if (mode < 0 && find_range_lower(index_clauses, relid, last_attno, opfamily, &value) >= 0)
	{
		mode = BATCH_SEEK_RANGE;
	}
	if (mode < 0)
	{
		return false;
	}

	/*
	 * Batches per value: a value with more rows than a batch spans several
	 * batches. Without statistics, assume unique values, which is common for
	 * the leading orderby column of a chunk without segmentby.
	 */
	double batches = Max(1.0, info->compressed_rel->tuples);
	double ndistinct = batches * Max(1.0, info->compressed_batch_size);
	Oid vartype;
	int32 vartypmod;
	Oid varcollid;
	get_atttypetypmodcoll(chunk_relid, chunk_attno, &vartype, &vartypmod, &varcollid);
	Var *var = makeVar(info->chunk_rel->relid, chunk_attno, vartype, vartypmod, varcollid, 0);
	VariableStatData vardata;
	examine_variable(root, (Node *) var, 0, &vardata);
	bool isdefault;
	double stats_ndistinct = get_variable_numdistinct(&vardata, &isdefault);
	ReleaseVariableStats(vardata);
	if (!isdefault)
	{
		ndistinct = Max(1.0, stats_ndistinct);
	}
	double batches_per_value = Min(batches, 1.0 + batches / ndistinct);
	int last_indexcol = index->nkeycolumns > 1 && index->indexkeys[1] == last_attno ? 2 : 0;
	double share = mode == BATCH_SEEK_EQUALITY ?
					   batch_seek_chunk_share(root, info, index, last_indexcol, value) :
					   1.0;

	double index_tuples = Max(2.0, index->tuples);
	double entries_per_leaf = Max(1.0, index_tuples / Max(1.0, (double) index->pages));
	Cost descent = ceil(log2(index_tuples)) * cpu_operator_cost +
				   (Max(index->tree_height, 0) + 1) * 50.0 * cpu_operator_cost;
	Cost per_batch = cpu_tuple_cost + cpu_index_tuple_cost;

	if (mode == BATCH_SEEK_RANGE)
	{
		est->batches = ipath->path.rows;
		est->leaf_pages = 1 + est->batches / entries_per_leaf;
		est->index_cost = descent + random_page_cost + est->leaf_pages * seq_page_cost;
		est->total_cost =
			est->index_cost + random_page_cost + est->batches * (seq_page_cost + per_batch);
	}
	else
	{
#if PG17_GE
		double nvalues =
			mode == BATCH_SEEK_VALUES ? estimate_array_length(root, (Node *) value) : 1;
#else
		double nvalues = mode == BATCH_SEEK_VALUES ? estimate_array_length((Node *) value) : 1;
#endif
		nvalues = Max(1.0, nvalues);
		/* Every lookup reads a leaf page, but only some of them find batches. */
		double previous_leaf = share * Min(1.0, (batches_per_value + 1) / entries_per_leaf);
		est->batches = Min(batches, nvalues * share * batches_per_value);
		est->leaf_pages = nvalues * (1 + previous_leaf);
		est->index_cost = nvalues * descent + est->leaf_pages * random_page_cost;
		est->total_cost = est->index_cost + est->batches * (random_page_cost + per_batch);
	}
	est->startup_cost = Min(descent, est->index_cost);
	return true;
}

/*
 * The compressed rel for which the index paths are being costed, see
 * batch_seek_index_costing_begin().
 */
static const CompressionInfo *costing_info = NULL;

/*
 * Btree cost estimate for the index paths of a compressed chunk, with the
 * index part costed as a seek when the path can use it. The index scan gets
 * the selectivity of the batches the seek returns, so the table part of its
 * cost is right too.
 */
static void
batch_seek_costestimate(PlannerInfo *root, IndexPath *path, double loop_count,
						Cost *indexStartupCost, Cost *indexTotalCost, Selectivity *indexSelectivity,
						double *indexCorrelation, double *indexPages)
{
	btcostestimate(root,
				   path,
				   loop_count,
				   indexStartupCost,
				   indexTotalCost,
				   indexSelectivity,
				   indexCorrelation,
				   indexPages);

	BatchSeekEstimate est;
	if (costing_info == NULL || !batch_seek_estimate(root, costing_info, path, &est) ||
		est.index_cost >= *indexTotalCost)
	{
		return;
	}

	*indexStartupCost = est.startup_cost;
	*indexTotalCost = est.index_cost;
	*indexSelectivity =
		Min(*indexSelectivity, est.batches / Max(1.0, path->indexinfo->rel->tuples));
	*indexCorrelation = 1.0;
	*indexPages = Min(*indexPages, est.leaf_pages);
}

/*
 * Postgres keeps only the cheapest index path of the compressed chunk, so the
 * seek has to be costed when the index paths are made, not when ColumnarScan
 * is costed. Until batch_seek_index_costing_end(), the btree indexes of the
 * compressed chunk use batch_seek_costestimate().
 */
void
batch_seek_index_costing_begin(const CompressionInfo *info)
{
	if (!ts_guc_enable_columnar_batch_seek)
	{
		return;
	}

	costing_info = info;
	ListCell *lc;
	foreach (lc, info->compressed_rel->indexlist)
	{
		IndexOptInfo *index = lfirst_node(IndexOptInfo, lc);
		if (index->amcostestimate == btcostestimate)
		{
			index->amcostestimate = batch_seek_costestimate;
		}
	}
}

void
batch_seek_index_costing_end(const CompressionInfo *info)
{
	ListCell *lc;
	foreach (lc, info->compressed_rel->indexlist)
	{
		IndexOptInfo *index = lfirst_node(IndexOptInfo, lc);
		if (index->amcostestimate == batch_seek_costestimate)
		{
			index->amcostestimate = btcostestimate;
		}
	}
	costing_info = NULL;
}

/* Whether the compressed path is an index path that was costed as a seek. */
bool
batch_seek_path_costed(PlannerInfo *root, const CompressionInfo *info, Path *compressed_path)
{
	BatchSeekEstimate est;
	return IsA(compressed_path, IndexPath) &&
		   batch_seek_estimate(root, info, castNode(IndexPath, compressed_path), &est);
}

/*
 * Whether ColumnarScan on this compressed path uses batch seek. If so,
 * seek_path gets the rows and cost of the compressed scan done as a seek, and
 * batch_seek_plan_create() follows this decision. At worst a seek reads what
 * the plain index scan reads, which is its fallback.
 */
bool
batch_seek_cost_path(PlannerInfo *root, const CompressionInfo *info, Path *compressed_path,
					 Path *seek_path)
{
	BatchSeekEstimate est;
	if (!IsA(compressed_path, IndexPath) || compressed_path->parallel_aware ||
		!batch_seek_estimate(root, info, castNode(IndexPath, compressed_path), &est))
	{
		return false;
	}

	*seek_path = *compressed_path;
	seek_path->rows = est.batches;
	seek_path->total_cost = Min(est.total_cost, compressed_path->total_cost);
	seek_path->startup_cost = Min(est.startup_cost, seek_path->total_cost);
	return true;
}

/*
 * The index scan below ColumnarScan. The leaf page read does not need it, so
 * it is set up only for unordered chunks, ranges and the fallback.
 */
static PlanState *
batch_seek_child(BatchSeekState *state)
{
	CustomScanState *parent = state->parent;
	if (parent->custom_ps == NIL)
	{
		MemoryContext old = MemoryContextSwitchTo(state->estate->es_query_cxt);
		PlanState *child = ExecInitNode(&state->iscan->scan.plan, state->estate, state->eflags);

		/* We check the filter ourselves, after the stop check. */
		if (state->mode != BATCH_SEEK_RANGE)
		{
			child->qual = NULL;
		}
		parent->custom_ps = list_make1(child);
		MemoryContextSwitchTo(old);
	}
	return linitial(parent->custom_ps);
}

BatchSeekState *
batch_seek_begin(CustomScanState *parent, List *private, Expr *seek_expr, Plan *compressed_scan,
				 Oid chunk_relid, EState *estate, int eflags)
{
	BatchSeekState *state = palloc0(sizeof(BatchSeekState));
	state->parent = parent;
	state->iscan = castNode(IndexScan, compressed_scan);
	state->estate = estate;
	state->eflags = eflags;
	state->chunk_relid = chunk_relid;
	state->value_expr = seek_expr;
	state->mode = list_nth_int(private, BSP_Mode);
	state->last_indexcol = list_nth_int(private, BSP_LastIndexCol);
	state->forward = list_nth_int(private, BSP_Forward);
	state->first_attno = list_nth_int(private, BSP_FirstAttno);

	/* Plain EXPLAIN shows the index scan, but nothing runs. */
	if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
	{
		batch_seek_child(state);
	}
	return state;
}

static void
batch_seek_init(BatchSeekState *state)
{
	EState *estate = state->estate;
	PlanState *ps = &state->parent->ss.ps;
	MemoryContext old = MemoryContextSwitchTo(estate->es_query_cxt);

	state->econtext = CreateExprContext(estate);
	state->value = ExecInitExpr(state->value_expr, ps);

	/*
	 * The filter runs on the compressed tuple, not on the ColumnarScan scan
	 * slot, so it can't assume the slot type of the parent.
	 */
	if (state->mode != BATCH_SEEK_RANGE)
	{
		bool scanopsfixed = ps->scanopsfixed;
		ps->scanopsfixed = false;
		state->qual = ExecInitQual(state->iscan->scan.plan.qual, ps);
		ps->scanopsfixed = scanopsfixed;
	}
	state->seek_context =
		AllocSetContextCreate(CurrentMemoryContext, "batch seek", ALLOCSET_SMALL_SIZES);
	state->collect_context =
		AllocSetContextCreate(CurrentMemoryContext, "batch seek collect", ALLOCSET_SMALL_SIZES);

	state->heaprel = ExecOpenScanRelation(estate, state->iscan->scan.scanrelid, state->eflags);
	state->index = index_open(state->iscan->indexid, AccessShareLock);
	state->key = _bt_mkscankey(state->index, NULL);
	_bt_metaversion(state->index, &state->key->heapkeyspace, &state->key->allequalimage);
	state->fetch = table_index_fetch_begin(state->heaprel);
	state->vmbuffer = InvalidBuffer;
	state->entries = palloc(sizeof(BatchSeekEntry) * BATCH_SEEK_MAX_ENTRIES);

	TupleDesc desc = RelationGetDescr(state->heaprel);
	const TupleTableSlotOps *ops = table_slot_callbacks(state->heaprel);
	state->heap_slot = ExecInitExtraTupleSlot(estate, desc, ops);
	state->stop_slot = ExecInitExtraTupleSlot(estate, desc, ops);
	state->collected_slot = ExecInitExtraTupleSlot(estate, desc, &TTSOpsMinimalTuple);

	state->initialized = true;
	MemoryContextSwitchTo(old);
}

/*
 * Whether the chunk became unordered after planning. Only the status is
 * needed, so read the catalog row without building the whole chunk. This is
 * checked only when a lookup depends on the order of the batches, because in
 * a lookup over many chunks most of them have no batch that starts at or
 * before the value, and then the order doesn't matter.
 */
static bool
batch_seek_unordered(BatchSeekState *state)
{
	if (!state->order_checked)
	{
		FormData_chunk form;
		bool found =
			ts_chunk_simple_scan_by_reloid(state->chunk_relid, &form, /* missing_ok = */ true);
		state->unordered =
			!found || ts_flags_are_set_32(form.status, CHUNK_STATUS_COMPRESSED_UNORDERED);
		state->order_checked = true;
	}
	return state->unordered;
}

static int
batch_seek_compare(BatchSeekState *state, Datum a, Datum b)
{
	ScanKey key = &state->key->scankeys[0];
	return DatumGetInt32(FunctionCall2Coll(&key->sk_func, key->sk_collation, a, b));
}

static int
batch_seek_compare_values(const void *a, const void *b, void *arg)
{
	return batch_seek_compare((BatchSeekState *) arg, *(const Datum *) a, *(const Datum *) b);
}

/*
 * Copy the leaf entries from the last one with "first <= X" backward, up to a
 * few entries that start before X. This is what a backward index scan would
 * return first, without reading the rest of the page.
 */
static void
batch_seek_read_leaf(BatchSeekState *state, Datum value)
{
	Relation rel = state->index;
	BTScanInsert key = state->key;

	state->num_entries = 0;
	state->leaf_reads++;

	key->keysz = 1;
	key->nextkey = true;
#if PG17_GE
	/*
	 * Land on the page with the last entry <= X. Before PG 17, the search can
	 * land on the next page when that entry ends a page, and then the lookup
	 * continues with the index scan.
	 */
	key->backward = true;
#endif
	key->scantid = NULL;
	key->anynullkeys = false;
	key->scankeys[0].sk_argument = value;
	key->scankeys[0].sk_flags &= ~SK_ISNULL;

	Buffer buf;
	BTStack stack = bt_search(rel, state->heaprel, key, &buf, BT_READ, state->estate->es_snapshot);
	_bt_freestack(stack);
	if (!BufferIsValid(buf))
	{
		/* Empty index. */
		state->page_start_reached = true;
		state->leftmost = true;
		return;
	}

	Page page = BufferGetPage(buf);
	BTPageOpaque opaque = BTPageGetOpaque(page);
	OffsetNumber minoff = P_FIRSTDATAKEY(opaque);

	/* First entry with "first > X", see _bt_binsrch(). */
	OffsetNumber low = minoff;
	OffsetNumber high = PageGetMaxOffsetNumber(page);
	if (high >= low)
	{
		high++;
		while (high > low)
		{
			OffsetNumber mid = low + ((high - low) / 2);
			if (_bt_compare(rel, key, page, mid) >= 0)
			{
				low = mid + 1;
			}
			else
			{
				high = mid;
			}
		}
	}

	int before = 0;
	OffsetNumber off = low;
	while (off > minoff && before < BATCH_SEEK_BEFORE_ENTRIES &&
		   state->num_entries <= BATCH_SEEK_MAX_ENTRIES - MaxTIDsPerBTreePage)
	{
		off = OffsetNumberPrev(off);
		ItemId itemid = PageGetItemId(page, off);
		if (ItemIdIsDead(itemid))
		{
			continue;
		}

		IndexTuple itup = (IndexTuple) PageGetItem(page, itemid);
		bool is_before = _bt_compare(rel, key, page, off) > 0;
		before += is_before;

		/*
		 * The batch end is in the index too, so for a batch that starts
		 * before X we can see if it also ends before X.
		 */
		bool ends_before = false;
		if (is_before && state->last_indexcol > 0)
		{
			bool isnull;
			Datum last = index_getattr(itup, state->last_indexcol, RelationGetDescr(rel), &isnull);
			ends_before = !isnull && batch_seek_compare(state, last, value) < 0;
		}

		Datum first = (Datum) 0;
		if (state->mode == BATCH_SEEK_RANGE && is_before)
		{
			bool isnull;
			first = index_getattr(itup, 1, RelationGetDescr(rel), &isnull);
			Form_pg_attribute attr = TupleDescAttr(RelationGetDescr(rel), 0);
			MemoryContext old = MemoryContextSwitchTo(state->seek_context);
			first = datumCopy(first, attr->attbyval, attr->attlen);
			MemoryContextSwitchTo(old);
		}

		int nposting = BTreeTupleIsPosting(itup) ? BTreeTupleGetNPosting(itup) : 1;
		for (int i = nposting - 1; i >= 0; i--)
		{
			BatchSeekEntry *entry = &state->entries[state->num_entries++];
			entry->tid = BTreeTupleIsPosting(itup) ? *BTreeTupleGetPostingN(itup, i) : itup->t_tid;
			entry->before = is_before;
			entry->ends_before = ends_before;
			entry->first = first;
		}
	}

	state->page_start_reached = off <= minoff && before < BATCH_SEEK_BEFORE_ENTRIES;
	state->leftmost = P_LEFTMOST(opaque);
	_bt_relbuf(rel, buf);
}

static bool
batch_seek_fetch(BatchSeekState *state, ItemPointer tid, TupleTableSlot *slot)
{
	bool call_again = false;
	bool all_dead = false;
	return table_index_fetch_tuple(state->fetch,
								   tid,
								   state->estate->es_snapshot,
								   slot,
								   &call_again,
								   &all_dead);
}

/*
 * Find the batch where the lookup stops: the first visible copied entry that
 * starts before X. Returns false when it is not among the copied entries.
 */
static bool
batch_seek_find_stop(BatchSeekState *state)
{
	state->first_before = 0;
	while (state->first_before < state->num_entries && !state->entries[state->first_before].before)
	{
		state->first_before++;
	}
	state->stop_pending = false;

	for (int i = state->first_before; i < state->num_entries; i++)
	{
		BatchSeekEntry *entry = &state->entries[i];

		/* Visible to everyone and without matching rows: nothing to return. */
		if (entry->ends_before && VM_ALL_VISIBLE(state->heaprel,
												 ItemPointerGetBlockNumber(&entry->tid),
												 &state->vmbuffer))
		{
			state->fetches_skipped++;
			return true;
		}

		if (batch_seek_fetch(state, &entry->tid, state->stop_slot))
		{
			state->stop_pending = true;
			state->stop_tid = entry->tid;
			return true;
		}
	}

	/* No batch starts before X. */
	return state->page_start_reached && state->leftmost;
}

/* Whether a batch was returned already, for lookups of several values. */
static bool
batch_seek_seen(BatchSeekState *state, ItemPointer tid, HASHACTION action)
{
	bool found = false;
	if (state->returned != NULL)
	{
		hash_search(state->returned, tid, action, &found);
	}
	return found;
}

static TupleTableSlot *
batch_seek_check(BatchSeekState *state, TupleTableSlot *slot)
{
	state->econtext->ecxt_scantuple = slot;
	if (state->qual == NULL || ExecQual(state->qual, state->econtext))
	{
		return slot;
	}

	InstrCountFiltered2(&state->parent->ss.ps, 1);
	return NULL;
}

/* The next batch from the index scan, see batch_seek_start_child(). */
static TupleTableSlot *
batch_seek_next_from_child(BatchSeekState *state)
{
	PlanState *child = batch_seek_child(state);

	while (!state->child_done)
	{
		CHECK_FOR_INTERRUPTS();

		TupleTableSlot *slot = ExecProcNode(child);
		if (TupIsNull(slot))
		{
			state->child_done = true;
			break;
		}

		ResetExprContext(state->econtext);
		if (state->has_child_stop)
		{
			bool isnull;
			Datum first = slot_getattr(slot, state->first_attno, &isnull);
			if (!isnull && batch_seek_compare(state, first, state->child_stop) < 0)
			{
				state->child_done = true;
			}
		}

		if (batch_seek_seen(state, &slot->tts_tid, HASH_FIND))
		{
			continue;
		}

		if (batch_seek_check(state, slot) != NULL)
		{
			return slot;
		}
	}

	return NULL;
}

/*
 * Continue with the index scan for the remaining values. In an ordered chunk
 * it stops after the first batch that starts before the smallest of them.
 */
static void
batch_seek_start_child(BatchSeekState *state)
{
	state->use_child = true;
	state->child_done = false;
	state->has_child_stop = !batch_seek_unordered(state);
	if (state->has_child_stop)
	{
		int smallest = state->forward ? state->next_value - 1 : 0;
		state->child_stop = state->values[smallest];
	}
	ExecReScan(batch_seek_child(state));

	if (!state->forward)
	{
		return;
	}

	TupleTableSlot *slot;
	while ((slot = batch_seek_next_from_child(state)) != NULL)
	{
		MemoryContext old = MemoryContextSwitchTo(state->collect_context);
		if (state->num_collected == state->max_collected)
		{
			state->max_collected = Max(8, 2 * state->max_collected);
			state->collected =
				state->collected == NULL ?
					palloc(sizeof(MinimalTuple) * state->max_collected) :
					repalloc(state->collected, sizeof(MinimalTuple) * state->max_collected);
		}
		state->collected[state->num_collected++] = ExecCopySlotMinimalTuple(slot);
		MemoryContextSwitchTo(old);
	}
}

static TupleTableSlot *
batch_seek_next_child(BatchSeekState *state)
{
	if (!state->forward)
	{
		return batch_seek_next_from_child(state);
	}

	if (state->num_collected == 0)
	{
		return NULL;
	}
	ExecStoreMinimalTuple(state->collected[--state->num_collected], state->collected_slot, false);
	state->collected_slot->tts_tableOid = RelationGetRelid(state->heaprel);
	return state->collected_slot;
}

static TupleTableSlot *
batch_seek_next_stop(BatchSeekState *state)
{
	state->stop_pending = false;
	if (batch_seek_seen(state, &state->stop_tid, HASH_ENTER))
	{
		return NULL;
	}
	return batch_seek_check(state, state->stop_slot);
}

/*
 * The next batch found for the current value. In index order, the batch where
 * the lookup stops comes first, then the copied entries from the last one
 * down to 0.
 */
static TupleTableSlot *
batch_seek_next_entry(BatchSeekState *state)
{
	if (state->forward && state->stop_pending)
	{
		TupleTableSlot *slot = batch_seek_next_stop(state);
		if (slot != NULL)
		{
			return slot;
		}
	}

	while (state->num_emitted < state->first_before)
	{
		CHECK_FOR_INTERRUPTS();

		int i = state->forward ? state->first_before - 1 - state->num_emitted : state->num_emitted;
		BatchSeekEntry *entry = &state->entries[i];
		state->num_emitted++;
		if (batch_seek_seen(state, &entry->tid, HASH_ENTER))
		{
			continue;
		}

		ResetExprContext(state->econtext);
		if (batch_seek_fetch(state, &entry->tid, state->heap_slot) &&
			batch_seek_check(state, state->heap_slot) != NULL)
		{
			return state->heap_slot;
		}
	}

	if (!state->forward && state->stop_pending)
	{
		return batch_seek_next_stop(state);
	}
	return NULL;
}

/*
 * Evaluate the value or array to look up. The values are kept sorted, without
 * duplicates and NULLs. Returns false when there is nothing to look up.
 */
static bool
batch_seek_prepare_values(BatchSeekState *state)
{
	MemoryContextReset(state->seek_context);
	ResetExprContext(state->econtext);
	state->num_values = 0;
	state->next_value = 0;
	state->returned = NULL;

	bool isnull;
	Datum datum = ExecEvalExpr(state->value, state->econtext, &isnull);
	if (isnull)
	{
		return false;
	}

	MemoryContext old = MemoryContextSwitchTo(state->seek_context);
	Form_pg_attribute attr = TupleDescAttr(RelationGetDescr(state->index), 0);
	if (state->mode != BATCH_SEEK_VALUES)
	{
		state->values = palloc(sizeof(Datum));
		state->values[0] = datumCopy(datum, attr->attbyval, attr->attlen);
		state->num_values = 1;
		MemoryContextSwitchTo(old);
		return true;
	}

	ArrayType *array = DatumGetArrayTypePCopy(datum);
	bool *nulls;
	int n;
	deconstruct_array(array,
					  ARR_ELEMTYPE(array),
					  attr->attlen,
					  attr->attbyval,
					  attr->attalign,
					  &state->values,
					  &nulls,
					  &n);

	for (int i = 0; i < n; i++)
	{
		if (!nulls[i])
		{
			state->values[state->num_values++] = state->values[i];
		}
	}
	qsort_arg(state->values, state->num_values, sizeof(Datum), batch_seek_compare_values, state);

	int unique = 0;
	for (int i = 0; i < state->num_values; i++)
	{
		if (unique == 0 ||
			batch_seek_compare(state, state->values[unique - 1], state->values[i]) != 0)
		{
			state->values[unique++] = state->values[i];
		}
	}
	state->num_values = unique;

	/* A batch can contain several of the values, and is returned once. */
	if (unique > 1)
	{
		HASHCTL ctl = {
			.keysize = sizeof(ItemPointerData),
			.entrysize = sizeof(ItemPointerData),
			.hcxt = state->seek_context,
		};
		state->returned = hash_create("batch seek returned batches",
									  4 * unique,
									  &ctl,
									  HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
	}
	MemoryContextSwitchTo(old);
	return unique > 0;
}

/*
 * Equality and "= ANY": look up the values in the order of the batches, and
 * return the batches found for each of them.
 */
static TupleTableSlot *
batch_seek_next_lookup(BatchSeekState *state)
{
	if (!state->positioned)
	{
		state->positioned = true;
		if (!batch_seek_prepare_values(state))
		{
			state->done = true;
			return NULL;
		}
	}

	for (;;)
	{
		if (state->use_child)
		{
			return batch_seek_next_child(state);
		}

		TupleTableSlot *slot = batch_seek_next_entry(state);
		if (slot != NULL)
		{
			return slot;
		}

		if (state->next_value == state->num_values)
		{
			state->done = true;
			return NULL;
		}

		int i = state->forward ? state->next_value : state->num_values - 1 - state->next_value;
		state->next_value++;
		batch_seek_read_leaf(state, state->values[i]);

		/*
		 * No batch starts at or before the value, so none contains it, whether
		 * the batches are sorted or not.
		 */
		if (state->num_entries == 0 && state->page_start_reached && state->leftmost)
		{
			continue;
		}

		if (batch_seek_unordered(state))
		{
			batch_seek_start_child(state);
			continue;
		}

		if (!batch_seek_find_stop(state))
		{
			/* The batch where this lookup stops is on another leaf page. */
			state->fallbacks++;
			batch_seek_start_child(state);
			continue;
		}
		state->num_emitted = 0;
	}
}

/*
 * The first index entry, which is a lower bound that filters nothing.
 */
static bool
batch_seek_index_start(BatchSeekState *state, Datum *start)
{
	Relation rel = state->index;
	Buffer buf = bt_get_endpoint(rel, 0, false, state->estate->es_snapshot);
	while (BufferIsValid(buf))
	{
		Page page = BufferGetPage(buf);
		BTPageOpaque opaque = BTPageGetOpaque(page);
		OffsetNumber minoff = P_FIRSTDATAKEY(opaque);
		if (minoff <= PageGetMaxOffsetNumber(page))
		{
			IndexTuple itup = (IndexTuple) PageGetItem(page, PageGetItemId(page, minoff));
			bool isnull;
			Datum first = index_getattr(itup, 1, RelationGetDescr(rel), &isnull);
			Form_pg_attribute attr = TupleDescAttr(RelationGetDescr(rel), 0);
			MemoryContext old = MemoryContextSwitchTo(state->seek_context);
			*start = datumCopy(first, attr->attbyval, attr->attlen);
			MemoryContextSwitchTo(old);
			_bt_relbuf(rel, buf);
			return true;
		}

		/* The leftmost page is empty after vacuum, move right. */
		BlockNumber next = opaque->btpo_next;
		bool rightmost = P_RIGHTMOST(opaque);
		_bt_relbuf(rel, buf);
		buf = rightmost ? InvalidBuffer : _bt_getbuf(rel, next, BT_READ);
	}
	return false;
}

/*
 * The start of the last visible batch that starts before the lower bound L.
 * No batch before it has values above L. When no visible batch starts before
 * L, L itself is the start.
 */
static Datum
batch_seek_range_start(BatchSeekState *state, Datum lower)
{
	batch_seek_read_leaf(state, lower);
	bool any_before = false;
	for (int i = 0; i < state->num_entries; i++)
	{
		any_before |= state->entries[i].before;
	}

	/* No batch starts before L, whether the batches are sorted or not. */
	if (!any_before && state->page_start_reached && state->leftmost)
	{
		return lower;
	}

	if (!batch_seek_unordered(state))
	{
		for (int i = 0; i < state->num_entries; i++)
		{
			BatchSeekEntry *entry = &state->entries[i];
			if (entry->before && (VM_ALL_VISIBLE(state->heaprel,
												 ItemPointerGetBlockNumber(&entry->tid),
												 &state->vmbuffer) ||
								  batch_seek_fetch(state, &entry->tid, state->heap_slot)))
			{
				state->range_starts_found++;
				return entry->first;
			}
		}

		if (state->page_start_reached && state->leftmost)
		{
			return lower;
		}
		state->fallbacks++;
	}

	Datum start;
	if (batch_seek_index_start(state, &start))
	{
		return start;
	}
	return lower;
}

static TupleTableSlot *
batch_seek_next_range(BatchSeekState *state)
{
	IndexScanState *child = (IndexScanState *) batch_seek_child(state);

	if (!state->positioned)
	{
		state->positioned = true;
		if (!batch_seek_prepare_values(state))
		{
			state->done = true;
			return NULL;
		}

		ScanKey key = &child->iss_ScanKeys[0];
		key->sk_argument = batch_seek_range_start(state, state->values[0]);
		key->sk_flags &= ~SK_ISNULL;
		ExecReScan(&child->ss.ps);
	}

	TupleTableSlot *slot = ExecProcNode(&child->ss.ps);
	if (TupIsNull(slot))
	{
		state->done = true;
		return NULL;
	}
	return slot;
}

/* The next compressed batch for ColumnarScan, or NULL when there are no more. */
TupleTableSlot *
batch_seek_next(BatchSeekState *state)
{
	if (!state->initialized)
	{
		batch_seek_init(state);
	}

	if (state->done)
	{
		return NULL;
	}

	return state->mode == BATCH_SEEK_RANGE ? batch_seek_next_range(state) :
											 batch_seek_next_lookup(state);
}

void
batch_seek_rescan(BatchSeekState *state, Bitmapset *chgParam)
{
	state->done = false;
	state->positioned = false;
	state->num_entries = 0;
	state->first_before = 0;
	state->num_emitted = 0;
	state->stop_pending = false;
	state->use_child = false;
	state->returned = NULL;
	if (state->num_collected > 0 || state->collected != NULL)
	{
		ExecClearTuple(state->collected_slot);
		MemoryContextReset(state->collect_context);
		state->collected = NULL;
		state->num_collected = 0;
		state->max_collected = 0;
	}

	/* The index scan is rescanned when it is used again. */
	if (state->parent->custom_ps != NIL && chgParam != NULL)
	{
		UpdateChangedParamSet(linitial(state->parent->custom_ps), chgParam);
	}
}

void
batch_seek_end(BatchSeekState *state)
{
	if (state->fetch != NULL)
	{
		table_index_fetch_end(state->fetch);
	}
	if (BufferIsValid(state->vmbuffer))
	{
		ReleaseBuffer(state->vmbuffer);
	}
	if (state->index != NULL)
	{
		index_close(state->index, AccessShareLock);
	}
	if (state->parent->custom_ps != NIL)
	{
		ExecEndNode(linitial(state->parent->custom_ps));
	}
}

void
batch_seek_explain(BatchSeekState *state, List *ancestors, ExplainState *es)
{
	const char *label = state->mode == BATCH_SEEK_RANGE	 ? "Batch Seek From Last Batch Before" :
						state->mode == BATCH_SEEK_VALUES ? "Batch Seek Values" :
														   "Batch Seek";
	ts_show_scan_qual(list_make1(state->value_expr), label, &state->parent->ss.ps, ancestors, es);

	if (es->analyze)
	{
		ExplainPropertyUInteger("Leaf Reads", NULL, state->leaf_reads, es);
		ExplainPropertyUInteger("Index Scan Fallbacks", NULL, state->fallbacks, es);
		if (state->mode == BATCH_SEEK_RANGE)
		{
			ExplainPropertyUInteger("Range Starts Found", NULL, state->range_starts_found, es);
		}
		else
		{
			ExplainPropertyUInteger("Heap Fetches Skipped", NULL, state->fetches_skipped, es);
		}
	}
}
