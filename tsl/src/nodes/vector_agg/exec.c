/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <postgres.h>

#include <commands/explain.h>
#include <common/hashfn.h>
#include <executor/executor.h>
#include <executor/tuptable.h>
#include <fmgr.h>
#include <funcapi.h>
#include <nodes/extensible.h>
#include <nodes/makefuncs.h>
#include <nodes/nodeFuncs.h>
#include <nodes/pg_list.h>
#include <optimizer/optimizer.h>
#include <utils/ruleutils.h>

#include "nodes/vector_agg/exec.h"

#include "compat/compat.h"
#include "compression/arrow_c_data_interface.h"
#include "nodes/columnar_scan/columnar_scan.h"
#include "nodes/columnar_scan/compressed_batch.h"
#include "nodes/columnar_scan/exec.h"
#include "nodes/columnar_scan/vector_quals.h"
#include "nodes/vector_agg.h"
#include "nodes/vector_agg/filter_word_iterator.h"
#include "nodes/vector_agg/plan.h"
#include "nodes/vector_agg/vector_slot.h"

/*
 * Cache for common subexpression elimination during vectorized expression
 * evaluation. At init time, expression subtrees are interned (structurally
 * equal subtrees replaced with a single canonical pointer), then a separate
 * refcount pass identifies nodes reachable from multiple expression roots.
 * Only those nodes get entries here. The values (CompressedColumnValues) are
 * filled per-batch and reset between batches, since they point into
 * per-batch memory. Lookup is pointer comparison on the interned keys.
 */
typedef struct ExprCacheEntry
{
	Expr *key;
	CompressedColumnValues result; /* DT_Invalid means not yet computed */
	uint32 status;				   /* required by simplehash */
} ExprCacheEntry;

#define SH_PREFIX expr_cache
#define SH_ELEMENT_TYPE ExprCacheEntry
#define SH_KEY_TYPE Expr *
#define SH_KEY key
#define SH_HASH_KEY(tb, key) murmurhash64((uint64) (key))
#define SH_EQUAL(tb, a, b) ((a) == (b))
#define SH_SCOPE static inline
#define SH_DECLARE
#define SH_DEFINE
#include <lib/simplehash.h>

#if PG18_GE
#include "commands/explain_format.h"
#include "commands/explain_state.h"
#endif

static CompressedBatchVectorQualState
compressed_batch_init_vector_quals(DecompressContext *dcontext, List *quals, TupleTableSlot *slot);

static int
get_input_offset(const DecompressContext *dcontext, const Var *var)
{
	const CompressionColumnDescription *value_column_description = NULL;
	for (int i = 0; i < dcontext->num_data_columns; i++)
	{
		const CompressionColumnDescription *current_column = &dcontext->compressed_chunk_columns[i];
		if (current_column->uncompressed_chunk_attno == var->varattno)
		{
			value_column_description = current_column;
			break;
		}
	}
	Ensure(value_column_description != NULL,
		   "compressed column %d not found in columnar aggregation",
		   var->varattno);

	Assert(value_column_description->type == COMPRESSED_COLUMN ||
		   value_column_description->type == SEGMENTBY_COLUMN);

	const int index = value_column_description - dcontext->compressed_chunk_columns;
	return index;
}

/*
 * Workspace for converting the results of a Postgres function into a columnar
 * format.
 */
typedef struct
{
	DecompressionType type;

	uint64 *restrict validity;

	int allocated_body_bytes;
	uint8 *restrict body_buffer;

	uint32 *restrict offset_buffer;
	uint32 current_offset;
} ColumnarResult;

static void
columnar_result_init_for_type(ColumnarResult *columnar_result,
							  DecompressBatchState const *batch_state, Oid typeoid)
{
	int16 typlen;
	bool typbyval;
	get_typlenbyval(typeoid, &typlen, &typbyval);
	if (typeoid == BOOLOID)
	{
		columnar_result->type = DT_ArrowBits;
	}
	else if (typlen == -1)
	{
		columnar_result->type = DT_ArrowText;
	}
	else
	{
		Assert(typlen > 0);
		columnar_result->type = typlen;
	}

	const int nrows = batch_state->total_batch_rows;
	const size_t num_validity_words = (nrows + 63) / 64;
	if (columnar_result->type == DT_ArrowBits)
	{
		columnar_result->allocated_body_bytes = sizeof(uint64) * num_validity_words;
	}
	else if (columnar_result->type == DT_ArrowText)
	{
		/*
		 * Arrow variable-length types require n + 1 offsets to store the end
		 * position of the last element. Pad to 64 bytes per Arrow spec.
		 */
		columnar_result->offset_buffer =
			MemoryContextAllocZero(batch_state->per_batch_context,
								   pad_to_multiple(64,
												   sizeof(*columnar_result->offset_buffer) *
													   (nrows + 1)));
		columnar_result->allocated_body_bytes = pad_to_multiple(64, 10);
	}
	else
	{
		Assert(columnar_result->type > 0);
		columnar_result->allocated_body_bytes =
			pad_to_multiple(64, 1 + columnar_result->type * nrows);
	}

	columnar_result->body_buffer = MemoryContextAllocZero(batch_state->per_batch_context,
														  columnar_result->allocated_body_bytes);
}

static pg_attribute_always_inline void
columnar_result_set_row(ColumnarResult *columnar_result, DecompressBatchState const *batch_state,
						int row, Datum datum, bool isnull)
{
	const int nrows = batch_state->total_batch_rows;
	Assert(row < nrows);

	if (isnull)
	{
		if (columnar_result->validity == NULL)
		{
			const int num_validity_words = (nrows + 63) / 64;
			columnar_result->validity =
				MemoryContextAlloc(batch_state->per_batch_context,
								   num_validity_words * sizeof(*columnar_result->validity));
			memset(columnar_result->validity,
				   -1,
				   num_validity_words * sizeof(*columnar_result->validity));
			if (nrows % 64 != 0)
			{
				const uint64 tail_mask = ~0ULL >> (64 - nrows % 64);
				columnar_result->validity[nrows / 64] &= tail_mask;
			}
		}

		arrow_set_row_validity(columnar_result->validity, row, false);

		return;
	}

	switch ((int) columnar_result->type)
	{
		case DT_ArrowBits:
		{
			arrow_set_row_validity((uint64 *restrict) columnar_result->body_buffer,
								   row,
								   DatumGetBool(datum));
			break;
		}
		case DT_ArrowText:
		{
			const int result_bytes = VARSIZE_ANY_EXHDR(datum);
			const int required_body_bytes =
				pad_to_multiple(64, columnar_result->current_offset + result_bytes);
			if (required_body_bytes > columnar_result->allocated_body_bytes)
			{
				/*
				 * We reallocate based on how many rows in the batch we have
				 * left, not to overshoot too much. At the same time, we
				 * shouldn't reallocate too often either. The parameters were
				 * tuned manually on a few real data sets until this balance
				 * looked somewhat acceptable.
				 */
				const int new_body_bytes =
					required_body_bytes * Min(10, Max(1.2, 1.2 * nrows / ((float) row + 1))) + 1;
				Assert(new_body_bytes >= required_body_bytes);
				columnar_result->body_buffer =
					repalloc(columnar_result->body_buffer, new_body_bytes);
				columnar_result->allocated_body_bytes = new_body_bytes;
			}

			memcpy(&columnar_result->body_buffer[columnar_result->current_offset],
				   VARDATA_ANY(datum),
				   result_bytes);
			columnar_result->offset_buffer[row] = columnar_result->current_offset;
			columnar_result->current_offset += result_bytes;
			break;
		}
		case 2:
		case 4:
#ifdef USE_FLOAT8_BYVAL
		case 8:
#endif
			memcpy(row * columnar_result->type + (uint8 *restrict) columnar_result->body_buffer,
				   &datum,
				   sizeof(Datum));
			break;
#ifndef USE_FLOAT8_BYVAL
		case 8:
#endif
		case 16:
			memcpy(row * columnar_result->type + (uint8 *restrict) columnar_result->body_buffer,
				   DatumGetPointer(datum),
				   columnar_result->type);
			break;
		default:
			elog(ERROR, "wrong arrow result type %d", columnar_result->type);
	}
}

static CompressedColumnValues
columnar_result_finalize(ColumnarResult *columnar_result, DecompressBatchState const *batch_state)
{
	const int nrows = batch_state->total_batch_rows;

	ArrowArray *arrow_result = NULL;
	if (columnar_result->type == DT_ArrowBits)
	{
		arrow_result = MemoryContextAllocZero(batch_state->per_batch_context,
											  sizeof(ArrowArray) + 2 * sizeof(void *));
		arrow_result->buffers = (void *) &arrow_result[1];
		arrow_result->buffers[1] = columnar_result->body_buffer;
	}
	else if (columnar_result->type == DT_ArrowText)
	{
		columnar_result->offset_buffer[nrows] = columnar_result->current_offset;

		arrow_result = MemoryContextAllocZero(batch_state->per_batch_context,
											  sizeof(ArrowArray) + 3 * sizeof(void *));
		arrow_result->buffers = (void *) &arrow_result[1];
		arrow_result->buffers[1] = columnar_result->offset_buffer;
		arrow_result->buffers[2] = columnar_result->body_buffer;
	}
	else
	{
		Assert(columnar_result->type > 0);

		arrow_result = MemoryContextAllocZero(batch_state->per_batch_context,
											  sizeof(ArrowArray) + 2 * sizeof(void *));
		arrow_result->buffers = (void *) &arrow_result[1];
		arrow_result->buffers[1] = columnar_result->body_buffer;
	}

	arrow_result->length = nrows;

	arrow_result->buffers[0] = columnar_result->validity;
	arrow_result->null_count =
		arrow_result->length - arrow_num_valid(arrow_result->buffers[0], nrows);

	CompressedColumnValues result = {
		.decompression_type = columnar_result->type,
		.buffers = { arrow_result->buffers[0],
					 arrow_result->buffers[1],
					 columnar_result->type == DT_ArrowText ? arrow_result->buffers[2] : NULL },
		.arrow = arrow_result,
	};
	return result;
}

static pg_noinline CompressedColumnValues
vector_slot_evaluate_function(DecompressContext *dcontext, TupleTableSlot *slot,
							  uint64 const *filter, List *args, Oid funcoid, Oid inputcollid,
							  struct expr_cache_hash *expr_cache)
{
	const DecompressBatchState *batch_state = (const DecompressBatchState *) slot;

	const int nargs = list_length(args);

	FmgrInfo flinfo;
	fmgr_info(funcoid, &flinfo);
	FunctionCallInfo fcinfo = palloc0(SizeForFunctionCallInfo(nargs));
	InitFunctionCallInfoData(*fcinfo, &flinfo, nargs, inputcollid, NULL, NULL);

	CompressedColumnValues *arg_values = palloc0(nargs * sizeof(*arg_values));
	bool have_null_bitmap = false;
	bool have_null_scalars = false;
	ListCell *lc;
	foreach (lc, args)
	{
		const int i = foreach_current_index(lc);
		CompressedColumnValues arg_value =
			vector_slot_evaluate_expression(dcontext, slot, filter, lfirst(lc), expr_cache);
		Ensure(arg_value.decompression_type != DT_Invalid, "got DT_Invalid for argument %d", i);

		have_null_bitmap =
			(arg_value.arrow != NULL && arg_value.arrow->null_count > 0) || have_null_bitmap;

		arg_value.output_value = &fcinfo->args[i].value;
		arg_value.output_isnull = &fcinfo->args[i].isnull;

		if (arg_value.decompression_type == DT_ArrowText ||
			arg_value.decompression_type == DT_ArrowTextDict)
		{
			const int maxbytes = get_max_varlena_bytes(arg_value.arrow);
			*arg_value.output_value =
				PointerGetDatum(MemoryContextAlloc(batch_state->per_batch_context, maxbytes));
		}
		else if (arg_value.decompression_type == DT_Scalar)
		{
			/*
			 * The values of the scalar columns have to be stored once at
			 * initialization, they won't be updated per-row.
			 */
			*arg_value.output_value = PointerGetDatum(arg_value.buffers[1]);
			*arg_value.output_isnull = DatumGetBool(PointerGetDatum(arg_value.buffers[0]));

			have_null_scalars = *arg_value.output_isnull || have_null_scalars;
		}

		arg_values[i] = arg_value;
	}

	/*
	 * We only evaluate strict functions, so if we have a scalar null argument,
	 * return a scalar null.
	 */
	if (have_null_scalars)
	{
		pfree(fcinfo);
		pfree(arg_values);
		return (CompressedColumnValues){ .decompression_type = DT_Scalar,
										 .buffers[0] = DatumGetPointer(BoolGetDatum(true)) };
	}

	/*
	 * Our Postgres function is strict, so we should avoid calling it on null
	 * inputs.
	 */
	const int nrows = batch_state->total_batch_rows;
	const size_t num_validity_words = (nrows + 63) / 64;
	uint64 *input_validity = NULL;
	if (have_null_bitmap || filter != NULL)
	{
		uint64 *restrict combined_validity =
			MemoryContextAlloc(batch_state->per_batch_context,
							   sizeof(*combined_validity) * num_validity_words);
		memset(combined_validity, -1, num_validity_words * sizeof(*combined_validity));
		arrow_validity_and(num_validity_words, combined_validity, filter);
		for (int i = 0; i < nargs; i++)
		{
			arrow_validity_and(num_validity_words, combined_validity, arg_values[i].buffers[0]);
		}
		input_validity = combined_validity;
	}

	/*
	 * Call the Postgres function on every row. Here as well, we have to deal
	 * with very selective filters and avoid evaluating the functions on long
	 * consecutive ranges of filtered out rows, to improve the performance.
	 */
	ColumnarResult columnar_result = { 0 };
	columnar_result_init_for_type(&columnar_result, batch_state, get_func_rettype(funcoid));
	MemoryContext function_call_context =
		AllocSetContextCreate(CurrentMemoryContext, "bulk function call", ALLOCSET_DEFAULT_SIZES);
	MemoryContext old = MemoryContextSwitchTo(function_call_context);
	FilterWordIterator iter = filter_word_iterator_init(nrows, input_validity);
	int last_processed_row = 0;
	for (;;)
	{
		/*
		 * The Arrow format requires the offsets to monotonically increase even
		 * for the invalid rows.
		 */
		if (columnar_result.offset_buffer != NULL)
		{
			for (int row = last_processed_row; row < iter.start_row; row++)
			{
				columnar_result.offset_buffer[row] = columnar_result.current_offset;
			}
		}
		if (!filter_word_iterator_is_valid(&iter))
		{
			break;
		}
		for (int row = iter.start_row; row < iter.end_row; row++)
		{
			/*
			 * The Arrow format requires the offsets to monotonically increase even
			 * for the invalid rows.
			 */
			if (columnar_result.offset_buffer != NULL)
			{
				columnar_result.offset_buffer[row] = columnar_result.current_offset;
			}

			/*
			 * Do not evaluate the function on null inputs because it is strict.
			 */
			if (!arrow_row_is_valid(input_validity, row))
			{
				continue;
			}

			compressed_columns_to_postgres_data(arg_values, nargs, row);

			const Datum datum = FunctionCallInvoke(fcinfo);

			/*
			 * A strict function can still return a null for a non-null argument.
			 */
			const bool isnull = fcinfo->isnull;

			columnar_result_set_row(&columnar_result, batch_state, row, datum, isnull);

			MemoryContextReset(function_call_context);
		}

		last_processed_row = iter.end_row;
		filter_word_iterator_advance(&iter);
	}
	MemoryContextSwitchTo(old);
	MemoryContextDelete(function_call_context);

	/*
	 * Figure out the validity bitmap of the result rows. Besides the null
	 * inputs, the function itself can return nulls for some rows.
	 */
	if (columnar_result.validity != NULL)
	{
		arrow_validity_and(num_validity_words, columnar_result.validity, input_validity);
	}
	else
	{
		columnar_result.validity = (uint64 *) input_validity;
	}

	pfree(fcinfo);
	pfree(arg_values);

	return columnar_result_finalize(&columnar_result, batch_state);
}

/*
 * Return the arrow array or the datum (in case of single scalar value) for a
 * given expression as a CompressedColumnValues struct. If expr_cache is not
 * NULL, evaluation results for common subexpressions are cached and reused
 * within the current batch.
 */
CompressedColumnValues
vector_slot_evaluate_expression(DecompressContext *dcontext, TupleTableSlot *slot,
								uint64 const *filter, const Expr *argument,
								struct expr_cache_hash *expr_cache)
{
	/*
	 * Check if we already have this expression in the cache.
	 */
	ExprCacheEntry *cache_entry = NULL;
	if (expr_cache != NULL)
	{
		/*
		 * The cache entries are evaluated under the batch-global filter in
		 * DecompressBatchState.vector_qual_results. When another filter is
		 * needed, like with the aggregate FILTER clause, the cache cannot be
		 * used.
		 *
		 * The only exception is during the progressive evaluation of additional
		 * clauses that contribute to the batch-global filter bitmaps. In this
		 * case, the filter narrows progressively, so a cached result may have
		 * been computed under a broader filter than the current filter. This
		 * broader result is safe to cache, because it has the correct values
		 * for all rows of a narrower result.
		 */
		Assert(filter == ((const DecompressBatchState *) slot)->vector_qual_result);

		cache_entry = expr_cache_lookup(expr_cache, (Expr *) argument);
	}

	if (cache_entry != NULL && cache_entry->result.decompression_type != DT_Invalid)
	{
		return cache_entry->result;
	}

	/*
	 * No cache hit, so have to actually evaluate the expression.
	 */
	CompressedColumnValues result;
	const DecompressBatchState *batch_state = (const DecompressBatchState *) slot;
	switch (((Node *) argument)->type)
	{
		case T_Const:
		{
			const Const *c = (const Const *) argument;
			result = (CompressedColumnValues){ .decompression_type = DT_Scalar,
											   .buffers[1] = DatumGetPointer(c->constvalue),
											   .buffers[0] =
												   DatumGetPointer(BoolGetDatum(c->constisnull)) };
			break;
		}
		case T_Var:
		{
			const Var *var = (const Var *) argument;
			const uint16 offset = get_input_offset(dcontext, var);
			const CompressedColumnValues *values = &batch_state->compressed_columns[offset];
			Ensure(values->decompression_type != DT_Invalid,
				   "got DT_Invalid decompression type at offset %d",
				   offset);
			result = *values;
			break;
		}
		case T_OpExpr:
		{
			const OpExpr *o = (const OpExpr *) argument;
			result = vector_slot_evaluate_function(dcontext,
												   slot,
												   filter,
												   o->args,
												   o->opfuncid,
												   o->inputcollid,
												   expr_cache);
			break;
		}
		case T_FuncExpr:
		{
			const FuncExpr *f = (const FuncExpr *) argument;
			result = vector_slot_evaluate_function(dcontext,
												   slot,
												   filter,
												   f->args,
												   f->funcid,
												   f->inputcollid,
												   expr_cache);
			break;
		}
		default:
			Ensure(false,
				   "wrong node type %s for vector expression",
				   ts_get_node_name((Node *) argument));
			return (CompressedColumnValues){ .decompression_type = DT_Invalid };
	}

	/*
	 * Cache the evaluation result.
	 */
	if (cache_entry != NULL)
	{
		cache_entry->result = result;
		Assert(cache_entry->result.decompression_type != DT_Invalid);
	}

	return result;
}

/*
 * For common expression elimination, we start with applying a hash table to
 * deduplicate expression subtrees via interning. Each unique subtree is stored
 * once, and all structurally equal copies are replaced with a pointer to that
 * canonical instance. After interning, pointer comparison alone detects
 * equality. The table compares subtrees by equal(), with the hash produced from
 * the nodeToString() representation.
 */
typedef struct CSEInterningTableEntry
{
	Expr *key;

	/* Stored hash for simplehash.h. */
	uint32 hash;

	/* Internal status field for simplehash.h */
	uint32 status;
} CSEInterningTableEntry;

static uint32
cse_interning_hash_expr(Expr *expr)
{
	char *s = nodeToString(expr);
	uint32 h = hash_bytes((unsigned char const *) s, strlen(s));
	pfree(s);
	return h;
}

#define SH_PREFIX cse_interning
#define SH_ELEMENT_TYPE CSEInterningTableEntry
#define SH_KEY_TYPE Expr *
#define SH_KEY key
#define SH_HASH_KEY(tb, key) cse_interning_hash_expr(key)
#define SH_EQUAL(tb, a, b) equal(a, b)
#define SH_STORE_HASH
#define SH_GET_HASH(tb, a) (a)->hash
#define SH_SCOPE static inline
#define SH_DECLARE
#define SH_DEFINE
#include <lib/simplehash.h>

/*
 * Common subexpression elimination mutator. Walks the expression tree
 * bottom-up via expression_tree_mutator, interning each subtree. Structurally
 * equal subtrees end up sharing the same canonical Expr pointer.
 */
static Node *
cse_interning_expression_mutator(Node *node, void *context)
{
	if (node == NULL)
		return NULL;

	/*
	 * Recurse into children first (bottom-up) so that by the time we intern
	 * this node, its children are already canonical.
	 */
	Node *result = expression_tree_mutator(node, cse_interning_expression_mutator, context);

	struct cse_interning_hash *table = (struct cse_interning_hash *) context;
	bool found;
	CSEInterningTableEntry *entry = cse_interning_insert(table, (Expr *) result, &found);
	if (found)
	{
		return (Node *) entry->key;
	}
	return result;
}

/*
 * Refcount table for post-interning reference counting. Keyed by Expr pointer
 * identity (not structural equality). Counts how many top-level expression
 * roots reach each interned node.
 */
typedef struct CSERefcountTableEntry
{
	Expr *key;
	int refcount;

	/* Internal status field for simplehash.h. */
	uint32 status;
} CSERefcountTableEntry;

#define SH_PREFIX cse_refcount
#define SH_ELEMENT_TYPE CSERefcountTableEntry
#define SH_KEY_TYPE Expr *
#define SH_KEY key
#define SH_HASH_KEY(tb, key) murmurhash64((uint64) (key))
#define SH_EQUAL(tb, a, b) ((a) == (b))
#define SH_SCOPE static inline
#define SH_DECLARE
#define SH_DEFINE
#include <lib/simplehash.h>

/*
 * Walker callback for counting how many top-level expression roots reach
 * each non-leaf node. Var and Const are skipped: Var just looks up the
 * already-decompressed column array, and Const returns a literal.
 */
static bool
count_expr_refs_walker(Node *node, void *context)
{
	if (node == NULL)
	{
		return false;
	}

	if (IsA(node, Var) || IsA(node, Const))
	{
		return false;
	}

	struct cse_refcount_hash *refcounts = (struct cse_refcount_hash *) context;
	bool found;
	CSERefcountTableEntry *entry = cse_refcount_insert(refcounts, (Expr *) node, &found);
	if (!found)
	{
		entry->refcount = 0;
	}
	entry->refcount++;

	/*
	 * On second+ visit, skip recursion into children -- they were already
	 * counted during the first visit. Return false (not true) so the
	 * walker continues visiting sibling nodes at the parent level.
	 */
	if (entry->refcount > 1)
	{
		return false;
	}

	return expression_tree_walker(node, count_expr_refs_walker, context);
}

/*
 * Build the expression cache from the refcount table. Only expressions
 * reached by more than one top-level root get entries -- their presence
 * marks them as common subexpressions. The result values start as DT_Invalid
 * and are filled per-batch during evaluation.
 */
static struct expr_cache_hash *
build_expr_cache(struct cse_refcount_hash *refcounts)
{
	struct expr_cache_hash *cache = NULL;
	struct cse_refcount_iterator iter;

	cse_refcount_start_iterate(refcounts, &iter);
	for (CSERefcountTableEntry *entry = cse_refcount_iterate(refcounts, &iter); entry != NULL;
		 entry = cse_refcount_iterate(refcounts, &iter))
	{
		if (entry->refcount <= 1)
		{
			continue;
		}

		if (cache == NULL)
		{
			cache = expr_cache_create(CurrentMemoryContext, 16, NULL);
		}

		bool found;
		ExprCacheEntry *ce = expr_cache_insert(cache, entry->key, &found);
		Assert(!found);
		memset(&ce->result, 0, sizeof(ce->result));
		Assert(ce->result.decompression_type == DT_Invalid);
	}

	return cache;
}

/*
 * Reset the cached expression values between batches. The keys (interned
 * Expr pointers) persist, but the CompressedColumnValues are invalidated
 * because they point into per-batch memory.
 */
static void
reset_expr_cache(struct expr_cache_hash *cache)
{
	if (cache == NULL)
		return;

	struct expr_cache_iterator iter;
	expr_cache_start_iterate(cache, &iter);
	for (ExprCacheEntry *entry = expr_cache_iterate(cache, &iter); entry != NULL;
		 entry = expr_cache_iterate(cache, &iter))
	{
		memset(&entry->result, 0, sizeof(entry->result));
	}
}

static void
vector_agg_begin(CustomScanState *node, EState *estate, int eflags)
{
	CustomScan *cscan = castNode(CustomScan, node->ss.ps.plan);
	node->custom_ps =
		lappend(node->custom_ps, ExecInitNode(linitial(cscan->custom_plans), estate, eflags));

	VectorAggState *vector_agg_state = (VectorAggState *) node;
	vector_agg_state->input_ended = false;

	/*
	 * Set up the helper structures used to evaluate stable expressions in
	 * vectorized FILTER clauses.
	 */
	PlannerGlobal glob = {
		.boundParams = node->ss.ps.state->es_param_list_info,
	};
	PlannerInfo root = {
		.glob = &glob,
	};

	/*
	 * The aggregated targetlist with Aggrefs is in the custom scan targetlist
	 * of the custom scan node that is performing the vectorized aggregation.
	 * We do this to avoid projections at this node, because the postgres
	 * projection functions complain when they see an Aggref in a custom
	 * node output targetlist.
	 * The output targetlist, in turn, consists of just the INDEX_VAR references
	 * into the custom_scan_tlist.
	 * Now, iterate through the aggregated targetlist to collect aggregates and
	 * output grouping columns.
	 */
	List *aggregated_tlist =
		castNode(CustomScan, vector_agg_state->custom.ss.ps.plan)->custom_scan_tlist;
	const int tlist_length = list_length(aggregated_tlist);

	/*
	 * First, count how many grouping columns and aggregate functions we have.
	 */
	int agg_functions_counter = 0;
	int grouping_column_counter = 0;
	for (int i = 0; i < tlist_length; i++)
	{
		TargetEntry *tlentry = list_nth_node(TargetEntry, aggregated_tlist, i);
		if (IsA(tlentry->expr, Aggref))
		{
			agg_functions_counter++;
		}
		else
		{
			/* This is a grouping column. */
			grouping_column_counter++;
		}
	}
	Assert(agg_functions_counter + grouping_column_counter == tlist_length);

	/*
	 * Allocate the storage for definitions of aggregate function and grouping
	 * columns.
	 */
	vector_agg_state->num_agg_defs = agg_functions_counter;
	vector_agg_state->agg_defs =
		palloc0(sizeof(*vector_agg_state->agg_defs) * vector_agg_state->num_agg_defs);

	vector_agg_state->num_grouping_columns = grouping_column_counter;
	vector_agg_state->grouping_columns = palloc0(sizeof(*vector_agg_state->grouping_columns) *
												 vector_agg_state->num_grouping_columns);

	/*
	 * Loop through the aggregated targetlist again and fill the definitions.
	 */
	agg_functions_counter = 0;
	grouping_column_counter = 0;
	for (int i = 0; i < tlist_length; i++)
	{
		TargetEntry *tlentry = list_nth_node(TargetEntry, aggregated_tlist, i);
		if (IsA(tlentry->expr, Aggref))
		{
			/* This is an aggregate function. */
			VectorAggDef *def = &vector_agg_state->agg_defs[agg_functions_counter++];
			def->output_offset = i;

			Aggref *aggref = castNode(Aggref, tlentry->expr);

			VectorAggFunctions *func = get_vector_aggregate(aggref->aggfnoid, aggref->inputcollid);
			Assert(func != NULL);
			def->func = *func;

			if (list_length(aggref->args) > 0)
			{
				Assert(list_length(aggref->args) == 1);

				/* The aggregate should be a partial aggregate */
				Assert(aggref->aggsplit == AGGSPLIT_INITIAL_SERIAL);

				def->argument = castNode(TargetEntry, linitial(aggref->args))->expr;
			}
			else
			{
				def->argument = NULL;
			}

			if (aggref->aggfilter != NULL)
			{
				Node *constified = estimate_expression_value(&root, (Node *) aggref->aggfilter);
				def->filter_clauses = list_make1(constified);
			}
		}
		else
		{
			/* This is a grouping column. */

			GroupingColumn *col = &vector_agg_state->grouping_columns[grouping_column_counter++];
			col->expr = tlentry->expr;
			col->output_offset = i;

			TupleDesc tdesc = NULL;
			Oid type = InvalidOid;
			TypeFuncClass type_class = get_expr_result_type((Node *) tlentry->expr, &type, &tdesc);
			Ensure(type_class == TYPEFUNC_SCALAR,
				   "wrong grouping column type class %d",
				   type_class);
			get_typlenbyval(type, &col->value_bytes, &col->by_value);
		}
	}

	/*
	 * We might have non-vectorized filters that we still can evaluate in the
	 * columnar pipeline.
	 */
	vector_agg_state->pg_quals =
		list_nth(castNode(CustomScan, vector_agg_state->custom.ss.ps.plan)->custom_private,
				 VASI_PostgresQuals);

	/*
	 * Intern the expression subtrees for common subexpression elimination.
	 * Aggregates with FILTER clauses are excluded because caching would
	 * require evaluating the expression under the batch filter, which could
	 * run the function on rows excluded by FILTER that cause runtime errors
	 * (e.g. division by zero).
	 */
	struct cse_interning_hash *interning_table =
		cse_interning_create(CurrentMemoryContext, 32, NULL);
	for (int i = 0; i < vector_agg_state->num_agg_defs; i++)
	{
		VectorAggDef *def = &vector_agg_state->agg_defs[i];
		if (def->filter_clauses == NIL && def->argument != NULL)
		{
			def->argument =
				(Expr *) cse_interning_expression_mutator((Node *) def->argument, interning_table);
		}
	}

	for (int i = 0; i < vector_agg_state->num_grouping_columns; i++)
	{
		GroupingColumn *col = &vector_agg_state->grouping_columns[i];
		col->expr = (Expr *) cse_interning_expression_mutator((Node *) col->expr, interning_table);
	}

	vector_agg_state->pg_quals =
		castNode(List,
				 cse_interning_expression_mutator((Node *) vector_agg_state->pg_quals,
												  interning_table));

	cse_interning_destroy(interning_table);

	/*
	 * Count how many top-level expression roots reach each interned node.
	 * Only nodes reachable from multiple roots need caching. This correctly
	 * avoids caching subtrees that are shared only because they appear inside
	 * a parent that is itself cached.
	 */
	struct cse_refcount_hash *refcounts_table = cse_refcount_create(CurrentMemoryContext, 32, NULL);
	for (int i = 0; i < vector_agg_state->num_agg_defs; i++)
	{
		VectorAggDef *def = &vector_agg_state->agg_defs[i];
		if (def->filter_clauses == NIL && def->argument != NULL)
		{
			count_expr_refs_walker((Node *) def->argument, refcounts_table);
		}
	}

	for (int i = 0; i < vector_agg_state->num_grouping_columns; i++)
	{
		count_expr_refs_walker((Node *) vector_agg_state->grouping_columns[i].expr,
							   refcounts_table);
	}

	count_expr_refs_walker((Node *) vector_agg_state->pg_quals, refcounts_table);

	vector_agg_state->expr_cache = build_expr_cache(refcounts_table);

	cse_refcount_destroy(refcounts_table);

	/*
	 * Create the grouping policy chosen at plan time.
	 */
	const VectorAggGroupingType grouping_type =
		intVal(list_nth(cscan->custom_private, VASI_GroupingType));
	if (grouping_type == VAGT_Batch)
	{
		/*
		 * Per-batch grouping.
		 */
		vector_agg_state->grouping =
			create_grouping_policy_batch(vector_agg_state->num_agg_defs,
										 vector_agg_state->agg_defs,
										 vector_agg_state->num_grouping_columns,
										 vector_agg_state->grouping_columns);
	}
	else
	{
		/*
		 * Hash grouping.
		 */
		vector_agg_state->grouping =
			create_grouping_policy_hash(vector_agg_state->num_agg_defs,
										vector_agg_state->agg_defs,
										vector_agg_state->num_grouping_columns,
										vector_agg_state->grouping_columns,
										grouping_type);
	}
}

static void
vector_agg_end(CustomScanState *node)
{
	ExecEndNode(linitial(node->custom_ps));
}

static void
vector_agg_rescan(CustomScanState *node)
{
	if (node->ss.ps.chgParam != NULL)
		UpdateChangedParamSet(linitial(node->custom_ps), node->ss.ps.chgParam);

	ExecReScan(linitial(node->custom_ps));

	VectorAggState *state = (VectorAggState *) node;
	state->input_ended = false;

	state->grouping->gp_reset(state->grouping);
}

static void
vector_agg_evaluate_postgres_quals(DecompressContext *dcontext, DecompressBatchState *batch_state,
								   List *quals, struct expr_cache_hash *expr_cache)
{
	Assert(batch_state->next_batch_row == 0);

	const int num_words = (batch_state->total_batch_rows + 63) / 64;

	uint64 *restrict combined_qual_result = NULL;

	ListCell *lc;
	foreach (lc, quals)
	{
		const CompressedColumnValues single_qual_result =
			vector_slot_evaluate_expression(dcontext,
											&batch_state->decompressed_scan_slot_data.base,
											combined_qual_result != NULL ?
												combined_qual_result :
												batch_state->vector_qual_result,
											/* argument = */ lfirst(lc),
											expr_cache);

		/*
		 * The result is a nullable bool. We are checking a qualifier, so both
		 * false and null mean "doesn't pass".
		 *
		 * Typically we expect to get the standard DT_ArrowBits representation
		 * of bools there, but we can also get a DT_Scalar. In this case, the
		 * entire batch either passes or is filtered out.
		 *
		 * First, determine if the qual filtered something out.
		 */
		bool some_rows_are_filtered_out_by_this_qual = false;
		if (single_qual_result.decompression_type == DT_ArrowBits)
		{
			some_rows_are_filtered_out_by_this_qual |=
				(get_vector_qual_summary(single_qual_result.buffers[0],
										 batch_state->total_batch_rows) != AllRowsPass);
			some_rows_are_filtered_out_by_this_qual |=
				(get_vector_qual_summary(single_qual_result.buffers[1],
										 batch_state->total_batch_rows) != AllRowsPass);
		}
		else if (single_qual_result.decompression_type == DT_Scalar)
		{
			const bool isnull = DatumGetBool(PointerGetDatum(single_qual_result.buffers[0]));
			const bool value = DatumGetBool(PointerGetDatum(single_qual_result.buffers[1]));
			some_rows_are_filtered_out_by_this_qual |= isnull;
			some_rows_are_filtered_out_by_this_qual |= !value;
		}
		else
		{
			Ensure(false,
				   "unexpected decompression type %d for postgres qual result",
				   single_qual_result.decompression_type);
		}

		if (!some_rows_are_filtered_out_by_this_qual)
		{
			continue;
		}

		/*
		 * Some rows were filtered out by this qual, we might need to allocate
		 * the qual result storage.
		 */
		const int bitmap_bytes = num_words * sizeof(*combined_qual_result);
		if (combined_qual_result == NULL)
		{
			combined_qual_result = MemoryContextAlloc(batch_state->per_batch_context, bitmap_bytes);
			memset(combined_qual_result, 0xFF, bitmap_bytes);
		}

		/*
		 * Integrate the results of this qual into the current results for the
		 * batch using bool AND.
		 */
		if (single_qual_result.decompression_type == DT_ArrowBits)
		{
			arrow_validity_and(num_words, combined_qual_result, single_qual_result.buffers[0]);
			arrow_validity_and(num_words, combined_qual_result, single_qual_result.buffers[1]);
		}
		else
		{
			/* The other option is scalar bool as we have checked above. */
			Assert(single_qual_result.decompression_type == DT_Scalar);
			const bool isnull = DatumGetBool(PointerGetDatum(single_qual_result.buffers[0]));
			const bool value = DatumGetBool(PointerGetDatum(single_qual_result.buffers[1]));
			if (isnull || !value)
			{
				memset(combined_qual_result, 0x0, bitmap_bytes);
			}
		}

		/* Early exit when all rows are already filtered out. */
		if (get_vector_qual_summary(combined_qual_result, batch_state->total_batch_rows) ==
			NoRowsPass)
		{
			break;
		}
	}

	if (combined_qual_result != NULL)
	{
		batch_state->vector_qual_result = combined_qual_result;

		/* If no rows pass, mark the batch as fully consumed. */
		if (get_vector_qual_summary(batch_state->vector_qual_result,
									batch_state->total_batch_rows) == NoRowsPass)
		{
			compressed_batch_discard_tuples(batch_state);

			InstrCountTuples2(dcontext->ps, 1);
			InstrCountFiltered1(dcontext->ps, batch_state->total_batch_rows);
		}
	}
}

/*
 * Get the next slot to aggregate for a compressed batch.
 *
 * Implements "get next slot" on top of ColumnarScan. Note that compressed
 * tuples are read directly from the ColumnarScan child node, which means
 * that the processing normally done in ColumnarScan is actually done here
 * (batch processing and filtering).
 *
 * Returns an TupleTableSlot that implements a compressed batch.
 */
static TupleTableSlot *
compressed_batch_get_next_slot(VectorAggState *vector_agg_state)
{
	ColumnarScanState *decompress_state =
		(ColumnarScanState *) linitial(vector_agg_state->custom.custom_ps);
	DecompressContext *dcontext = &decompress_state->decompress_context;
	BatchQueue *batch_queue = decompress_state->batch_queue;
	DecompressBatchState *batch_state = batch_array_get_at(&batch_queue->batch_array, 0);

	do
	{
		/*
		 * We discard the previous compressed batch here and not earlier,
		 * because the grouping column values returned by the batch grouping
		 * policy are owned by the compressed batch memory context. This is done
		 * to avoid generic value copying in the grouping policy to simplify its
		 * code.
		 */
		compressed_batch_discard_tuples(batch_state);

		/*
		 * Discard the common subexpression cache before starting with the new
		 * batch.
		 */
		reset_expr_cache(vector_agg_state->expr_cache);

		TupleTableSlot *compressed_slot =
			ExecProcNode(linitial(decompress_state->csstate.custom_ps));

		if (TupIsNull(compressed_slot))
		{
			vector_agg_state->input_ended = true;
			return NULL;
		}

		if (dcontext->ps->instrument)
		{
			/*
			 * Ensure proper EXPLAIN output for the underlying ColumnarScan
			 * node.
			 *
			 * This value is normally updated by InstrStopNode(), and is
			 * required so that the calculations in InstrEndLoop() run properly.
			 * We have to call it manually because we run the underlying
			 * ColumnarScan manually and not as a normal Postgres node.
			 */
			dcontext->ps->instrument->running = true;
		}

		compressed_batch_set_compressed_tuple(dcontext, batch_state, compressed_slot);

		/*
		 * If we have PG quals and there are some rows that passed the vectorized
		 * quals, run the PG quals next.
		 */
		if (vector_agg_state->pg_quals &&
			batch_state->next_batch_row < batch_state->total_batch_rows)
		{
			vector_agg_evaluate_postgres_quals(dcontext,
											   batch_state,
											   vector_agg_state->pg_quals,
											   vector_agg_state->expr_cache);
		}

		/* If the entire batch is filtered out, then immediately read the next
		 * one */
	} while (batch_state->next_batch_row >= batch_state->total_batch_rows);

	/*
	 * Count rows filtered out by vectorized filters for EXPLAIN. Normally
	 * this is done in tuple-by-tuple interface of ColumnarScan, so that
	 * it doesn't say it filtered out more rows that were returned (e.g.
	 * with LIMIT). Here we always work in full batches. The batches that
	 * were fully filtered out, and their rows, were already counted in
	 * compressed_batch_set_compressed_tuple().
	 */
	const int not_filtered_rows =
		arrow_num_valid(batch_state->vector_qual_result, batch_state->total_batch_rows);
	InstrCountFiltered1(dcontext->ps, batch_state->total_batch_rows - not_filtered_rows);
	if (dcontext->ps->instrument)
	{
		/*
		 * Ensure proper EXPLAIN output for the underlying ColumnarScan
		 * node.
		 *
		 * This value is normally updated by InstrStopNode(), and is
		 * required so that the calculations in InstrEndLoop() run properly.
		 * We have to call it manually because we run the underlying
		 * ColumnarScan manually and not as a normal Postgres node.
		 */
		dcontext->ps->instrument->tuplecount += not_filtered_rows;
	}

	return &batch_state->decompressed_scan_slot_data.base;
}

/*
 * Initialize vector quals for a compressed batch.
 *
 * Used to implement vectorized aggregate function filter clause.
 */
static CompressedBatchVectorQualState
compressed_batch_init_vector_quals(DecompressContext *dcontext, List *quals, TupleTableSlot *slot)
{
	DecompressBatchState *batch_state = (DecompressBatchState *) slot;

	return (CompressedBatchVectorQualState) {
				.vqstate = {
					.vectorized_quals_constified = quals,
					.num_results = batch_state->total_batch_rows,
					.per_vector_mcxt = batch_state->per_batch_context,
					.slot = slot,
					.get_arrow_array = compressed_batch_get_arrow_array,
				},
				.batch_state = batch_state,
				.dcontext = dcontext,
			};
}

static TupleTableSlot *
vector_agg_exec(CustomScanState *node)
{
	VectorAggState *vector_agg_state = (VectorAggState *) node;

	ColumnarScanState *decompress_state =
		(ColumnarScanState *) linitial(vector_agg_state->custom.custom_ps);
	DecompressContext *dcontext = &decompress_state->decompress_context;

	ExprContext *econtext = node->ss.ps.ps_ExprContext;
	ResetExprContext(econtext);

	TupleTableSlot *aggregated_slot = vector_agg_state->custom.ss.ps.ps_ResultTupleSlot;
	ExecClearTuple(aggregated_slot);

	/*
	 * If we have more partial aggregation results, continue returning them.
	 */
	GroupingPolicy *grouping = vector_agg_state->grouping;
	MemoryContext old_context = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);
	bool have_partial = grouping->gp_do_emit(grouping, aggregated_slot);
	MemoryContextSwitchTo(old_context);
	if (have_partial)
	{
		/* The grouping policy produced a partial aggregation result. */
		return ExecStoreVirtualTuple(aggregated_slot);
	}

	/*
	 * Have no more partial aggregation results but might still have input.
	 * Reset the grouping policy and start a new cycle of partial aggregation.
	 */
	grouping->gp_reset(grouping);

	/*
	 * If the partial aggregation results have ended, and the input has ended,
	 * we're done.
	 */
	if (vector_agg_state->input_ended)
	{
		return NULL;
	}

	/*
	 * Now we loop through the input compressed tuples, until they end or until
	 * the grouping policy asks us to emit partials.
	 */
	while (!grouping->gp_should_emit(grouping))
	{
		/*
		 * Get the next slot to aggregate. It will be either a compressed
		 * batch or an arrow tuple table slot. Both hold arrow arrays of data
		 * that can be vectorized.
		 */
		TupleTableSlot *slot = vector_agg_state->get_next_slot(vector_agg_state);

		/*
		 * Exit if there is no more data. Note that it is not possible to do
		 * the standard TupIsNull() check here because the compressed batch's
		 * implementation of TupleTableSlot never clears the empty flag bit
		 * (TTS_EMPTY), so it will always look empty. Therefore, look at the
		 * "input_ended" flag instead.
		 */
		if (vector_agg_state->input_ended)
			break;

		/*
		 * Compute the vectorized filters for the aggregate function FILTER
		 * clauses.
		 */
		const int naggs = vector_agg_state->num_agg_defs;
		for (int i = 0; i < naggs; i++)
		{
			VectorAggDef *agg_def = &vector_agg_state->agg_defs[i];
			uint64 *filter_clause_result = NULL;
			if (agg_def->filter_clauses != NIL)
			{
				CompressedBatchVectorQualState vqstate =
					compressed_batch_init_vector_quals(dcontext, agg_def->filter_clauses, slot);
				if (vector_qual_compute(&vqstate.vqstate) != AllRowsPass)
				{
					filter_clause_result = vqstate.vqstate.vector_qual_result;
				}
			}

			DecompressBatchState *batch_state = (DecompressBatchState *) slot;
			if (filter_clause_result != NULL)
			{
				const int num_validity_words = (batch_state->total_batch_rows + 63) / 64;
				arrow_validity_and(num_validity_words,
								   filter_clause_result,
								   batch_state->vector_qual_result);
				agg_def->effective_batch_filter = filter_clause_result;
			}
			else
			{
				agg_def->effective_batch_filter = batch_state->vector_qual_result;
			}
		}

		/*
		 * Pass the batch to the grouping policy.
		 */
		grouping->gp_add_batch(grouping, dcontext, slot, vector_agg_state->expr_cache);
	}

	/*
	 * If we have partial aggregation results, start returning them.
	 */
	old_context = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);
	have_partial = grouping->gp_do_emit(grouping, aggregated_slot);
	MemoryContextSwitchTo(old_context);
	if (have_partial)
	{
		/* Have partial aggregation results. */
		return ExecStoreVirtualTuple(aggregated_slot);
	}

	if (vector_agg_state->input_ended)
	{
		/*
		 * Have no partial aggregation results and the input has ended, so we're
		 * done. We can get here only if we had no input at all, otherwise the
		 * grouping policy would have produced some partials above.
		 */
		return NULL;
	}

	/*
	 * We cannot get here. This would mean we still have input, and the
	 * grouping policy asked us to stop but couldn't produce any partials.
	 */
	Assert(false);
	pg_unreachable();
	return NULL;
}

#ifndef NDEBUG
static int
cmp_string_cells(const ListCell *a, const ListCell *b)
{
	return strcmp(lfirst(a), lfirst(b));
}
#endif

static void
vector_agg_explain(CustomScanState *node, List *ancestors, ExplainState *es)
{
	VectorAggState *state = (VectorAggState *) node;
	if (es->verbose || es->format != EXPLAIN_FORMAT_TEXT)
	{
		ExplainPropertyText("Grouping Policy", state->grouping->gp_explain(state->grouping), es);
	}

#ifndef NDEBUG
	if (es->verbose && state->expr_cache != NULL)
	{
		List *context = set_deparse_context_plan(es->deparse_cxt, node->ss.ps.plan, ancestors);
		List *cached_exprs = NIL;
		struct expr_cache_iterator iter;
		expr_cache_start_iterate(state->expr_cache, &iter);
		for (ExprCacheEntry *entry = expr_cache_iterate(state->expr_cache, &iter); entry != NULL;
			 entry = expr_cache_iterate(state->expr_cache, &iter))
		{
			cached_exprs = lappend(cached_exprs,
								   deparse_expression((Node *) entry->key, context, true, false));
		}
		list_sort(cached_exprs, cmp_string_cells);
		ExplainPropertyList("Cached Subexpressions", cached_exprs, es);
		list_free_deep(cached_exprs);
	}
#endif
}

static struct CustomExecMethods exec_methods = {
	.CustomName = VECTOR_AGG_NODE_NAME,
	.BeginCustomScan = vector_agg_begin,
	.ExecCustomScan = vector_agg_exec,
	.EndCustomScan = vector_agg_end,
	.ReScanCustomScan = vector_agg_rescan,
	.ExplainCustomScan = vector_agg_explain,
};

Node *
vector_agg_state_create(CustomScan *cscan)
{
	VectorAggState *state = (VectorAggState *) newNode(sizeof(VectorAggState), T_CustomScanState);
	Assert(ts_is_columnar_scan_plan((Plan *) linitial(cscan->custom_plans)));

	state->custom.methods = &exec_methods;

	/*
	 * Initialize VectorAggState to process vector slots from different
	 * subnodes.
	 *
	 * When the child is ColumnarScan, VectorAgg doesn't read the slot from
	 * the child node. Instead, it bypasses ColumnarScan and reads
	 * compressed tuples directly from the grandchild. It therefore needs to
	 * handle batch decompression and vectorized qual filtering itself, in its
	 * own "get next slot" implementation.
	 *
	 * The vector qual init functions are needed to implement vectorized
	 * aggregate function FILTER clauses for arrow tuple table slots and
	 * compressed batches, respectively.
	 */
	state->get_next_slot = compressed_batch_get_next_slot;

	return (Node *) state;
}
