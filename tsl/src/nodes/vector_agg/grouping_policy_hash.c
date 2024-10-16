/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

/*
 * This grouping policy groups the rows using a hash table. Currently it only
 * supports a single fixed-size by-value compressed column that fits into a Datum.
 */

#include <postgres.h>

#include <common/hashfn.h>
#include <executor/tuptable.h>
#include <nodes/pg_list.h>

#include "grouping_policy.h"

#include "nodes/decompress_chunk/compressed_batch.h"
#include "nodes/vector_agg/exec.h"

#include "grouping_policy_hash.h"

#ifdef USE_FLOAT8_BYVAL
#define DEBUG_LOG(MSG, ...) elog(DEBUG3, MSG, __VA_ARGS__)
#else
/*
 * On 32-bit platforms we'd have to use the cross-platform int width printf
 * specifiers which are really unreadable.
 */
#define DEBUG_LOG(...)
#endif

/*
 * We can use crc32 as a hash function, it has bad properties but takes only one
 * cycle, which is why it is sometimes used in the existing hash table
 * implementations.
 */

#ifdef USE_SSE42_CRC32C
#include <nmmintrin.h>
static pg_attribute_always_inline uint64
hash64(uint64 x)
{
	return _mm_crc32_u64(~0ULL, x);
}

#else
/*
 * When we don't have the crc32 instruction, use the SplitMix64 finalizer.
 */
static pg_attribute_always_inline uint64
hash64(uint64 x)
{
	x ^= x >> 30;
	x *= 0xbf58476d1ce4e5b9U;
	x ^= x >> 27;
	x *= 0x94d049bb133111ebU;
	x ^= x >> 31;
	return x;
}
#endif

#define KEY_VARIANT single_fixed_2
#define KEY_BYTES 2
#define KEY_HASH hash64
#define KEY_EQUAL(a, b) a == b
#define CTYPE int16
#define DATUM_TO_CTYPE DatumGetInt16
#define CTYPE_TO_DATUM Int16GetDatum

#include "single_fixed_key_impl.c"

#include "hash_table_functions_impl.c"

#define KEY_VARIANT single_fixed_4
#define KEY_BYTES 4
#define KEY_HASH hash64
#define KEY_EQUAL(a, b) a == b
#define CTYPE int32
#define DATUM_TO_CTYPE DatumGetInt32
#define CTYPE_TO_DATUM Int32GetDatum

#include "single_fixed_key_impl.c"

#include "hash_table_functions_impl.c"


#define KEY_VARIANT single_fixed_8
#define KEY_BYTES 8
#define KEY_HASH hash64
#define KEY_EQUAL(a, b) a == b
#define CTYPE int64
#define DATUM_TO_CTYPE DatumGetInt64
#define CTYPE_TO_DATUM Int64GetDatum

#include "single_fixed_key_impl.c"

#include "hash_table_functions_impl.c"

static void
store_text_datum(CompressedColumnValues *column_values, int arrow_row)
{
	const uint32 start = ((uint32 *) column_values->buffers[1])[arrow_row];
	const int32 value_bytes = ((uint32 *) column_values->buffers[1])[arrow_row + 1] - start;
	Assert(value_bytes >= 0);

	const int total_bytes = value_bytes + VARHDRSZ;
	Assert(DatumGetPointer(*column_values->output_value) != NULL);
	SET_VARSIZE(*column_values->output_value, total_bytes);
	memcpy(VARDATA(*column_values->output_value),
		   &((uint8 *) column_values->buffers[2])[start],
		   value_bytes);
}

static pg_attribute_always_inline void
single_text_get_key(CompressedColumnValues column, int row, text **restrict key,
					bool *restrict valid)
{
	if (unlikely(column.decompression_type == DT_Scalar))
	{
		/* Already stored. */
	}
	else if (column.decompression_type == DT_ArrowText)
	{
		store_text_datum(&column, row);
		*column.output_isnull = !arrow_row_is_valid(column.buffers[0], row);
	}
	else if (column.decompression_type == DT_ArrowTextDict)
	{
		const int16 index = ((int16 *) column.buffers[3])[row];
		store_text_datum(&column, index);
		*column.output_isnull = !arrow_row_is_valid(column.buffers[0], row);
	}
	else
	{
		pg_unreachable();
	}
	*key = (text *) DatumGetPointer(*column.output_value);
	*valid = !*column.output_isnull;
}

static pg_attribute_always_inline text *
single_text_store_key(text *key, Datum *key_storage, MemoryContext key_memory_context)
{
	size_t size = VARSIZE_ANY(key);
	text *stored = (text *) MemoryContextAlloc(key_memory_context, size);
	memcpy(stored, key, size);
	*key_storage = PointerGetDatum(stored);
	return stored;
}

#define KEY_VARIANT single_text
#define KEY_HASH(X) hash_bytes((const unsigned char *) VARDATA_ANY(X), VARSIZE_ANY_EXHDR(X))
#define KEY_EQUAL(a, b)                                                                            \
	(VARSIZE_ANY(a) == VARSIZE_ANY(b) &&                                                           \
	 memcmp(VARDATA_ANY(a), VARDATA_ANY(b), VARSIZE_ANY_EXHDR(a)) == 0)
#define SH_STORE_HASH
#define CTYPE text *
#include "hash_table_functions_impl.c"

static const GroupingPolicy grouping_policy_hash_functions;

GroupingPolicy *
create_grouping_policy_hash(List *agg_defs, List *output_grouping_columns)
{
	GroupingPolicyHash *policy = palloc0(sizeof(GroupingPolicyHash));
	policy->funcs = grouping_policy_hash_functions;
	policy->output_grouping_columns = output_grouping_columns;
	policy->agg_defs = agg_defs;
	policy->agg_extra_mctx =
		AllocSetContextCreate(CurrentMemoryContext, "agg extra", ALLOCSET_DEFAULT_SIZES);
	policy->allocated_aggstate_rows = TARGET_COMPRESSED_BATCH_SIZE;
	ListCell *lc;
	foreach (lc, agg_defs)
	{
		VectorAggDef *agg_def = lfirst(lc);
		policy->per_agg_states =
			lappend(policy->per_agg_states,
					palloc0(agg_def->func.state_bytes * policy->allocated_aggstate_rows));
	}

	Assert(list_length(policy->output_grouping_columns) == 1);
	GroupingColumn *g = linitial(policy->output_grouping_columns);
	//  policy->key_bytes = g->value_bytes;
	policy->key_bytes = sizeof(Datum);
	Assert(policy->key_bytes > 0);

	policy->num_allocated_keys = policy->allocated_aggstate_rows;
	policy->keys = palloc(policy->key_bytes * policy->num_allocated_keys);
	policy->key_body_mctx = policy->agg_extra_mctx;

	switch (g->value_bytes)
	{
		case 8:
			policy->functions = single_fixed_8_functions;
			break;
		case 4:
			policy->functions = single_fixed_4_functions;
			break;
		case 2:
			policy->functions = single_fixed_2_functions;
			break;
		case -1:
			Assert(g->typid == TEXTOID);
			policy->functions = single_text_functions;
			break;
		default:
			Assert(false);
			break;
	}

	policy->table =
		policy->functions.create(CurrentMemoryContext, policy->allocated_aggstate_rows, NULL);
	policy->have_null_key = false;

	policy->returning_results = false;

	return &policy->funcs;
}

static void
gp_hash_reset(GroupingPolicy *obj)
{
	GroupingPolicyHash *policy = (GroupingPolicyHash *) obj;

	MemoryContextReset(policy->agg_extra_mctx);

	policy->returning_results = false;

	policy->functions.reset(policy->table);
	policy->have_null_key = false;

	policy->stat_input_valid_rows = 0;
	policy->stat_input_total_rows = 0;
	policy->stat_bulk_filtered_rows = 0;
}

static void
compute_single_aggregate(DecompressBatchState *batch_state, int start_row, int end_row,
						 VectorAggDef *agg_def, void *agg_states, uint32 *offsets,
						 MemoryContext agg_extra_mctx)
{
	ArrowArray *arg_arrow = NULL;
	Datum arg_datum = 0;
	bool arg_isnull = true;

	/*
	 * We have functions with one argument, and one function with no arguments
	 * (count(*)). Collect the arguments.
	 */
	if (agg_def->input_offset >= 0)
	{
		CompressedColumnValues *values = &batch_state->compressed_columns[agg_def->input_offset];
		Assert(values->decompression_type != DT_Invalid);
		Assert(values->decompression_type != DT_Iterator);

		if (values->arrow != NULL)
		{
			arg_arrow = values->arrow;
		}
		else
		{
			Assert(values->decompression_type == DT_Scalar);
			arg_datum = *values->output_value;
			arg_isnull = *values->output_isnull;
		}
	}

	/*
	 * Now call the function.
	 */
	if (arg_arrow != NULL)
	{
		/* Arrow argument. */
		agg_def->func
			.agg_many_vector(agg_states, offsets, start_row, end_row, arg_arrow, agg_extra_mctx);
	}
	else
	{
		/*
		 * Scalar argument, or count(*). The latter has an optimized
		 * implementation.
		 */
		if (agg_def->func.agg_many_scalar != NULL)
		{
			agg_def->func.agg_many_scalar(agg_states,
										  offsets,
										  start_row,
										  end_row,
										  arg_datum,
										  arg_isnull,
										  agg_extra_mctx);
		}
		else
		{
			for (int i = start_row; i < end_row; i++)
			{
				if (offsets[i] == 0)
				{
					continue;
				}

				void *state = (offsets[i] * agg_def->func.state_bytes + (char *) agg_states);
				agg_def->func.agg_scalar(state, arg_datum, arg_isnull, 1, agg_extra_mctx);
			}
		}
	}
}

static void
gp_hash_add_batch(GroupingPolicy *gp, DecompressBatchState *batch_state)
{
	GroupingPolicyHash *policy = (GroupingPolicyHash *) gp;

	Assert(!policy->returning_results);

	const uint64_t *restrict filter = batch_state->vector_qual_result;
	const int n = batch_state->total_batch_rows;

	/*
	 * Initialize the array for storing the aggregate state offsets corresponding
	 * to a given batch row.
	 */
	if ((size_t) n > policy->num_allocated_offsets)
	{
		policy->num_allocated_offsets = n;
		policy->offsets = palloc(sizeof(policy->offsets[0]) * policy->num_allocated_offsets);
	}
	memset(policy->offsets, 0, n * sizeof(policy->offsets[0]));

	/*
	 * For the partial aggregation node, the grouping columns are always in the
	 * output, so we don't have to separately look at the list of the grouping
	 * columns.
	 */
	Assert(list_length(policy->output_grouping_columns) == 1);
	// GroupingColumn *g = linitial(policy->output_grouping_columns);
	// CompressedColumnValues *key_column = &batch_state->compressed_columns[g->input_offset];
	int start_row = 0;
	int end_row = 0;
	for (start_row = 0; start_row < n; start_row = end_row)
	{
		/*
		 * If we have a highly selective filter, it's easy to skip the rows for
		 * which the entire words of the filter bitmap are zero.
		 */
		if (filter)
		{
			if (filter[start_row / 64] == 0)
			{
				end_row = MIN(start_row + 64, n);
				policy->stat_bulk_filtered_rows += 64;
				continue;
			}

			for (end_row = start_row; end_row < n; end_row = MIN(end_row + 64, n))
			{
				if (filter[end_row / 64] == 0)
				{
					break;
				}
			}
		}
		else
		{
			end_row = n;
		}
		Assert(start_row <= end_row);
		Assert(end_row <= n);

		/*
		 * Remember which aggregation states have already existed, and which we
		 * have to initialize. State index zero is invalid, and state index one
		 * is for null key. We have to initialize the null key state at the
		 * first run.
		 */
		const uint32 hash_keys = policy->functions.get_num_keys(policy->table);
		const uint32 first_uninitialized_state_index = hash_keys ? hash_keys + 2 : 1;

		/*
		 * Allocate enough storage for keys, given that each bach row might be
		 * a separate new key.
		 */
		const uint32 num_possible_keys = first_uninitialized_state_index + (end_row - start_row);
		if (num_possible_keys > policy->num_allocated_keys)
		{
			policy->num_allocated_keys = num_possible_keys;
			policy->keys = repalloc(policy->keys, policy->key_bytes * num_possible_keys);
		}

		/*
		 * Match rows to aggregation states using a hash table.
		 */
		Assert((size_t) end_row <= policy->num_allocated_offsets);
		uint32 next_unused_state_index = hash_keys + 2;
		next_unused_state_index = policy->functions.fill_offsets(policy,
																 batch_state,
																 next_unused_state_index,
																 start_row,
																 end_row);

		ListCell *aggdeflc;
		ListCell *aggstatelc;

		/*
		 * Initialize the aggregate function states for the newly added keys.
		 */
		if (next_unused_state_index > first_uninitialized_state_index)
		{
			if (next_unused_state_index > policy->allocated_aggstate_rows)
			{
				policy->allocated_aggstate_rows = policy->allocated_aggstate_rows * 2 + 1;
				forboth (aggdeflc, policy->agg_defs, aggstatelc, policy->per_agg_states)
				{
					VectorAggDef *agg_def = lfirst(aggdeflc);
					lfirst(aggstatelc) =
						repalloc(lfirst(aggstatelc),
								 policy->allocated_aggstate_rows * agg_def->func.state_bytes);
				}
			}

			forboth (aggdeflc, policy->agg_defs, aggstatelc, policy->per_agg_states)
			{
				const VectorAggDef *agg_def = lfirst(aggdeflc);
				agg_def->func.agg_init(agg_def->func.state_bytes * first_uninitialized_state_index +
										   (char *) lfirst(aggstatelc),
									   next_unused_state_index - first_uninitialized_state_index);
			}
		}

		/*
		 * Update the aggregate function states.
		 */
		forboth (aggdeflc, policy->agg_defs, aggstatelc, policy->per_agg_states)
		{
			compute_single_aggregate(batch_state,
									 start_row,
									 end_row,
									 lfirst(aggdeflc),
									 lfirst(aggstatelc),
									 policy->offsets,
									 policy->agg_extra_mctx);
		}
	}
	Assert(end_row == n);
}

static bool
gp_hash_should_emit(GroupingPolicy *gp)
{
	GroupingPolicyHash *policy = (GroupingPolicyHash *) gp;

	/*
	 * Don't grow the hash table cardinality too much, otherwise we become bound
	 * by memory reads. In general, when this first stage of grouping doesn't
	 * significantly reduce the cardinality, it becomes pure overhead and the
	 * work will be done by the final Postgres aggregation, so we should bail
	 * out early here.
	 */
	return policy->functions.get_size_bytes(policy->table) > 128 * 1024;
}

static bool
gp_hash_do_emit(GroupingPolicy *gp, TupleTableSlot *aggregated_slot)
{
	GroupingPolicyHash *policy = (GroupingPolicyHash *) gp;

	if (!policy->returning_results)
	{
		policy->returning_results = true;
		policy->last_returned_key = 1 + !policy->have_null_key;

		const float keys = policy->functions.get_num_keys(policy->table) + policy->have_null_key;
		if (keys > 0)
		{
			DEBUG_LOG("spill after %ld input %ld valid %ld bulk filtered %.0f keys %f ratio %ld "
					  "curctx bytes %ld aggstate bytes",
					  policy->stat_input_total_rows,
					  policy->stat_input_valid_rows,
					  policy->stat_bulk_filtered_rows,
					  keys,
					  policy->stat_input_valid_rows / keys,
					  MemoryContextMemAllocated(CurrentMemoryContext, false),
					  MemoryContextMemAllocated(policy->agg_extra_mctx, false));
		}
	}
	else
	{
		policy->last_returned_key++;
	}

	const uint32 current_key = policy->last_returned_key;
	const uint32 keys_end = policy->functions.get_num_keys(policy->table) + 2;
	if (current_key >= keys_end)
	{
		policy->returning_results = false;
		return false;
	}

	const int naggs = list_length(policy->agg_defs);
	for (int i = 0; i < naggs; i++)
	{
		VectorAggDef *agg_def = (VectorAggDef *) list_nth(policy->agg_defs, i);
		void *agg_states = list_nth(policy->per_agg_states, i);
		void *agg_state = current_key * agg_def->func.state_bytes + (char *) agg_states;
		agg_def->func.agg_emit(agg_state,
							   &aggregated_slot->tts_values[agg_def->output_offset],
							   &aggregated_slot->tts_isnull[agg_def->output_offset]);
	}

	Assert(list_length(policy->output_grouping_columns) == 1);
	GroupingColumn *col = linitial(policy->output_grouping_columns);
	aggregated_slot->tts_values[col->output_offset] = ((Datum *) policy->keys)[current_key];
	aggregated_slot->tts_isnull[col->output_offset] = current_key == 1;

	return true;
}

static const GroupingPolicy grouping_policy_hash_functions = {
	.gp_reset = gp_hash_reset,
	.gp_add_batch = gp_hash_add_batch,
	.gp_should_emit = gp_hash_should_emit,
	.gp_do_emit = gp_hash_do_emit,
};
