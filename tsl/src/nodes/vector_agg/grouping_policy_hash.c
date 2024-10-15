/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

/*
 * This grouping policy aggregates entire compressed batches. It can be used to
 * aggregate with no grouping, or to produce partial aggregates per each batch
 * to group by segmentby columns.
 */

#include <postgres.h>

#include <executor/tuptable.h>
#include <nodes/pg_list.h>

#include "grouping_policy.h"

#include "nodes/decompress_chunk/compressed_batch.h"
#include "nodes/vector_agg/exec.h"

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

/*
 * For the hash table, use the generic Datum key that is mapped to the aggregate
 * state index.
 */
typedef struct
{
	Datum key;
	uint32 status;
	uint32 agg_state_index;
} HashEntry;

#define SH_PREFIX h
#define SH_ELEMENT_TYPE HashEntry
#define SH_KEY_TYPE Datum
#define SH_KEY key
#define SH_HASH_KEY(tb, key) hash64(key)
#define SH_EQUAL(tb, a, b) a == b
#define SH_SCOPE static inline
#define SH_DECLARE
#define SH_DEFINE
#include <lib/simplehash.h>

struct h_hash;

typedef struct
{
	GroupingPolicy funcs;
	List *agg_defs;
	List *output_grouping_columns;
	bool partial_per_batch;
	struct h_hash *table;
	bool have_null_key;
	struct h_iterator iter;
	bool returning_results;

	/*
	 * A memory context for aggregate functions to allocate additional data,
	 * i.e. if they store strings or float8 datum on 32-bit systems. Valid until
	 * the grouping policy is reset.
	 */
	MemoryContext agg_extra_mctx;

	/*
	 * Temporary storage of aggregate state offsets for a given batch. We keep
	 * it in the policy because it is too big to keep on stack, and we don't
	 * want to reallocate it each batch.
	 */
	uint32 *offsets;
	uint64 num_allocated_offsets;

	/*
	 * Storage of aggregate function states, each List entry is the array of
	 * states for the respective function from agg_defs.
	 */
	List *per_agg_states;
	uint64 allocated_aggstate_rows;

	uint64 stat_input_total_rows;
	uint64 stat_input_valid_rows;
	uint64 stat_bulk_filtered_rows;
} GroupingPolicyHash;

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

	policy->table = h_create(CurrentMemoryContext, policy->allocated_aggstate_rows, NULL);
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

	h_reset(policy->table);
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
		agg_def->func.agg_many(agg_states, offsets, start_row, end_row, arg_arrow, agg_extra_mctx);
	}
	else
	{
		/*
		 * Scalar argument, or count(*). The latter has an optimized
		 * implementation for this case.
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
				agg_def->func.agg_const(state, arg_datum, arg_isnull, 1, agg_extra_mctx);
			}
		}
	}
}

/*
 * Fill the aggregation state offsets for all rows using a hash table.
 */
static pg_attribute_always_inline uint32
fill_offsets_impl(GroupingPolicyHash *policy, CompressedColumnValues column,
				  const uint64 *restrict filter, uint32 next_unused_state_index, int start_row,
				  int end_row, uint32 *restrict offsets,
				  void (*get_key)(CompressedColumnValues column, int row, Datum *restrict key,
								  bool *restrict valid))
{
	struct h_hash *restrict table = policy->table;
	for (int row = start_row; row < end_row; row++)
	{
		bool key_valid = false;
		Datum key = { 0 };
		get_key(column, row, &key, &key_valid);

		if (!arrow_row_is_valid(filter, row))
		{
			continue;
		}

		if (key_valid)
		{
			bool found = false;
			HashEntry *restrict entry = h_insert(table, key, &found);
			if (!found)
			{
				entry->agg_state_index = next_unused_state_index++;
			}
			offsets[row] = entry->agg_state_index;
		}
		else
		{
			policy->have_null_key = true;
			offsets[row] = 1;
		}
	}
	return next_unused_state_index;
}

/*
 * This function exists just to nudge the compiler to generate simplified
 * implementation for the important case where the entire batch matches and the
 * key has no null values.
 */
static pg_attribute_always_inline uint32
fill_offsets_dispatch(GroupingPolicyHash *policy, DecompressBatchState *batch_state,
					  int key_column_index, uint32 next_unused_state_index, int start_row,
					  int end_row, uint32 *restrict offsets,
					  void (*get_key)(CompressedColumnValues column, int row, Datum *restrict key,
									  bool *restrict valid))
{
	CompressedColumnValues column = batch_state->compressed_columns[key_column_index];
	const uint64 *restrict filter = batch_state->vector_qual_result;

	if (filter == NULL && column.buffers[0] == NULL)
	{
		next_unused_state_index = fill_offsets_impl(policy,
													column,
													filter,
													next_unused_state_index,
													start_row,
													end_row,
													offsets,
													get_key);
	}
	else if (filter != NULL && column.buffers[0] == NULL)
	{
		next_unused_state_index = fill_offsets_impl(policy,
													column,
													filter,
													next_unused_state_index,
													start_row,
													end_row,
													offsets,
													get_key);
	}
	else if (filter == NULL && column.buffers[0] != NULL)
	{
		next_unused_state_index = fill_offsets_impl(policy,
													column,
													filter,
													next_unused_state_index,
													start_row,
													end_row,
													offsets,
													get_key);
	}
	else if (filter != NULL && column.buffers[0] != NULL)
	{
		next_unused_state_index = fill_offsets_impl(policy,
													column,
													filter,
													next_unused_state_index,
													start_row,
													end_row,
													offsets,
													get_key);
	}
	else
	{
		Assert(false);
	}

	policy->stat_input_total_rows += batch_state->total_batch_rows;
	policy->stat_input_valid_rows += arrow_num_valid(filter, batch_state->total_batch_rows);
	return next_unused_state_index;
}

/*
 * Functions to get the key value from the decompressed column, depending on its
 * width and whether it's a scalar column.
 */
static pg_attribute_always_inline void
get_key_scalar(CompressedColumnValues column, int row, Datum *restrict key, bool *restrict valid)
{
	Assert(column.decompression_type == DT_Scalar);
	*key = *column.output_value;
	*valid = !*column.output_isnull;
}

static pg_attribute_always_inline void
get_key_arrow_fixed(CompressedColumnValues column, int row, int key_bytes, Datum *restrict key,
					bool *restrict valid)
{
	Assert(column.decompression_type == key_bytes);
	const void *values = column.buffers[1];
	const uint64 *key_validity = column.buffers[0];
	*valid = arrow_row_is_valid(key_validity, row);
	memcpy(key, key_bytes * row + (char *) values, key_bytes);
}

static pg_attribute_always_inline void
get_key_arrow_fixed_2(CompressedColumnValues column, int row, Datum *restrict key,
					  bool *restrict valid)
{
	get_key_arrow_fixed(column, row, 2, key, valid);
}

static pg_attribute_always_inline void
get_key_arrow_fixed_4(CompressedColumnValues column, int row, Datum *restrict key,
					  bool *restrict valid)
{
	get_key_arrow_fixed(column, row, 4, key, valid);
}

static pg_attribute_always_inline void
get_key_arrow_fixed_8(CompressedColumnValues column, int row, Datum *restrict key,
					  bool *restrict valid)
{
#ifndef USE_FLOAT8_BYVAL
	/*
	 * Shouldn't be called for this configuration, because we only use this
	 * grouping strategy for by-value types.
	 */
	Assert(false);
#endif

	get_key_arrow_fixed(column, row, 8, key, valid);
}

/*
 * Implementation of bulk hashing specialized for a given key width.
 */
static pg_noinline uint32
fill_offsets_arrow_fixed_8(GroupingPolicyHash *policy, DecompressBatchState *batch_state,
						   int key_column_index, uint32 next_unused_state_index, int start_row,
						   int end_row, uint32 *restrict offsets)
{
	return fill_offsets_dispatch(policy,
								 batch_state,
								 key_column_index,
								 next_unused_state_index,
								 start_row,
								 end_row,
								 offsets,
								 get_key_arrow_fixed_8);
}

static pg_noinline uint32
fill_offsets_arrow_fixed_4(GroupingPolicyHash *policy, DecompressBatchState *batch_state,
						   int key_column_index, uint32 next_unused_state_index, int start_row,
						   int end_row, uint32 *restrict offsets)
{
	return fill_offsets_dispatch(policy,
								 batch_state,
								 key_column_index,
								 next_unused_state_index,
								 start_row,
								 end_row,
								 offsets,
								 get_key_arrow_fixed_4);
}

static pg_noinline uint32
fill_offsets_arrow_fixed_2(GroupingPolicyHash *policy, DecompressBatchState *batch_state,
						   int key_column_index, uint32 next_unused_state_index, int start_row,
						   int end_row, uint32 *restrict offsets)
{
	return fill_offsets_dispatch(policy,
								 batch_state,
								 key_column_index,
								 next_unused_state_index,
								 start_row,
								 end_row,
								 offsets,
								 get_key_arrow_fixed_2);
}

static pg_noinline uint32
fill_offsets_scalar(GroupingPolicyHash *policy, DecompressBatchState *batch_state,
					int key_column_index, uint32 next_unused_state_index, int start_row,
					int end_row, uint32 *restrict offsets)
{
	return fill_offsets_dispatch(policy,
								 batch_state,
								 key_column_index,
								 next_unused_state_index,
								 start_row,
								 end_row,
								 offsets,
								 get_key_scalar);
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
	GroupingColumn *g = linitial(policy->output_grouping_columns);
	CompressedColumnValues *key_column = &batch_state->compressed_columns[g->input_offset];
	int start_row = 0;
	int end_row = 0;
	for (start_row = 0; start_row < n; start_row = end_row)
	{
		/*
		 * If we have a highly selective filter, it's easy to skip the rows for
		 * which the entire filter bitmap words are zero.
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
		const uint32 last_initialized_state_index =
			policy->table->members ? policy->table->members + 2 : 1;
		uint32 next_unused_state_index = policy->table->members + 2;

		/*
		 * Match rows to aggregation states using a hash table.
		 */
		Assert((size_t) end_row <= policy->num_allocated_offsets);
		switch ((int) key_column->decompression_type)
		{
			case DT_Scalar:
				next_unused_state_index = fill_offsets_scalar(policy,
															  batch_state,
															  g->input_offset,
															  next_unused_state_index,
															  start_row,
															  end_row,
															  policy->offsets);
				break;
			case 8:
				next_unused_state_index = fill_offsets_arrow_fixed_8(policy,
																	 batch_state,
																	 g->input_offset,
																	 next_unused_state_index,
																	 start_row,
																	 end_row,
																	 policy->offsets);
				break;
			case 4:
				next_unused_state_index = fill_offsets_arrow_fixed_4(policy,
																	 batch_state,
																	 g->input_offset,
																	 next_unused_state_index,
																	 start_row,
																	 end_row,
																	 policy->offsets);
				break;
			case 2:
				next_unused_state_index = fill_offsets_arrow_fixed_2(policy,
																	 batch_state,
																	 g->input_offset,
																	 next_unused_state_index,
																	 start_row,
																	 end_row,
																	 policy->offsets);
				break;
			default:
				Assert(false);
				break;
		}

		ListCell *aggdeflc;
		ListCell *aggstatelc;

		/*
		 * Initialize the aggregate function states for the newly added keys.
		 */
		if (next_unused_state_index > last_initialized_state_index)
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
				agg_def->func.agg_init(agg_def->func.state_bytes * last_initialized_state_index +
										   (char *) lfirst(aggstatelc),
									   next_unused_state_index - last_initialized_state_index);
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
	return policy->table->members * sizeof(HashEntry) > 128 * 1024;
}

static bool
gp_hash_do_emit(GroupingPolicy *gp, TupleTableSlot *aggregated_slot)
{
	GroupingPolicyHash *policy = (GroupingPolicyHash *) gp;

	if (!policy->returning_results)
	{
		policy->returning_results = true;
		h_start_iterate(policy->table, &policy->iter);
		//		fprintf(stderr,
		//				"spill after %ld input %ld valid %ld bulk filtered, %d keys, %f ratio, %ld
		// aggctx bytes, %ld aggstate bytes\n", 				policy->stat_input_total_rows,
		// policy->stat_input_valid_rows, policy->stat_bulk_filtered_rows,
		// policy->table->members
		//				+ 		policy->have_null_key, 				policy->stat_input_valid_rows /
		//(float) 				(policy->table->members + 				 policy->have_null_key),
		// MemoryContextMemAllocated(policy->table->ctx,
		// false), MemoryContextMemAllocated(policy->agg_extra_mctx, false));
	}

	HashEntry null_key_entry = { .agg_state_index = 1 };
	HashEntry *entry = h_iterate(policy->table, &policy->iter);
	bool key_is_null = false;
	if (entry == NULL && policy->have_null_key)
	{
		policy->have_null_key = false;
		entry = &null_key_entry;
		key_is_null = true;
	}

	if (entry == NULL)
	{
		policy->returning_results = false;
		return false;
	}

	const int naggs = list_length(policy->agg_defs);
	for (int i = 0; i < naggs; i++)
	{
		VectorAggDef *agg_def = (VectorAggDef *) list_nth(policy->agg_defs, i);
		void *agg_states = list_nth(policy->per_agg_states, i);
		void *agg_state = entry->agg_state_index * agg_def->func.state_bytes + (char *) agg_states;
		agg_def->func.agg_emit(agg_state,
							   &aggregated_slot->tts_values[agg_def->output_offset],
							   &aggregated_slot->tts_isnull[agg_def->output_offset]);
	}

	Assert(list_length(policy->output_grouping_columns) == 1);
	GroupingColumn *col = linitial(policy->output_grouping_columns);
	aggregated_slot->tts_values[col->output_offset] = entry->key;
	aggregated_slot->tts_isnull[col->output_offset] = key_is_null;

	return true;
}

static const GroupingPolicy grouping_policy_hash_functions = {
	.gp_reset = gp_hash_reset,
	.gp_add_batch = gp_hash_add_batch,
	.gp_should_emit = gp_hash_should_emit,
	.gp_do_emit = gp_hash_do_emit,
};
