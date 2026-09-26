/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>

#include <access/attnum.h>
#include <catalog/namespace.h>
#include <utils/lsyscache.h>

#include "compression/compression.h"
#include "compression/create.h"
#include "custom_type_cache.h"
#include "guc.h"
#include "nodes/columnar_scan/compressed_batch_util.h"
#include "nodes/columnar_scan/exec.h"

/*
 * Resolve a data column name to its attno in the uncompressed chunk
 * descriptor. Column names are shared between the compressed and
 * uncompressed chunk. Dropped columns are skipped, so a name that only
 * exists as dropped resolves to InvalidAttrNumber.
 */
static AttrNumber
utility_find_uncompressed_attno(TupleDesc uncompressed_desc, const char *name)
{
	for (int i = 0; i < uncompressed_desc->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(uncompressed_desc, i);
		if (attr->attisdropped)
		{
			continue;
		}
		if (strcmp(NameStr(attr->attname), name) == 0)
		{
			return i + 1;
		}
	}
	return InvalidAttrNumber;
}

/*
 * Build a DecompressContext for utility consumers, mirroring the column
 * mapping the executor derives from planner lists (exec.c), but driven by
 * the compressed/uncompressed chunk tuple descriptors instead.
 *
 * Segmentby columns are detected by physical type, the same way
 * create_per_compressed_column() does: a segmentby column keeps its
 * original type in the compressed chunk, while a compressed column has
 * the compressed_data type. This works without catalog settings, so the
 * record-returning SRF can use the builder too.
 *
 * Utility callers have no custom targetlist: the custom scan attno of every
 * data column is its uncompressed chunk attno. Metadata columns other than
 * count/sequence_num are not needed for decompression and are skipped.
 * ps is NULL: with NIL vectorized quals no qual or instrumentation path is
 * reached (postgres_qual() returns true early, and the skip/end-check
 * branches that call InstrCount* are not taken).
 */
DecompressContext *
decompress_context_create_utility_desc(TupleDesc uncompressed_desc, TupleDesc compressed_desc)
{
	DecompressContext *dcontext = palloc0(sizeof(DecompressContext));
	const Oid compressed_data_type_oid =
		ts_custom_type_cache_get(CUSTOM_TYPE_COMPRESSED_DATA)->type_oid;
	Assert(OidIsValid(compressed_data_type_oid));

	/*
	 * First pass: data columns (segmentby and compressed), in compressed
	 * chunk physical order. Column names are shared between the compressed
	 * and uncompressed chunk, except for metadata.
	 */
	int num_data = 0;
	for (int attno = 1; attno <= compressed_desc->natts; attno++)
	{
		Form_pg_attribute attr = TupleDescAttr(compressed_desc, attno - 1);
		const char *name = NameStr(attr->attname);

		if (attr->attisdropped)
		{
			continue;
		}
		if (strncmp(name, COMPRESSION_COLUMN_METADATA_PREFIX,
					strlen(COMPRESSION_COLUMN_METADATA_PREFIX)) == 0)
		{
			continue;
		}
		if (utility_find_uncompressed_attno(uncompressed_desc, name) == InvalidAttrNumber)
		{
			/* Column dropped in the uncompressed chunk, nothing to produce. */
			continue;
		}
		num_data++;
	}

	/* Second pass: the metadata columns used for decompression. */
	int num_meta = 0;
	for (int attno = 1; attno <= compressed_desc->natts; attno++)
	{
		Form_pg_attribute attr = TupleDescAttr(compressed_desc, attno - 1);
		const char *name = NameStr(attr->attname);

		if (attr->attisdropped)
		{
			continue;
		}
		if (strcmp(name, COMPRESSION_COLUMN_METADATA_COUNT_NAME) == 0 ||
			strcmp(name, COMPRESSION_COLUMN_METADATA_SEQUENCE_NUM_NAME) == 0)
		{
			num_meta++;
		}
	}

	dcontext->num_data_columns = num_data;
	dcontext->num_columns_with_metadata = num_data + num_meta;
	dcontext->compressed_chunk_columns =
		palloc0(sizeof(CompressionColumnDescription) * (num_data + num_meta));

	/* Data columns go in front. */
	int data_i = 0;
	for (int attno = 1; attno <= compressed_desc->natts; attno++)
	{
		Form_pg_attribute attr = TupleDescAttr(compressed_desc, attno - 1);
		const char *name = NameStr(attr->attname);

		if (attr->attisdropped ||
			strncmp(name, COMPRESSION_COLUMN_METADATA_PREFIX,
					strlen(COMPRESSION_COLUMN_METADATA_PREFIX)) == 0)
		{
			continue;
		}

		const AttrNumber out_attno = utility_find_uncompressed_attno(uncompressed_desc, name);
		if (out_attno == InvalidAttrNumber)
		{
			continue;
		}

		Form_pg_attribute out_attr = TupleDescAttr(uncompressed_desc, out_attno - 1);
		const bool is_segmentby = attr->atttypid != compressed_data_type_oid;
		CompressionColumnDescription *column = &dcontext->compressed_chunk_columns[data_i++];

		column->type = is_segmentby ? SEGMENTBY_COLUMN : COMPRESSED_COLUMN;
		column->typid = out_attr->atttypid;
		get_typlenbyval(column->typid, &column->value_bytes, &column->by_value);
		column->custom_scan_attno = out_attno;
		column->uncompressed_chunk_attno = out_attno;
		column->compressed_scan_attno = attno;
		column->bulk_decompression_supported =
			!is_segmentby &&
			tsl_get_decompress_all_function(compression_get_default_algorithm(column->typid),
											column->typid) != NULL;
	}
	Assert(data_i == num_data);

	/* Metadata columns go to the back. */
	int meta_i = num_data;
	for (int attno = 1; attno <= compressed_desc->natts; attno++)
	{
		Form_pg_attribute attr = TupleDescAttr(compressed_desc, attno - 1);
		const char *name = NameStr(attr->attname);

		if (attr->attisdropped)
		{
			continue;
		}
		CompressionColumnDescription *column = &dcontext->compressed_chunk_columns[meta_i];

		if (strcmp(name, COMPRESSION_COLUMN_METADATA_COUNT_NAME) == 0)
		{
			column->type = COUNT_COLUMN;
			column->custom_scan_attno = COLUMNAR_SCAN_COUNT_ID;
			column->compressed_scan_attno = attno;
			meta_i++;
		}
		else if (strcmp(name, COMPRESSION_COLUMN_METADATA_SEQUENCE_NUM_NAME) == 0)
		{
			column->type = SEQUENCE_NUM_COLUMN;
			column->custom_scan_attno = COLUMNAR_SCAN_SEQUENCE_NUM_ID;
			column->compressed_scan_attno = attno;
			meta_i++;
		}
	}
	Assert(meta_i == num_data + num_meta);

	dcontext->custom_scan_slot = MakeSingleTupleTableSlot(uncompressed_desc, &TTSOpsVirtual);
	dcontext->uncompressed_chunk_tdesc = uncompressed_desc;
	dcontext->ps = NULL;
	dcontext->vectorized_quals_constified = NIL;
	dcontext->enable_bulk_decompression =
		ts_guc_enable_optimizations && ts_guc_enable_bulk_decompression;
	dcontext->reverse = false;
	dcontext->batch_sorted_merge = false;
	detoaster_init(&dcontext->detoaster, CurrentMemoryContext);
	return dcontext;
}

DecompressContext *
decompress_context_create_utility(Relation uncompressed_rel, Relation compressed_rel,
								  CompressionSettings *settings)
{
	return decompress_context_create_utility_desc(RelationGetDescr(uncompressed_rel),
												  RelationGetDescr(compressed_rel));
}

void
decompress_context_destroy_utility(DecompressContext *dcontext)
{
	ExecDropSingleTupleTableSlot(dcontext->custom_scan_slot);
	detoaster_close(&dcontext->detoaster);
	pfree(dcontext->compressed_chunk_columns);
	pfree(dcontext);
}

DecompressBatchState *
decompress_batch_state_create_utility(DecompressContext *dcontext)
{
	return palloc0(sizeof(DecompressBatchState) +
				   sizeof(CompressedColumnValues) * dcontext->num_data_columns);
}

void
decompress_batch_state_destroy_utility(DecompressBatchState *batch_state)
{
	compressed_batch_destroy(batch_state);
	pfree(batch_state);
}

UtilityEmitState *
utility_emit_create(Relation uncompressed_rel, Relation compressed_rel,
					CompressionSettings *settings)
{
	UtilityEmitState *state = palloc0(sizeof(UtilityEmitState));
	state->dcontext = decompress_context_create_utility(uncompressed_rel, compressed_rel, settings);
	state->batch_state = decompress_batch_state_create_utility(state->dcontext);
	state->slots = (TupleTableSlot **) palloc0(sizeof(void *) * GLOBAL_MAX_ROWS_PER_COMPRESSION);
	state->slots_capacity = GLOBAL_MAX_ROWS_PER_COMPRESSION;
	return state;
}

UtilityEmitState *
utility_emit_create_desc(TupleDesc uncompressed_desc, TupleDesc compressed_desc)
{
	UtilityEmitState *state = palloc0(sizeof(UtilityEmitState));
	state->dcontext = decompress_context_create_utility_desc(uncompressed_desc, compressed_desc);
	state->batch_state = decompress_batch_state_create_utility(state->dcontext);
	state->slots = (TupleTableSlot **) palloc0(sizeof(void *) * GLOBAL_MAX_ROWS_PER_COMPRESSION);
	state->slots_capacity = GLOBAL_MAX_ROWS_PER_COMPRESSION;
	return state;
}

void
utility_emit_destroy(UtilityEmitState *state)
{
	for (int row = 0; row < state->slots_capacity; row++)
	{
		if (state->slots[row] != NULL)
		{
			ExecDropSingleTupleTableSlot(state->slots[row]);
		}
	}
	pfree(state->slots);
	decompress_batch_state_destroy_utility(state->batch_state);
	decompress_context_destroy_utility(state->dcontext);
	pfree(state);
}

int
utility_emit_batch(UtilityEmitState *state, TupleTableSlot *compressed_slot)
{
	DecompressBatchState *batch_state = state->batch_state;
	DecompressContext *dcontext = state->dcontext;

	compressed_batch_prepare(dcontext, batch_state, compressed_slot);
	compressed_batch_decode_remaining(dcontext, batch_state, compressed_slot, AllRowsPass);

	return utility_emit_rows(state);
}

/*
 * Produce the rows of the prepared and decoded batch into state->slots and
 * return the row count. Caller must have run compressed_batch_prepare() and
 * decompressed the wanted columns. DML uses this after its own qual
 * evaluation and iterator rewind.
 */
int
utility_emit_rows(UtilityEmitState *state)
{
	DecompressBatchState *batch_state = state->batch_state;
	DecompressContext *dcontext = state->dcontext;
	const uint16 n_rows = batch_state->total_batch_rows;

	if (n_rows > state->slots_capacity)
	{
		state->slots = repalloc(state->slots, sizeof(void *) * n_rows);
		for (int row = state->slots_capacity; row < n_rows; row++)
		{
			state->slots[row] = NULL;
		}
		state->slots_capacity = n_rows;
	}

	/*
	 * Heap tuple copies live in the per-batch context and are freed by the
	 * next batch's reset, so the slots never own them (should_free = false),
	 * mirroring the RowDecompressor's decompressed_slots.
	 */
	MemoryContext old_ctx = MemoryContextSwitchTo(batch_state->per_batch_context);
	TupleTableSlot *out_slot = &batch_state->decompressed_scan_slot_data.base;
	for (uint16 row = 0; row < n_rows; row++)
	{
		if (state->slots[row] == NULL)
		{
			/*
			 * The slots must outlive the per-batch context (which the next
			 * batch's prepare resets), so allocate them in the caller's
			 * context, mirroring the RowDecompressor's slot allocation.
			 */
			MemoryContextSwitchTo(old_ctx);
			state->slots[row] =
				MakeSingleTupleTableSlot(dcontext->custom_scan_slot->tts_tupleDescriptor,
										 &TTSOpsHeapTuple);
			MemoryContextSwitchTo(batch_state->per_batch_context);
		}
		else
		{
			ExecClearTuple(state->slots[row]);
		}

		make_next_tuple(batch_state, row, dcontext->num_data_columns);
		bool should_free;
		HeapTuple tuple = ExecFetchSlotHeapTuple(out_slot, false, &should_free);
		ExecStoreHeapTuple(tuple, state->slots[row], /* should_free = */ false);
	}
	MemoryContextSwitchTo(old_ctx);

	/*
	 * Mark the virtual slot empty for the next prepare. The per-batch
	 * context is deliberately NOT reset here: the heap tuple copies in
	 * state->slots are consumed by the caller's sink after this function
	 * returns, and the next batch's compressed_batch_prepare() resets the
	 * context (mirroring row_decompressor_reset() after the write).
	 */
	ExecClearTuple(out_slot);
	return n_rows;
}
