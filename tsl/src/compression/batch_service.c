/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>

#include <access/htup_details.h>
#include <utils/lsyscache.h>

#include "compression/batch_service.h"

/*
 * Shared owned batch decompression service (design: doc/research/012).
 * The implementation is intentionally close to the columnar scan's
 * decompress_column() and compressed_batch_set_compressed_tuple(), from
 * which the scalar, bulk, and iterator mechanics are extracted.
 */

typedef struct BatchDecompressOwner
{
	MemoryContext operation_context; /* owns everything below */
	TupleDesc source_desc;			 /* copied descriptor with defaults */
	int ncolumns;
	BatchDecompressColumnSpec *columns;
	int output_natts;
	int row_limit;
	bool bulk_enabled;
	bool reverse;
	BatchDecompressMemoryPolicy policy;
	Detoaster detoaster;
	MemoryContext bulk_scratch; /* lazily created, reset after each bulk call */
	List *batches;				/* live DecompressedBatch children */
} BatchDecompressOwner;

typedef struct DecompressedBatch
{
	BatchDecompressOwner *owner;
	MemoryContext value_context;	  /* reset at begin/discard */
	CompressedColumnValues *columns;  /* persistent wrapper array */
	Datum *retained_inputs;			  /* detoasted compressed datum per column */
	uint16 *positions;				  /* iterator position per column */
	bool *end_checks;				  /* iterator end check memoized */
	bool active;
	bool replayed;
	uint16 total_rows;
} DecompressedBatch;

static void
require(bool condition, const char *message)
{
	if (!condition)
	{
		elog(ERROR, "batch service contract violation: %s", message);
	}
}

static void
batch_scalar_column(CompressedColumnValues *column_values, Datum value, bool isnull)
{
	column_values->decompression_type = DT_Scalar;
	column_values->buffers[0] = DatumGetPointer(BoolGetDatum(isnull));
	column_values->buffers[1] = DatumGetPointer(value);

	*column_values->output_isnull = isnull;
	*column_values->output_value = value;
}

/* Mirrors the Arrow wrapping half of decompress_column(), text pre-alloc
 * through the output pointer included. */
static void
batch_fill_arrow(CompressedColumnValues *column_values, Oid typid, ArrowArray *arrow,
				 MemoryContext value_context)
{
	const int value_bytes = get_typlen(typid);
	Assert(value_bytes != 0);

	column_values->arrow = arrow;

	if (value_bytes > 0)
	{
		column_values->decompression_type = value_bytes;
		column_values->buffers[0] = arrow->buffers[0];
		column_values->buffers[1] = arrow->buffers[1];
		column_values->buffers[2] = NULL;
		column_values->buffers[3] = NULL;

		if (typid == BOOLOID)
		{
			column_values->decompression_type = DT_ArrowBits;
		}
	}
	else
	{
		const int maxbytes = get_max_varlena_bytes(arrow);
		*column_values->output_value =
			PointerGetDatum(MemoryContextAlloc(value_context, maxbytes));

		if (arrow->dictionary == NULL)
		{
			column_values->decompression_type = DT_ArrowText;
			column_values->buffers[0] = arrow->buffers[0];
			column_values->buffers[1] = arrow->buffers[1];
			column_values->buffers[2] = arrow->buffers[2];
			column_values->buffers[3] = NULL;
		}
		else
		{
			column_values->decompression_type = DT_ArrowTextDict;
			column_values->buffers[0] = arrow->buffers[0];
			column_values->buffers[1] = arrow->dictionary->buffers[1];
			column_values->buffers[2] = arrow->dictionary->buffers[2];
			column_values->buffers[3] = arrow->buffers[1];
		}
	}
}

BatchDecompressOwner *
batch_decompress_owner_create(MemoryContext parent, TupleDesc source_desc,
							  const BatchDecompressColumnSpec *specs, int ncols, int output_natts,
							  int row_limit, bool bulk_enabled, bool reverse,
							  BatchDecompressMemoryPolicy policy)
{
	MemoryContext operation_context =
		AllocSetContextCreate(parent, "batch decompression owner", ALLOCSET_DEFAULT_SIZES);
	MemoryContext old_ctx = MemoryContextSwitchTo(operation_context);

	BatchDecompressOwner *owner = palloc0(sizeof(BatchDecompressOwner));
	owner->operation_context = operation_context;
	owner->source_desc = CreateTupleDescCopyConstr(source_desc);
	owner->ncolumns = ncols;
	owner->columns = palloc0(sizeof(BatchDecompressColumnSpec) * ncols);
	owner->output_natts = output_natts;
	owner->row_limit = row_limit;
	owner->bulk_enabled = bulk_enabled;
	owner->reverse = reverse;
	owner->policy = policy;
	owner->bulk_scratch = NULL;
	owner->batches = NIL;

	for (int i = 0; i < ncols; i++)
	{
		require(specs[i].output_attno >= 1 && specs[i].output_attno <= output_natts,
				"column output attno out of range");
		require(OidIsValid(specs[i].typid), "column output type is invalid");
		owner->columns[i] = specs[i];
	}

	detoaster_init(&owner->detoaster, operation_context);
	MemoryContextSwitchTo(old_ctx);
	return owner;
}

void
batch_decompress_owner_destroy(BatchDecompressOwner *owner)
{
	if (owner == NULL)
	{
		return;
	}

	ListCell *lc;
	foreach (lc, owner->batches)
	{
		DecompressedBatch *batch = lfirst(lc);
		batch->owner = NULL;
		decompressed_batch_destroy(batch);
	}
	detoaster_close(&owner->detoaster);
	MemoryContextDelete(owner->operation_context);
}

int
batch_decompress_owner_num_columns(const BatchDecompressOwner *owner)
{
	return owner->ncolumns;
}

const BatchDecompressColumnSpec *
batch_decompress_owner_spec(const BatchDecompressOwner *owner, int column)
{
	require(column >= 0 && column < owner->ncolumns, "column index out of range");
	return &owner->columns[column];
}

const BatchDecompressColumnSet *
batch_decompress_owner_columns(BatchDecompressOwner *owner, const AttrNumber *source_attnos,
							   int nattnos)
{
	MemoryContext old_ctx = MemoryContextSwitchTo(owner->operation_context);
	BatchDecompressColumnSet *set = palloc0(sizeof(BatchDecompressColumnSet));
	int *indexes = palloc(sizeof(int) * nattnos);
	int n = 0;

	for (int i = 0; i < nattnos; i++)
	{
		int column = -1;
		for (int j = 0; j < owner->ncolumns; j++)
		{
			if (owner->columns[j].source_attno == source_attnos[i])
			{
				column = j;
				break;
			}
		}
		require(column >= 0, "column set references an attribute outside the layout");

		bool duplicate = false;
		for (int q = 0; q < n; q++)
		{
			if (indexes[q] == column)
			{
				duplicate = true;
				break;
			}
		}
		if (!duplicate)
		{
			indexes[n++] = column;
		}
	}

	set->owner = owner;
	set->ncolumns = n;
	set->indexes = indexes;
	MemoryContextSwitchTo(old_ctx);
	return set;
}

DecompressedBatch *
decompressed_batch_create(BatchDecompressOwner *owner)
{
	MemoryContext old_ctx = MemoryContextSwitchTo(owner->operation_context);
	DecompressedBatch *batch = palloc0(sizeof(DecompressedBatch));
	batch->owner = owner;
	batch->columns = palloc0(sizeof(CompressedColumnValues) * owner->ncolumns);
	batch->retained_inputs = palloc0(sizeof(Datum) * owner->ncolumns);
	batch->positions = palloc0(sizeof(uint16) * owner->ncolumns);
	batch->end_checks = palloc0(sizeof(bool) * owner->ncolumns);

	if (owner->policy == BATCH_DECOMPRESS_MEMORY_SCAN)
	{
		batch->value_context = GenerationContextCreate(owner->operation_context,
													   "DecompressedBatch values",
													   0,
													   owner->bulk_enabled ? 64 * 1024 : 8 * 1024,
													   owner->bulk_enabled ? 64 * 1024 : 8 * 1024);
	}
	else
	{
		batch->value_context = AllocSetContextCreate(owner->operation_context,
													 "DecompressedBatch values",
													 ALLOCSET_DEFAULT_SIZES);
	}

	owner->batches = lappend(owner->batches, batch);
	MemoryContextSwitchTo(old_ctx);
	return batch;
}

void
decompressed_batch_destroy(DecompressedBatch *batch)
{
	if (batch == NULL)
	{
		return;
	}

	decompressed_batch_discard(batch);
	if (batch->owner != NULL)
	{
		batch->owner->batches = list_delete_ptr(batch->owner->batches, batch);
	}
	MemoryContextDelete(batch->value_context);
	pfree(batch->columns);
	pfree(batch->retained_inputs);
	pfree(batch->positions);
	pfree(batch->end_checks);
	pfree(batch);
}

bool
decompressed_batch_active(const DecompressedBatch *batch)
{
	return batch != NULL && batch->active;
}

void
decompressed_batch_begin(DecompressedBatch *batch, int rows, bool count_isnull,
						 Datum *output_values, bool *output_isnull, int output_natts)
{
	require(!batch->active, "begin on an active batch");
	require(!count_isnull, "batch row count must not be NULL");
	require(rows > 0 && rows <= batch->owner->row_limit && batch->owner->row_limit <= UINT16_MAX,
			"batch row count out of range");
	require(output_natts == batch->owner->output_natts, "output array size mismatch");
	require(output_values != NULL && output_isnull != NULL, "output arrays are required");

	MemoryContextReset(batch->value_context);

	batch->total_rows = (uint16) rows;
	batch->replayed = false;
	for (int i = 0; i < batch->owner->ncolumns; i++)
	{
		CompressedColumnValues *column_values = &batch->columns[i];
		column_values->decompression_type = DT_Invalid;
		column_values->arrow = NULL;
		column_values->buffers[0] = NULL;
		column_values->buffers[1] = NULL;
		column_values->buffers[2] = NULL;
		column_values->buffers[3] = NULL;
		column_values->output_value = &output_values[batch->owner->columns[i].output_attno - 1];
		column_values->output_isnull = &output_isnull[batch->owner->columns[i].output_attno - 1];
		*column_values->output_isnull = true;
		batch->retained_inputs[i] = (Datum) 0;
		batch->positions[i] = 0;
		batch->end_checks[i] = false;
	}
	batch->active = true;
}

BatchDecompressLoadMetrics
decompressed_batch_load(DecompressedBatch *batch, int column, Datum input, bool input_isnull)
{
	BatchDecompressLoadMetrics metrics = {
		.new_compressed_column = false,
		.compressed_bytes = 0,
	};
	BatchDecompressOwner *owner = batch->owner;
	const BatchDecompressColumnSpec *spec = &owner->columns[column];
	CompressedColumnValues *column_values = &batch->columns[column];

	require(batch->active, "load on an idle batch");
	require(column >= 0 && column < owner->ncolumns, "column index out of range");

	/* Lazy: decompress each column at most once per batch. */
	if (column_values->decompression_type != DT_Invalid)
	{
		return metrics;
	}

	if (spec->segmentby)
	{
		/*
		 * Segmentby columns are constant for the batch. Copy by-reference
		 * values into the value context, mirroring the scan's setup.
		 */
		Datum value = input;
		if (!input_isnull && DatumGetPointer(value) != NULL)
		{
			int16 typlen;
			bool typbyval;
			get_typlenbyval(spec->typid, &typlen, &typbyval);

			if (!typbyval)
			{
				if (typlen < 0)
				{
					value = PointerGetDatum(detoaster_detoast_attr_copy((struct varlena *) value,
																		&owner->detoaster,
																		batch->value_context));
				}
				else
				{
					void *tmp = MemoryContextAlloc(batch->value_context, typlen);
					memcpy(tmp, DatumGetPointer(value), typlen);
					value = PointerGetDatum(tmp);
				}
			}
		}
		batch_scalar_column(column_values, value, input_isnull);
		return metrics;
	}

	if (input_isnull)
	{
		bool isnull;
		Datum defval = getmissingattr(owner->source_desc, spec->source_attno, &isnull);
		batch_scalar_column(column_values, defval, isnull);
		return metrics;
	}

	/* Detoast the compressed datum into the value context and retain it. */
	Datum value = PointerGetDatum(detoaster_detoast_attr_copy((struct varlena *) DatumGetPointer(input),
															  &owner->detoaster,
															  batch->value_context));
	batch->retained_inputs[column] = value;

	CompressedDataHeader *header = (CompressedDataHeader *) value;
	if (header->compression_algorithm == COMPRESSION_ALGORITHM_NULL)
	{
		batch_scalar_column(column_values, (Datum) 0, true);
		return metrics;
	}

	metrics.new_compressed_column = true;
	metrics.compressed_bytes = VARSIZE_ANY_EXHDR(DatumGetPointer(value));

	ArrowArray *arrow = NULL;
	if (owner->bulk_enabled && spec->bulk_supported)
	{
		if (owner->bulk_scratch == NULL)
		{
			owner->bulk_scratch =
				create_bulk_decompression_mctx(owner->operation_context);
		}

		DecompressAllFunction decompress_all =
			tsl_get_decompress_all_function(header->compression_algorithm, spec->typid);
		Assert(decompress_all != NULL);

		MemoryContext save = MemoryContextSwitchTo(owner->bulk_scratch);
		arrow = decompress_all(PointerGetDatum(header), spec->typid, batch->value_context);
		MemoryContextSwitchTo(save);
		MemoryContextReset(owner->bulk_scratch);
	}

	if (arrow == NULL)
	{
		MemoryContext save = MemoryContextSwitchTo(batch->value_context);
		column_values->decompression_type = DT_Iterator;
		column_values->buffers[0] =
			tsl_get_decompression_iterator_init(header->compression_algorithm,
												owner->reverse)(PointerGetDatum(header),
																spec->typid);
		MemoryContextSwitchTo(save);
		return metrics;
	}

	Assert(batch->total_rows != 0);
	if (batch->total_rows != arrow->length)
	{
		elog(ERROR, "compressed column out of sync with batch counter");
	}

	batch_fill_arrow(column_values, spec->typid, arrow, batch->value_context);
	return metrics;
}

const CompressedColumnValues *
decompressed_batch_columns(const DecompressedBatch *batch)
{
	return batch->columns;
}

MemoryContext
decompressed_batch_context(const DecompressedBatch *batch)
{
	return batch->value_context;
}

int
decompressed_batch_rows(const DecompressedBatch *batch)
{
	return batch->total_rows;
}

static void
batch_column_set_foreach(DecompressedBatch *batch, const BatchDecompressColumnSet *set,
						 void (*callback)(DecompressedBatch *, int, void *), void *arg)
{
	require(!batch->active || set == NULL || set->owner == batch->owner,
			"column set belongs to another owner");

	if (set == NULL)
	{
		for (int i = 0; i < batch->owner->ncolumns; i++)
		{
			callback(batch, i, arg);
		}
		return;
	}

	for (int q = 0; q < set->ncolumns; q++)
	{
		callback(batch, set->indexes[q], arg);
	}
}

static void
require_loaded(const DecompressedBatch *batch, int column)
{
	require(batch->active, "row access on an idle batch");
	require(batch->columns[column].decompression_type != DT_Invalid,
			"row access before the column was loaded");
}

static void
read_column(DecompressedBatch *batch, int column, void *arg)
{
	const int arrow_row = *(const int *) arg;
	CompressedColumnValues *column_values = &batch->columns[column];

	require_loaded(batch, column);

	if (column_values->decompression_type == DT_Iterator)
	{
		const int expected = batch->owner->reverse ?
			batch->total_rows - 1 - batch->positions[column] :
			batch->positions[column];
		require(arrow_row == expected, "iterator read out of order");

		compressed_columns_to_postgres_data(column_values, 1, arrow_row);
		batch->positions[column]++;
		return;
	}

	compressed_columns_to_postgres_data(column_values, 1, arrow_row);
}

void
decompressed_batch_read(DecompressedBatch *batch, const BatchDecompressColumnSet *set,
						int arrow_row)
{
	require(batch->active, "read on an idle batch");
	require(arrow_row >= 0 && arrow_row < batch->total_rows, "read row out of range");

	MemoryContext old_ctx = MemoryContextSwitchTo(batch->value_context);
	batch_column_set_foreach(batch, set, read_column, &arrow_row);
	MemoryContextSwitchTo(old_ctx);
}

static void
skip_column(DecompressedBatch *batch, int column, void *arg)
{
	CompressedColumnValues *column_values = &batch->columns[column];

	require_loaded(batch, column);

	if (column_values->decompression_type == DT_Iterator)
	{
		DecompressionIterator *iterator = (DecompressionIterator *) column_values->buffers[0];
		DecompressResult result = iterator->try_next(iterator);
		require(!result.is_done, "iterator ended before the batch row count");
		batch->positions[column]++;
	}
}

void
decompressed_batch_skip(DecompressedBatch *batch, const BatchDecompressColumnSet *set)
{
	require(batch->active, "skip on an idle batch");

	MemoryContext old_ctx = MemoryContextSwitchTo(batch->value_context);
	batch_column_set_foreach(batch, set, skip_column, NULL);
	MemoryContextSwitchTo(old_ctx);
}

static void
finish_column(DecompressedBatch *batch, int column, void *arg)
{
	CompressedColumnValues *column_values = &batch->columns[column];

	require_loaded(batch, column);

	if (column_values->decompression_type == DT_Iterator && !batch->end_checks[column])
	{
		require(batch->positions[column] == batch->total_rows,
				"finish before the iterator produced the whole batch");
		DecompressionIterator *iterator = (DecompressionIterator *) column_values->buffers[0];
		DecompressResult result = iterator->try_next(iterator);
		require(result.is_done, "iterator did not end after the batch row count");
		batch->end_checks[column] = true;
	}
}

void
decompressed_batch_finish(DecompressedBatch *batch, const BatchDecompressColumnSet *set)
{
	require(batch->active, "finish on an idle batch");

	MemoryContext old_ctx = MemoryContextSwitchTo(batch->value_context);
	batch_column_set_foreach(batch, set, finish_column, NULL);
	MemoryContextSwitchTo(old_ctx);
}

void
decompressed_batch_rewind_consumed(DecompressedBatch *batch)
{
	require(batch->active, "rewind on an idle batch");

	BatchDecompressOwner *owner = batch->owner;
	bool any = false;
	for (int i = 0; i < owner->ncolumns; i++)
	{
		CompressedColumnValues *column_values = &batch->columns[i];
		if (column_values->decompression_type == DT_Iterator && batch->positions[i] > 0)
		{
			any = true;
			break;
		}
	}

	if (!any)
	{
		return;
	}

	require(!batch->replayed, "iterator replay requested twice for one batch");

	MemoryContext old_ctx = MemoryContextSwitchTo(batch->value_context);
	for (int i = 0; i < owner->ncolumns; i++)
	{
		CompressedColumnValues *column_values = &batch->columns[i];
		if (column_values->decompression_type == DT_Iterator && batch->positions[i] > 0)
		{
			require(batch->retained_inputs[i] != (Datum) 0,
					"cannot rewind an iterator without the retained input");
			CompressedDataHeader *header =
				(CompressedDataHeader *) DatumGetPointer(batch->retained_inputs[i]);
			column_values->buffers[0] =
				tsl_get_decompression_iterator_init(header->compression_algorithm,
													owner->reverse)(batch->retained_inputs[i],
																	owner->columns[i].typid);
			batch->positions[i] = 0;
			batch->end_checks[i] = false;
		}
	}
	MemoryContextSwitchTo(old_ctx);
	batch->replayed = true;
}

void
decompressed_batch_discard(DecompressedBatch *batch)
{
	if (batch == NULL || !batch->active)
	{
		return;
	}

	for (int i = 0; i < batch->owner->ncolumns; i++)
	{
		CompressedColumnValues *column_values = &batch->columns[i];
		column_values->decompression_type = DT_Invalid;
		column_values->arrow = NULL;
		column_values->buffers[0] = NULL;
		column_values->buffers[1] = NULL;
		column_values->buffers[2] = NULL;
		column_values->buffers[3] = NULL;
		batch->retained_inputs[i] = (Datum) 0;
		batch->positions[i] = 0;
		batch->end_checks[i] = false;
	}

	MemoryContextReset(batch->value_context);
	batch->active = false;
	batch->replayed = false;
}
