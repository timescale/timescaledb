/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

/*
 * Implementation of column hashing for a single text column.
 */

#include <postgres.h>

#include <common/hashfn.h>

#include "compression/arrow_c_data_interface.h"
#include "grouping_policy_hash.h"
#include "hash64.h"
#include "nodes/decompress_chunk/compressed_batch.h"
#include "nodes/vector_agg/exec.h"

typedef struct TextView
{
	uint32 len;
	const uint8 *data;
} TextView;

static TextView
get_text_view(CompressedColumnValues *column_values, int arrow_row)
{
	const uint32 start = ((uint32 *) column_values->buffers[1])[arrow_row];
	const int32 value_bytes = ((uint32 *) column_values->buffers[1])[arrow_row + 1] - start;
	Assert(value_bytes >= 0);

	return (TextView){ .len = value_bytes, .data = &((uint8 *) column_values->buffers[2])[start] };
}

static pg_attribute_always_inline void
single_text_get_key(CompressedColumnValues column, int row, TextView *restrict key,
					bool *restrict valid)
{
	if (unlikely(column.decompression_type == DT_Scalar))
	{
		/* Already stored. */
		key->len = VARSIZE_ANY_EXHDR(*column.output_value);
		key->data = (const uint8 *) VARDATA_ANY(*column.output_value);
		*valid = !*column.output_isnull;
	}
	else if (column.decompression_type == DT_ArrowText)
	{
		*key = get_text_view(&column, row);
		*valid = arrow_row_is_valid(column.buffers[0], row);
	}
	else if (column.decompression_type == DT_ArrowTextDict)
	{
		const int16 index = ((int16 *) column.buffers[3])[row];
		*key = get_text_view(&column, index);
		*valid = arrow_row_is_valid(column.buffers[0], row);
	}
	else
	{
		pg_unreachable();
	}
}

static pg_attribute_always_inline TextView
single_text_store_key(TextView key, Datum *key_storage, MemoryContext key_memory_context)
{
	const int total_bytes = key.len + VARHDRSZ;
	text *stored = (text *) MemoryContextAlloc(key_memory_context, total_bytes);
	SET_VARSIZE(stored, total_bytes);
	memcpy(VARDATA(stored), key.data, key.len);
	key.data = (uint8 *) VARDATA(stored);
	*key_storage = PointerGetDatum(stored);
	return key;
}

#define KEY_VARIANT single_text
#define KEY_HASH(X) hash_bytes(X.data, X.len)
#define KEY_EQUAL(a, b) (a.len == b.len && memcmp(a.data, b.data, a.len) == 0)
#define SH_STORE_HASH
#define CTYPE TextView
#include "hash_table_functions_impl.c"
