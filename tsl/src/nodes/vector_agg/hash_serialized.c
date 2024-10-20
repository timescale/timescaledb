/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

/*
 * Implementation of column hashing for multiple serialized columns.
 */

#include <postgres.h>

#include <common/hashfn.h>

#include "bytes_view.h"
#include "compression/arrow_c_data_interface.h"
#include "grouping_policy_hash.h"
#include "nodes/decompress_chunk/compressed_batch.h"
#include "nodes/vector_agg/exec.h"

static pg_attribute_always_inline void
serialized_get_key(CompressedColumnValues column, int row, BytesView *restrict key,
				   bool *restrict valid)
{
	Assert(false);
}

static pg_attribute_always_inline BytesView
serialized_store_key(BytesView key, Datum *key_storage, MemoryContext key_memory_context)
{
	Assert(false);
	return key;
}

#define KEY_VARIANT serialized
#define KEY_HASH(X) hash_bytes_view(X)
#define KEY_EQUAL(a, b) (a.len == b.len && memcmp(a.data, b.data, a.len) == 0)
#define STORE_HASH
#define CTYPE BytesView
#include "hash_table_functions_impl.c"
