/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

/*
 * Implementation of column hashing for a single fixed size 8-byte column.
 */

#include <postgres.h>

#include "compression/arrow_c_data_interface.h"
#include "hash64.h"
#include "nodes/decompress_chunk/compressed_batch.h"
#include "nodes/vector_agg/exec.h"
#include "nodes/vector_agg/grouping_policy_hash.h"
#include "template_helper.h"

#define EXPLAIN_NAME "single 8-byte"
#define KEY_VARIANT single_fixed_8
#define OUTPUT_KEY_TYPE int64
#define HASH_TABLE_KEY_TYPE int64
#define DATUM_TO_OUTPUT_KEY DatumGetInt64
#define OUTPUT_KEY_TO_DATUM Int64GetDatum

#include "hash_strategy_impl_single_fixed_key.c"

#define KEY_EQUAL(a, b) a == b
#define KEY_HASH(X) HASH64(X)

#include "hash_strategy_impl.c"
