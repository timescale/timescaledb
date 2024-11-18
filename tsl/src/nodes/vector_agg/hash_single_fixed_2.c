/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

/*
 * Implementation of column hashing for a single fixed size 2-byte column.
 */

#include <postgres.h>

#include "compression/arrow_c_data_interface.h"
#include "grouping_policy_hash.h"
#include "hash64.h"
#include "nodes/decompress_chunk/compressed_batch.h"
#include "nodes/vector_agg/exec.h"

#define EXPLAIN_NAME "single 2-byte"
#define KEY_VARIANT single_fixed_2
#define KEY_BYTES 2
#define OUTPUT_KEY_TYPE int16
#define HASH_TABLE_KEY_TYPE OUTPUT_KEY_TYPE
#define DATUM_TO_output_key DatumGetInt16
#define output_key_TO_DATUM Int16GetDatum

#define ABBREVIATE(X) (X)
#define KEY_HASH(X) HASH64(X)

#include "single_fixed_key_impl.c"

#define KEY_EQUAL(a, b) a == b
#include "hash_table_functions_impl.c"
