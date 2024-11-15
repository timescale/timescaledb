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

#define EXPLAIN_NAME "single 4-byte"
#define KEY_VARIANT single_fixed_4
#define KEY_BYTES 4
#define FULL_KEY_TYPE int32
#define ABBREV_KEY_TYPE int32
#define DATUM_TO_FULL_KEY DatumGetInt32
#define FULL_KEY_TO_DATUM Int32GetDatum

#define ABBREVIATE(X) (X)
#define KEY_HASH(X) HASH64(X)

#include "single_fixed_key_impl.c"

#define KEY_EQUAL(a, b) a == b

#include "hash_table_functions_impl.c"
