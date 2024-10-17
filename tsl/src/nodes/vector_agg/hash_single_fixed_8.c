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
#include "grouping_policy_hash.h"
#include "hash64.h"
#include "nodes/decompress_chunk/compressed_batch.h"
#include "nodes/vector_agg/exec.h"

#define KEY_VARIANT single_fixed_8
#define KEY_BYTES 8
#define KEY_HASH hash64
#define KEY_EQUAL(a, b) a == b
#define CTYPE int64
#define DATUM_TO_CTYPE DatumGetInt64
#define CTYPE_TO_DATUM Int64GetDatum

#include "single_fixed_key_impl.c"

#include "hash_table_functions_impl.c"
