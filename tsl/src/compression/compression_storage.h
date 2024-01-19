/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

/*
 * This file contains functions for manipulating compression related
 * internal storage objects like creating the underlying tables and
 * setting storage options.
 */

#include <postgres.h>

#include "chunk.h"
#include "hypertable.h"

int32 compression_hypertable_create(Hypertable *ht, Oid owner, Oid tablespace_oid);
Oid compression_chunk_create(Chunk *src_chunk, Chunk *chunk, List *column_defs, Oid tablespace_oid);
void modify_compressed_toast_table_storage(CompressionSettings *settings, List *coldefs,
										   Oid compress_relid);
