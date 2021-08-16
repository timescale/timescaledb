/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_COMPRESSION_CHUNK_SIZE_H
#define TIMESCALEDB_COMPRESSION_CHUNK_SIZE_H
#include <postgres.h>
#include <compat/compat.h>

extern TSDLLEXPORT int ts_compression_chunk_size_delete(int32 uncompressed_chunk_id);

typedef struct TotalSizes
{
	int64 uncompressed_heap_size;
	int64 uncompressed_toast_size;
	int64 uncompressed_index_size;
	int64 compressed_heap_size;
	int64 compressed_toast_size;
	int64 compressed_index_size;
} TotalSizes;

extern TSDLLEXPORT TotalSizes ts_compression_chunk_size_totals(void);
extern TSDLLEXPORT int64 ts_compression_chunk_size_row_count(int32 uncompressed_chunk_id);

#endif
