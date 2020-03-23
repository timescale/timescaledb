/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_TSL_COMPRESSION_CREATE_H
#define TIMESCALEDB_TSL_COMPRESSION_CREATE_H
#include <postgres.h>
#include <nodes/parsenodes.h>

#include "with_clause_parser.h"
#include "hypertable.h"

#define COMPRESSION_COLUMN_METADATA_PREFIX "_ts_meta_"
#define COMPRESSION_COLUMN_METADATA_COUNT_NAME COMPRESSION_COLUMN_METADATA_PREFIX "count"
#define COMPRESSION_COLUMN_METADATA_SEQUENCE_NUM_NAME                                              \
	COMPRESSION_COLUMN_METADATA_PREFIX "sequence_num"

bool tsl_process_compress_table(AlterTableCmd *cmd, Hypertable *ht,
								WithClauseResult *with_clause_options);
Chunk *create_compress_chunk_table(Hypertable *compress_ht, Chunk *src_chunk);

char *compression_column_segment_min_name(const FormData_hypertable_compression *fd);
char *compression_column_segment_max_name(const FormData_hypertable_compression *fd);

#endif /* TIMESCALEDB_TSL_COMPRESSION_CREATE_H */
