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
#define COMPRESSION_COLUMN_METADATA_MIN_COLUMN_NAME "min"
#define COMPRESSION_COLUMN_METADATA_MAX_COLUMN_NAME "max"

bool tsl_process_compress_table(AlterTableCmd *cmd, Hypertable *ht,
								WithClauseResult *with_clause_options);
void tsl_process_compress_table_add_column(Hypertable *ht, ColumnDef *orig_def);
void tsl_process_compress_table_drop_column(Hypertable *ht, char *name);
void tsl_process_compress_table_rename_column(Hypertable *ht, const RenameStmt *stmt);
Chunk *create_compress_chunk(Hypertable *compress_ht, Chunk *src_chunk, Oid table_id);

char *compression_column_segment_min_name(const FormData_hypertable_compression *fd);
char *compression_column_segment_max_name(const FormData_hypertable_compression *fd);

char *column_segment_min_name(int16 column_index);
char *column_segment_max_name(int16 column_index);

#endif /* TIMESCALEDB_TSL_COMPRESSION_CREATE_H */
