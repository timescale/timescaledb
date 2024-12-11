/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

#include <postgres.h>
#include <nodes/parsenodes.h>

#include "hypertable.h"
#include "with_clause_parser.h"

#define COMPRESSION_COLUMN_METADATA_PREFIX "_ts_meta_"
#define COMPRESSION_COLUMN_METADATA_COUNT_NAME COMPRESSION_COLUMN_METADATA_PREFIX "count"
#define COMPRESSION_COLUMN_METADATA_SEQUENCE_NUM_NAME                                              \
	COMPRESSION_COLUMN_METADATA_PREFIX "sequence_num"

#define COMPRESSION_COLUMN_METADATA_PATTERN_V1 "_ts_meta_%s_%d"

bool tsl_process_compress_table(AlterTableCmd *cmd, Hypertable *ht,
								WithClauseResult *with_clause_options);
void tsl_process_compress_table_add_column(Hypertable *ht, ColumnDef *orig_def);
void tsl_process_compress_table_drop_column(Hypertable *ht, char *name);
void tsl_process_compress_table_rename_column(Hypertable *ht, const RenameStmt *stmt);
Chunk *create_compress_chunk(Hypertable *compress_ht, Chunk *src_chunk, Oid table_id);

char *column_segment_min_name(int16 column_index);
char *column_segment_max_name(int16 column_index);
char *compressed_column_metadata_name_v2(const char *metadata_type, const char *column_name);

typedef struct CompressionSettings CompressionSettings;
int compressed_column_metadata_attno(CompressionSettings *settings, Oid chunk_reloid,
									 AttrNumber chunk_attno, Oid compressed_reloid,
									 char *metadata_type);
