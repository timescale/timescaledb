/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

#include <postgres.h>
#include <nodes/parsenodes.h>

#include "hypertable.h"
#include "with_clause/with_clause_parser.h"

#define COMPRESSION_COLUMN_METADATA_PREFIX "_ts_meta_"
#define COMPRESSION_COLUMN_METADATA_COUNT_NAME COMPRESSION_COLUMN_METADATA_PREFIX "count"
#define COMPRESSION_COLUMN_METADATA_SEQUENCE_NUM_NAME                                              \
	COMPRESSION_COLUMN_METADATA_PREFIX "sequence_num"

#define COMPRESSION_COLUMN_METADATA_PATTERN_V1 "_ts_meta_%s_%d"

typedef struct CompressionSettings CompressionSettings;

bool tsl_process_compress_table(Hypertable *ht, WithClauseResult *with_clause_options);
void tsl_process_compress_table_add_column(Hypertable *ht, ColumnDef *orig_def);
void tsl_process_compress_table_drop_column(Hypertable *ht, char *name);
void tsl_process_compress_table_rename_column(Hypertable *ht, const RenameStmt *stmt);
Chunk *create_compress_chunk(Hypertable *compress_ht, Chunk *src_chunk, Oid table_id,
							 bool skip_segmentby_default, CompressionSettings *settings);

char *column_segment_min_name(int16 column_index);
char *column_segment_max_name(int16 column_index);
char *compressed_column_metadata_name_v2(const char *metadata_type, const char **column_names,
										 int num_columns);
char *compressed_column_metadata_name_list_v2(const char *metadata_type, List *column_names_list);

int compressed_column_metadata_attno(const CompressionSettings *settings, Oid chunk_reloid,
									 AttrNumber chunk_attno, Oid compressed_reloid,
									 char const *metadata_type);

/*
 * Per-orderby-column sparse-index metadata dispatch.
 *
 * Each orderby has a pair of metadata columns that bound the orderby values
 * in each batch. Two shapes are supported:
 *
 *   minmax    - statistical extremes; direction-blind (lower=min, upper=max).
 *   firstlast - first/last row values in sort order. Under ASC, lower=first
 *               and upper=last; under DESC, the roles flip.
 *
 * Callers should ask for "lower"/"upper" boundaries instead of min/max
 * directly so the same code works across both shapes. orderby_pos is 1-based
 * to match settings->fd.orderby and the column_segment_*_name helpers.
 */
typedef enum OrderbySparseKind
{
	ORDERBY_SPARSE_MINMAX,
	ORDERBY_SPARSE_FIRSTLAST,
} OrderbySparseKind;

OrderbySparseKind orderby_sparse_kind(const CompressionSettings *settings, int orderby_pos);
void orderby_sparse_metadata_names(const CompressionSettings *settings, int orderby_pos,
								   char **lower_name, char **upper_name);
void orderby_sparse_metadata_attnos(const CompressionSettings *settings, Oid compressed_relid,
									int orderby_pos, AttrNumber *lower_attno,
									AttrNumber *upper_attno);

void tsl_columnstore_setup(Hypertable *ht, WithClauseResult *with_clause_options);

void compression_settings_set_defaults(Hypertable *ht, CompressionSettings *settings,
									   WithClauseResult *with_clause_options,
									   bool skip_segmentby_default);
ArrayType *tsl_compression_setting_segmentby_get_default(const Hypertable *ht);
