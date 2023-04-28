/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_NODES_COMMON_H
#define TIMESCALEDB_NODES_COMMON_H

#include <postgres.h>
#include "ts_catalog/hypertable_compression.h"

#define DECOMPRESS_CHUNK_COUNT_ID -9
#define DECOMPRESS_CHUNK_SEQUENCE_NUM_ID -10

typedef enum CompressedChunkColumnType
{
	SEGMENTBY_COLUMN,
	COMPRESSED_COLUMN,
	COUNT_COLUMN,
	SEQUENCE_NUM_COLUMN,
} CompressedChunkColumnType;

typedef struct ConstifyTableOidContext
{
	Index chunk_index;
	Oid chunk_relid;
	bool made_changes;
} ConstifyTableOidContext;

typedef int32 DecompressSlotNumber;

typedef struct CompressionInfo
{
	RelOptInfo *chunk_rel;
	RelOptInfo *compressed_rel;
	RangeTblEntry *chunk_rte;
	RangeTblEntry *compressed_rte;
	RangeTblEntry *ht_rte;

	int hypertable_id;
	List *hypertable_compression_info;

	int num_orderby_columns;
	int num_segmentby_columns;

	/* chunk attribute numbers that are segmentby columns */
	Bitmapset *chunk_segmentby_attnos;
	/*
	 * Chunk segmentby attribute numbers that are equated to a constant by a
	 * baserestrictinfo.
	 */
	Bitmapset *chunk_const_segmentby;
	/* compressed chunk attribute numbers for columns that are compressed */
	Bitmapset *compressed_attnos_in_compressed_chunk;

	bool single_chunk; /* query on explicit chunk */

} CompressionInfo;

FormData_hypertable_compression *get_column_compressioninfo(List *hypertable_compression_info,
															char *column_name);
List *constify_tableoid(List *node, Index chunk_index, Oid chunk_relid);

Node *constify_tableoid_walker(Node *node, ConstifyTableOidContext *ctx);

void check_for_system_columns(Bitmapset *attrs_used);

AttrNumber find_attr_pos_in_tlist(List *targetlist, AttrNumber pos);
Node *replace_compressed_vars(Node *node, CompressionInfo *info);
#endif /* TIMESCALEDB_NODES_COMMON_H */
