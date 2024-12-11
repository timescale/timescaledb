/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#pragma once

#include <postgres.h>

#include "chunk.h"
#include "export.h"
#include "hypertable_restrict_info.h"

/*
 * A rangespace tracks all columns that need min/max value ranges calculation
 * for chunks from a given hypertable
 */
typedef struct ChunkRangeSpace
{
	int32 hypertable_id;
	uint16 capacity;
	uint16 num_range_cols;
	FormData_chunk_column_stats range_cols[FLEXIBLE_ARRAY_MEMBER];
} ChunkRangeSpace;

#define CHUNKRANGESPACE_SIZE(num_columns)                                                          \
	(sizeof(ChunkRangeSpace) + (sizeof(NameData) * (num_columns)))

extern ChunkRangeSpace *ts_chunk_column_stats_range_space_scan(int32 hypertable_id, Oid ht_reloid,
															   MemoryContext mctx);

extern int ts_chunk_column_stats_update_by_id(int32 chunk_column_stats_id,
											  FormData_chunk_column_stats *fd_range);

extern Form_chunk_column_stats ts_chunk_column_stats_lookup(int32 hypertable_id, int32 chunk_id,
															const char *col_name);

extern TSDLLEXPORT int ts_chunk_column_stats_calculate(const Hypertable *ht, const Chunk *chunk);
extern int ts_chunk_column_stats_insert(const Hypertable *ht, const Chunk *chunk);

extern void ts_chunk_column_stats_drop(const Hypertable *ht, const char *col_name, bool *dropped);
extern int ts_chunk_column_stats_delete_by_ht_colname(int32 hypertable_id, const char *col_name);
extern TSDLLEXPORT int ts_chunk_column_stats_delete_by_chunk_id(int32 chunk_id);
extern TSDLLEXPORT int ts_chunk_column_stats_reset_by_chunk_id(int32 chunk_id);
extern int ts_chunk_column_stats_delete_by_hypertable_id(int32 hypertable_id);
extern Dimension *ts_chunk_column_stats_fill_dummy_dimension(FormData_chunk_column_stats *r,
															 Oid main_table_relid);
extern List *ts_chunk_column_stats_get_chunk_ids_by_scan(DimensionRestrictInfo *dri);
extern void ts_chunk_column_stats_set_invalid(int32 hypertable_id, int32 chunk_id);
extern int ts_chunk_column_stats_set_name(FormData_chunk_column_stats *in_fd, char *new_colname);
extern List *ts_chunk_column_stats_construct_check_constraints(Relation relation, Oid reloid,
															   Index varno);
