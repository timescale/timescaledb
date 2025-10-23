/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#pragma once

#include <postgres.h>
#include <fmgr.h>
#include <nodes/execnodes.h>
#include <nodes/parsenodes.h>
#include <utils/relcache.h>

#include "compat/compat.h"
#include "export.h"

typedef struct Chunk Chunk;
typedef struct Hypertable Hypertable;

typedef struct ChunkIndexMapping
{
	Oid chunkoid;
	Oid parent_indexoid;
	Oid indexoid;
	Oid hypertableoid;
} ChunkIndexMapping;

extern void ts_chunk_index_create(Relation hypertable_rel, int32 hypertable_id,
								  Relation hypertable_idxrel, int32 chunk_id, Relation chunkrel);

void ts_adjust_indexinfo_attnos(IndexInfo *indexinfo, Oid ht_relid, Relation chunkrel);
extern void ts_chunk_index_create_from_adjusted_index_info(int32 hypertable_id,
														   Relation hypertable_idxrel,
														   int32 chunk_id, Relation chunkrel,
														   IndexInfo *indexinfo);
extern TSDLLEXPORT void ts_chunk_index_create_all(int32 hypertable_id, Oid hypertable_relid,
												  int32 chunk_id, Oid chunkrelid, Oid index_tblspc);
extern TSDLLEXPORT void ts_chunk_index_move_all(Oid chunk_relid, Oid index_tblspc);
extern void ts_chunk_index_rename(Hypertable *ht, Oid hypertable_indexrelid, const char *ht_name);
extern void ts_chunk_index_set_tablespace(Hypertable *ht, Oid hypertable_indexrelid,
										  char *tablespace);
extern List *ts_chunk_index_get_mappings(Hypertable *ht, Oid hypertable_indexrelid);
extern TSDLLEXPORT Oid ts_chunk_index_get_by_hypertable_indexrelid(Relation chunk_rel,
																   Oid ht_indexoid);

extern TSDLLEXPORT void ts_chunk_index_mark_clustered(Oid chunkrelid, Oid indexrelid);

extern TSDLLEXPORT List *ts_chunk_index_duplicate(Oid src_chunkrelid, Oid dest_chunkrelid,
												  List **src_index_oids, Oid index_tablespace);

extern Oid ts_chunk_index_get_tablespace(int32 hypertable_id, Relation template_indexrel,
										 Relation chunkrel);

static inline bool
chunk_index_columns_changed(int hypertable_natts, TupleDesc chunkdesc)
{
	/*
	 * We should be able to safely assume that the only reason the number of
	 * attributes differ is because we have removed columns in the base table,
	 * leaving junk attributes that aren't inherited by the chunk.
	 */
	return hypertable_natts != chunkdesc->natts;
}

static inline bool
chunk_index_need_attnos_adjustment(TupleDesc htdesc, TupleDesc chunkdesc)
{
	return chunk_index_columns_changed(htdesc->natts, chunkdesc);
}
