/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_CHUNK_INDEX_H
#define TIMESCALEDB_CHUNK_INDEX_H

#include <postgres.h>
#include <nodes/execnodes.h>
#include <nodes/parsenodes.h>
#include <fmgr.h>
#include <utils/relcache.h>

#include <compat.h>
#include <export.h>

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

extern List *ts_get_expr_index_attnames(IndexInfo *ii, Relation htrel);
void ts_adjust_indexinfo_attnos(IndexInfo *indexinfo, Oid ht_relid, Relation template_indexrel,
								Relation chunkrel);
extern void ts_chunk_index_create_from_adjusted_index_info(int32 hypertable_id,
														   Relation hypertable_idxrel,
														   int32 chunk_id, Relation chunkrel,
														   IndexInfo *indexinfo);
extern TSDLLEXPORT void ts_chunk_index_create_all(int32 hypertable_id, Oid hypertable_relid,
												  int32 chunk_id, Oid chunkrelid);
extern Oid ts_chunk_index_create_from_stmt(IndexStmt *stmt, int32 chunk_id, Oid chunkrelid,
										   int32 hypertable_id, Oid hypertable_indexrelid);
extern int ts_chunk_index_delete(Chunk *chunk, Oid chunk_indexrelid, bool drop_index);
extern int ts_chunk_index_delete_by_chunk_id(int32 chunk_id, bool drop_index);
extern void ts_chunk_index_delete_by_name(const char *schema, const char *index_name,
										  bool drop_index);
extern int ts_chunk_index_rename(Chunk *chunk, Oid chunk_indexrelid, const char *newname);
extern int ts_chunk_index_rename_parent(Hypertable *ht, Oid hypertable_indexrelid,
										const char *newname);
extern int ts_chunk_index_set_tablespace(Hypertable *ht, Oid hypertable_indexrelid,
										 const char *tablespace);
extern void ts_chunk_index_create_from_constraint(int32 hypertable_id, Oid hypertable_constraint,
												  int32 chunk_id, Oid chunk_constraint);
extern List *ts_chunk_index_get_mappings(Hypertable *ht, Oid hypertable_indexrelid);
extern TSDLLEXPORT bool ts_chunk_index_get_by_hypertable_indexrelid(Chunk *chunk,
																	Oid hypertable_indexrelid,
																	ChunkIndexMapping *cim_out);
extern TSDLLEXPORT bool ts_chunk_index_get_by_indexrelid(Chunk *chunk, Oid chunk_indexrelid,
														 ChunkIndexMapping *cim_out);

extern TSDLLEXPORT void ts_chunk_index_mark_clustered(Oid chunkrelid, Oid indexrelid);

extern TSDLLEXPORT List *ts_chunk_index_duplicate(Oid src_chunkrelid, Oid dest_chunkrelid,
												  List **src_index_oids, Oid index_tablespace);

/* chunk_index_recreate  is a process akin to reindex
 * except that indexes are created in 2 steps
 *	 1) (create) CREATE INDEX to make new index
 *	 2) (rename) DROP INDEX old index. rename NEW INDEX to OLD INDEX
 *
 * chunk_index_recreate is used instead of REINDEX to avoid locking reads.
 * Namely, reindex actually locks the index so a query that may potentially
 * use the index is blocked on read. In contrast CREATE INDEX does not block reads.
 *
 * The process is split up into phase 1 and 2 because phase 1 does not lock reads and is slow but
 * phase 2 takes read locks but is quick. So if processing multiple tables you first want to
 * process all tables in phase 1 to completion and then run phase 2 on all tables.
 *
 * Note that both reindex and recreate both block writes to table. Also note that recreate
 * will use more disk space than reindex during phase 1 and does more total work.
 */
TSDLLEXPORT Datum ts_chunk_index_clone(PG_FUNCTION_ARGS);
TSDLLEXPORT Datum ts_chunk_index_replace(PG_FUNCTION_ARGS);

extern Oid ts_chunk_index_get_tablespace(int32 hypertable_id, Relation template_indexrel,
										 Relation chunkrel);

static inline bool
chunk_index_columns_changed(int hypertable_natts, bool hypertable_has_oid, TupleDesc chunkdesc)
{
	/*
	 * We should be able to safely assume that the only reason the number of
	 * attributes differ is because we have removed columns in the base table,
	 * leaving junk attributes that aren't inherited by the chunk.
	 */
	return !(hypertable_natts == chunkdesc->natts &&
			 hypertable_has_oid == TupleDescHasOids(chunkdesc));
}

static inline bool
chunk_index_need_attnos_adjustment(TupleDesc htdesc, TupleDesc chunkdesc)
{
	return chunk_index_columns_changed(htdesc->natts, TupleDescHasOids(htdesc), chunkdesc);
}

#endif /* TIMESCALEDB_CHUNK_INDEX_H */
