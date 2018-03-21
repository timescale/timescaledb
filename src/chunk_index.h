#ifndef TIMESCALEDB_CHUNK_INDEX_H
#define TIMESCALEDB_CHUNK_INDEX_H

#include <postgres.h>
#include <nodes/parsenodes.h>
#include <fmgr.h>

typedef struct Chunk Chunk;
typedef struct Hypertable Hypertable;

typedef struct ChunkIndexMapping
{
	Oid			chunkoid;
	Oid			parent_indexoid;
	Oid			indexoid;
	Oid			hypertableoid;
} ChunkIndexMapping;

extern void chunk_index_create_all(int32 hypertable_id, Oid hypertable_relid, int32 chunk_id, Oid chunkrelid);
extern Oid	chunk_index_create_from_stmt(IndexStmt *stmt, int32 chunk_id, Oid chunkrelid, int32 hypertable_id, Oid hypertable_indexrelid);
extern int	chunk_index_delete_children_of(Hypertable *ht, Oid hypertable_indexrelid, bool should_drop);
extern int	chunk_index_delete(Chunk *chunk, Oid chunk_indexrelid, bool drop_index);
extern int	chunk_index_delete_by_chunk_id(int32 chunk_id, bool drop_index);
extern int	chunk_index_delete_by_hypertable_id(int32 hypertable_id, bool drop_index);
extern void chunk_index_delete_by_name(const char *schema, const char *index_name, bool drop_index);
extern int	chunk_index_rename(Chunk *chunk, Oid chunk_indexrelid, const char *newname);
extern int	chunk_index_rename_parent(Hypertable *ht, Oid hypertable_indexrelid, const char *newname);
extern int	chunk_index_set_tablespace(Hypertable *ht, Oid hypertable_indexrelid, const char *tablespace);
extern void chunk_index_create_from_constraint(int32 hypertable_id, Oid hypertable_constaint, int32 chunk_id, Oid chunk_constraint);
extern List *chunk_index_get_mappings(Hypertable *ht, Oid hypertable_indexrelid);
extern ChunkIndexMapping *chunk_index_get_by_hypertable_indexrelid(Chunk *chunk, Oid hypertable_indexrelid);
extern void chunk_index_mark_clustered(Oid chunkrelid, Oid indexrelid);

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
PGDLLEXPORT Datum chunk_index_clone(PG_FUNCTION_ARGS);
PGDLLEXPORT Datum chunk_index_replace(PG_FUNCTION_ARGS);

#endif							/* TIMESCALEDB_CHUNK_INDEX_H */
