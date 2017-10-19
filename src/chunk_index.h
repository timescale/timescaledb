#ifndef TIMESCALEDB_CHUNK_INDEX_H
#define TIMESCALEDB_CHUNK_INDEX_H

#include <postgres.h>
#include <nodes/parsenodes.h>

typedef struct Chunk Chunk;
typedef struct Hypertable Hypertable;

extern void chunk_index_create_all(int32 hypertable_id, Oid hypertable_relid, int32 chunk_id, Oid chunkrelid);
extern Oid	chunk_index_create_from_stmt(IndexStmt *stmt, int32 chunk_id, Oid chunkrelid, int32 hypertable_id, Oid hypertable_indexrelid);
extern int	chunk_index_delete_children_of(Hypertable *ht, Oid hypertable_indexrelid, bool should_drop);
extern int	chunk_index_delete(Chunk *chunk, Oid chunk_indexrelid, bool drop_index);
extern int	chunk_index_rename(Chunk *chunk, Oid chunk_indexrelid, const char *newname);
extern int	chunk_index_rename_parent(Hypertable *ht, Oid hypertable_indexrelid, const char *newname);
extern int	chunk_index_set_tablespace(Hypertable *ht, Oid hypertable_indexrelid, const char *tablespace);
extern void chunk_index_create_from_constraint(int32 hypertable_id, Oid hypertable_constaint, int32 chunk_id, Oid chunk_constraint);
#endif   /* TIMESCALEDB_CHUNK_INDEX_H */
