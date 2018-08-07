#ifndef TIMESCALEDB_HYPERTABLE_H
#define TIMESCALEDB_HYPERTABLE_H

#include <postgres.h>
#include <nodes/primnodes.h>

#include "catalog.h"
#include "dimension.h"
#include "tablespace.h"
#include "scanner.h"

typedef struct SubspaceStore SubspaceStore;
typedef struct Chunk Chunk;
typedef struct HeapTupleData *HeapTuple;

typedef struct Hypertable
{
	FormData_hypertable fd;
	Oid			main_table_relid;
	Oid			chunk_sizing_func;
	Hyperspace *space;
	SubspaceStore *chunk_cache;
} Hypertable;


extern Oid	rel_get_owner(Oid relid);
extern Hypertable *hypertable_get_by_id(int32 hypertable_id);
extern Hypertable *hypertable_get_by_name(char *schema, char *name);
extern bool hypertable_has_privs_of(Oid hypertable_oid, Oid userid);
extern Oid	hypertable_permissions_check(Oid hypertable_oid, Oid userid);
extern Hypertable *hypertable_from_tuple(HeapTuple tuple, MemoryContext mctx);
extern int	hypertable_scan_with_memory_context(const char *schema, const char *table, tuple_found_func tuple_found, void *data, LOCKMODE lockmode, bool tuplock, MemoryContext mctx);
extern int	hypertable_scan_relid(Oid table_relid, tuple_found_func tuple_found, void *data, LOCKMODE lockmode, bool tuplock);
extern HTSU_Result hypertable_lock_tuple(Oid table_relid);
extern bool hypertable_lock_tuple_simple(Oid table_relid);
extern int	hypertable_update(Hypertable *ht);
extern int	hypertable_set_name(Hypertable *ht, const char *newname);
extern int	hypertable_set_schema(Hypertable *ht, const char *newname);
extern int	hypertable_set_num_dimensions(Hypertable *ht, int16 num_dimensions);
extern int	hypertable_delete_by_id(int32 hypertable_id);
extern int	hypertable_delete_by_name(const char *schema_name, const char *table_name);
extern int	hypertable_reset_associated_schema_name(const char *associated_schema);
extern Oid	hypertable_id_to_relid(int32 hypertable_id);
extern Chunk *hypertable_get_chunk(Hypertable *h, Point *point);
extern Oid	hypertable_relid(RangeVar *rv);
extern bool is_hypertable(Oid relid);
extern bool hypertable_has_tablespace(Hypertable *ht, Oid tspc_oid);
extern Tablespace *hypertable_select_tablespace(Hypertable *ht, Chunk *chunk);
extern char *hypertable_select_tablespace_name(Hypertable *ht, Chunk *chunk);
extern Tablespace *hypertable_get_tablespace_at_offset_from(Hypertable *ht, Oid tablespace_oid, int16 offset);
extern bool hypertable_has_tuples(Oid table_relid, LOCKMODE lockmode);

#define hypertable_scan(schema, table, tuple_found, data, lockmode, tuplock) \
	hypertable_scan_with_memory_context(schema, table, tuple_found, data, lockmode, tuplock, CurrentMemoryContext)

#define hypertable_adaptive_chunking_enabled(ht)						\
	(OidIsValid((ht)->chunk_sizing_func) && (ht)->fd.chunk_target_size > 0)

#endif							/* TIMESCALEDB_HYPERTABLE_H */
