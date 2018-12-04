/*
 * Copyright (c) 2016-2018  Timescale, Inc. All Rights Reserved.
 *
 * This file is licensed under the Apache License,
 * see LICENSE-APACHE at the top level directory.
 */
#ifndef TIMESCALEDB_HYPERTABLE_H
#define TIMESCALEDB_HYPERTABLE_H

#include <postgres.h>
#include <nodes/primnodes.h>

#include "catalog.h"
#include "dimension.h"
#include "tablespace.h"
#include "scanner.h"

#define OLD_INSERT_BLOCKER_NAME	"insert_blocker"
#define INSERT_BLOCKER_NAME "ts_insert_blocker"

typedef struct SubspaceStore SubspaceStore;
typedef struct Chunk Chunk;

typedef struct Hypertable
{
	FormData_hypertable fd;
	Oid			main_table_relid;
	Oid			chunk_sizing_func;
	Hyperspace *space;
	SubspaceStore *chunk_cache;
} Hypertable;

/* create_hypertable record attribute numbers */
enum Anum_create_hypertable
{
	Anum_create_hypertable_id = 1,
	Anum_create_hypertable_schema_name,
	Anum_create_hypertable_table_name,
	Anum_create_hypertable_created,
	_Anum_create_hypertable_max,
};

#define Natts_create_hypertable \
	(_Anum_create_hypertable_max - 1)

extern int	ts_number_of_hypertables(void);

extern Oid	ts_rel_get_owner(Oid relid);
extern List *ts_hypertable_get_all(void);
extern Hypertable *ts_hypertable_get_by_id(int32 hypertable_id);
extern Hypertable *ts_hypertable_get_by_name(char *schema, char *name);
extern bool ts_hypertable_has_privs_of(Oid hypertable_oid, Oid userid);
extern Oid	ts_hypertable_permissions_check(Oid hypertable_oid, Oid userid);
extern Hypertable *ts_hypertable_from_tuple(HeapTuple tuple, MemoryContext mctx);
extern int	ts_hypertable_scan_with_memory_context(const char *schema, const char *table, tuple_found_func tuple_found, void *data, LOCKMODE lockmode, bool tuplock, MemoryContext mctx);
extern int	ts_hypertable_scan_relid(Oid table_relid, tuple_found_func tuple_found, void *data, LOCKMODE lockmode, bool tuplock);
extern HTSU_Result ts_hypertable_lock_tuple(Oid table_relid);
extern bool ts_hypertable_lock_tuple_simple(Oid table_relid);
extern int	ts_hypertable_update(Hypertable *ht);
extern int	ts_hypertable_set_name(Hypertable *ht, const char *newname);
extern int	ts_hypertable_set_schema(Hypertable *ht, const char *newname);
extern int	ts_hypertable_set_num_dimensions(Hypertable *ht, int16 num_dimensions);
extern int	ts_hypertable_delete_by_name(const char *schema_name, const char *table_name);
extern int	ts_hypertable_reset_associated_schema_name(const char *associated_schema);
extern Oid	ts_hypertable_id_to_relid(int32 hypertable_id);
extern Chunk *ts_hypertable_get_chunk(Hypertable *h, Point *point);
extern Oid	ts_hypertable_relid(RangeVar *rv);
extern bool ts_is_hypertable(Oid relid);
extern bool ts_hypertable_has_tablespace(Hypertable *ht, Oid tspc_oid);
extern Tablespace *ts_hypertable_select_tablespace(Hypertable *ht, Chunk *chunk);
extern char *ts_hypertable_select_tablespace_name(Hypertable *ht, Chunk *chunk);
extern Tablespace *ts_hypertable_get_tablespace_at_offset_from(Hypertable *ht, Oid tablespace_oid, int16 offset);
extern bool ts_hypertable_has_tuples(Oid table_relid, LOCKMODE lockmode);
extern void ts_hypertables_rename_schema_name(const char *old_name, const char *new_name);

#define hypertable_scan(schema, table, tuple_found, data, lockmode, tuplock) \
	ts_hypertable_scan_with_memory_context(schema, table, tuple_found, data, lockmode, tuplock, CurrentMemoryContext)

#define hypertable_adaptive_chunking_enabled(ht)						\
	(OidIsValid((ht)->chunk_sizing_func) && (ht)->fd.chunk_target_size > 0)

#endif							/* TIMESCALEDB_HYPERTABLE_H */
