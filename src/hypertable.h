/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_HYPERTABLE_H
#define TIMESCALEDB_HYPERTABLE_H

#include <postgres.h>
#include <nodes/primnodes.h>

#include "compat.h"

#include "catalog.h"
#include "dimension.h"
#include "export.h"
#include "tablespace.h"
#include "scanner.h"
#include "chunk_adaptive.h"

#if PG96
#include <catalog/objectaddress.h>
#endif

#define OLD_INSERT_BLOCKER_NAME "insert_blocker"
#define INSERT_BLOCKER_NAME "ts_insert_blocker"

#define INVALID_HYPERTABLE_ID 0

typedef struct SubspaceStore SubspaceStore;
typedef struct Chunk Chunk;

#define TS_HYPERTABLE_HAS_COMPRESSION(ht)                                                          \
	((ht)->fd.compressed_hypertable_id != INVALID_HYPERTABLE_ID)

typedef struct Hypertable
{
	FormData_hypertable fd;
	Oid main_table_relid;
	Oid chunk_sizing_func;
	Hyperspace *space;
	SubspaceStore *chunk_cache;
	int64 max_ignore_invalidation_older_than; /* lazy-loaded, do not access directly, use
											ts_hypertable_get_ignore_invalidation_older_than */
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

#define Natts_create_hypertable (_Anum_create_hypertable_max - 1)

extern int ts_number_of_user_hypertables(void);

extern int ts_number_compressed_hypertables(void);

extern TSDLLEXPORT Oid ts_rel_get_owner(Oid relid);
extern List *ts_hypertable_get_all(void);

typedef enum HypertableCreateFlags
{
	HYPERTABLE_CREATE_DISABLE_DEFAULT_INDEXES = 1 << 0,
	HYPERTABLE_CREATE_IF_NOT_EXISTS = 1 << 1,
	HYPERTABLE_CREATE_MIGRATE_DATA = 1 << 2,
} HypertableCreateFlags;

extern TSDLLEXPORT bool ts_hypertable_create_from_info(Oid table_relid, int32 hypertable_id,
													   uint32 flags, DimensionInfo *time_dim_info,
													   DimensionInfo *space_dim_info,
													   Name associated_schema_name,
													   Name associated_table_prefix,
													   ChunkSizingInfo *chunk_sizing_info);
extern TSDLLEXPORT bool ts_hypertable_create_compressed(Oid table_relid, int32 hypertable_id);
extern TSDLLEXPORT Hypertable *ts_hypertable_get_by_id(int32 hypertable_id);
extern Hypertable *ts_hypertable_get_by_name(char *schema, char *name);
extern bool ts_hypertable_has_privs_of(Oid hypertable_oid, Oid userid);
extern TSDLLEXPORT Oid ts_hypertable_permissions_check(Oid hypertable_oid, Oid userid);

extern TSDLLEXPORT void ts_hypertable_permissions_check_by_id(int32 hypertable_id);
extern Hypertable *ts_hypertable_from_tupleinfo(TupleInfo *ti);
extern int ts_hypertable_scan_with_memory_context(const char *schema, const char *table,
												  tuple_found_func tuple_found, void *data,
												  LOCKMODE lockmode, bool tuplock,
												  MemoryContext mctx);
extern TM_Result ts_hypertable_lock_tuple(Oid table_relid);
extern bool ts_hypertable_lock_tuple_simple(Oid table_relid);
extern int ts_hypertable_update(Hypertable *ht);
extern int ts_hypertable_set_name(Hypertable *ht, const char *newname);
extern int ts_hypertable_set_schema(Hypertable *ht, const char *newname);
extern int ts_hypertable_set_num_dimensions(Hypertable *ht, int16 num_dimensions);
extern TSDLLEXPORT int64 ts_hypertable_get_max_ignore_invalidation_older_than(Hypertable *ht);
extern int ts_hypertable_delete_by_name(const char *schema_name, const char *table_name);
extern TSDLLEXPORT ObjectAddress ts_hypertable_create_trigger(Hypertable *ht, CreateTrigStmt *stmt,
															  const char *query);
extern TSDLLEXPORT void ts_hypertable_drop_trigger(Hypertable *ht, const char *trigger_name);
extern TSDLLEXPORT void ts_hypertable_drop(Hypertable *hypertable, DropBehavior behavior);

extern int ts_hypertable_reset_associated_schema_name(const char *associated_schema);
extern TSDLLEXPORT Oid ts_hypertable_id_to_relid(int32 hypertable_id);
extern TSDLLEXPORT int32 ts_hypertable_relid_to_id(Oid relid);
extern Chunk *ts_hypertable_find_chunk_if_exists(Hypertable *h, Point *point);
extern Chunk *ts_hypertable_get_or_create_chunk(Hypertable *h, Point *point);
extern Oid ts_hypertable_relid(RangeVar *rv);
extern TSDLLEXPORT bool ts_is_hypertable(Oid relid);
extern bool ts_hypertable_has_tablespace(Hypertable *ht, Oid tspc_oid);
extern Tablespace *ts_hypertable_select_tablespace(Hypertable *ht, Chunk *chunk);
extern char *ts_hypertable_select_tablespace_name(Hypertable *ht, Chunk *chunk);
extern Tablespace *ts_hypertable_get_tablespace_at_offset_from(int32 hypertable_id,
															   Oid tablespace_oid, int16 offset);
extern bool ts_hypertable_has_tuples(Oid table_relid, LOCKMODE lockmode);
extern bool ts_hypertable_has_chunks(Oid table_relid, LOCKMODE lockmode);
extern void ts_hypertables_rename_schema_name(const char *old_name, const char *new_name);
extern List *ts_hypertable_get_all_by_name(Name schema_name, Name table_name, MemoryContext mctx);
extern bool ts_is_partitioning_column(Hypertable *ht, Index column_attno);
extern TSDLLEXPORT bool ts_hypertable_set_compressed_id(Hypertable *ht,
														int32 compressed_hypertable_id);
extern TSDLLEXPORT bool ts_hypertable_unset_compressed_id(Hypertable *ht);
extern TSDLLEXPORT void ts_hypertable_clone_constraints_to_compressed(Hypertable *ht,
																	  List *constraint_list);

#define hypertable_scan(schema, table, tuple_found, data, lockmode, tuplock)                       \
	ts_hypertable_scan_with_memory_context(schema,                                                 \
										   table,                                                  \
										   tuple_found,                                            \
										   data,                                                   \
										   lockmode,                                               \
										   tuplock,                                                \
										   CurrentMemoryContext)

#define hypertable_adaptive_chunking_enabled(ht)                                                   \
	(OidIsValid((ht)->chunk_sizing_func) && (ht)->fd.chunk_target_size > 0)

#endif /* TIMESCALEDB_HYPERTABLE_H */
