/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#pragma once

#include <postgres.h>
#include <nodes/primnodes.h>
#include <utils/array.h>

#include "chunk_adaptive.h"
#include "dimension.h"
#include "export.h"
#include "hypertable_cache.h"
#include "scan_iterator.h"
#include "scanner.h"
#include "ts_catalog/catalog.h"
#include "ts_catalog/tablespace.h"

#define OLD_INSERT_BLOCKER_NAME "insert_blocker"
#define INSERT_BLOCKER_NAME "ts_insert_blocker"

#define INVALID_HYPERTABLE_ID 0

typedef struct SubspaceStore SubspaceStore;
typedef struct Chunk Chunk;
typedef struct Hypercube Hypercube;
typedef struct ChunkRangeSpace ChunkRangeSpace;

enum
{
	HypertableCompressionOff = 0,
	HypertableCompressionEnabled = 1,
	HypertableInternalCompressionTable = 2,
};

#define TS_HYPERTABLE_HAS_COMPRESSION_TABLE(ht) ts_hypertable_has_compression_table(ht)

#define TS_HYPERTABLE_HAS_COMPRESSION_ENABLED(ht)                                                  \
	((ht)->fd.compression_state == HypertableCompressionEnabled)

#define TS_HYPERTABLE_IS_INTERNAL_COMPRESSION_TABLE(ht)                                            \
	((ht)->fd.compression_state == HypertableInternalCompressionTable)
typedef struct Hypertable
{
	FormData_hypertable fd;
	Oid main_table_relid;
	Oid chunk_sizing_func;
	Oid amoid;
	Hyperspace *space;
	SubspaceStore *chunk_cache;
	ChunkRangeSpace *range_space;
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

/* Create a generic hypertable */
enum Anum_generic_create_hypertable
{
	Anum_generic_create_hypertable_id = 1,
	Anum_generic_create_hypertable_created,
	_Anum_generic_create_hypertable_max,
};

#define Natts_generic_create_hypertable (_Anum_generic_create_hypertable_max - 1)

extern TSDLLEXPORT Oid ts_rel_get_owner(Oid relid);

typedef enum HypertableCreateFlags
{
	HYPERTABLE_CREATE_DISABLE_DEFAULT_INDEXES = 1 << 0,
	HYPERTABLE_CREATE_IF_NOT_EXISTS = 1 << 1,
	HYPERTABLE_CREATE_MIGRATE_DATA = 1 << 2,
} HypertableCreateFlags;

extern TSDLLEXPORT bool ts_hypertable_create_from_info(Oid table_relid, int32 hypertable_id,
													   uint32 flags, DimensionInfo *time_dim_info,
													   DimensionInfo *closed_dim_info,
													   Name associated_schema_name,
													   Name associated_table_prefix,
													   ChunkSizingInfo *chunk_sizing_info);
extern TSDLLEXPORT bool ts_hypertable_create_compressed(Oid table_relid, int32 hypertable_id);

extern TSDLLEXPORT Hypertable *ts_hypertable_get_by_id(int32 hypertable_id);
extern Hypertable *ts_hypertable_get_by_name(const char *schema, const char *name);
extern TSDLLEXPORT bool ts_hypertable_get_attributes_by_name(const char *schema, const char *name,
															 FormData_hypertable *form);
extern TSDLLEXPORT bool ts_hypertable_has_privs_of(Oid hypertable_oid, Oid userid);
extern TSDLLEXPORT Oid ts_hypertable_permissions_check(Oid hypertable_oid, Oid userid);

extern TSDLLEXPORT void ts_hypertable_permissions_check_by_id(int32 hypertable_id);
extern Hypertable *ts_hypertable_from_tupleinfo(const TupleInfo *ti);
extern Hypertable *ts_resolve_hypertable_from_table_or_cagg(Cache *hcache, Oid relid,
															bool allow_matht);
extern int ts_hypertable_scan_with_memory_context(const char *schema, const char *table,
												  tuple_found_func tuple_found, void *data,
												  LOCKMODE lockmode, MemoryContext mctx);
extern bool ts_hypertable_update_status_osm(Hypertable *ht);
extern bool ts_hypertable_update_chunk_sizing(Hypertable *ht);
extern int ts_hypertable_set_name(Hypertable *ht, const char *newname);
extern int ts_hypertable_set_schema(Hypertable *ht, const char *newname);
extern int ts_hypertable_set_num_dimensions(Hypertable *ht, int16 num_dimensions);
extern int ts_hypertable_delete_by_name(const char *schema_name, const char *table_name);
extern int ts_hypertable_delete_by_id(int32 hypertable_id);
extern TSDLLEXPORT ObjectAddress ts_hypertable_create_trigger(const Hypertable *ht,
															  CreateTrigStmt *stmt,
															  const char *query);
extern TSDLLEXPORT void ts_hypertable_drop_invalidation_replication_slot(const char *slot_name);
extern TSDLLEXPORT void ts_hypertable_drop_trigger(Oid relid, const char *trigger_name);
extern TSDLLEXPORT void ts_hypertable_drop(Hypertable *hypertable, DropBehavior behavior);

extern int ts_hypertable_reset_associated_schema_name(const char *associated_schema);
extern TSDLLEXPORT Oid ts_hypertable_id_to_relid(int32 hypertable_id, bool return_invalid);
extern TSDLLEXPORT int32 ts_hypertable_relid_to_id(Oid relid);
extern TSDLLEXPORT Chunk *ts_hypertable_find_chunk_for_point(const Hypertable *h,
															 const Point *point, LOCKMODE lockmode);
extern TSDLLEXPORT Chunk *ts_hypertable_chunk_store_add(const Hypertable *h,
														const Chunk *input_chunk);
extern TSDLLEXPORT Chunk *ts_hypertable_create_chunk_for_point(const Hypertable *h,
															   const Point *point,
															   LOCKMODE chunk_lockmode);
extern Oid ts_hypertable_relid(RangeVar *rv);
extern TSDLLEXPORT bool ts_is_hypertable(Oid relid);
extern bool ts_hypertable_has_tablespace(const Hypertable *ht, Oid tspc_oid);
extern Tablespace *ts_hypertable_select_tablespace(const Hypertable *ht, const Chunk *chunk);
extern const char *ts_hypertable_select_tablespace_name(const Hypertable *ht, const Chunk *chunk);
extern Tablespace *ts_hypertable_get_tablespace_at_offset_from(int32 hypertable_id,
															   Oid tablespace_oid, int16 offset);
extern TSDLLEXPORT bool ts_hypertable_has_chunks(Oid table_relid, LOCKMODE lockmode);
extern void ts_hypertables_rename_schema_name(const char *old_name, const char *new_name);
extern bool ts_is_partitioning_column(const Hypertable *ht, AttrNumber column_attno);
extern bool ts_is_partitioning_column_name(const Hypertable *ht, NameData column_name);
extern TSDLLEXPORT bool ts_hypertable_set_compressed(Hypertable *ht,
													 int32 compressed_hypertable_id);
extern TSDLLEXPORT bool ts_hypertable_unset_compressed(Hypertable *ht);
extern TSDLLEXPORT bool ts_hypertable_set_compress_interval(Hypertable *ht,
															int64 compress_interval);
extern TSDLLEXPORT int64 ts_hypertable_get_open_dim_max_value(const Hypertable *ht,
															  int dimension_index, bool *isnull);

extern TSDLLEXPORT bool ts_hypertable_has_compression_table(const Hypertable *ht);
extern TSDLLEXPORT void ts_hypertable_formdata_fill(FormData_hypertable *fd, const TupleInfo *ti);

#define hypertable_scan(schema, table, tuple_found, data, lockmode)                                \
	ts_hypertable_scan_with_memory_context(schema,                                                 \
										   table,                                                  \
										   tuple_found,                                            \
										   data,                                                   \
										   lockmode,                                               \
										   CurrentMemoryContext)

#define hypertable_adaptive_chunking_enabled(ht)                                                   \
	(OidIsValid((ht)->chunk_sizing_func) && (ht)->fd.chunk_target_size > 0)
