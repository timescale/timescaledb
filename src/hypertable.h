/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_HYPERTABLE_H
#define TIMESCALEDB_HYPERTABLE_H

#include <postgres.h>
#include <nodes/primnodes.h>
#include <utils/array.h>

#include "ts_catalog/catalog.h"
#include "chunk_adaptive.h"
#include "dimension.h"
#include "export.h"
#include "scanner.h"
#include "scan_iterator.h"
#include "ts_catalog/tablespace.h"
#include "ts_catalog/dimension_partition.h"

#define OLD_INSERT_BLOCKER_NAME "insert_blocker"
#define INSERT_BLOCKER_NAME "ts_insert_blocker"

#define INVALID_HYPERTABLE_ID 0

typedef struct SubspaceStore SubspaceStore;
typedef struct Chunk Chunk;
typedef struct Hypercube Hypercube;

/* For the distributed node case, we would have compression enabled
 * but don't have a corresponding internal table on the access
 * node
 */
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
	Hyperspace *space;
	SubspaceStore *chunk_cache;
	/*
	 * Allows restricting the data nodes to use for the hypertable. Default is to
	 * use all available data nodes.
	 */
	List *data_nodes;
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

extern TSDLLEXPORT Oid ts_rel_get_owner(Oid relid);
extern List *ts_hypertable_get_all(void);

typedef enum HypertableCreateFlags
{
	HYPERTABLE_CREATE_DISABLE_DEFAULT_INDEXES = 1 << 0,
	HYPERTABLE_CREATE_IF_NOT_EXISTS = 1 << 1,
	HYPERTABLE_CREATE_MIGRATE_DATA = 1 << 2,
} HypertableCreateFlags;

/* Hypertable type defined by replication_factor value */
typedef enum HypertableType
{
	/* Hypertable created on a data node as part of any other
	 * distributed hypertable */
	HYPERTABLE_DISTRIBUTED_MEMBER = -1,
	/* Non-replicated hypertable (default for a single node) */
	HYPERTABLE_REGULAR = 0,
	/* Replicated hypertable (replication_factor is >= 1) */
	HYPERTABLE_DISTRIBUTED
} HypertableType;

extern TSDLLEXPORT bool ts_hypertable_create_from_info(
	Oid table_relid, int32 hypertable_id, uint32 flags, DimensionInfo *time_dim_info,
	DimensionInfo *space_dim_info, Name associated_schema_name, Name associated_table_prefix,
	ChunkSizingInfo *chunk_sizing_info, int16 replication_factor, List *data_node_names);
extern TSDLLEXPORT bool ts_hypertable_create_compressed(Oid table_relid, int32 hypertable_id);

extern TSDLLEXPORT Hypertable *ts_hypertable_get_by_id(int32 hypertable_id);
extern Hypertable *ts_hypertable_get_by_name(const char *schema, const char *name);
extern TSDLLEXPORT bool ts_hypertable_get_attributes_by_name(const char *schema, const char *name,
															 FormData_hypertable *form);
extern TSDLLEXPORT bool ts_hypertable_has_privs_of(Oid hypertable_oid, Oid userid);
extern TSDLLEXPORT Oid ts_hypertable_permissions_check(Oid hypertable_oid, Oid userid);

extern TSDLLEXPORT void ts_hypertable_permissions_check_by_id(int32 hypertable_id);
extern Hypertable *ts_hypertable_from_tupleinfo(const TupleInfo *ti);
extern int ts_hypertable_scan_with_memory_context(const char *schema, const char *table,
												  tuple_found_func tuple_found, void *data,
												  LOCKMODE lockmode, bool tuplock,
												  MemoryContext mctx);
extern TM_Result ts_hypertable_lock_tuple(Oid table_relid);
extern bool ts_hypertable_lock_tuple_simple(Oid table_relid);
extern TSDLLEXPORT int ts_hypertable_update(Hypertable *ht);
extern int ts_hypertable_set_name(Hypertable *ht, const char *newname);
extern int ts_hypertable_set_schema(Hypertable *ht, const char *newname);
extern int ts_hypertable_set_num_dimensions(Hypertable *ht, int16 num_dimensions);
extern int ts_hypertable_delete_by_name(const char *schema_name, const char *table_name);
extern int ts_hypertable_delete_by_id(int32 hypertable_id);
extern TSDLLEXPORT ObjectAddress ts_hypertable_create_trigger(const Hypertable *ht,
															  CreateTrigStmt *stmt,
															  const char *query);
extern TSDLLEXPORT void ts_hypertable_drop_trigger(Oid relid, const char *trigger_name);
extern TSDLLEXPORT void ts_hypertable_drop(Hypertable *hypertable, DropBehavior behavior);

extern TSDLLEXPORT void ts_hypertable_check_partitioning(const Hypertable *ht,
														 int32 id_of_updated_dimension);
extern int ts_hypertable_reset_associated_schema_name(const char *associated_schema);
extern TSDLLEXPORT Oid ts_hypertable_id_to_relid(int32 hypertable_id);
extern TSDLLEXPORT int32 ts_hypertable_relid_to_id(Oid relid);
extern TSDLLEXPORT Chunk *ts_hypertable_find_chunk_for_point(const Hypertable *h,
															 const Point *point);
extern TSDLLEXPORT Chunk *ts_hypertable_create_chunk_for_point(const Hypertable *h,
															   const Point *point, bool *found);
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
extern TSDLLEXPORT bool ts_hypertable_set_compressed(Hypertable *ht,
													 int32 compressed_hypertable_id);
extern TSDLLEXPORT bool ts_hypertable_unset_compressed(Hypertable *ht);
extern TSDLLEXPORT bool ts_hypertable_set_compress_interval(Hypertable *ht,
															int64 compress_interval);
extern TSDLLEXPORT void ts_hypertable_clone_constraints_to_compressed(const Hypertable *ht,
																	  List *constraint_list);
extern TSDLLEXPORT List *ts_hypertable_assign_chunk_data_nodes(const Hypertable *ht,
															   const Hypercube *cube);
extern TSDLLEXPORT List *ts_hypertable_get_data_node_name_list(const Hypertable *ht);
extern TSDLLEXPORT List *ts_hypertable_get_data_node_serverids_list(const Hypertable *ht);
extern TSDLLEXPORT List *ts_hypertable_get_available_data_nodes(const Hypertable *ht,
																bool error_if_missing);
extern TSDLLEXPORT List *ts_hypertable_get_available_data_node_names(const Hypertable *ht,
																	 bool error_if_missing);
extern TSDLLEXPORT List *ts_hypertable_get_available_data_node_server_oids(const Hypertable *ht);
extern TSDLLEXPORT HypertableType ts_hypertable_get_type(const Hypertable *ht);
extern TSDLLEXPORT void ts_hypertable_func_call_on_data_nodes(const Hypertable *ht,
															  FunctionCallInfo fcinfo);
extern TSDLLEXPORT Datum ts_hypertable_get_open_dim_max_value(const Hypertable *ht,
															  int dimension_index, bool *isnull);

extern TSDLLEXPORT bool ts_hypertable_has_compression_table(const Hypertable *ht);
extern TSDLLEXPORT void ts_hypertable_formdata_fill(FormData_hypertable *fd, const TupleInfo *ti);
extern TSDLLEXPORT void ts_hypertable_scan_by_name(ScanIterator *iterator, const char *schema,
												   const char *name);
extern TSDLLEXPORT bool ts_hypertable_update_dimension_partitions(const Hypertable *ht);
extern TSDLLEXPORT int16 ts_validate_replication_factor(const char *hypertable_name,
														int32 replication_factor,
														int num_data_nodes);

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

#define hypertable_is_distributed(ht) ((ht)->fd.replication_factor > 0)
#define hypertable_chunk_relkind(ht)                                                               \
	(hypertable_is_distributed(ht) ? RELKIND_FOREIGN_TABLE : RELKIND_RELATION)
#define hypertable_is_distributed_member(ht)                                                       \
	((ht)->fd.replication_factor == HYPERTABLE_DISTRIBUTED_MEMBER)

#endif /* TIMESCALEDB_HYPERTABLE_H */
