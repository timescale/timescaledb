#ifndef TIMESCALEDB_CATALOG_H
#define TIMESCALEDB_CATALOG_H

#include <postgres.h>

/*
 * TimescaleDB catalog.
 *
 * The TimescaleDB catalog contains schema metadata for hypertables, among other
 * things. The metadata is stored in regular tables. This header file contains
 * definitions for those tables and should match any table declarations in
 * sql/common/tables.sql.
 *
 * A source file that includes this header has access to a catalog object,
 * which contains cached information about catalog tables, such as relation
 * OIDs.
 *
 * Generally, definitions and naming should roughly follow how things are done
 * in Postgres internally.
 */
enum CatalogTable
{
	HYPERTABLE = 0,
	HYPERTABLE_REPLICA,
	DEFAULT_REPLICA_NODE,
	PARTITION_EPOCH,
	PARTITION,
	CHUNK,
	CHUNK_REPLICA_NODE,
	_MAX_CATALOG_TABLES,
};

#define CATALOG_SCHEMA_NAME "_timescaledb_catalog"
#define CACHE_SCHEMA_NAME "_timescaledb_cache"
#define EXTENSION_NAME "timescaledb"

/******************************
 *
 * Hypertable table definitions
 *
 ******************************/

#define HYPERTABLE_TABLE_NAME "hypertable"

enum
{
	HYPERTABLE_ID_INDEX = 0,
	HYPERTABLE_NAME_INDEX,
	_MAX_HYPERTABLE_INDEX,
};

/* Hypertable table attribute numbers */
enum Anum_hypertable
{
	Anum_hypertable_id = 1,
	Anum_hypertable_schema_name,
	Anum_hypertable_table_name,
	Anum_hypertable_associated_schema_name,
	Anum_hypertable_associated_table_prefix,
	Anum_hypertable_root_schema_name,
	Anum_hypertable_root_table_name,
	Anum_hypertable_replication_factor,
	Anum_hypertable_placement,
	Anum_hypertable_time_column_name,
	Anum_hypertable_time_column_type,
	Anum_hypertable_chunk_time_interval,
	_Anum_hypertable_max,
};

#define Natts_hypertable \
	(_Anum_hypertable_max - 1)

/* Hypertable primary index attribute numbers */
enum Anum_hypertable_pkey_idx
{
	Anum_hypertable_pkey_idx_id = 1,
	_Anum_hypertable_pkey_max,
};

#define Natts_hypertable_pkey_idx \
	(_Anum_hypertable_pkey_max - 1)

/* Hypertable name (schema,table) index attribute numbers */
enum Anum_hypertable_name_idx
{
	Anum_hypertable_name_idx_schema = 1,
	Anum_hypertable_name_idx_table,
	_Anum_hypertable_name_max,
};

#define Natts_hypertable_name_idx (_Anum_hypertable_name_max - 1)

/***********************************
 *
 * Hypertable replica table definitions
 *
 ***********************************/

#define HYPERTABLE_REPLICA_TABLE_NAME "hypertable_replica"

enum Anum_hypertable_replica
{
	Anum_hypertable_replica_hypertable_id = 1,
	Anum_hypertable_replica_replica_id,
	Anum_hypertable_replica_schema_name,
	Anum_hypertable_replica_table_name,
	_Anum_hypertable_replica_max,
};

#define Natts_hypertable_replica (_Anum_hypertable_replica_max - 1)

enum
{
	HYPERTABLE_REPLICA_HYPERTABLE_REPLICA_INDEX = 0,
	_MAX_HYPERTABLE_REPLICA_INDEX,
};

enum Anum_hypertable_replica_hypertable_replica_idx
{
	Anum_hypertable_replica_hypertable_replica_idx_hypertable = 1,
	Anum_hypertable_replica_hypertable_replica_idx_replica,
	_Anum_hypertable_replica_hypertable_replica_idx_max,
};


typedef struct FormData_hypertable_replica
{
	int32		hypertable_id;
	int16		replica_id;
	NameData	schema_name;
	NameData	table_name;
} FormData_hypertable_replica;
typedef FormData_hypertable_replica *Form_hypertable_replica;

/***********************************
 *
 * default replica table definitions
 *
 ***********************************/

#define DEFAULT_REPLICA_NODE_TABLE_NAME "default_replica_node"

enum Anum_default_replica_node
{
	Anum_default_replica_node_database_name = 1,
	Anum_default_replica_node_hypertable_id,
	Anum_default_replica_node_replica_id,
	_Anum_default_replica_node_max,
};

#define Natts_default_replica_node (_Anum_default_replica_node_max - 1)

enum
{
	DEFAULT_REPLICA_NODE_DATABASE_HYPERTABLE_INDEX = 0,
	_MAX_DEFAULT_REPLICA_NODE_INDEX,
};

enum Anum_default_replica_node_database_hypertable_idx
{
	Anum_default_replica_node_database_hypertable_idx_database = 1,
	Anum_default_replica_node_database_hypertable_idx_hypertable,
	_Anum_default_replica_node_hypertable_replica_idx_max,
};

typedef struct FormData_default_replica_node
{
	NameData	database_name;
	int32		hypertable_id;
	int16		replica_id;
} FormData_default_replica_node;

typedef FormData_default_replica_node *Form_default_replica_node;

/***********************************
 *
 * Partition epoch table definitions
 *
 ***********************************/

#define PARTITION_EPOCH_TABLE_NAME "partition_epoch"

enum
{
	PARTITION_EPOCH_ID_INDEX = 0,
	PARTITION_EPOCH_TIME_INDEX,
	_MAX_PARTITION_EPOCH_INDEX,
};

enum Anum_partition_epoch
{
	Anum_partition_epoch_id = 1,
	Anum_partition_epoch_hypertable_id,
	Anum_partition_epoch_start_time,
	Anum_partition_epoch_end_time,
	Anum_partition_epoch_num_partitions,
	Anum_partition_epoch_partitioning_func_schema,
	Anum_partition_epoch_partitioning_func,
	Anum_partition_epoch_partitioning_mod,
	Anum_partition_epoch_partitioning_column,
	_Anum_partition_epoch_max,
};

#define Natts_partition_epoch \
	(_Anum_partition_epoch_max - 1)

enum Anum_partition_epoch_hypertable_start_time_end_time_idx
{
	Anum_partition_epoch_hypertable_start_time_end_time_idx_hypertable_id = 1,
	Anum_partition_epoch_hypertable_start_time_end_time_idx_start_time,
	Anum_partition_epoch_hypertable_start_time_end_time_idx_end_time,
	_Anum_partition_epoch_hypertable_start_time_end_time_idx_max,
};

#define Natts_partition_epoch_hypertable_start_time_end_time_idx \
	(_Anum_partition_epoch_hypertable_start_time_end_time_idx_max - 1)

/*****************************
 *
 * Partition table definitions
 *
 *****************************/

#define PARTITION_TABLE_NAME "partition"

enum
{
	PARTITION_ID_INDEX = 0,
	PARTITION_PARTITION_EPOCH_ID_INDEX,
	_MAX_PARTITION_INDEX,
};

enum Anum_partition
{
	Anum_partition_id = 1,
	Anum_partition_partition_epoch_id,
	Anum_partition_keyspace_start,
	Anum_partition_keyspace_end,
	Anum_partition_tablespace,
	_Anum_partition_max,
};

#define Natts_partition \
	(_Anum_partition_max - 1)

enum Anum_partition_epoch_id_idx
{
	Anum_partition_epoch_id_idx_epoch_id = 1,
	_Anum_partition_epoch_id_idx_max,
};

#define Natts_partition_epoch_id_idx \
	(_Anum_partition_epoch_id_idx_max - 1)

/*************************
 *
 * Chunk table definitions
 *
 *************************/

#define CHUNK_TABLE_NAME "chunk"

enum
{
	CHUNK_ID_INDEX = 0,
	CHUNK_PARTITION_TIME_INDEX,
	_MAX_CHUNK_INDEX,
};

enum Anum_chunk
{
	Anum_chunk_id = 1,
	Anum_chunk_partition_id,
	Anum_chunk_start_time,
	Anum_chunk_end_time,
	_Anum_chunk_max,
};

#define Natts_chunk \
	(_Anum_chunk_max - 1)

enum Anum_chunk_partition_start_time_end_time_idx
{
	Anum_chunk_partition_start_time_end_time_idx_partition_id = 1,
	Anum_chunk_partition_start_time_end_time_idx_start_time,
	Anum_chunk_partition_start_time_end_time_idx_end_time,
	_Anum_chunk_partition_start_time_end_time_idx_max,
};

#define Natts_chunk_partition_start_time_end_time_idx \
	(_Anum_chunk_partition_start_time_end_time_idx_max -1)

/**************************************
 *
 * Chunk replica node table definitions
 *
 **************************************/

#define CHUNK_REPLICA_NODE_TABLE_NAME "chunk_replica_node"

enum
{
	CHUNK_REPLICA_NODE_ID_INDEX = 0,
	_MAX_CHUNK_REPLICA_NODE_INDEX,
};

enum Anum_chunk_replica_node
{
	Anum_chunk_replica_node_id = 1,
	Anum_chunk_replica_node_partition_replica_id,
	Anum_chunk_replica_node_database_name,
	Anum_chunk_replica_node_schema_name,
	Anum_chunk_replica_node_table_name,
	_Anum_chunk_replica_node_max,
};

#define Natts_chunk_replica_node \
	(_Anum_chunk_replica_node_max - 1)

enum Anum_chunk_replica_node_pkey_idx
{
	Anum_chunk_replica_node_pkey_idx_chunk_id = 1,
	_Anum_chunk_replica_node_pkey_idx_max,
};

#define Natts_chunk_replica_node_pkey_idx \
	(_Anum_chunk_replica_node_pkey_idx_max - 1)



#define MAX(a, b) \
	((long)(a) > (long)(b) ? (a) : (b))

#define _MAX_TABLE_INDEXES MAX(_MAX_HYPERTABLE_INDEX,\
							   MAX(_MAX_PARTITION_EPOCH_INDEX, \
								   MAX(_MAX_PARTITION_INDEX, \
									   MAX(_MAX_HYPERTABLE_REPLICA_INDEX, \
										  MAX(_MAX_DEFAULT_REPLICA_NODE_INDEX, \
											MAX(_MAX_CHUNK_INDEX, _MAX_CHUNK_REPLICA_NODE_INDEX))))))

typedef enum CacheType
{
	CACHE_TYPE_HYPERTABLE,
	CACHE_TYPE_CHUNK,
	_MAX_CACHE_TYPES
} CacheType;

typedef struct Catalog
{
	char		database_name[NAMEDATALEN];
	Oid			database_id;
	Oid			schema_id;
	struct
	{
		const char *name;
		Oid			id;
		Oid			index_ids[_MAX_TABLE_INDEXES];
	}			tables[_MAX_CATALOG_TABLES];

	Oid			cache_schema_id;
	struct
	{
		Oid			inval_proxy_id;
	}			caches[_MAX_CACHE_TYPES];
} Catalog;

bool		catalog_is_valid(Catalog *catalog);
Catalog    *catalog_get(void);
void		catalog_reset(void);

Oid			catalog_get_cache_proxy_id(Catalog *catalog, CacheType type);
Oid			catalog_get_cache_proxy_id_by_name(Catalog *catalog, const char *relname);

const char *catalog_get_cache_proxy_name(CacheType type);

#endif   /* TIMESCALEDB_CATALOG_H */
