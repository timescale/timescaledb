#ifndef TIMESCALEDB_CATALOG_H
#define TIMESCALEDB_CATALOG_H

#include <postgres.h>

enum catalog_table
{
	HYPERTABLE = 0,
	CHUNK,
	PARTITION,
	PARTITION_EPOCH,
	_MAX_CATALOG_TABLES,
};

#define CATALOG_SCHEMA_NAME "_timescaledb_catalog"

/* Hypertable table definitions */
#define HYPERTABLE_TABLE_NAME "hypertable"
#define HYPERTABLE_INDEX_NAME "hypertable_pkey"

/* Partition epoch table definitions */
#define PARTITION_EPOCH_TABLE_NAME "partition_epoch"
#define PARTITION_EPOCH_INDEX_NAME "partition_epoch_pkey"
#define PARTITION_EPOCH_TIME_INDEX_NAME "partition_epoch_hypertable_id_start_time_end_time_idx"

/* Partition table definitions */
#define PARTITION_TABLE_NAME "partition"
#define PARTITION_INDEX_NAME "partition_pkey"
#define PARTITION_EPOCH_ID_INDEX_NAME "partition_epoch_id_idx"

/* Chunk table definitions */
#define CHUNK_TABLE_NAME "chunk"
#define CHUNK_INDEX_NAME "chunk_pkey"
#define CHUNK_PARTITION_TIME_INDEX_NAME "chunk_partition_id_start_time_end_time_idx"

typedef struct Catalog
{
	char		database_name[NAMEDATALEN];
	Oid			database_id;
	Oid			schema_id;
	struct
	{
		const char *name;
		Oid			id;
		Oid			index_id;
	}			tables[_MAX_CATALOG_TABLES];
}	Catalog;

Catalog    *catalog_get(void);

#endif   /* TIMESCALEDB_CATALOG_H */
