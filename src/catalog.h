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

enum
{
	HYPERTABLE_ID_INDEX = 0,
	_MAX_HYPERTABLE_INDEX,
};

/* Partition epoch table definitions */
#define PARTITION_EPOCH_TABLE_NAME "partition_epoch"

enum
{
	PARTITION_EPOCH_ID_INDEX = 0,
	PARTITION_EPOCH_TIME_INDEX,
	_MAX_PARTITION_EPOCH_INDEX,
};

/* Partition table definitions */
#define PARTITION_TABLE_NAME "partition"

enum
{
	PARTITION_ID_INDEX = 0,
	PARTITION_PARTITION_EPOCH_ID_INDEX,
	_MAX_PARTITION_INDEX,
};

/* Chunk table definitions */
#define CHUNK_TABLE_NAME "chunk"

enum
{
	CHUNK_ID_INDEX = 0,
	CHUNK_PARTITION_TIME_INDEX,
	_MAX_CHUNK_INDEX,
};

#define MAX(a, b) \
	((long)(a) > (long)(b) ? (a) : (b))

#define _MAX_TABLE_INDEXES MAX(_MAX_HYPERTABLE_INDEX, \
							   MAX(_MAX_PARTITION_EPOCH_INDEX, \
								   MAX(_MAX_PARTITION_INDEX, _MAX_CHUNK_INDEX)))

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
}	Catalog;

Catalog    *catalog_get(void);

#endif   /* TIMESCALEDB_CATALOG_H */
