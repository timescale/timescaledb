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
	DIMENSION,
	DIMENSION_SLICE,
	CHUNK,
	CHUNK_CONSTRAINT,
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

/* Hypertable table attribute numbers */
enum Anum_hypertable
{
	Anum_hypertable_id = 1,
	Anum_hypertable_schema_name,
	Anum_hypertable_table_name,
	Anum_hypertable_associated_schema_name,
	Anum_hypertable_associated_table_prefix,
	_Anum_hypertable_max,
};

#define Natts_hypertable \
	(_Anum_hypertable_max - 1)

typedef struct FormData_hypertable
{
    int32 id;
    NameData schema_name;
    NameData table_name;
    NameData associated_schema_name;
    NameData associated_table_prefix;
} FormData_hypertable;

typedef FormData_hypertable *Form_hypertable;

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

enum
{
	HYPERTABLE_ID_INDEX = 0,
	HYPERTABLE_NAME_INDEX,
	_MAX_HYPERTABLE_INDEX,
};


/******************************
 *
 * Dimension table definitions
 *
 ******************************/

#define DIMENSION_TABLE_NAME "dimension"

enum Anum_dimension
{
	Anum_dimension_id = 1,
	Anum_dimension_hypertable_id,
	Anum_dimension_column_name,
	Anum_dimension_column_type,
	Anum_dimension_num_slices,
	Anum_dimension_partitioning_func_schema,
	Anum_dimension_partitioning_func,
	Anum_dimension_interval_length,
	_Anum_dimension_max,
};

#define Natts_dimension \
	(_Anum_dimension_max - 1)

typedef struct FormData_dimension
{
	int32 id;
    int32 hypertable_id;
    NameData column_name;
	Oid column_type;
    /* closed (space) columns */
    int16 num_slices;
    NameData partitioning_func_schema;
    NameData partitioning_func;
    /* open (time) columns */
    int64 interval_length;
} FormData_dimension;

typedef FormData_dimension *Form_dimension;

enum Anum_dimension_hypertable_id_idx
{
	Anum_dimension_hypertable_id_idx_hypertable_id = 1,
	_Anum_dimension_hypertable_id_idx_max,
};

#define Natts_dimension_hypertable_id_idx \
	(_Anum_dimension_hypertable_id_idx_max - 1)

enum
{
	DIMENSION_ID_IDX = 0,
	DIMENSION_HYPERTABLE_ID_IDX,
	_MAX_DIMENSION_INDEX,
};

/******************************
 *
 * Dimension slice table definitions
 *
 ******************************/

#define DIMENSION_SLICE_TABLE_NAME "dimension_slice"

enum Anum_dimension_slice
{
	Anum_dimension_slice_id = 1,
	Anum_dimension_slice_dimension_id,
	Anum_dimension_slice_range_start,
	Anum_dimension_slice_range_end,
	_Anum_dimension_slice_max,
};

#define Natts_dimension_slice \
	(_Anum_dimension_slice_max - 1)

typedef struct FormData_dimension_slice
{
	int32 id;
	int32 dimension_id;
    int64 range_start;
    int64 range_end;
} FormData_dimension_slice;

typedef FormData_dimension_slice *Form_dimension_slice;

enum Anum_dimension_slice_dimension_id_range_start_range_end_idx
{
	Anum_dimension_slice_dimension_id_range_start_range_end_idx_dimension_id = 1,
	Anum_dimension_slice_dimension_id_range_start_range_end_idx_range_start,
	Anum_dimension_slice_dimension_id_range_start_range_end_idx_range_end,
	_Anum_dimension_slice_dimension_id_range_start_range_end_idx_max,
};

#define Natts_dimension_slice_dimension_id_range_start_range_end_idx \
	(_Anum_dimension_slice_dimension_id_range_start_range_end_idx_max - 1)
	
enum
{
	DIMENSION_SLICE_ID_IDX = 0,
	DIMENSION_SLICE_DIMENSION_ID_RANGE_START_RANGE_END_IDX,
	_MAX_DIMENSION_SLICE_INDEX,
};

/*************************
 *
 * Chunk table definitions
 *
 *************************/

#define CHUNK_TABLE_NAME "chunk"

enum Anum_chunk
{
	Anum_chunk_id = 1,
	Anum_chunk_hypertable_id,
	Anum_chunk_schema_name,
	Anum_chunk_table_name,
	_Anum_chunk_max,
};

#define Natts_chunk \
	(_Anum_chunk_max - 1)

typedef struct FormData_chunk
{
	int32 id;
	int32 hypertable_id;
    NameData schema_name;
	NameData table_name;
} FormData_chunk;

typedef FormData_chunk *Form_chunk;

enum
{
	CHUNK_ID_INDEX = 0,
	CHUNK_HYPERTABLE_ID_INDEX,
	_MAX_CHUNK_INDEX,
};

/************************************
 *
 * Chunk constraint table definitions
 *
 ************************************/

#define CHUNK_CONSTRAINT_TABLE_NAME "chunk_constraint"

enum Anum_chunk_constraint
{
	Anum_chunk_constraint_dimension_slice_id = 1,
	Anum_chunk_constraint_chunk_id,
	_Anum_chunk_constraint_max,
};

#define Natts_chunk_constraint \
	(_Anum_chunk_constraint_max - 1)

typedef struct FormData_chunk_constraint
{
	int32 dimension_slice_id;
	int32 chunk_id;
} FormData_chunk_constraint;

typedef FormData_chunk_constraint *Form_chunk_constraint;

enum
{
	CHUNK_CONSTRAINT_ID_IDX = 0,
	_MAX_CHUNK_CONSTRAINT_INDEX,
};

#define MAX(a, b) \
	((long)(a) > (long)(b) ? (a) : (b))

#define _MAX_TABLE_INDEXES								\
	MAX(_MAX_HYPERTABLE_INDEX,							\
		MAX(_MAX_DIMENSION_INDEX,						\
			MAX(_MAX_DIMENSION_SLICE_INDEX,				\
				MAX(_MAX_CHUNK_CONSTRAINT_INDEX,		\
					_MAX_CHUNK_INDEX))))

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
