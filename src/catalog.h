#ifndef TIMESCALEDB_CATALOG_H
#define TIMESCALEDB_CATALOG_H

#include <postgres.h>

#include <utils/rel.h>
#include <nodes/nodes.h>
#include <access/heapam.h>
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
typedef enum CatalogTable
{
	HYPERTABLE = 0,
	DIMENSION,
	DIMENSION_SLICE,
	CHUNK,
	CHUNK_CONSTRAINT,
	CHUNK_INDEX,
	TABLESPACE,
	_MAX_CATALOG_TABLES,
} CatalogTable;

#define INVALID_CATALOG_TABLE _MAX_CATALOG_TABLES
#define INVALID_INDEXID -1

#define CATALOG_INDEX(catalog, tableid, indexid) \
	(indexid == INVALID_INDEXID ? InvalidOid : (catalog)->tables[tableid].index_ids[indexid])

#define CatalogInternalCall1(func, datum1) \
	OidFunctionCall1(catalog_get_internal_function_id(catalog_get(), func), datum1)
#define CatalogInternalCall2(func, datum1, datum2) \
	OidFunctionCall2(catalog_get_internal_function_id(catalog_get(), func), datum1, datum2)
#define CatalogInternalCall3(func, datum1, datum2, datum3) \
	OidFunctionCall3(catalog_get_internal_function_id(catalog_get(), func), datum1, datum2, datum3)
#define CatalogInternalCall4(func, datum1, datum2, datum3, datum4) \
	OidFunctionCall4(catalog_get_internal_function_id(catalog_get(), func), datum1, datum2, datum3, datum4)

typedef enum InternalFunction
{
	DDL_ADD_CHUNK_CONSTRAINT,
	_MAX_INTERNAL_FUNCTIONS,
} InternalFunction;

#define CATALOG_SCHEMA_NAME "_timescaledb_catalog"
#define CACHE_SCHEMA_NAME "_timescaledb_cache"
#define INTERNAL_SCHEMA_NAME "_timescaledb_internal"

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
	Anum_hypertable_num_dimensions,
	Anum_hypertable_chunk_sizing_func_schema,
	Anum_hypertable_chunk_sizing_func_name,
	Anum_hypertable_chunk_target_size,
	_Anum_hypertable_max,
};

#define Natts_hypertable \
	(_Anum_hypertable_max - 1)

typedef struct FormData_hypertable
{
	int32		id;
	NameData	schema_name;
	NameData	table_name;
	NameData	associated_schema_name;
	NameData	associated_table_prefix;
	int16		num_dimensions;
	NameData	chunk_sizing_func_schema;
	NameData	chunk_sizing_func_name;
	int64		chunk_target_size;
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
	Anum_dimension_aligned,
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
	int32		id;
	int32		hypertable_id;
	NameData	column_name;
	Oid			column_type;
	bool		aligned;
	/* closed (space) columns */
	int16		num_slices;
	NameData	partitioning_func_schema;
	NameData	partitioning_func;
	/* open (time) columns */
	int64		interval_length;
} FormData_dimension;

typedef FormData_dimension *Form_dimension;

enum Anum_dimension_id_idx
{
	Anum_dimension_id_idx_id = 1,
	_Anum_dimension_id_idx_max,
};

#define Natts_dimension_id_idx \
	(_Anum_dimension_id_idx_max - 1)

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
	int32		id;
	int32		dimension_id;
	int64		range_start;
	int64		range_end;
} FormData_dimension_slice;

typedef FormData_dimension_slice *Form_dimension_slice;

enum Anum_dimension_slice_id_idx
{
	Anum_dimension_slice_id_idx_id = 1,
	_Anum_dimension_slice_id_idx_max,
};

#define Natts_dimension_slice_id_idx \
	(_Anum_dimension_slice_id_idx_max - 1)

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
	int32		id;
	int32		hypertable_id;
	NameData	schema_name;
	NameData	table_name;
} FormData_chunk;

typedef FormData_chunk *Form_chunk;

enum
{
	CHUNK_ID_INDEX = 0,
	CHUNK_HYPERTABLE_ID_INDEX,
	CHUNK_SCHEMA_NAME_INDEX,
	_MAX_CHUNK_INDEX,
};

enum Anum_chunk_idx
{
	Anum_chunk_idx_id = 1,
};

enum Anum_chunk_hypertable_id_idx
{
	Anum_chunk_hypertable_id_idx_hypertable_id = 1,
};

enum Anum_chunk_schema_name_idx
{
	Anum_chunk_schema_name_idx_schema_name = 1,
	Anum_chunk_schema_name_idx_table_name,
};


/************************************
 *
 * Chunk constraint table definitions
 *
 ************************************/

#define CHUNK_CONSTRAINT_TABLE_NAME "chunk_constraint"

enum Anum_chunk_constraint
{
	Anum_chunk_constraint_chunk_id = 1,
	Anum_chunk_constraint_dimension_slice_id,
	Anum_chunk_constraint_constraint_name,
	Anum_chunk_constraint_hypertable_constraint_name,
	_Anum_chunk_constraint_max,
};

#define Natts_chunk_constraint \
	(_Anum_chunk_constraint_max - 1)

/* Do Not use GET_STRUCT with FormData_chunk_constraint. It contains NULLS */
typedef struct FormData_chunk_constraint
{
	int32		chunk_id;
	int32		dimension_slice_id;
	NameData	constraint_name;
	NameData	hypertable_constraint_name;
} FormData_chunk_constraint;

typedef FormData_chunk_constraint *Form_chunk_constraint;

enum
{
	CHUNK_CONSTRAINT_CHUNK_ID_CONSTRAINT_NAME_IDX = 0,
	CHUNK_CONSTRAINT_CHUNK_ID_DIMENSION_SLICE_ID_IDX,
	_MAX_CHUNK_CONSTRAINT_INDEX,
};

enum Anum_chunk_constraint_chunk_id_dimension_slice_id_idx
{
	Anum_chunk_constraint_chunk_id_dimension_slice_id_idx_chunk_id = 1,
	Anum_chunk_constraint_chunk_id_dimension_slice_id_idx_dimension_slice_id,
	_Anum_chunk_constraint_chunk_id_dimension_slice_id_idx_max,
};

enum Anum_chunk_constraint_chunk_id_constraint_name_idx
{
	Anum_chunk_constraint_chunk_id_constraint_name_idx_chunk_id = 1,
	Anum_chunk_constraint_chunk_id_constraint_name_idx_constraint_name,
	_Anum_chunk_constraint_chunk_id_constraint_name_idx_max,
};

/************************************
 *
 * Chunk index table definitions
 *
 ************************************/

#define CHUNK_INDEX_TABLE_NAME "chunk_index"

enum Anum_chunk_index
{
	Anum_chunk_index_chunk_id = 1,
	Anum_chunk_index_index_name,
	Anum_chunk_index_hypertable_id,
	Anum_chunk_index_hypertable_index_name,
	_Anum_chunk_index_max,
};

#define Natts_chunk_index \
	(_Anum_chunk_index_max - 1)

typedef struct FormData_chunk_index
{
	int32		chunk_id;
	NameData	index_name;
	int32		hypertable_id;
	NameData	hypertable_index_name;
} FormData_chunk_index;

typedef FormData_chunk_index *Form_chunk_index;

enum
{
	CHUNK_INDEX_CHUNK_ID_INDEX_NAME_IDX = 0,
	CHUNK_INDEX_HYPERTABLE_ID_HYPERTABLE_INDEX_NAME_IDX,
	_MAX_CHUNK_INDEX_INDEX,
};

enum Anum_chunk_index_chunk_id_index_name_idx
{
	Anum_chunk_index_chunk_id_index_name_idx_chunk_id = 1,
	Anum_chunk_index_chunk_id_index_name_idx_index_name,
	_Anum_chunk_index_chunk_id_index_name_idx_max,
};

enum Anum_chunk_index_hypertable_id_hypertable_index_name_idx
{
	Anum_chunk_index_hypertable_id_hypertable_index_name_idx_hypertable_id = 1,
	Anum_chunk_index_hypertable_id_hypertable_index_name_idx_hypertable_index_name,
	Anum_chunk_index_hypertable_id_hypertable_index_name_idx_max,
};

/************************************
 *
 * Tablespace table definitions
 *
 ************************************/

#define TABLESPACE_TABLE_NAME "tablespace"

enum Anum_tablespace
{
	Anum_tablespace_id = 1,
	Anum_tablespace_hypertable_id,
	Anum_tablespace_tablespace_name,
	_Anum_tablespace_max,
};

#define Natts_tablespace \
	(_Anum_tablespace_max - 1)

typedef struct FormData_tablespace
{
	int32		id;
	int32		hypertable_id;
	NameData	tablespace_name;
} FormData_tablespace;

typedef FormData_tablespace *Form_tablespace;

enum
{
	TABLESPACE_PKEY_IDX = 0,
	TABLESPACE_HYPERTABLE_ID_TABLESPACE_NAME_IDX,
	_MAX_TABLESPACE_INDEX,
};

enum Anum_tablespace_pkey_idx
{
	Anum_tablespace_pkey_idx_tablespace_id = 1,
	_Anum_tablespace_pkey_idx_max,
};

typedef struct FormData_tablespace_pkey_idx
{
	int32		tablespace_id;
}			FormData_tablespace_pkey_idx;

enum Anum_tablespace_hypertable_id_tablespace_name_idx
{
	Anum_tablespace_hypertable_id_tablespace_name_idx_hypertable_id = 1,
	Anum_tablespace_hypertable_id_tablespace_name_idx_tablespace_name,
	_Anum_tablespace_hypertable_id_tablespace_name_idx_max,
};

typedef struct FormData_tablespace_hypertable_id_tablespace_name_idx
{
	int32		hypertable_id;
	NameData	tablespace_name;
}			FormData_tablespace_hypertable_id_tablespace_name_idx;


#define MAX(a, b) \
	((long)(a) > (long)(b) ? (a) : (b))

#define _MAX_TABLE_INDEXES								\
	MAX(_MAX_HYPERTABLE_INDEX,							\
		MAX(_MAX_DIMENSION_INDEX,						\
			MAX(_MAX_DIMENSION_SLICE_INDEX,				\
				MAX(_MAX_CHUNK_CONSTRAINT_INDEX,		\
					MAX(_MAX_CHUNK_INDEX_INDEX,			\
						MAX(_MAX_TABLESPACE_INDEX,		\
							_MAX_CHUNK_INDEX))))))

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
		Oid			serial_relid;
	}			tables[_MAX_CATALOG_TABLES];

	Oid			cache_schema_id;
	struct
	{
		Oid			inval_proxy_id;
	}			caches[_MAX_CACHE_TYPES];

	Oid			owner_uid;
	Oid			internal_schema_id;
	struct
	{
		Oid			function_id;
	}			functions[_MAX_INTERNAL_FUNCTIONS];
} Catalog;


typedef struct CatalogSecurityContext
{
	Oid			saved_uid;
	int			saved_security_context;
} CatalogSecurityContext;

bool		catalog_is_valid(Catalog *catalog);
Catalog    *catalog_get(void);
void		catalog_reset(void);

Oid			catalog_get_cache_proxy_id(Catalog *catalog, CacheType type);

Oid			catalog_get_internal_function_id(Catalog *catalog, InternalFunction func);

bool		catalog_become_owner(Catalog *catalog, CatalogSecurityContext *sec_ctx);
void		catalog_restore_user(CatalogSecurityContext *sec_ctx);

int64		catalog_table_next_seq_id(Catalog *catalog, CatalogTable table);
Oid			catalog_table_get_id(Catalog *catalog, CatalogTable table);
CatalogTable catalog_table_get(Catalog *catalog, Oid relid);
const char *catalog_table_name(CatalogTable table);

void		catalog_insert(Relation rel, HeapTuple tuple);
void		catalog_insert_values(Relation rel, TupleDesc tupdesc, Datum *values, bool *nulls);
void		catalog_update_tid(Relation rel, ItemPointer tid, HeapTuple tuple);
void		catalog_update(Relation rel, HeapTuple tuple);
void		catalog_delete_tid(Relation rel, ItemPointer tid);
void		catalog_delete(Relation rel, HeapTuple tuple);
void		catalog_invalidate_cache(Oid catalog_relid, CmdType operation);

/* Delete only: do not increment command counter or invalidate caches */
void		catalog_delete_only(Relation rel, HeapTuple tuple);

#endif							/* TIMESCALEDB_CATALOG_H */
