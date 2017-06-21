#include <postgres.h>
#include <catalog/namespace.h>
#include <utils/lsyscache.h>
#include <miscadmin.h>
#include <commands/dbcommands.h>

#include "catalog.h"
#include "extension.h"

static const char *catalog_table_names[_MAX_CATALOG_TABLES] = {
	[HYPERTABLE] = HYPERTABLE_TABLE_NAME,
	[DIMENSION] = DIMENSION_TABLE_NAME,
	[DIMENSION_SLICE] = DIMENSION_SLICE_TABLE_NAME,
	[CHUNK] = CHUNK_TABLE_NAME,
	[CHUNK_CONSTRAINT] = CHUNK_CONSTRAINT_TABLE_NAME
};

typedef struct TableIndexDef
{
	size_t		length;
	char	  **names;
} TableIndexDef;

const static TableIndexDef catalog_table_index_definitions[_MAX_CATALOG_TABLES] = {
	[HYPERTABLE] = {
		.length = _MAX_HYPERTABLE_INDEX,
		.names = (char *[]) {
			[HYPERTABLE_ID_INDEX] = "hypertable_pkey",
			[HYPERTABLE_NAME_INDEX] = "hypertable_schema_name_table_name_key",
		}
	},
	[DIMENSION] = {
		.length = _MAX_DIMENSION_INDEX,
		.names = (char *[]) {
			[DIMENSION_ID_IDX] = "dimension_pkey",
			[DIMENSION_HYPERTABLE_ID_IDX] = "dimension_hypertable_id_idx",
		}
	},
	[DIMENSION_SLICE] = {
		.length = _MAX_DIMENSION_SLICE_INDEX,
		.names = (char *[]) {
			[DIMENSION_SLICE_ID_IDX] = "dimension_slice_pkey",
			[DIMENSION_SLICE_DIMENSION_ID_RANGE_START_RANGE_END_IDX] = "dimension_slice_dimension_id_range_start_range_end_idx",
		}
	},
	[CHUNK] = {
		.length = _MAX_CHUNK_INDEX,
		.names = (char *[]) {
			[CHUNK_ID_INDEX] = "chunk_pkey",
			[CHUNK_HYPERTABLE_ID_INDEX] = "chunk_hypertable_id_idx",
		}
	},
	[CHUNK_CONSTRAINT] = {
		.length = _MAX_CHUNK_CONSTRAINT_INDEX,
		.names = (char *[]) {
			[CHUNK_CONSTRAINT_CHUNK_ID_DIMENSION_SLICE_ID_IDX] = "chunk_constraint_pkey",
		}
	}
};

/* Names for proxy tables used for cache invalidation. Must match names in
 * sql/common/caches.sql */
static const char *cache_proxy_table_names[_MAX_CACHE_TYPES] = {
	[CACHE_TYPE_HYPERTABLE] = "cache_inval_hypertable",
	[CACHE_TYPE_CHUNK] = "cache_inval_chunk",
};

/* Catalog information for the current database. */
static Catalog catalog = {
	.database_id = InvalidOid,
};

bool
catalog_is_valid(Catalog *catalog)
{
	return catalog != NULL && OidIsValid(catalog->database_id);
}

Catalog *
catalog_get(void)
{
	int			i;

	if (!OidIsValid(MyDatabaseId))
		elog(ERROR, "Invalid database ID");

	if (MyDatabaseId == catalog.database_id)
		return &catalog;

	if (!extension_is_loaded())
		return &catalog;

	memset(&catalog, 0, sizeof(Catalog));
	catalog.database_id = MyDatabaseId;
	strncpy(catalog.database_name, get_database_name(MyDatabaseId), NAMEDATALEN);
	catalog.schema_id = get_namespace_oid(CATALOG_SCHEMA_NAME, false);

	if (catalog.schema_id == InvalidOid)
		elog(ERROR, "Oid lookup failed for schema %s", CATALOG_SCHEMA_NAME);

	for (i = 0; i < _MAX_CATALOG_TABLES; i++)
	{
		Oid			id;
		int			number_indexes,
					j;

		id = get_relname_relid(catalog_table_names[i], catalog.schema_id);

		if (id == InvalidOid)
			elog(ERROR, "Oid lookup failed for table %s", catalog_table_names[i]);

		catalog.tables[i].id = id;

		number_indexes = catalog_table_index_definitions[i].length;
		Assert(number_indexes <= _MAX_TABLE_INDEXES);

		for (j = 0; j < number_indexes; j++)
		{
			id = get_relname_relid(catalog_table_index_definitions[i].names[j],
								   catalog.schema_id);

			if (id == InvalidOid)
				elog(ERROR, "Oid lookup failed for table index %s",
					 catalog_table_index_definitions[i].names[j]);

			catalog.tables[i].index_ids[j] = id;
		}

		catalog.tables[i].name = catalog_table_names[i];
	}

	catalog.cache_schema_id = get_namespace_oid(CACHE_SCHEMA_NAME, false);

	for (i = 0; i < _MAX_CACHE_TYPES; i++)
		catalog.caches[i].inval_proxy_id = get_relname_relid(cache_proxy_table_names[i],
															 catalog.cache_schema_id);

	return &catalog;
}

void
catalog_reset(void)
{
	catalog.database_id = InvalidOid;
}

const char *
catalog_get_cache_proxy_name(CacheType type)
{
	return cache_proxy_table_names[type];
}

Oid
catalog_get_cache_proxy_id(Catalog *catalog, CacheType type)
{
	return catalog->caches[type].inval_proxy_id;
}

Oid
catalog_get_cache_proxy_id_by_name(Catalog *catalog, const char *relname)
{
	int			i;

	if (!catalog_is_valid(catalog))
		return InvalidOid;

	for (i = 0; i < _MAX_CACHE_TYPES; i++)
	{
		if (strcmp(relname, cache_proxy_table_names[i]) == 0)
			break;
	}

	if (_MAX_CACHE_TYPES == i)
		return InvalidOid;

	return catalog->caches[i].inval_proxy_id;
}
