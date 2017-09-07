#include <postgres.h>
#include <catalog/pg_namespace.h>
#include <catalog/namespace.h>
#include <catalog/indexing.h>
#include <utils/lsyscache.h>
#include <utils/builtins.h>
#include <utils/syscache.h>
#include <access/xact.h>
#include <access/htup_details.h>
#include <miscadmin.h>
#include <commands/dbcommands.h>
#include <commands/sequence.h>
#include <access/xact.h>

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

static const TableIndexDef catalog_table_index_definitions[_MAX_CATALOG_TABLES] = {
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
			[CHUNK_CONSTRAINT_CHUNK_ID_DIMENSION_SLICE_ID_IDX] = "chunk_constraint_chunk_id_dimension_slice_id_idx",
		}
	}
};

static const char *catalog_table_serial_id_names[_MAX_CATALOG_TABLES] = {
	[HYPERTABLE] = CATALOG_SCHEMA_NAME ".hypertable_id_seq",
	[DIMENSION] = CATALOG_SCHEMA_NAME ".dimension_id_seq",
	[DIMENSION_SLICE] = CATALOG_SCHEMA_NAME ".dimension_slice_id_seq",
	[CHUNK] = CATALOG_SCHEMA_NAME ".chunk_id_seq",
	[CHUNK_CONSTRAINT] = NULL,
};

typedef struct InternalFunctionDef
{
	char	   *name;
	size_t		args;
} InternalFunctionDef;

const static InternalFunctionDef internal_function_definitions[_MAX_INTERNAL_FUNCTIONS] = {
	[DDL_CHANGE_OWNER] = {
		.name = "ddl_change_owner",
		.args = 2,
	},
	[DDL_ADD_CONSTRAINT] = {
		.name = "add_constraint_by_name",
		.args = 2,
	},
	[DDL_DROP_CONSTRAINT] = {
		.name = "drop_constraint",
		.args = 2,
	},
	[DDL_DROP_HYPERTABLE] = {
		.name = "drop_hypertable",
		.args = 2
	},
	[DDL_RENAME_HYPERTABLE] = {
		.name = "rename_hypertable",
		.args = 4
	},
	[DDL_RENAME_COLUMN] = {
		.name = "rename_column",
		.args = 3
	},
	[DDL_CHANGE_COLUMN_TYPE] = {
		.name = "change_column_type",
		.args = 3
	},
	[CHUNK_CREATE] = {
		.name = "chunk_create",
		.args = 4
	}
};

/* Names for proxy tables used for cache invalidation. Must match names in
 * sql/cache.sql */
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

/*
 * Get the user ID of the catalog owner.
 */
static Oid
catalog_owner(void)
{
	HeapTuple	tuple;
	Oid			owner_oid;
	Oid			nsp_oid = get_namespace_oid(CATALOG_SCHEMA_NAME, false);

	tuple = SearchSysCache1(NAMESPACEOID, ObjectIdGetDatum(nsp_oid));

	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_SCHEMA),
				 errmsg("schema with OID %u does not exist", nsp_oid)));

	owner_oid = ((Form_pg_namespace) GETSTRUCT(tuple))->nspowner;

	ReleaseSysCache(tuple);

	return owner_oid;
}

Catalog *
catalog_get(void)
{
	int			i;

	if (!OidIsValid(MyDatabaseId))
		elog(ERROR, "Invalid database ID");

	if (MyDatabaseId == catalog.database_id)
		return &catalog;

	if (!extension_is_loaded() || !IsTransactionState())
		return &catalog;

	memset(&catalog, 0, sizeof(Catalog));
	catalog.database_id = MyDatabaseId;
	strncpy(catalog.database_name, get_database_name(MyDatabaseId), NAMEDATALEN);
	catalog.schema_id = get_namespace_oid(CATALOG_SCHEMA_NAME, false);
	catalog.owner_uid = catalog_owner();

	if (catalog.schema_id == InvalidOid)
		elog(ERROR, "Oid lookup failed for schema %s", CATALOG_SCHEMA_NAME);

	for (i = 0; i < _MAX_CATALOG_TABLES; i++)
	{
		Oid			id;
		const char *sequence_name;
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
		sequence_name = catalog_table_serial_id_names[i];

		if (NULL != sequence_name)
		{
			RangeVar   *sequence;

			sequence = makeRangeVarFromNameList(stringToQualifiedNameList(sequence_name));
			catalog.tables[i].serial_relid = RangeVarGetRelid(sequence, NoLock, false);
		}
		else
			catalog.tables[i].serial_relid = InvalidOid;
	}

	catalog.cache_schema_id = get_namespace_oid(CACHE_SCHEMA_NAME, false);

	for (i = 0; i < _MAX_CACHE_TYPES; i++)
		catalog.caches[i].inval_proxy_id = get_relname_relid(cache_proxy_table_names[i],
													catalog.cache_schema_id);

	catalog.internal_schema_id = get_namespace_oid(INTERNAL_SCHEMA_NAME, false);
	for (i = 0; i < _MAX_INTERNAL_FUNCTIONS; i++)
	{
		InternalFunctionDef def = internal_function_definitions[i];
		FuncCandidateList funclist =
		FuncnameGetCandidates(list_make2(makeString(INTERNAL_SCHEMA_NAME), makeString(def.name)),
							  def.args, NULL, false, false, false);

		if (funclist == NULL || funclist->next)
			elog(ERROR, "Oid lookup failed for the function %s with %lu args", def.name, def.args);

		catalog.functions[i].function_id = funclist->oid;
	}


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
catalog_get_internal_function_id(Catalog *catalog, InternalFunction func)
{
	return catalog->functions[func].function_id;
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

/*
 * Become the user that owns the catalog schema.
 *
 * This might be necessary for users that do operations that require changes to
 * the catalog.
 *
 * The caller should pass a CatalogSecurityContext where the current security
 * context will be saved. The original security context can later be restored
 * with catalog_restore_user().
 */
bool
catalog_become_owner(Catalog *catalog, CatalogSecurityContext *sec_ctx)
{
	GetUserIdAndSecContext(&sec_ctx->saved_uid, &sec_ctx->saved_security_context);

	if (sec_ctx->saved_uid != catalog->owner_uid)
	{
		SetUserIdAndSecContext(catalog->owner_uid,
			 sec_ctx->saved_security_context | SECURITY_LOCAL_USERID_CHANGE);
		return true;
	}

	return false;
}

/*
 * Restore the security context of the original user after becoming the catalog
 * owner. The user should pass the original CatalogSecurityContext that was used
 * with catalog_become_owner().
 */
void
catalog_restore_user(CatalogSecurityContext *sec_ctx)
{
	SetUserIdAndSecContext(sec_ctx->saved_uid, sec_ctx->saved_security_context);
}

/*
 * Get the next serial ID for a catalog table, if one exists for the given table.
 */
int64
catalog_table_next_seq_id(Catalog *catalog, enum CatalogTable table)
{
	Oid			relid = catalog->tables[table].serial_relid;

	if (!OidIsValid(relid))
		elog(ERROR, "No serial id column for table %s", catalog_table_names[table]);

	return DatumGetInt64(DirectFunctionCall1(nextval_oid, ObjectIdGetDatum(relid)));
}

/*
 * Insert a new row into a catalog table.
 */
void
catalog_insert(Relation rel, HeapTuple tuple)
{
	simple_heap_insert(rel, tuple);
	CatalogUpdateIndexes(rel, tuple);
	/* Make changes visible */
	CommandCounterIncrement();
}

/*
 * Insert a new row into a catalog table.
 */
void
catalog_insert_values(Relation rel, TupleDesc tupdesc, Datum *values, bool *nulls)
{
	HeapTuple	tuple = heap_form_tuple(tupdesc, values, nulls);

	catalog_insert(rel, tuple);
	heap_freetuple(tuple);
}
