#include <postgres.h>
#include <catalog/pg_namespace.h>
#include <catalog/namespace.h>
#include <catalog/indexing.h>
#include <utils/lsyscache.h>
#include <utils/builtins.h>
#include <utils/syscache.h>
#include <utils/inval.h>
#include <access/xact.h>
#include <access/htup_details.h>
#include <miscadmin.h>
#include <commands/dbcommands.h>
#include <commands/sequence.h>
#include <access/xact.h>

#include "compat.h"
#include "catalog.h"
#include "extension.h"

#if PG10
#include <utils/regproc.h>
#endif

static const char *catalog_table_names[_MAX_CATALOG_TABLES + 1] = {
	[HYPERTABLE] = HYPERTABLE_TABLE_NAME,
	[DIMENSION] = DIMENSION_TABLE_NAME,
	[DIMENSION_SLICE] = DIMENSION_SLICE_TABLE_NAME,
	[CHUNK] = CHUNK_TABLE_NAME,
	[CHUNK_CONSTRAINT] = CHUNK_CONSTRAINT_TABLE_NAME,
	[CHUNK_INDEX] = CHUNK_INDEX_TABLE_NAME,
	[TABLESPACE] = TABLESPACE_TABLE_NAME,
	[_MAX_CATALOG_TABLES] = "invalid table",
};

typedef struct TableIndexDef
{
	int			length;
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
			[DIMENSION_SLICE_DIMENSION_ID_RANGE_START_RANGE_END_IDX] = "dimension_slice_dimension_id_range_start_range_end_key",
		}
	},
	[CHUNK] = {
		.length = _MAX_CHUNK_INDEX,
		.names = (char *[]) {
			[CHUNK_ID_INDEX] = "chunk_pkey",
			[CHUNK_HYPERTABLE_ID_INDEX] = "chunk_hypertable_id_idx",
			[CHUNK_SCHEMA_NAME_INDEX] = "chunk_schema_name_table_name_key",
		}
	},
	[CHUNK_CONSTRAINT] = {
		.length = _MAX_CHUNK_CONSTRAINT_INDEX,
		.names = (char *[]) {
			[CHUNK_CONSTRAINT_CHUNK_ID_CONSTRAINT_NAME_IDX] = "chunk_constraint_chunk_id_constraint_name_key",
			[CHUNK_CONSTRAINT_CHUNK_ID_DIMENSION_SLICE_ID_IDX] = "chunk_constraint_chunk_id_dimension_slice_id_idx",
		}
	},
	[CHUNK_INDEX] = {
		.length = _MAX_CHUNK_INDEX_INDEX,
		.names = (char *[]) {
			[CHUNK_INDEX_CHUNK_ID_INDEX_NAME_IDX] = "chunk_index_chunk_id_index_name_key",
			[CHUNK_INDEX_HYPERTABLE_ID_HYPERTABLE_INDEX_NAME_IDX] = "chunk_index_hypertable_id_hypertable_index_name_idx",
		}
	},
	[TABLESPACE] = {
		.length = _MAX_TABLESPACE_INDEX,
		.names = (char *[]) {
			[TABLESPACE_PKEY_IDX] = "tablespace_pkey",
			[TABLESPACE_HYPERTABLE_ID_TABLESPACE_NAME_IDX] = "tablespace_hypertable_id_tablespace_name_key",
		}
	}
};

static const char *catalog_table_serial_id_names[_MAX_CATALOG_TABLES] = {
	[HYPERTABLE] = CATALOG_SCHEMA_NAME ".hypertable_id_seq",
	[DIMENSION] = CATALOG_SCHEMA_NAME ".dimension_id_seq",
	[DIMENSION_SLICE] = CATALOG_SCHEMA_NAME ".dimension_slice_id_seq",
	[CHUNK] = CATALOG_SCHEMA_NAME ".chunk_id_seq",
	[CHUNK_CONSTRAINT] = CATALOG_SCHEMA_NAME ".chunk_constraint_name",
	[CHUNK_INDEX] = NULL,
	[TABLESPACE] = CATALOG_SCHEMA_NAME ".tablespace_id_seq",
};

typedef struct InternalFunctionDef
{
	char	   *name;
	int			args;
} InternalFunctionDef;

const static InternalFunctionDef internal_function_definitions[_MAX_INTERNAL_FUNCTIONS] = {
	[DDL_ADD_CHUNK_CONSTRAINT] = {
		.name = "chunk_constraint_add_table_constraint",
		.args = 1,
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
		elog(ERROR, "invalid database ID");

	if (MyDatabaseId == catalog.database_id)
		return &catalog;

	if (!extension_is_loaded() || !IsTransactionState())
		return &catalog;

	memset(&catalog, 0, sizeof(Catalog));
	catalog.database_id = MyDatabaseId;
	StrNCpy(catalog.database_name, get_database_name(MyDatabaseId), NAMEDATALEN);
	catalog.schema_id = get_namespace_oid(CATALOG_SCHEMA_NAME, false);
	catalog.owner_uid = catalog_owner();

	if (catalog.schema_id == InvalidOid)
		elog(ERROR, "OID lookup failed for schema \"%s\"", CATALOG_SCHEMA_NAME);

	for (i = 0; i < _MAX_CATALOG_TABLES; i++)
	{
		Oid			id;
		const char *sequence_name;
		Size		number_indexes,
					j;

		id = get_relname_relid(catalog_table_names[i], catalog.schema_id);

		if (id == InvalidOid)
			elog(ERROR, "OID lookup failed for table \"%s\"", catalog_table_names[i]);

		catalog.tables[i].id = id;

		number_indexes = catalog_table_index_definitions[i].length;
		Assert(number_indexes <= _MAX_TABLE_INDEXES);

		for (j = 0; j < number_indexes; j++)
		{
			id = get_relname_relid(catalog_table_index_definitions[i].names[j],
								   catalog.schema_id);

			if (id == InvalidOid)
				elog(ERROR, "OID lookup failed for table index \"%s\"",
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
		FuncnameGetCandidates(list_make2(makeString(INTERNAL_SCHEMA_NAME),
										 makeString(def.name)),
							  def.args, NULL, false, false, false);

		if (funclist == NULL || funclist->next)
			elog(ERROR, "OID lookup failed for the function \"%s\" with %d args", def.name, def.args);

		catalog.functions[i].function_id = funclist->oid;
	}


	return &catalog;
}

void
catalog_reset(void)
{
	catalog.database_id = InvalidOid;
}

Oid
catalog_get_cache_proxy_id(Catalog *catalog, CacheType type)
{
	if (!catalog_is_valid(catalog))
	{
		Oid			schema;

		/*
		 * The catalog can be invalid during upgrade scripts. Try a non-cached
		 * relation lookup, but we need to be in a transaction for
		 * get_namespace_oid() to work.
		 */
		if (!IsTransactionState())
			return InvalidOid;

		schema = get_namespace_oid(CACHE_SCHEMA_NAME, true);

		if (!OidIsValid(schema))
			return InvalidOid;

		return get_relname_relid(cache_proxy_table_names[type], schema);
	}

	return catalog->caches[type].inval_proxy_id;
}

Oid
catalog_get_internal_function_id(Catalog *catalog, InternalFunction func)
{
	return catalog->functions[func].function_id;
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

Oid
catalog_table_get_id(Catalog *catalog, CatalogTable table)
{
	return catalog->tables[table].id;
}

CatalogTable
catalog_table_get(Catalog *catalog, Oid relid)
{
	unsigned int i;

	if (!catalog_is_valid(catalog))
	{
		const char *relname = get_rel_name(relid);

		for (i = 0; i < _MAX_CATALOG_TABLES; i++)
			if (strcmp(catalog_table_names[i], relname) == 0)
				return (CatalogTable) i;

		return INVALID_CATALOG_TABLE;
	}

	for (i = 0; i < _MAX_CATALOG_TABLES; i++)
		if (catalog->tables[i].id == relid)
			return (CatalogTable) i;

	return INVALID_CATALOG_TABLE;
}

const char *
catalog_table_name(CatalogTable table)
{
	return catalog_table_names[table];
}

/*
 * Get the next serial ID for a catalog table, if one exists for the given table.
 */
int64
catalog_table_next_seq_id(Catalog *catalog, CatalogTable table)
{
	Oid			relid = catalog->tables[table].serial_relid;

	if (!OidIsValid(relid))
		elog(ERROR, "no serial ID column for table \"%s\"", catalog_table_names[table]);

	return DatumGetInt64(DirectFunctionCall1(nextval_oid, ObjectIdGetDatum(relid)));
}

#if PG96
#define CatalogTupleInsert(relation, tuple)		\
	do {										\
		simple_heap_insert(relation, tuple);	\
		CatalogUpdateIndexes(relation, tuple);	\
	} while (0);

#define CatalogTupleUpdate(relation, tid, tuple)	\
	do {											\
		simple_heap_update(relation, tid, tuple);	\
		CatalogUpdateIndexes(relation, tuple);		\
	} while (0);

#define CatalogTupleDelete(relation, tid)		\
	simple_heap_delete(relation, tid);

#endif							/* PG96 */

/*
 * Insert a new row into a catalog table.
 */
void
catalog_insert(Relation rel, HeapTuple tuple)
{
	CatalogTupleInsert(rel, tuple);
	catalog_invalidate_cache(RelationGetRelid(rel), CMD_INSERT);
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

void
catalog_update_tid(Relation rel, ItemPointer tid, HeapTuple tuple)
{
	CatalogTupleUpdate(rel, tid, tuple);
	catalog_invalidate_cache(RelationGetRelid(rel), CMD_UPDATE);
	/* Make changes visible */
	CommandCounterIncrement();
}

void
catalog_update(Relation rel, HeapTuple tuple)
{
	catalog_update_tid(rel, &tuple->t_self, tuple);
}

void
catalog_delete_tid(Relation rel, ItemPointer tid)
{
	CatalogTupleDelete(rel, tid);
	catalog_invalidate_cache(RelationGetRelid(rel), CMD_DELETE);
	CommandCounterIncrement();
}

void
catalog_delete(Relation rel, HeapTuple tuple)
{
	catalog_delete_tid(rel, &tuple->t_self);
}

void
catalog_delete_only(Relation rel, HeapTuple tuple)
{
	CatalogTupleDelete(rel, &tuple->t_self);
}

/*
 * Invalidate TimescaleDB catalog caches.
 *
 * This function should be called whenever a TimescaleDB catalog table changes
 * in a way that might invalidate associated caches. It is currently called in
 * two distinct ways:
 *
 * 1. If a catalog table changes via the catalog API in catalog.c
 * 2. Via a trigger if a SQL INSERT/UPDATE/DELETE occurs on a catalog table
 *
 * Since triggers (2) require full parsing, planning and execution of SQL
 * statements, they aren't supported for simple catalog updates via (1) in
 * native code and are therefore discouraged. Ideally, catalog updates should
 * happen consistently via method (1) in the future, obviating the need for
 * triggers on catalog tables that cause side effects.
 *
 * The invalidation event is signaled to other backends (processes) via the
 * relcache invalidation mechanism on a dummy relation (table).
 *
 * Parameters: The OID of the catalog table that changed, and the operation
 * involved (e.g., INSERT, UPDATE, DELETE).
 */
void
catalog_invalidate_cache(Oid catalog_relid, CmdType operation)
{
	Catalog    *catalog = catalog_get();
	CatalogTable table = catalog_table_get(catalog, catalog_relid);
	Oid			relid;

	switch (table)
	{
		case CHUNK:
		case CHUNK_CONSTRAINT:
		case DIMENSION_SLICE:
			if (operation == CMD_UPDATE || operation == CMD_DELETE)
			{
				relid = catalog_get_cache_proxy_id(catalog, CACHE_TYPE_HYPERTABLE);
				CacheInvalidateRelcacheByRelid(relid);
			}
			break;
		case HYPERTABLE:
		case DIMENSION:
			relid = catalog_get_cache_proxy_id(catalog, CACHE_TYPE_HYPERTABLE);
			CacheInvalidateRelcacheByRelid(relid);
			break;
		case CHUNK_INDEX:
		default:
			break;
	}
}
