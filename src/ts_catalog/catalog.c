/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <access/htup_details.h>
#include <access/xact.h>
#include <catalog/indexing.h>
#include <catalog/namespace.h>
#include <catalog/pg_namespace.h>
#include <commands/dbcommands.h>
#include <commands/sequence.h>
#include <miscadmin.h>
#include <utils/builtins.h>
#include <utils/inval.h>
#include <utils/lsyscache.h>
#include <utils/regproc.h>
#include <utils/syscache.h>

#include "compat/compat.h"
#include "cache_invalidate.h"
#include "extension.h"
#include "ts_catalog/catalog.h"
#include "utils.h"

static const TableInfoDef catalog_table_names[_MAX_CATALOG_TABLES + 1] = {
	[HYPERTABLE] = {
		.schema_name = CATALOG_SCHEMA_NAME,
		.table_name = HYPERTABLE_TABLE_NAME,
	},
	[DIMENSION] = {
		.schema_name = CATALOG_SCHEMA_NAME,
		.table_name = DIMENSION_TABLE_NAME,
	},
	[DIMENSION_SLICE] = {
		.schema_name = CATALOG_SCHEMA_NAME,
		.table_name = DIMENSION_SLICE_TABLE_NAME,
	},
	[CHUNK] = {
		.schema_name = CATALOG_SCHEMA_NAME,
		.table_name = CHUNK_TABLE_NAME,
	},
	[CHUNK_CONSTRAINT] = {
		.schema_name = CATALOG_SCHEMA_NAME,
		.table_name = CHUNK_CONSTRAINT_TABLE_NAME,
	},
	[CHUNK_INDEX] = {
		.schema_name = CATALOG_SCHEMA_NAME,
		.table_name = CHUNK_INDEX_TABLE_NAME,
	},
	[TABLESPACE] = {
		.schema_name = CATALOG_SCHEMA_NAME,
		.table_name = TABLESPACE_TABLE_NAME,
	},
	[BGW_JOB] = {
		.schema_name = CONFIG_SCHEMA_NAME,
		.table_name = BGW_JOB_TABLE_NAME,
	},
	[BGW_JOB_STAT] = {
		.schema_name = INTERNAL_SCHEMA_NAME,
		.table_name = BGW_JOB_STAT_TABLE_NAME,
	},
	[BGW_JOB_STAT_HISTORY] = {
		.schema_name = INTERNAL_SCHEMA_NAME,
		.table_name = BGW_JOB_STAT_HISTORY_TABLE_NAME,
	},
	[METADATA] = {
		.schema_name = CATALOG_SCHEMA_NAME,
		.table_name = METADATA_TABLE_NAME,
	},
	[BGW_POLICY_CHUNK_STATS] = {
		.schema_name = INTERNAL_SCHEMA_NAME,
		.table_name = BGW_POLICY_CHUNK_STATS_TABLE_NAME,
	},
	[CONTINUOUS_AGG] = {
		.schema_name = CATALOG_SCHEMA_NAME,
		.table_name = CONTINUOUS_AGG_TABLE_NAME,
	},
	[CONTINUOUS_AGGS_HYPERTABLE_INVALIDATION_LOG] = {
		.schema_name = CATALOG_SCHEMA_NAME,
		.table_name = CONTINUOUS_AGGS_HYPERTABLE_INVALIDATION_LOG_TABLE_NAME,
	},
	[CONTINUOUS_AGGS_INVALIDATION_THRESHOLD] = {
		.schema_name = CATALOG_SCHEMA_NAME,
		.table_name = CONTINUOUS_AGGS_INVALIDATION_THRESHOLD_TABLE_NAME,
	},
	[CONTINUOUS_AGGS_MATERIALIZATION_INVALIDATION_LOG] = {
		.schema_name = CATALOG_SCHEMA_NAME,
		.table_name = CONTINUOUS_AGGS_MATERIALIZATION_INVALIDATION_LOG_TABLE_NAME,
	},
	[COMPRESSION_SETTINGS] = {
		.schema_name = CATALOG_SCHEMA_NAME,
		.table_name = COMPRESSION_SETTINGS_TABLE_NAME,
	},
	[COMPRESSION_CHUNK_SIZE] = {
		.schema_name = CATALOG_SCHEMA_NAME,
		.table_name = COMPRESSION_CHUNK_SIZE_TABLE_NAME,
	},
	[CONTINUOUS_AGGS_BUCKET_FUNCTION] = {
		.schema_name = CATALOG_SCHEMA_NAME,
		.table_name = CONTINUOUS_AGGS_BUCKET_FUNCTION_TABLE_NAME,
	},
	[CONTINUOUS_AGGS_WATERMARK] = {
		.schema_name = CATALOG_SCHEMA_NAME,
		.table_name = CONTINUOUS_AGGS_WATERMARK_TABLE_NAME,
	},
	[TELEMETRY_EVENT] = {
		.schema_name = CATALOG_SCHEMA_NAME,
		.table_name = TELEMETRY_EVENT_TABLE_NAME,
	},
	[CHUNK_COLUMN_STATS] = {
		.schema_name = CATALOG_SCHEMA_NAME,
		.table_name = CHUNK_COLUMN_STATS_TABLE_NAME,
	},
	[_MAX_CATALOG_TABLES] = {
		.schema_name = "invalid schema",
		.table_name = "invalid table",
	}
};

static const TableIndexDef catalog_table_index_definitions[_MAX_CATALOG_TABLES] = {
	[HYPERTABLE] = {
		.length = _MAX_HYPERTABLE_INDEX,
		.names = (char *[]) {
			[HYPERTABLE_ID_INDEX] = "hypertable_pkey",
			[HYPERTABLE_NAME_INDEX] = "hypertable_table_name_schema_name_key",
		},
	},
	[DIMENSION] = {
		.length = _MAX_DIMENSION_INDEX,
		.names = (char *[]) {
			[DIMENSION_ID_IDX] = "dimension_pkey",
			[DIMENSION_HYPERTABLE_ID_COLUMN_NAME_IDX] = "dimension_hypertable_id_column_name_key",
		},
	},
	[DIMENSION_SLICE] = {
		.length = _MAX_DIMENSION_SLICE_INDEX,
		.names = (char *[]) {
			[DIMENSION_SLICE_ID_IDX] = "dimension_slice_pkey",
			[DIMENSION_SLICE_DIMENSION_ID_RANGE_START_RANGE_END_IDX] = "dimension_slice_dimension_id_range_start_range_end_key",
		},
	},
	[CHUNK_COLUMN_STATS] = {
		.length = _MAX_CHUNK_COLUMN_STATS_INDEX,
		.names = (char *[]) {
			[CHUNK_COLUMN_STATS_ID_IDX] = "chunk_column_stats_pkey",
			[CHUNK_COLUMN_STATS_HT_ID_CHUNK_ID_COLUMN_NAME_IDX] = "chunk_column_stats_ht_id_chunk_id_colname_key",
		},
	},
	[CHUNK] = {
		.length = _MAX_CHUNK_INDEX,
		.names = (char *[]) {
			[CHUNK_ID_INDEX] = "chunk_pkey",
			[CHUNK_HYPERTABLE_ID_INDEX] = "chunk_hypertable_id_idx",
			[CHUNK_SCHEMA_NAME_INDEX] = "chunk_schema_name_table_name_key",
			[CHUNK_COMPRESSED_CHUNK_ID_INDEX] = "chunk_compressed_chunk_id_idx",
			[CHUNK_OSM_CHUNK_INDEX] = "chunk_osm_chunk_idx",
			[CHUNK_HYPERTABLE_ID_CREATION_TIME_INDEX] = "chunk_hypertable_id_creation_time_idx",
		},
	},
	[CHUNK_CONSTRAINT] = {
		.length = _MAX_CHUNK_CONSTRAINT_INDEX,
		.names = (char *[]) {
			[CHUNK_CONSTRAINT_CHUNK_ID_CONSTRAINT_NAME_IDX] = "chunk_constraint_chunk_id_constraint_name_key",
			[CHUNK_CONSTRAINT_DIMENSION_SLICE_ID_IDX] = "chunk_constraint_dimension_slice_id_idx",
		},
	},
	[CHUNK_INDEX] = {
		.length = _MAX_CHUNK_INDEX_INDEX,
		.names = (char *[]) {
			[CHUNK_INDEX_CHUNK_ID_INDEX_NAME_IDX] = "chunk_index_chunk_id_index_name_key",
			[CHUNK_INDEX_HYPERTABLE_ID_HYPERTABLE_INDEX_NAME_IDX] = "chunk_index_hypertable_id_hypertable_index_name_idx",
		},
	},
	[TABLESPACE] = {
		.length = _MAX_TABLESPACE_INDEX,
		.names = (char *[]) {
			[TABLESPACE_PKEY_IDX] = "tablespace_pkey",
			[TABLESPACE_HYPERTABLE_ID_TABLESPACE_NAME_IDX] = "tablespace_hypertable_id_tablespace_name_key",
		},
	},
	[BGW_JOB] = {
		.length = _MAX_BGW_JOB_INDEX,
		.names = (char *[]) {
			[BGW_JOB_PKEY_IDX] = "bgw_job_pkey",
			[BGW_JOB_PROC_HYPERTABLE_ID_IDX] = "bgw_job_proc_hypertable_id_idx",
		},
	},
	[BGW_JOB_STAT] = {
		.length = _MAX_BGW_JOB_STAT_INDEX,
		.names = (char *[]) {
			[BGW_JOB_STAT_PKEY_IDX] = "bgw_job_stat_pkey",
		},
	},
	[BGW_JOB_STAT_HISTORY] = {
		.length = _MAX_BGW_JOB_STAT_HISTORY_INDEX,
		.names = (char *[]) {
			[BGW_JOB_STAT_HISTORY_PKEY_IDX] = "bgw_job_stat_history_pkey",
		},
	},
	[METADATA] = {
		.length = _MAX_METADATA_INDEX,
		.names = (char *[]) {
			[METADATA_PKEY_IDX] = "metadata_pkey",
		},
	},
	[BGW_POLICY_CHUNK_STATS] = {
		.length = _MAX_BGW_POLICY_CHUNK_STATS_INDEX,
		.names = (char *[]) {
			[BGW_POLICY_CHUNK_STATS_JOB_ID_CHUNK_ID_IDX] = "bgw_policy_chunk_stats_job_id_chunk_id_key",
		}
	},
	[CONTINUOUS_AGG] = {
		.length = _MAX_CONTINUOUS_AGG_INDEX,
		.names = (char *[]) {
			[CONTINUOUS_AGG_PARTIAL_VIEW_SCHEMA_PARTIAL_VIEW_NAME_KEY] = "continuous_agg_partial_view_schema_partial_view_name_key",
			[CONTINUOUS_AGG_PKEY] = TS_CAGG_CATALOG_IDX,
			[CONTINUOUS_AGG_USER_VIEW_SCHEMA_USER_VIEW_NAME_KEY] = "continuous_agg_user_view_schema_user_view_name_key",
			[CONTINUOUS_AGG_RAW_HYPERTABLE_ID_IDX] = "continuous_agg_raw_hypertable_id_idx"
		},
	},
	[CONTINUOUS_AGGS_HYPERTABLE_INVALIDATION_LOG] = {
		.length = _MAX_CONTINUOUS_AGGS_HYPERTABLE_INVALIDATION_LOG_INDEX,
		.names = (char *[]) {
			[CONTINUOUS_AGGS_HYPERTABLE_INVALIDATION_LOG_IDX] = "continuous_aggs_hypertable_invalidation_log_idx",
		},
	},
	[CONTINUOUS_AGGS_INVALIDATION_THRESHOLD] = {
		.length = _MAX_CONTINUOUS_AGGS_INVALIDATION_THRESHOLD_INDEX,
		.names = (char *[]) {
			[CONTINUOUS_AGGS_INVALIDATION_THRESHOLD_PKEY] = "continuous_aggs_invalidation_threshold_pkey",
		},
	},
	[CONTINUOUS_AGGS_MATERIALIZATION_INVALIDATION_LOG] = {
		.length = _MAX_CONTINUOUS_AGGS_MATERIALIZATION_INVALIDATION_LOG_INDEX,
		.names = (char *[]) {
			[CONTINUOUS_AGGS_MATERIALIZATION_INVALIDATION_LOG_IDX] = "continuous_aggs_materialization_invalidation_log_idx",
		},
	},
	[CONTINUOUS_AGGS_WATERMARK] = {
		.length = _MAX_CONTINUOUS_AGGS_WATERMARK_INDEX,
		.names = (char *[]) {
			[CONTINUOUS_AGGS_WATERMARK_PKEY] = "continuous_aggs_watermark_pkey",
		},
	},
	[COMPRESSION_SETTINGS] = {
		.length =  _MAX_COMPRESSION_SETTINGS_INDEX,
		.names = (char *[]) {
			[COMPRESSION_SETTINGS_PKEY] = "compression_settings_pkey",
			[COMPRESSION_SETTINGS_COMPRESS_RELID_IDX] = "compression_settings_compress_relid_idx",
		},
	},
	[COMPRESSION_CHUNK_SIZE] = {
		.length =  _MAX_COMPRESSION_CHUNK_SIZE_INDEX,
		.names = (char *[]) {
			[COMPRESSION_CHUNK_SIZE_PKEY] = "compression_chunk_size_pkey",
		},
	},
	[CONTINUOUS_AGGS_BUCKET_FUNCTION] = {
		.length = _MAX_CONTINUOUS_AGGS_BUCKET_FUNCTION_INDEX,
		.names = (char *[]) {
			[CONTINUOUS_AGGS_BUCKET_FUNCTION_PKEY_IDX] = "continuous_aggs_bucket_function_pkey",
		},
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
	[BGW_JOB] = CONFIG_SCHEMA_NAME ".bgw_job_id_seq",
	[BGW_JOB_STAT] = NULL,
	[BGW_JOB_STAT_HISTORY] = INTERNAL_SCHEMA_NAME ".bgw_job_stat_history_id_seq",
	[CONTINUOUS_AGGS_HYPERTABLE_INVALIDATION_LOG] = NULL,
	[CONTINUOUS_AGGS_INVALIDATION_THRESHOLD] = NULL,
	[CONTINUOUS_AGGS_MATERIALIZATION_INVALIDATION_LOG] = NULL,
	[COMPRESSION_SETTINGS] = NULL,
	[COMPRESSION_CHUNK_SIZE] = NULL,
	[CHUNK_COLUMN_STATS] = CATALOG_SCHEMA_NAME ".chunk_column_stats_id_seq",
};

typedef struct InternalFunctionDef
{
	char *name;
	int args;
} InternalFunctionDef;

static const InternalFunctionDef internal_function_definitions[_MAX_INTERNAL_FUNCTIONS] = {
	[DDL_ADD_CHUNK_CONSTRAINT] = {
		.name = "chunk_constraint_add_table_constraint",
		.args = 1,
	},
	[DDL_CONSTRAINT_CLONE] = {
		.name = "constraint_clone",
		.args = 2,
	},
};

/* Names for proxy tables used for cache invalidation. Must match names in
 * sql/cache.sql */
static const char *cache_proxy_table_names[_MAX_CACHE_TYPES] = {
	[CACHE_TYPE_HYPERTABLE] = "cache_inval_hypertable",
	[CACHE_TYPE_BGW_JOB] = "cache_inval_bgw_job",
	[CACHE_TYPE_EXTENSION] = "cache_inval_extension",
};

/* Catalog information for the current database. */
static Catalog s_catalog = {
	.initialized = false,
};

static CatalogDatabaseInfo database_info = {
	.database_id = InvalidOid,
};

static bool
catalog_is_valid(Catalog *catalog)
{
	return catalog != NULL && catalog->initialized;
}

/*
 * Get the user ID of the catalog owner.
 */
static Oid
catalog_owner(void)
{
	HeapTuple tuple;
	Oid owner_oid;
	Oid nsp_oid = get_namespace_oid(CATALOG_SCHEMA_NAME, false);

	tuple = SearchSysCache1(NAMESPACEOID, ObjectIdGetDatum(nsp_oid));

	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_SCHEMA),
				 errmsg("schema with OID %u does not exist", nsp_oid)));

	owner_oid = ((Form_pg_namespace) GETSTRUCT(tuple))->nspowner;

	ReleaseSysCache(tuple);

	return owner_oid;
}

static const char *
catalog_table_name(CatalogTable table)
{
	return catalog_table_names[table].table_name;
}

static void
catalog_database_info_init(CatalogDatabaseInfo *info)
{
	info->database_id = MyDatabaseId;
	namestrcpy(&info->database_name, get_database_name(MyDatabaseId));
	info->schema_id = get_namespace_oid(CATALOG_SCHEMA_NAME, false);
	info->owner_uid = catalog_owner();

	if (!OidIsValid(info->schema_id))
		elog(ERROR, "OID lookup failed for schema \"%s\"", CATALOG_SCHEMA_NAME);
}

TSDLLEXPORT CatalogDatabaseInfo *
ts_catalog_database_info_get()
{
	if (!ts_extension_is_loaded())
		elog(ERROR, "tried calling catalog_database_info_get when extension isn't loaded");

	if (!OidIsValid(database_info.database_id))
	{
		if (!IsTransactionState())
			elog(ERROR, "cannot initialize catalog_database_info outside of a transaction");

		memset(&database_info, 0, sizeof(CatalogDatabaseInfo));
		catalog_database_info_init(&database_info);
	}

	return &database_info;
}

/*
 * The rest of the arguments are used to populate the first arg.
 */
void
ts_catalog_table_info_init(CatalogTableInfo *tables_info, int max_tables,
						   const TableInfoDef *table_ary, const TableIndexDef *index_ary,
						   const char **serial_id_ary)
{
	int i;

	for (i = 0; i < max_tables; i++)
	{
		Oid id;
		const char *sequence_name;
		Size number_indexes, j;

		id = ts_get_relation_relid((char *) table_ary[i].schema_name,
								   (char *) table_ary[i].table_name,
								   false);

		if (!OidIsValid(id))
			elog(ERROR,
				 "OID lookup failed for table \"%s.%s\"",
				 table_ary[i].schema_name,
				 table_ary[i].table_name);

		tables_info[i].id = id;

		number_indexes = index_ary[i].length;
		Assert(number_indexes <= _MAX_TABLE_INDEXES);

		for (j = 0; j < number_indexes; j++)
		{
			id = ts_get_relation_relid(table_ary[i].schema_name, index_ary[i].names[j], true);

			if (!OidIsValid(id))
				elog(ERROR, "OID lookup failed for table index \"%s\"", index_ary[i].names[j]);

			tables_info[i].index_ids[j] = id;
		}

		tables_info[i].name = table_ary[i].table_name;
		tables_info[i].schema_name = table_ary[i].schema_name;
		sequence_name = serial_id_ary[i];

		if (NULL != sequence_name)
		{
			RangeVar *sequence;

#if PG16_LT
			sequence = makeRangeVarFromNameList(stringToQualifiedNameList(sequence_name));
#else
			sequence = makeRangeVarFromNameList(stringToQualifiedNameList(sequence_name, NULL));
#endif
			tables_info[i].serial_relid = RangeVarGetRelid(sequence, NoLock, false);
		}
		else
			tables_info[i].serial_relid = InvalidOid;
	}
}

TSDLLEXPORT Catalog *
ts_catalog_get(void)
{
	int i;

	if (!OidIsValid(MyDatabaseId))
		elog(ERROR, "invalid database ID");

	if (!ts_extension_is_loaded())
		elog(ERROR, "tried calling catalog_get when extension isn't loaded");

	if (s_catalog.initialized || !IsTransactionState())
		return &s_catalog;

	memset(&s_catalog, 0, sizeof(Catalog));
	ts_catalog_table_info_init(s_catalog.tables,
							   _MAX_CATALOG_TABLES,
							   catalog_table_names,
							   catalog_table_index_definitions,
							   catalog_table_serial_id_names);

	for (i = 0; i < _TS_MAX_SCHEMA; i++)
		s_catalog.extension_schema_id[i] = get_namespace_oid(ts_extension_schema_names[i], false);

	for (i = 0; i < _MAX_CACHE_TYPES; i++)
		s_catalog.caches[i].inval_proxy_id =
			get_relname_relid(cache_proxy_table_names[i],
							  s_catalog.extension_schema_id[TS_CACHE_SCHEMA]);

	ts_cache_invalidate_set_proxy_tables(s_catalog.caches[CACHE_TYPE_HYPERTABLE].inval_proxy_id,
										 s_catalog.caches[CACHE_TYPE_BGW_JOB].inval_proxy_id);

	for (i = 0; i < _MAX_INTERNAL_FUNCTIONS; i++)
	{
		InternalFunctionDef def = internal_function_definitions[i];
		FuncCandidateList funclist =
			FuncnameGetCandidates(list_make2(makeString(FUNCTIONS_SCHEMA_NAME),
											 makeString(def.name)),
								  def.args,
								  NULL,
								  false,
								  false, /* include_out_arguments */
								  false,
								  false);

		if (funclist == NULL || funclist->next)
			elog(ERROR,
				 "OID lookup failed for the function \"%s\" with %d args",
				 def.name,
				 def.args);

		s_catalog.functions[i].function_id = funclist->oid;
	}
	s_catalog.initialized = true;

	return &s_catalog;
}

void
ts_catalog_reset(void)
{
	s_catalog.initialized = false;
	database_info.database_id = InvalidOid;

	ts_cache_invalidate_set_proxy_tables(InvalidOid, InvalidOid);
}

static CatalogTable
catalog_get_table(Catalog *catalog, Oid relid)
{
	unsigned int i;

	if (!catalog_is_valid(catalog))
	{
		const char *schema_name = get_namespace_name(get_rel_namespace(relid));
		const char *relname = get_rel_name(relid);

		for (i = 0; i < _MAX_CATALOG_TABLES; i++)
			if (strcmp(catalog_table_names[i].schema_name, schema_name) == 0 &&
				strcmp(catalog_table_name(i), relname) == 0)
				return (CatalogTable) i;

		return INVALID_CATALOG_TABLE;
	}

	for (i = 0; i < _MAX_CATALOG_TABLES; i++)
		if (catalog->tables[i].id == relid)
			return (CatalogTable) i;

	return INVALID_CATALOG_TABLE;
}

bool
ts_is_catalog_table(Oid relid)
{
	return catalog_get_table(ts_catalog_get(), relid) != INVALID_CATALOG_TABLE;
}

/*
 * Get the next serial ID for a catalog table, if one exists for the given table.
 */
TSDLLEXPORT int64
ts_catalog_table_next_seq_id(const Catalog *catalog, CatalogTable table)
{
	Oid relid = catalog->tables[table].serial_relid;

	if (!OidIsValid(relid))
		elog(ERROR,
			 "no serial ID column for table \"%s.%s\"",
			 catalog_table_names[table].schema_name,
			 catalog_table_name(table));

	return DatumGetInt64(DirectFunctionCall1(nextval_oid, ObjectIdGetDatum(relid)));
}

Oid
ts_catalog_get_cache_proxy_id(Catalog *catalog, CacheType type)
{
	if (!catalog_is_valid(catalog))
	{
		/*
		 * The catalog can be invalid during upgrade scripts. Try a non-cached
		 * relation lookup, but we need to be in a transaction for
		 * get_namespace_oid() to work.
		 */
		if (!IsTransactionState())
			return InvalidOid;

		return ts_get_relation_relid(CACHE_SCHEMA_NAME,
									 (char *) cache_proxy_table_names[type],
									 true);
	}

	return catalog->caches[type].inval_proxy_id;
}

/*
 * Become the user that owns the catalog schema.
 *
 * This might be necessary for users that do operations that require changes to
 * the catalog.
 *
 * The caller should pass a CatalogSecurityContext where the current security
 * context will be saved. The original security context can later be restored
 * with ts_catalog_restore_user().
 */
TSDLLEXPORT bool
ts_catalog_database_info_become_owner(CatalogDatabaseInfo *database_info,
									  CatalogSecurityContext *sec_ctx)
{
	GetUserIdAndSecContext(&sec_ctx->saved_uid, &sec_ctx->saved_security_context);

	if (sec_ctx->saved_uid != database_info->owner_uid)
	{
		SetUserIdAndSecContext(database_info->owner_uid,
							   sec_ctx->saved_security_context | SECURITY_LOCAL_USERID_CHANGE);
		return true;
	}

	return false;
}

/*
 * Restore the security context of the original user after becoming the catalog
 * owner. The user should pass the original CatalogSecurityContext that was used
 * with ts_catalog_database_info_become_owner().
 */
TSDLLEXPORT void
ts_catalog_restore_user(CatalogSecurityContext *sec_ctx)
{
	SetUserIdAndSecContext(sec_ctx->saved_uid, sec_ctx->saved_security_context);
}

/*
 * Insert a new row into a catalog table.
 */
void
ts_catalog_insert_only(Relation rel, HeapTuple tuple)
{
	CatalogTupleInsert(rel, tuple);
	ts_catalog_invalidate_cache(RelationGetRelid(rel), CMD_INSERT);
}

void
ts_catalog_insert(Relation rel, HeapTuple tuple)
{
	ts_catalog_insert_only(rel, tuple);
	/* Make changes visible */
	CommandCounterIncrement();
}

/*
 * Insert a new row into a catalog table.
 */
TSDLLEXPORT void
ts_catalog_insert_values(Relation rel, TupleDesc tupdesc, Datum *values, bool *nulls)
{
	HeapTuple tuple = heap_form_tuple(tupdesc, values, nulls);

	ts_catalog_insert(rel, tuple);
	heap_freetuple(tuple);
}

TSDLLEXPORT void
ts_catalog_insert_datums(Relation rel, TupleDesc tupdesc, NullableDatum *datums)
{
	HeapTuple tuple = ts_heap_form_tuple(tupdesc, datums);

	ts_catalog_insert(rel, tuple);
	heap_freetuple(tuple);
}

void
ts_catalog_update_tid_only(Relation rel, ItemPointer tid, HeapTuple tuple)
{
	CatalogTupleUpdate(rel, tid, tuple);
	ts_catalog_invalidate_cache(RelationGetRelid(rel), CMD_UPDATE);
}

void
ts_catalog_update_tid(Relation rel, ItemPointer tid, HeapTuple tuple)
{
	ts_catalog_update_tid_only(rel, tid, tuple);
	/* Make changes visible */
	CommandCounterIncrement();
}

TSDLLEXPORT void
ts_catalog_update(Relation rel, HeapTuple tuple)
{
	ts_catalog_update_tid(rel, &tuple->t_self, tuple);
}

void
ts_catalog_delete_tid_only(Relation rel, ItemPointer tid)
{
	CatalogTupleDelete(rel, tid);
	ts_catalog_invalidate_cache(RelationGetRelid(rel), CMD_DELETE);
}

void
ts_catalog_delete_tid(Relation rel, ItemPointer tid)
{
	ts_catalog_delete_tid_only(rel, tid);
	CommandCounterIncrement();
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
ts_catalog_invalidate_cache(Oid catalog_relid, CmdType operation)
{
	Catalog *catalog = ts_catalog_get();
	CatalogTable table = catalog_get_table(catalog, catalog_relid);
	Oid relid;

	switch (table)
	{
		case CHUNK:
		case CHUNK_CONSTRAINT:
		case DIMENSION_SLICE:
			if (operation == CMD_UPDATE || operation == CMD_DELETE)
			{
				relid = ts_catalog_get_cache_proxy_id(catalog, CACHE_TYPE_HYPERTABLE);
				CacheInvalidateRelcacheByRelid(relid);
			}
			break;
		case HYPERTABLE:
		case DIMENSION:
		case CONTINUOUS_AGG:
		case CHUNK_COLUMN_STATS:
			relid = ts_catalog_get_cache_proxy_id(catalog, CACHE_TYPE_HYPERTABLE);
			CacheInvalidateRelcacheByRelid(relid);
			break;
		case BGW_JOB:
			relid = ts_catalog_get_cache_proxy_id(catalog, CACHE_TYPE_BGW_JOB);
			CacheInvalidateRelcacheByRelid(relid);
			break;
		case CHUNK_INDEX:
		default:
			break;
	}
}

/* Scanner helper functions specifically for the catalog tables */
TSDLLEXPORT bool
ts_catalog_scan_one(CatalogTable table, int indexid, ScanKeyData *scankey, int num_keys,
					tuple_found_func tuple_found, LOCKMODE lockmode, char *table_name, void *data)
{
	Catalog *catalog = ts_catalog_get();

	ScannerCtx scanctx = {
		.table = catalog_get_table_id(catalog, table),
		.index = catalog_get_index(catalog, table, indexid),
		.nkeys = num_keys,
		.scankey = scankey,
		.tuple_found = tuple_found,
		.data = data,
		.lockmode = lockmode,
		.scandirection = ForwardScanDirection,
	};

	return ts_scanner_scan_one(&scanctx, false, table_name);
}

TSDLLEXPORT void
ts_catalog_scan_all(CatalogTable table, int indexid, ScanKeyData *scankey, int num_keys,
					tuple_found_func tuple_found, LOCKMODE lockmode, void *data)
{
	Catalog *catalog = ts_catalog_get();

	ScannerCtx scanctx = {
		.table = catalog_get_table_id(catalog, table),
		.index = catalog_get_index(catalog, table, indexid),
		.nkeys = num_keys,
		.scankey = scankey,
		.tuple_found = tuple_found,
		.data = data,
		.lockmode = lockmode,
		.scandirection = ForwardScanDirection,
	};

	ts_scanner_scan(&scanctx);
}

/*
 * Copied verbatim from postgres source CatalogIndexInsert which is static
 * in postgres source code.
 * We need to have this function available because we do not want to use
 * simple_heap_insert which is used by CatalogTupleInsert which would
 * prevent using bulk inserts.
 */
extern TSDLLEXPORT void
ts_catalog_index_insert(ResultRelInfo *indstate, HeapTuple heapTuple)
{
	int i;
	int numIndexes;
	RelationPtr relationDescs;
	Relation heapRelation;
	TupleTableSlot *slot;
	IndexInfo **indexInfoArray;
	Datum values[INDEX_MAX_KEYS];
	bool isnull[INDEX_MAX_KEYS];

	/*
	 * HOT update does not require index inserts. But with asserts enabled we
	 * want to check that it'd be legal to currently insert into the
	 * table/index.
	 */
#ifndef USE_ASSERT_CHECKING
	if (HeapTupleIsHeapOnly(heapTuple))
		return;
#endif

	/*
	 * Get information from the state structure.  Fall out if nothing to do.
	 */
	numIndexes = indstate->ri_NumIndices;
	if (numIndexes == 0)
		return;
	relationDescs = indstate->ri_IndexRelationDescs;
	indexInfoArray = indstate->ri_IndexRelationInfo;
	heapRelation = indstate->ri_RelationDesc;

	/* Need a slot to hold the tuple being examined */
	slot = MakeSingleTupleTableSlot(RelationGetDescr(heapRelation), &TTSOpsHeapTuple);
	ExecStoreHeapTuple(heapTuple, slot, false);

	/*
	 * for each index, form and insert the index tuple
	 */
	for (i = 0; i < numIndexes; i++)
	{
		IndexInfo *indexInfo;
		Relation index;

		indexInfo = indexInfoArray[i];
		index = relationDescs[i];

		/* If the index is marked as read-only, ignore it */
		if (!indexInfo->ii_ReadyForInserts)
			continue;

		/*
		 * Expressional and partial indexes on system catalogs are not
		 * supported, nor exclusion constraints, nor deferred uniqueness
		 */
		Assert(indexInfo->ii_Expressions == NIL);
		Assert(indexInfo->ii_Predicate == NIL);
		Assert(indexInfo->ii_ExclusionOps == NULL);
		Assert(index->rd_index->indimmediate);
		Assert(indexInfo->ii_NumIndexKeyAttrs != 0);

		/* see earlier check above */
#ifdef USE_ASSERT_CHECKING
		if (HeapTupleIsHeapOnly(heapTuple))
		{
			Assert(!ReindexIsProcessingIndex(RelationGetRelid(index)));
			continue;
		}
#endif /* USE_ASSERT_CHECKING */

		/*
		 * FormIndexDatum fills in its values and isnull parameters with the
		 * appropriate values for the column(s) of the index.
		 */
		FormIndexDatum(indexInfo,
					   slot,
					   NULL, /* no expression eval to do */
					   values,
					   isnull);

		/*
		 * The index AM does the rest.
		 */
		index_insert(index,				   /* index relation */
					 values,			   /* array of index Datums */
					 isnull,			   /* is-null flags */
					 &(heapTuple->t_self), /* tid of heap tuple */
					 heapRelation,
					 index->rd_index->indisunique ? UNIQUE_CHECK_YES : UNIQUE_CHECK_NO,
					 false,
					 indexInfo);
	}

	ExecDropSingleTupleTableSlot(slot);
}
