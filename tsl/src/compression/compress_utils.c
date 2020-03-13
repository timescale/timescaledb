/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

/* This file contains the implementation for SQL utility functions that
 *  compress and decompress chunks
 */
#include <postgres.h>
#include <catalog/dependency.h>
#include <commands/trigger.h>
#include <miscadmin.h>
#include <nodes/makefuncs.h>
#include <nodes/pg_list.h>
#include <storage/lmgr.h>
#include <trigger.h>
#include <utils/elog.h>
#include <utils/builtins.h>

#include "chunk.h"
#include "errors.h"
#include "hypertable.h"
#include "hypertable_cache.h"
#include "hypertable_compression.h"
#include "create.h"
#include "compress_utils.h"
#include "compression.h"
#include "compat.h"
#include "scanner.h"
#include "scan_iterator.h"
#include "license.h"
#include "compression_chunk_size.h"

#if !PG96
#include <utils/fmgrprotos.h>
#endif

#define CHUNK_DML_BLOCKER_TRIGGER "chunk_dml_blocker"
#define CHUNK_DML_BLOCKER_NAME "compressed_chunk_insert_blocker"

typedef struct CompressChunkCxt
{
	Hypertable *srcht;
	Chunk *srcht_chunk;		 /* chunk from srcht */
	Hypertable *compress_ht; /*compressed table for srcht */
} CompressChunkCxt;

typedef struct
{
	int64 heap_size;
	int64 toast_size;
	int64 index_size;
} ChunkSize;

static ChunkSize
compute_chunk_size(Oid chunk_relid)
{
	int64 tot_size;
	int i = 0;
	ChunkSize ret;
	Datum relid = ObjectIdGetDatum(chunk_relid);
	char *filtyp[] = { "main", "init", "fsm", "vm" };
	/* for heap get size from fsm, vm, init and main as this is included in
	 * pg_table_size calculation
	 */
	ret.heap_size = 0;
	for (i = 0; i < 4; i++)
	{
		ret.heap_size += DatumGetInt64(
			DirectFunctionCall2(pg_relation_size, relid, CStringGetTextDatum(filtyp[i])));
	}
	ret.index_size = DatumGetInt64(DirectFunctionCall1(pg_indexes_size, relid));
	tot_size = DatumGetInt64(DirectFunctionCall1(pg_table_size, relid));
	ret.toast_size = tot_size - ret.heap_size;
	return ret;
}

static void
compression_chunk_size_catalog_insert(int32 src_chunk_id, ChunkSize *src_size,
									  int32 compress_chunk_id, ChunkSize *compress_size)
{
	Catalog *catalog = ts_catalog_get();
	Relation rel;
	TupleDesc desc;
	CatalogSecurityContext sec_ctx;

	Datum values[Natts_compression_chunk_size];
	bool nulls[Natts_compression_chunk_size] = { false };

	rel = table_open(catalog_get_table_id(catalog, COMPRESSION_CHUNK_SIZE), RowExclusiveLock);
	desc = RelationGetDescr(rel);

	memset(values, 0, sizeof(values));

	values[AttrNumberGetAttrOffset(Anum_compression_chunk_size_chunk_id)] =
		Int32GetDatum(src_chunk_id);
	values[AttrNumberGetAttrOffset(Anum_compression_chunk_size_compressed_chunk_id)] =
		Int32GetDatum(compress_chunk_id);
	values[AttrNumberGetAttrOffset(Anum_compression_chunk_size_uncompressed_heap_size)] =
		Int64GetDatum(src_size->heap_size);
	values[AttrNumberGetAttrOffset(Anum_compression_chunk_size_uncompressed_toast_size)] =
		Int64GetDatum(src_size->toast_size);
	values[AttrNumberGetAttrOffset(Anum_compression_chunk_size_uncompressed_index_size)] =
		Int64GetDatum(src_size->index_size);
	values[AttrNumberGetAttrOffset(Anum_compression_chunk_size_compressed_heap_size)] =
		Int64GetDatum(compress_size->heap_size);
	values[AttrNumberGetAttrOffset(Anum_compression_chunk_size_compressed_toast_size)] =
		Int64GetDatum(compress_size->toast_size);
	values[AttrNumberGetAttrOffset(Anum_compression_chunk_size_compressed_index_size)] =
		Int64GetDatum(compress_size->index_size);

	ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);
	ts_catalog_insert_values(rel, desc, values, nulls);
	ts_catalog_restore_user(&sec_ctx);
	table_close(rel, RowExclusiveLock);
}

static void
chunk_dml_blocker_trigger_add(Oid relid)
{
	ObjectAddress objaddr;
	char *relname = get_rel_name(relid);
	Oid schemaid = get_rel_namespace(relid);
	char *schema = get_namespace_name(schemaid);

	/* stmt triggers are blocked on hypertable chunks */
	CreateTrigStmt stmt = {
		.type = T_CreateTrigStmt,
		.row = true,
		.timing = TRIGGER_TYPE_BEFORE,
		.trigname = CHUNK_DML_BLOCKER_NAME,
		.relation = makeRangeVar(schema, relname, -1),
		.funcname =
			list_make2(makeString(INTERNAL_SCHEMA_NAME), makeString(CHUNK_DML_BLOCKER_TRIGGER)),
		.args = NIL,
		.events = TRIGGER_TYPE_INSERT,
	};
	objaddr = CreateTriggerCompat(&stmt, NULL, relid, InvalidOid, InvalidOid, InvalidOid, false);

	if (!OidIsValid(objaddr.objectId))
		elog(ERROR, "could not create DML blocker trigger");

	return;
}

static void
chunk_dml_trigger_drop(Oid relid)
{
	if (OidIsValid(relid))
	{
		ObjectAddress objaddr = {
			.classId = TriggerRelationId,
			.objectId = get_trigger_oid(relid, CHUNK_DML_BLOCKER_NAME, true),
		};
		if (OidIsValid(objaddr.objectId))
			performDeletion(&objaddr, DROP_RESTRICT, 0);
	}
}

static void
compresschunkcxt_init(CompressChunkCxt *cxt, Cache *hcache, Oid hypertable_relid, Oid chunk_relid)
{
	Hypertable *srcht = ts_hypertable_cache_get_entry(hcache, hypertable_relid, CACHE_FLAG_NONE);
	Hypertable *compress_ht;
	Chunk *srcchunk;

	ts_hypertable_permissions_check(srcht->main_table_relid, GetUserId());
	if (!TS_HYPERTABLE_HAS_COMPRESSION(srcht))
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("chunks can be compressed only if compression property is set on the "
						"hypertable"),
				 errhint("Use ALTER TABLE with timescaledb.compress option.")));
	}
	compress_ht = ts_hypertable_get_by_id(srcht->fd.compressed_hypertable_id);
	if (compress_ht == NULL)
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("missing compress hypertable")));
	/* user has to be the owner of the compression table too */
	ts_hypertable_permissions_check(compress_ht->main_table_relid, GetUserId());

	if (!srcht->space) /* something is wrong */
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR), errmsg("missing hyperspace for hypertable")));
	/* refetch the srcchunk with all attributes filled in */
	srcchunk = ts_chunk_get_by_relid(chunk_relid, srcht->space->num_dimensions, true);
	cxt->srcht = srcht;
	cxt->compress_ht = compress_ht;
	cxt->srcht_chunk = srcchunk;
	return;
}

static void
compress_chunk_impl(Oid hypertable_relid, Oid chunk_relid)
{
	CompressChunkCxt cxt;
	Chunk *compress_ht_chunk;
	Cache *hcache;
	ListCell *lc;
	List *htcols_list = NIL;
	const ColumnCompressionInfo **colinfo_array;
	int i = 0, htcols_listlen;
	ChunkSize before_size, after_size;

	hcache = ts_hypertable_cache_pin();
	compresschunkcxt_init(&cxt, hcache, hypertable_relid, chunk_relid);

	/* acquire locks on src and compress hypertable and src chunk */
	LockRelationOid(cxt.srcht->main_table_relid, AccessShareLock);
	LockRelationOid(cxt.compress_ht->main_table_relid, AccessShareLock);
	LockRelationOid(cxt.srcht_chunk->table_id, AccessShareLock); /*upgrade when needed */

	/* aquire locks on catalog tables to keep till end of txn */
	LockRelationOid(catalog_get_table_id(ts_catalog_get(), HYPERTABLE_COMPRESSION),
					AccessShareLock);
	LockRelationOid(catalog_get_table_id(ts_catalog_get(), CHUNK), RowExclusiveLock);

	/* get compression properties for hypertable */
	htcols_list = ts_hypertable_compression_get(cxt.srcht->fd.id);
	htcols_listlen = list_length(htcols_list);
	/* create compressed chunk DDL and compress the data */
	compress_ht_chunk = create_compress_chunk_table(cxt.compress_ht, cxt.srcht_chunk);
	/* convert list to array of pointers for compress_chunk */
	colinfo_array = palloc(sizeof(ColumnCompressionInfo *) * htcols_listlen);
	foreach (lc, htcols_list)
	{
		FormData_hypertable_compression *fd = (FormData_hypertable_compression *) lfirst(lc);
		colinfo_array[i++] = fd;
	}
	before_size = compute_chunk_size(cxt.srcht_chunk->table_id);
	compress_chunk(cxt.srcht_chunk->table_id,
				   compress_ht_chunk->table_id,
				   colinfo_array,
				   htcols_listlen);
	/* Drop all FK constraints on the uncompressed chunk. This is needed to allow
	 * cascading deleted data in FK-referenced tables, while blocking deleting data
	 * directly on the hypertable or chunks.
	 */
	ts_chunk_drop_fks(cxt.srcht_chunk);
	chunk_dml_blocker_trigger_add(cxt.srcht_chunk->table_id);
	after_size = compute_chunk_size(compress_ht_chunk->table_id);
	compression_chunk_size_catalog_insert(cxt.srcht_chunk->fd.id,
										  &before_size,
										  compress_ht_chunk->fd.id,
										  &after_size);
	ts_chunk_set_compressed_chunk(cxt.srcht_chunk, compress_ht_chunk->fd.id, false);
	ts_cache_release(hcache);
}

static bool
decompress_chunk_impl(Oid uncompressed_hypertable_relid, Oid uncompressed_chunk_relid,
					  bool if_compressed)
{
	Cache *hcache;
	Hypertable *uncompressed_hypertable =
		ts_hypertable_cache_get_cache_and_entry(uncompressed_hypertable_relid,
												CACHE_FLAG_NONE,
												&hcache);
	Hypertable *compressed_hypertable;
	Chunk *uncompressed_chunk;
	Chunk *compressed_chunk;

	ts_hypertable_permissions_check(uncompressed_hypertable->main_table_relid, GetUserId());

	compressed_hypertable =
		ts_hypertable_get_by_id(uncompressed_hypertable->fd.compressed_hypertable_id);
	if (compressed_hypertable == NULL)
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("missing compressed hypertable")));

	uncompressed_chunk = ts_chunk_get_by_relid(uncompressed_chunk_relid,
											   uncompressed_hypertable->space->num_dimensions,
											   true);
	if (uncompressed_chunk == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("table \"%s\" is not a chunk", get_rel_name(uncompressed_chunk_relid))));

	if (uncompressed_chunk->fd.hypertable_id != uncompressed_hypertable->fd.id)
		elog(ERROR, "hypertable and chunk do not match");

	if (uncompressed_chunk->fd.compressed_chunk_id == INVALID_CHUNK_ID)
	{
		ts_cache_release(hcache);
		ereport((if_compressed ? NOTICE : ERROR),
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("chunk \"%s\" is not compressed", get_rel_name(uncompressed_chunk_relid))));
		return false;
	}

	compressed_chunk = ts_chunk_get_by_id(uncompressed_chunk->fd.compressed_chunk_id, 0, true);

	/* acquire locks on src and compress hypertable and src chunk */
	LockRelationOid(uncompressed_hypertable->main_table_relid, AccessShareLock);
	LockRelationOid(compressed_hypertable->main_table_relid, AccessShareLock);
	LockRelationOid(uncompressed_chunk->table_id, AccessShareLock); /*upgrade when needed */

	/* aquire locks on catalog tables to keep till end of txn */
	LockRelationOid(catalog_get_table_id(ts_catalog_get(), HYPERTABLE_COMPRESSION),
					AccessShareLock);
	LockRelationOid(catalog_get_table_id(ts_catalog_get(), CHUNK), RowExclusiveLock);

	chunk_dml_trigger_drop(uncompressed_chunk->table_id);
	decompress_chunk(compressed_chunk->table_id, uncompressed_chunk->table_id);
	/* Recreate FK constraints, since they were dropped during compression. */
	ts_chunk_create_fks(uncompressed_chunk);
	ts_compression_chunk_size_delete(uncompressed_chunk->fd.id);
	ts_chunk_set_compressed_chunk(uncompressed_chunk, INVALID_CHUNK_ID, true);
	ts_chunk_drop(compressed_chunk, DROP_RESTRICT, -1);

	ts_cache_release(hcache);
	return true;
}

bool
tsl_compress_chunk_wrapper(Oid chunk_relid, bool if_not_compressed)
{
	Chunk *srcchunk = ts_chunk_get_by_relid(chunk_relid, 0, true);
	if (srcchunk->fd.compressed_chunk_id != INVALID_CHUNK_ID)
	{
		ereport((if_not_compressed ? NOTICE : ERROR),
				(errcode(ERRCODE_DUPLICATE_OBJECT),
				 errmsg("chunk \"%s\" is already compressed", get_rel_name(chunk_relid))));
		return false;
	}

	compress_chunk_impl(srcchunk->hypertable_relid, chunk_relid);
	return true;
}

Datum
tsl_compress_chunk(PG_FUNCTION_ARGS)
{
	Oid uncompressed_chunk_id = PG_ARGISNULL(0) ? InvalidOid : PG_GETARG_OID(0);
	bool if_not_compressed = PG_ARGISNULL(1) ? false : PG_GETARG_BOOL(1);
	if (!tsl_compress_chunk_wrapper(uncompressed_chunk_id, if_not_compressed))
		PG_RETURN_NULL();
	PG_RETURN_OID(uncompressed_chunk_id);
}

Datum
tsl_decompress_chunk(PG_FUNCTION_ARGS)
{
	Oid uncompressed_chunk_id = PG_ARGISNULL(0) ? InvalidOid : PG_GETARG_OID(0);
	bool if_compressed = PG_ARGISNULL(1) ? false : PG_GETARG_BOOL(1);
	Chunk *uncompressed_chunk = ts_chunk_get_by_relid(uncompressed_chunk_id, 0, true);
	if (NULL == uncompressed_chunk)
		elog(ERROR, "unknown chunk id %d", uncompressed_chunk_id);

	if (!decompress_chunk_impl(uncompressed_chunk->hypertable_relid,
							   uncompressed_chunk_id,
							   if_compressed))
		PG_RETURN_NULL();
	PG_RETURN_OID(uncompressed_chunk_id);
}
