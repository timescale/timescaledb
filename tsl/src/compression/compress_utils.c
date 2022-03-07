/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

/* This file contains the implementation for SQL utility functions that
 *  compress and decompress chunks
 */
#include <postgres.h>
#include <access/xact.h>
#include <catalog/dependency.h>
#include <commands/tablecmds.h>
#include <commands/trigger.h>
#include <miscadmin.h>
#include <nodes/makefuncs.h>
#include <nodes/pg_list.h>
#include <nodes/parsenodes.h>
#include <parser/parse_func.h>
#include <storage/lmgr.h>
#include <trigger.h>
#include <utils/builtins.h>
#include <utils/elog.h>
#include <utils/fmgrprotos.h>
#include <libpq-fe.h>

#include <remote/dist_commands.h>
#include "compat/compat.h"
#include "cache.h"
#include "chunk.h"
#include "errors.h"
#include "hypertable.h"
#include "hypertable_cache.h"
#include "ts_catalog/continuous_agg.h"
#include "ts_catalog/hypertable_compression.h"
#include "ts_catalog/compression_chunk_size.h"
#include "create.h"
#include "compress_utils.h"
#include "compression.h"
#include "compat/compat.h"
#include "scanner.h"
#include "scan_iterator.h"
#include "utils.h"

typedef struct CompressChunkCxt
{
	Hypertable *srcht;
	Chunk *srcht_chunk;		 /* chunk from srcht */
	Hypertable *compress_ht; /*compressed table for srcht */
} CompressChunkCxt;

static void
compression_chunk_size_catalog_insert(int32 src_chunk_id, const RelationSize *src_size,
									  int32 compress_chunk_id, const RelationSize *compress_size,
									  int64 rowcnt_pre_compression, int64 rowcnt_post_compression)
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
	values[AttrNumberGetAttrOffset(Anum_compression_chunk_size_numrows_pre_compression)] =
		Int64GetDatum(rowcnt_pre_compression);
	values[AttrNumberGetAttrOffset(Anum_compression_chunk_size_numrows_post_compression)] =
		Int64GetDatum(rowcnt_post_compression);

	ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);
	ts_catalog_insert_values(rel, desc, values, nulls);
	ts_catalog_restore_user(&sec_ctx);
	table_close(rel, RowExclusiveLock);
}

static void
get_hypertable_or_cagg_name(Hypertable *ht, Name objname)
{
	ContinuousAggHypertableStatus status = ts_continuous_agg_hypertable_status(ht->fd.id);
	if (status == HypertableIsNotContinuousAgg)
		namestrcpy(objname, NameStr(ht->fd.table_name));
	else if (status == HypertableIsMaterialization)
	{
		ContinuousAgg *cagg = ts_continuous_agg_find_by_mat_hypertable_id(ht->fd.id);
		namestrcpy(objname, NameStr(cagg->data.user_view_name));
	}
	else
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("unexpected hypertable status for %s %d",
						NameStr(ht->fd.table_name),
						status)));
	}
}

static void
compresschunkcxt_init(CompressChunkCxt *cxt, Cache *hcache, Oid hypertable_relid, Oid chunk_relid)
{
	Hypertable *srcht = ts_hypertable_cache_get_entry(hcache, hypertable_relid, CACHE_FLAG_NONE);
	Hypertable *compress_ht;
	Chunk *srcchunk;

	ts_hypertable_permissions_check(srcht->main_table_relid, GetUserId());

	if (!TS_HYPERTABLE_HAS_COMPRESSION_TABLE(srcht))
	{
		NameData cagg_ht_name;
		get_hypertable_or_cagg_name(srcht, &cagg_ht_name);
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("compression not enabled on \"%s\"", NameStr(cagg_ht_name)),
				 errdetail("It is not possible to compress chunks on a hypertable or"
						   " continuous aggregate that does not have compression enabled."),
				 errhint("Enable compression using ALTER TABLE/MATERIALIZED VIEW with"
						 " the timescaledb.compress option.")));
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
	srcchunk = ts_chunk_get_by_relid(chunk_relid, true);
	cxt->srcht = srcht;
	cxt->compress_ht = compress_ht;
	cxt->srcht_chunk = srcchunk;
	return;
}

static void
preserve_uncompressed_chunk_stats(Oid chunk_relid)
{
	AlterTableCmd at_cmd = {
		.type = T_AlterTableCmd,
		.subtype = AT_SetRelOptions,
		.def = (Node *) list_make1(
			makeDefElem("autovacuum_enabled", (Node *) makeString("false"), -1)),
	};
	VacuumRelation vr = {
		.type = T_VacuumRelation,
		.relation = NULL,
		.oid = chunk_relid,
		.va_cols = NIL,
	};
	VacuumStmt vs = {
		.type = T_VacuumStmt,
		.rels = list_make1(&vr),
		.is_vacuumcmd = false,
		.options = NIL,
	};

	ExecVacuum(NULL, &vs, true);
	AlterTableInternal(chunk_relid, list_make1(&at_cmd), false);
}

/* This function is intended to undo the disabling of autovacuum done when we compressed a chunk.
 * Note that we do not cache the previous value for this (as we don't expect users to toggle this
 * for individual chunks), so we use the hypertable's setting to determine whether to enable this on
 * the decompressed chunk.
 */
static void
restore_autovacuum_on_decompress(Oid uncompressed_hypertable_relid, Oid uncompressed_chunk_relid)
{
	Relation tablerel = table_open(uncompressed_hypertable_relid, AccessShareLock);
	bool ht_autovac_enabled =
		tablerel->rd_options ? ((StdRdOptions *) (tablerel)->rd_options)->autovacuum.enabled : true;

	table_close(tablerel, AccessShareLock);
	if (ht_autovac_enabled)
	{
		AlterTableCmd at_cmd = {
			.type = T_AlterTableCmd,
			.subtype = AT_SetRelOptions,
			.def = (Node *) list_make1(
				makeDefElem("autovacuum_enabled", (Node *) makeString("true"), -1)),
		};

		AlterTableInternal(uncompressed_chunk_relid, list_make1(&at_cmd), false);
	}
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
	RelationSize before_size, after_size;
	CompressionStats cstat;

	hcache = ts_hypertable_cache_pin();
	compresschunkcxt_init(&cxt, hcache, hypertable_relid, chunk_relid);

	/* acquire locks on src and compress hypertable and src chunk */
	LockRelationOid(cxt.srcht->main_table_relid, AccessShareLock);
	LockRelationOid(cxt.compress_ht->main_table_relid, AccessShareLock);
	LockRelationOid(cxt.srcht_chunk->table_id, ShareLock);

	/* Perform an analyze on the chunk to get up-to-date stats before compressing */
	preserve_uncompressed_chunk_stats(chunk_relid);

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
	before_size = ts_relation_size_impl(cxt.srcht_chunk->table_id);
	cstat = compress_chunk(cxt.srcht_chunk->table_id,
						   compress_ht_chunk->table_id,
						   colinfo_array,
						   htcols_listlen);

	/* Copy chunk constraints (including fkey) to compressed chunk.
	 * Do this after compressing the chunk to avoid holding strong, unnecessary locks on the
	 * referenced table during compression.
	 */
	ts_chunk_constraints_create(compress_ht_chunk->constraints,
								compress_ht_chunk->table_id,
								compress_ht_chunk->fd.id,
								compress_ht_chunk->hypertable_relid,
								compress_ht_chunk->fd.hypertable_id);
	ts_trigger_create_all_on_chunk(compress_ht_chunk);

	/* Drop all FK constraints on the uncompressed chunk. This is needed to allow
	 * cascading deleted data in FK-referenced tables, while blocking deleting data
	 * directly on the hypertable or chunks.
	 */
	ts_chunk_drop_fks(cxt.srcht_chunk);
	after_size = ts_relation_size_impl(compress_ht_chunk->table_id);
	compression_chunk_size_catalog_insert(cxt.srcht_chunk->fd.id,
										  &before_size,
										  compress_ht_chunk->fd.id,
										  &after_size,
										  cstat.rowcnt_pre_compression,
										  cstat.rowcnt_post_compression);

	ts_chunk_set_compressed_chunk(cxt.srcht_chunk, compress_ht_chunk->fd.id);
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

	uncompressed_chunk = ts_chunk_get_by_relid(uncompressed_chunk_relid, true);
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

	compressed_chunk = ts_chunk_get_by_id(uncompressed_chunk->fd.compressed_chunk_id, true);

	/* acquire locks on src and compress hypertable and src chunk */
	LockRelationOid(uncompressed_hypertable->main_table_relid, AccessShareLock);
	LockRelationOid(compressed_hypertable->main_table_relid, AccessShareLock);
	LockRelationOid(uncompressed_chunk->table_id, AccessShareLock); /*upgrade when needed */

	/* aquire locks on catalog tables to keep till end of txn */
	LockRelationOid(catalog_get_table_id(ts_catalog_get(), HYPERTABLE_COMPRESSION),
					AccessShareLock);
	LockRelationOid(catalog_get_table_id(ts_catalog_get(), CHUNK), RowExclusiveLock);

	decompress_chunk(compressed_chunk->table_id, uncompressed_chunk->table_id);
	/* Recreate FK constraints, since they were dropped during compression. */
	ts_chunk_create_fks(uncompressed_chunk);
	ts_compression_chunk_size_delete(uncompressed_chunk->fd.id);
	ts_chunk_clear_compressed_chunk(uncompressed_chunk);
	ts_chunk_drop(compressed_chunk, DROP_RESTRICT, -1);
	/* reenable autovacuum if necessary */
	restore_autovacuum_on_decompress(uncompressed_hypertable_relid, uncompressed_chunk_relid);

	ts_cache_release(hcache);
	return true;
}

/*
 * Set if_not_compressed to true for idempotent operation. Aborts transaction if the chunk is
 * already compressed, unless it is running in idempotent mode.
 */

void
tsl_compress_chunk_wrapper(Chunk *chunk, bool if_not_compressed)
{
	if (chunk->fd.compressed_chunk_id != INVALID_CHUNK_ID)
	{
		ereport((if_not_compressed ? NOTICE : ERROR),
				(errcode(ERRCODE_DUPLICATE_OBJECT),
				 errmsg("chunk \"%s\" is already compressed", get_rel_name(chunk->table_id))));
		return;
	}

	compress_chunk_impl(chunk->hypertable_relid, chunk->table_id);
}

/*
 * Helper for remote invocation of chunk compression and decompression.
 */
static bool
invoke_compression_func_remotely(FunctionCallInfo fcinfo, const Chunk *chunk)
{
	List *datanodes;
	DistCmdResult *distres;
	bool isnull_result = true;
	Size i;

	Assert(chunk->relkind == RELKIND_FOREIGN_TABLE);
	Assert(chunk->data_nodes != NIL);
	datanodes = ts_chunk_get_data_node_name_list(chunk);
	distres = ts_dist_cmd_invoke_func_call_on_data_nodes(fcinfo, datanodes);

	for (i = 0; i < ts_dist_cmd_response_count(distres); i++)
	{
		const char *node_name;
		bool isnull;
		Datum PG_USED_FOR_ASSERTS_ONLY d;

		d = ts_dist_cmd_get_single_scalar_result_by_index(distres, i, &isnull, &node_name);

		/* Make sure data nodes either (1) all return NULL, or (2) all return
		 * a non-null result. */
		if (i > 0 && isnull_result != isnull)
			elog(ERROR, "inconsistent result from data node \"%s\"", node_name);

		isnull_result = isnull;

		if (!isnull)
		{
			Assert(OidIsValid(DatumGetObjectId(d)));
		}
	}

	ts_dist_cmd_close_response(distres);

	return !isnull_result;
}

static bool
compress_remote_chunk(FunctionCallInfo fcinfo, const Chunk *chunk, bool if_not_compressed)
{
	bool success = invoke_compression_func_remotely(fcinfo, chunk);

	if (!success)
		ereport((if_not_compressed ? NOTICE : ERROR),
				(errcode(ERRCODE_DUPLICATE_OBJECT),
				 errmsg("chunk \"%s\" is already compressed", get_rel_name(chunk->table_id))));

	return success;
}

static bool
decompress_remote_chunk(FunctionCallInfo fcinfo, const Chunk *chunk, bool if_compressed)
{
	bool success = invoke_compression_func_remotely(fcinfo, chunk);

	if (!success)
		ereport((if_compressed ? NOTICE : ERROR),
				(errcode(ERRCODE_DUPLICATE_OBJECT),
				 errmsg("chunk \"%s\" is not compressed", get_rel_name(chunk->table_id))));

	return success;
}

Datum
tsl_compress_chunk(PG_FUNCTION_ARGS)
{
	Oid uncompressed_chunk_id = PG_ARGISNULL(0) ? InvalidOid : PG_GETARG_OID(0);
	bool if_not_compressed = PG_ARGISNULL(1) ? false : PG_GETARG_BOOL(1);
	TS_PREVENT_FUNC_IF_READ_ONLY();
	Chunk *chunk = ts_chunk_get_by_relid(uncompressed_chunk_id, true);

	if (chunk->relkind == RELKIND_FOREIGN_TABLE)
	{
		/* chunks of distributed hypertables are foreign tables */
		if (!compress_remote_chunk(fcinfo, chunk, if_not_compressed))
			PG_RETURN_NULL();

		/*
		 * Updating the chunk compression status of the Access Node AFTER executing remote
		 * compression. In the event of failure, the compressed status will NOT be set. The
		 * distributed compression policy will attempt to compress again, which is idempotent, thus
		 * the metadata are eventually consistent.
		 */
		ts_chunk_set_compressed_chunk(chunk, INVALID_CHUNK_ID);
	}
	else
	{
		tsl_compress_chunk_wrapper(chunk, if_not_compressed);
	}

	PG_RETURN_OID(uncompressed_chunk_id);
}

Datum
tsl_decompress_chunk(PG_FUNCTION_ARGS)
{
	Oid uncompressed_chunk_id = PG_ARGISNULL(0) ? InvalidOid : PG_GETARG_OID(0);
	bool if_compressed = PG_ARGISNULL(1) ? false : PG_GETARG_BOOL(1);
	TS_PREVENT_FUNC_IF_READ_ONLY();
	Chunk *uncompressed_chunk = ts_chunk_get_by_relid(uncompressed_chunk_id, true);

	if (NULL == uncompressed_chunk)
		elog(ERROR, "unknown chunk id %d", uncompressed_chunk_id);

	if (uncompressed_chunk->relkind == RELKIND_FOREIGN_TABLE)
	{
		/*
		 * Updating the chunk compression status of the Access Node BEFORE executing remote
		 * decompression. In the event of failure, the compressed status will be cleared. The
		 * distributed compression policy will attempt to compress again, which is idempotent, thus
		 * the metadata are eventually consistent.
		 * If CHUNK_STATUS_COMPRESSED is cleared, then it is probable that a remote compress_chunk()
		 * has not taken place, but not certain. For this above reason, this flag should not be
		 * assumed to be consistent (when it is cleared) for Access-Nodes. When used in distributed
		 * hypertables one should take advantage of the idempotent properties of remote
		 * compress_chunk() and distributed compression policy to make progress.
		 */
		ts_chunk_clear_compressed_chunk(uncompressed_chunk);

		if (!decompress_remote_chunk(fcinfo, uncompressed_chunk, if_compressed))
			PG_RETURN_NULL();

		PG_RETURN_OID(uncompressed_chunk_id);
	}

	if (!decompress_chunk_impl(uncompressed_chunk->hypertable_relid,
							   uncompressed_chunk_id,
							   if_compressed))
		PG_RETURN_NULL();

	PG_RETURN_OID(uncompressed_chunk_id);
}

bool
tsl_recompress_chunk_wrapper(Chunk *uncompressed_chunk)
{
	Oid uncompressed_chunk_relid = uncompressed_chunk->table_id;
	if (ts_chunk_is_unordered(uncompressed_chunk))
	{
		if (!decompress_chunk_impl(uncompressed_chunk->hypertable_relid,
								   uncompressed_chunk_relid,
								   false))
			return false;
	}
	Chunk *chunk = ts_chunk_get_by_relid(uncompressed_chunk_relid, true);
	Assert(!ts_chunk_is_compressed(chunk));
	tsl_compress_chunk_wrapper(chunk, false);
	return true;
}
