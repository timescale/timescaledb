/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

/* This file contains the implementation for SQL utility functions that
 *  compress and decompress chunks
 */
#include <postgres.h>
#include <miscadmin.h>
#include <storage/lmgr.h>
#include <utils/elog.h>

#include "chunk.h"
#include "errors.h"
#include "hypertable.h"
#include "hypertable_cache.h"
#include "hypertable_compression.h"
#include "create.h"
#include "compress_utils.h"
#include "compression.h"

typedef struct CompressChunkCxt
{
	Hypertable *srcht;
	Chunk *srcht_chunk;		 /* chunk from srcht */
	Hypertable *compress_ht; /*compressed table for srcht */
} CompressChunkCxt;

static void
compresschunkcxt_init(CompressChunkCxt *cxt, Cache *hcache, Oid hypertable_relid, Oid chunk_relid)
{
	Hypertable *srcht = ts_hypertable_cache_get_entry(hcache, hypertable_relid);
	Hypertable *compress_ht;
	Chunk *srcchunk;
	if (srcht == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_TS_HYPERTABLE_NOT_EXIST),
				 errmsg("table \"%s\" is not a hypertable", get_rel_name(hypertable_relid))));
	ts_hypertable_permissions_check(srcht->main_table_relid, GetUserId());
	if (!TS_HYPERTABLE_HAS_COMPRESSION_ON(srcht))
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("chunks can be compressed only if compression property is set on the "
						"hypertable"),
				 errhint("use ALTER TABLE with timescaledb.compression option ")));
	}
	compress_ht = ts_hypertable_get_by_id(srcht->fd.compressed_hypertable_id);
	if (compress_ht == NULL)
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("missing compress hypertable")));
	/* user has to be the owner of the compression table too */
	ts_hypertable_permissions_check(compress_ht->main_table_relid, GetUserId());

	if (!srcht->space) // something is wrong
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

	hcache = ts_hypertable_cache_pin();
	compresschunkcxt_init(&cxt, hcache, hypertable_relid, chunk_relid);

	/* acquire locks on src and compress hypertable and src chunk */
	LockRelationOid(cxt.srcht->main_table_relid, AccessShareLock);
	LockRelationOid(cxt.compress_ht->main_table_relid, AccessShareLock);
	LockRelationOid(cxt.srcht_chunk->table_id, AccessShareLock); /*upgrade when needed */

	// get compression properties for hypertable
	htcols_list = get_hypertablecompression_info(cxt.srcht->fd.id);
	htcols_listlen = list_length(htcols_list);
	// create compressed chunk DDL and compress the data
	compress_ht_chunk = create_compress_chunk_table(cxt.compress_ht, cxt.srcht_chunk);
	/* convert list to array of pointers for compress_chunk */
	colinfo_array = palloc(sizeof(ColumnCompressionInfo *) * htcols_listlen);
	foreach (lc, htcols_list)
	{
		FormData_hypertable_compression *fd = (FormData_hypertable_compression *) lfirst(lc);
		colinfo_array[i++] = fd;
	}
	compress_chunk(cxt.srcht_chunk->table_id,
				   compress_ht_chunk->table_id,
				   colinfo_array,
				   htcols_listlen);
	ts_chunk_set_compressed_chunk(cxt.srcht_chunk, compress_ht_chunk->fd.id, false);
	ts_cache_release(hcache);
}

Datum
tsl_compress_chunk(PG_FUNCTION_ARGS)
{
	Oid chunk_id = PG_ARGISNULL(0) ? InvalidOid : PG_GETARG_OID(0);
	Chunk *srcchunk = ts_chunk_get_by_relid(chunk_id, 0, true);
	if (srcchunk->fd.compressed_chunk_id != INVALID_CHUNK_ID)
	{
		ereport(ERROR, (errcode(ERRCODE_DUPLICATE_OBJECT), errmsg("chunk is already compressed")));
	}
	compress_chunk_impl(srcchunk->hypertable_relid, chunk_id);
	PG_RETURN_VOID();
}
