/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

/*
 * This file contains source code that was copied and/or modified from the
 * PostgreSQL database, which is licensed under the open-source PostgreSQL
 * License. Please see the NOTICE at the top level directory for a copy of
 * the PostgreSQL License.
 */

/* see postgres commit ab5e9caa4a3ec4765348a0482e88edcf3f6aab4a */

#include <postgres.h>
#include <access/amapi.h>
#include <access/multixact.h>
#include <access/relscan.h>
#include <access/rewriteheap.h>
#include <access/transam.h>
#include <access/xact.h>
#include <access/xlog.h>
#include <catalog/catalog.h>
#include <catalog/dependency.h>
#include <catalog/heap.h>
#include <catalog/index.h>
#include <catalog/namespace.h>
#include <catalog/objectaccess.h>
#include <catalog/pg_am.h>
#include <catalog/toasting.h>
#include <commands/cluster.h>
#include <commands/tablecmds.h>
#include <commands/tablespace.h>
#include <commands/vacuum.h>
#include <miscadmin.h>
#include <nodes/pg_list.h>
#include <optimizer/planner.h>
#include <storage/bufmgr.h>
#include <storage/lmgr.h>
#include <storage/predicate.h>
#include <storage/smgr.h>
#include <utils/acl.h>
#include <utils/fmgroids.h>
#include <utils/guc.h>
#include <utils/inval.h>
#include <utils/lsyscache.h>
#include <utils/memutils.h>
#include <utils/pg_rusage.h>
#include <utils/relmapper.h>
#include <utils/snapmgr.h>
#include <utils/syscache.h>
#include <utils/tuplesort.h>
#include <executor/spi.h>
#include <utils/snapmgr.h>

#include "compat/compat.h"
#if PG13_LT
#include <access/tuptoaster.h>
#else
#include <access/toast_internals.h>
#endif

#include "annotations.h"
#include "chunk.h"
#include "chunk_copy.h"
#include "chunk_index.h"
#include "hypertable_cache.h"
#include "indexing.h"
#include "reorder.h"

extern void timescale_reorder_rel(Oid tableOid, Oid indexOid, bool verbose, Oid wait_id,
								  Oid destination_tablespace, Oid index_tablespace);

#define REORDER_ACCESS_EXCLUSIVE_DEADLOCK_TIMEOUT "101000"

static void timescale_rebuild_relation(Relation OldHeap, Oid indexOid, bool verbose, Oid wait_id,
									   Oid destination_tablespace, Oid index_tablespace);
static void copy_heap_data(Oid OIDNewHeap, Oid OIDOldHeap, Oid OIDOldIndex, bool verbose,
						   bool *pSwapToastByContent, TransactionId *pFreezeXid,
						   MultiXactId *pCutoffMulti);

static void finish_heap_swaps(Oid OIDOldHeap, Oid OIDNewHeap, List *old_index_oids,
							  List *new_index_oids, bool swap_toast_by_content, bool is_internal,
							  TransactionId frozenXid, MultiXactId cutoffMulti, Oid wait_id);

static void swap_relation_files(Oid r1, Oid r2, bool swap_toast_by_content, bool is_internal,
								TransactionId frozenXid, MultiXactId cutoffMulti);

static bool chunk_get_reorder_index(Hypertable *ht, Chunk *chunk, Oid index_relid,
									ChunkIndexMapping *cim_out);

Datum
tsl_reorder_chunk(PG_FUNCTION_ARGS)
{
	Oid chunk_id = PG_ARGISNULL(0) ? InvalidOid : PG_GETARG_OID(0);
	Oid index_id = PG_ARGISNULL(1) ? InvalidOid : PG_GETARG_OID(1);
	bool verbose = PG_ARGISNULL(2) ? false : PG_GETARG_BOOL(2);

	/* used for debugging purposes only see finish_heap_swaps */
	Oid wait_id = PG_NARGS() < 4 || PG_ARGISNULL(3) ? InvalidOid : PG_GETARG_OID(3);

	/*
	 * Allow reorder in transactions for testing purposes only
	 */
	if (!OidIsValid(wait_id))
		PreventInTransactionBlock(true, "reorder");

	reorder_chunk(chunk_id, index_id, verbose, wait_id, InvalidOid, InvalidOid);
	PG_RETURN_VOID();
}

Datum
tsl_move_chunk(PG_FUNCTION_ARGS)
{
	Oid chunk_id = PG_ARGISNULL(0) ? InvalidOid : PG_GETARG_OID(0);
	Oid destination_tablespace =
		PG_ARGISNULL(1) ? InvalidOid : get_tablespace_oid(PG_GETARG_NAME(1)->data, false);
	Oid index_destination_tablespace =
		PG_ARGISNULL(2) ? InvalidOid : get_tablespace_oid(PG_GETARG_NAME(2)->data, false);
	Oid index_id = PG_ARGISNULL(3) ? InvalidOid : PG_GETARG_OID(3);
	bool verbose = PG_ARGISNULL(4) ? false : PG_GETARG_BOOL(4);
	Chunk *chunk;

	/* used for debugging purposes only see finish_heap_swaps */
	Oid wait_id = PG_NARGS() < 6 || PG_ARGISNULL(5) ? InvalidOid : PG_GETARG_OID(5);

	/*
	 * Allow move in transactions for testing purposes only
	 */
	if (!OidIsValid(wait_id))
		PreventInTransactionBlock(true, "move");

	/*
	 * Index_destination_tablespace is currently a required parameter in order
	 * to avoid situations where there is ambiguity about where indexes should
	 * be placed based on where the index was created and the new tablespace
	 * (and avoid interactions with multi-tablespace hypertable functionality).
	 * Eventually we may want to offer an option to keep indexes in the
	 * tablespace of their parent if it is specified.
	 */
	if (!OidIsValid(chunk_id) || !OidIsValid(destination_tablespace) ||
		!OidIsValid(index_destination_tablespace))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("valid chunk, destination_tablespace, and index_destination_tablespaces "
						"are required")));

	chunk = ts_chunk_get_by_relid(chunk_id, false);

	if (NULL == chunk)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("\"%s\" is not a chunk", get_rel_name(chunk_id))));

	if (ts_chunk_contains_compressed_data(chunk))
	{
		Chunk *chunk_parent = ts_chunk_get_compressed_chunk_parent(chunk);

		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot directly move internal compression data"),
				 errdetail("Chunk \"%s\" contains compressed data for chunk \"%s\" and cannot be "
						   "moved directly.",
						   get_rel_name(chunk_id),
						   get_rel_name(chunk_parent->table_id)),
				 errhint("Moving chunk \"%s\" will also move the compressed data.",
						 get_rel_name(chunk_parent->table_id))));
	}

	/* If chunk is compressed move it by altering tablespace on both chunks */
	if (OidIsValid(chunk->fd.compressed_chunk_id))
	{
		Chunk *compressed_chunk = ts_chunk_get_by_id(chunk->fd.compressed_chunk_id, true);
		AlterTableCmd cmd = { .type = T_AlterTableCmd,
							  .subtype = AT_SetTableSpace,
							  .name = get_tablespace_name(destination_tablespace) };

		if (OidIsValid(index_id))
			ereport(NOTICE,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("ignoring index parameter"),
					 errdetail("Chunk will not be reordered as it has compressed data.")));

		AlterTableInternal(chunk_id, list_make1(&cmd), false);
		AlterTableInternal(compressed_chunk->table_id, list_make1(&cmd), false);
		/* move indexes on original and compressed chunk */
		ts_chunk_index_move_all(chunk_id, index_destination_tablespace);
		ts_chunk_index_move_all(compressed_chunk->table_id, index_destination_tablespace);
	}
	else
	{
		reorder_chunk(chunk_id,
					  index_id,
					  verbose,
					  wait_id,
					  destination_tablespace,
					  index_destination_tablespace);
	}

	PG_RETURN_VOID();
}

/*
 * Implement a distributed chunk copy/move operation.
 *
 * We use a procedure because multiple steps need to be performed via multiple
 * transactions across the access node and the two datanodes that are involved.
 * The progress of the various stages/steps are tracked in the
 * CHUNK_COPY_OPERATION catalog table
 */
static void
tsl_copy_or_move_chunk_proc(FunctionCallInfo fcinfo, bool delete_on_src_node)
{
	Oid chunk_id = PG_ARGISNULL(0) ? InvalidOid : PG_GETARG_OID(0);
	const char *src_node_name = PG_ARGISNULL(1) ? NULL : NameStr(*PG_GETARG_NAME(1));
	const char *dst_node_name = PG_ARGISNULL(2) ? NULL : NameStr(*PG_GETARG_NAME(2));
	int rc;
	bool nonatomic = fcinfo->context && IsA(fcinfo->context, CallContext) &&
					 !castNode(CallContext, fcinfo->context)->atomic;

	TS_PREVENT_FUNC_IF_READ_ONLY();

	PreventInTransactionBlock(true, get_func_name(FC_FN_OID(fcinfo)));

	/* src_node and dst_node both have to be non-NULL */
	if (src_node_name == NULL || dst_node_name == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid source or destination node")));

	if (!OidIsValid(chunk_id))
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("invalid chunk")));

	if ((rc = SPI_connect_ext(nonatomic ? SPI_OPT_NONATOMIC : 0)) != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect failed: %s", SPI_result_code_string(rc));

	/* perform the actual distributed chunk move after a few sanity checks */
	chunk_copy(chunk_id, src_node_name, dst_node_name, delete_on_src_node);

	if ((rc = SPI_finish()) != SPI_OK_FINISH)
		elog(ERROR, "SPI_finish failed: %s", SPI_result_code_string(rc));
}

Datum
tsl_move_chunk_proc(PG_FUNCTION_ARGS)
{
	tsl_copy_or_move_chunk_proc(fcinfo, true);

	PG_RETURN_VOID();
}

Datum
tsl_copy_chunk_proc(PG_FUNCTION_ARGS)
{
	tsl_copy_or_move_chunk_proc(fcinfo, false);

	PG_RETURN_VOID();
}

Datum
tsl_copy_chunk_cleanup_proc(PG_FUNCTION_ARGS)
{
	const char *operation_id = PG_ARGISNULL(0) ? NULL : NameStr(*PG_GETARG_NAME(0));
	int rc;
	bool nonatomic = fcinfo->context && IsA(fcinfo->context, CallContext) &&
					 !castNode(CallContext, fcinfo->context)->atomic;

	TS_PREVENT_FUNC_IF_READ_ONLY();

	PreventInTransactionBlock(true, get_func_name(FC_FN_OID(fcinfo)));

	/* valid input has to be provided */
	if (operation_id == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid chunk copy operation id")));

	if ((rc = SPI_connect_ext(nonatomic ? SPI_OPT_NONATOMIC : 0)) != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect failed: %s", SPI_result_code_string(rc));

	/* perform the cleanup/repair depending on the stage */
	chunk_copy_cleanup(operation_id);

	if ((rc = SPI_finish()) != SPI_OK_FINISH)
		elog(ERROR, "SPI_finish failed: %s", SPI_result_code_string(rc));

	PG_RETURN_VOID();
}

void
reorder_chunk(Oid chunk_id, Oid index_id, bool verbose, Oid wait_id, Oid destination_tablespace,
			  Oid index_tablespace)
{
	Chunk *chunk;
	Cache *hcache;
	Hypertable *ht;
	ChunkIndexMapping cim;

	if (!OidIsValid(chunk_id))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("must provide a valid chunk to cluster")));

	chunk = ts_chunk_get_by_relid(chunk_id, false);

	if (NULL == chunk)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("\"%s\" is not a chunk", get_rel_name(chunk_id))));

	ht = ts_hypertable_cache_get_cache_and_entry(chunk->hypertable_relid, CACHE_FLAG_NONE, &hcache);

	/* Our check gives better error messages, but keep the original one too. */
	ts_hypertable_permissions_check(ht->main_table_relid, GetUserId());

	if (!pg_class_ownercheck(ht->main_table_relid, GetUserId()))
	{
		Oid main_table_relid = ht->main_table_relid;

		ts_cache_release(hcache);
		aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_TABLE, get_rel_name(main_table_relid));
	}

	if (hypertable_is_distributed(ht))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("move_chunk() and reorder_chunk() cannot be used "
						"with distributed hypertables")));

	if (!chunk_get_reorder_index(ht, chunk, index_id, &cim))
	{
		ts_cache_release(hcache);
		if (OidIsValid(index_id))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("\"%s\" is not a valid clustering index for table \"%s\"",
							get_rel_name(index_id),
							get_rel_name(chunk_id))));
		else
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("there is no previously clustered index for table \"%s\"",
							get_rel_name(chunk_id))));
	}

	if (OidIsValid(destination_tablespace) && destination_tablespace != MyDatabaseTableSpace)
	{
		AclResult aclresult;

		aclresult = pg_tablespace_aclcheck(destination_tablespace, GetUserId(), ACL_CREATE);
		if (aclresult != ACLCHECK_OK)
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
					 errmsg("permission denied for tablespace \"%s\"",
							get_tablespace_name(destination_tablespace))));
		;
	}

	if (OidIsValid(index_tablespace) && index_tablespace != MyDatabaseTableSpace)
	{
		AclResult aclresult;

		aclresult = pg_tablespace_aclcheck(index_tablespace, GetUserId(), ACL_CREATE);
		if (aclresult != ACLCHECK_OK)
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
					 errmsg("permission denied for tablespace \"%s\"",
							get_tablespace_name(index_tablespace))));
	}

	Assert(cim.chunkoid == chunk_id);

	/*
	 * We must mark each chunk index as clustered before calling reorder_rel()
	 * because it expects indexes that need to be rechecked (due to new
	 * transaction) to already have that mark set
	 */
	ts_chunk_index_mark_clustered(cim.chunkoid, cim.indexoid);
	timescale_reorder_rel(cim.chunkoid,
						  cim.indexoid,
						  verbose,
						  wait_id,
						  destination_tablespace,
						  index_tablespace);
	ts_cache_release(hcache);
}

/*
 * Find the index to reorder a chunk on based on a possibly NULL indexname
 * returns NULL if no such index is found
 */
static bool
chunk_get_reorder_index(Hypertable *ht, Chunk *chunk, Oid index_relid, ChunkIndexMapping *cim_out)
{
	/*
	 * Index search order: 1. Explicitly named index 2. Chunk cluster index 3.
	 * Hypertable cluster index
	 */
	if (OidIsValid(index_relid))
	{
		if (ts_chunk_index_get_by_indexrelid(chunk, index_relid, cim_out))
			return true;

		return ts_chunk_index_get_by_hypertable_indexrelid(chunk, index_relid, cim_out);
	}

	index_relid = ts_indexing_find_clustered_index(chunk->table_id);
	if (OidIsValid(index_relid))
		return ts_chunk_index_get_by_indexrelid(chunk, index_relid, cim_out);

	index_relid = ts_indexing_find_clustered_index(ht->main_table_relid);
	if (OidIsValid(index_relid))
		return ts_chunk_index_get_by_hypertable_indexrelid(chunk, index_relid, cim_out);

	return false;
}

/* The following functions are based on their equivalents in postgres's cluster.c */

/*
 * timescale_reorder_rel
 *
 * This clusters the table by creating a new, clustered table and
 * swapping the relfilenodes of the new table and the old table, so
 * the OID of the original table is preserved.
 *
 * Indexes are rebuilt in the same manner.
 */
void
timescale_reorder_rel(Oid tableOid, Oid indexOid, bool verbose, Oid wait_id,
					  Oid destination_tablespace, Oid index_tablespace)
{
	Relation OldHeap;
	HeapTuple tuple;
	Form_pg_index indexForm;

	if (!OidIsValid(indexOid))
		elog(ERROR, "Reorder must specify an index.");

	/* Check for user-requested abort. */
	CHECK_FOR_INTERRUPTS();

	/*
	 * We grab exclusive access to the target rel and index for the duration
	 * of the transaction.  (This is redundant for the single-transaction
	 * case, since cluster() already did it.)  The index lock is taken inside
	 * check_index_is_clusterable.
	 */
	OldHeap = try_relation_open(tableOid, ExclusiveLock);

	/* If the table has gone away, we can skip processing it */
	if (!OldHeap)
	{
		ereport(WARNING, (errcode(ERRCODE_WARNING), errmsg("table disappeared during reorder")));
		return;
	}

	/*
	 * Since we may open a new transaction for each relation, we have to check
	 * that the relation still is what we think it is.
	 */
	/* Check that the user still owns the relation */
	if (!pg_class_ownercheck(tableOid, GetUserId()))
	{
		relation_close(OldHeap, ExclusiveLock);
		ereport(WARNING, (errcode(ERRCODE_WARNING), errmsg("ownership changed during reorder")));
		return;
	}

	if (IsSystemRelation(OldHeap))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot reorder a system relation")));

	if (OldHeap->rd_rel->relpersistence != RELPERSISTENCE_PERMANENT)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("can only reorder a permanent table")));

	/* We do not allow reordering on shared catalogs. */
	if (OldHeap->rd_rel->relisshared)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot reorder a shared catalog")));

	if (OldHeap->rd_rel->relkind != RELKIND_RELATION)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("can only reorder a relation")));

	/*
	 * Check that the index still exists
	 */
	if (!SearchSysCacheExists1(RELOID, ObjectIdGetDatum(indexOid)))
	{
		ereport(WARNING, (errcode(ERRCODE_WARNING), errmsg("index disappeared during reorder")));
		relation_close(OldHeap, ExclusiveLock);
		return;
	}

	/*
	 * Check that the index is still the one with indisclustered set.
	 */
	tuple = SearchSysCache1(INDEXRELID, ObjectIdGetDatum(indexOid));
	if (!HeapTupleIsValid(tuple)) /* probably can't happen */
	{
		ereport(WARNING, (errcode(ERRCODE_WARNING), errmsg("invalid index heap during reorder")));
		relation_close(OldHeap, ExclusiveLock);
		return;
	}
	indexForm = (Form_pg_index) GETSTRUCT(tuple);

	/*
	 * We always mark indexes as clustered when we intercept a cluster
	 * command, if it's not marked as such here, something has gone wrong
	 */
	if (!indexForm->indisclustered)
		ereport(ERROR,
				(errcode(ERRCODE_ASSERT_FAILURE), errmsg("invalid index heap during reorder")));
	ReleaseSysCache(tuple);

	/*
	 * Also check for active uses of the relation in the current transaction,
	 * including open scans and pending AFTER trigger events.
	 */
	CheckTableNotInUse(OldHeap, "CLUSTER");

	/* Check heap and index are valid to cluster on */
	check_index_is_clusterable(OldHeap, indexOid, true, ExclusiveLock);

	/* timescale_rebuild_relation does all the dirty work */
	timescale_rebuild_relation(OldHeap,
							   indexOid,
							   verbose,
							   wait_id,
							   destination_tablespace,
							   index_tablespace);

	/* NB: timescale_rebuild_relation does table_close() on OldHeap */
}

/*
 * timescale_rebuild_relation: rebuild an existing relation in index or physical order
 *
 * OldHeap: table to rebuild --- must be opened and exclusive-locked!
 * indexOid: index to cluster by, or InvalidOid to rewrite in physical order.
 *
 * NB: this routine closes OldHeap at the right time; caller should not.
 */
static void
timescale_rebuild_relation(Relation OldHeap, Oid indexOid, bool verbose, Oid wait_id,
						   Oid destination_tablespace, Oid index_tablespace)
{
	Oid tableOid = RelationGetRelid(OldHeap);
	Oid tableSpace = OidIsValid(destination_tablespace) ? destination_tablespace :
														  OldHeap->rd_rel->reltablespace;
	Oid OIDNewHeap;
	List *old_index_oids;
	List *new_index_oids;
	char relpersistence;
	bool swap_toast_by_content;
	TransactionId frozenXid;
	MultiXactId cutoffMulti;

	/* Mark the correct index as clustered */
	mark_index_clustered(OldHeap, indexOid, true);

	/* Remember info about rel before closing OldHeap */
	relpersistence = OldHeap->rd_rel->relpersistence;

	/* Close relcache entry, but keep lock until transaction commit */
	table_close(OldHeap, NoLock);

	/* Create the transient table that will receive the re-ordered data */
	OIDNewHeap = make_new_heap_compat(tableOid,
									  tableSpace,
									  OldHeap->rd_rel->relam,
									  relpersistence,
									  ExclusiveLock);

	/* Copy the heap data into the new table in the desired order */
	copy_heap_data(OIDNewHeap,
				   tableOid,
				   indexOid,
				   verbose,
				   &swap_toast_by_content,
				   &frozenXid,
				   &cutoffMulti);

	/* Create versions of the tables indexes for the new table */
	new_index_oids =
		ts_chunk_index_duplicate(tableOid, OIDNewHeap, &old_index_oids, index_tablespace);

	/*
	 * Swap the physical files of the target and transient tables, then
	 * rebuild the target's indexes and throw away the transient table.
	 */
	finish_heap_swaps(tableOid,
					  OIDNewHeap,
					  old_index_oids,
					  new_index_oids,
					  swap_toast_by_content,
					  true,
					  frozenXid,
					  cutoffMulti,
					  wait_id);
}

/*
 * Do the physical copying of heap data.
 *
 * There are three output parameters:
 * *pSwapToastByContent is set true if toast tables must be swapped by content.
 * *pFreezeXid receives the TransactionId used as freeze cutoff point.
 * *pCutoffMulti receives the MultiXactId used as a cutoff point.
 */
static void
copy_heap_data(Oid OIDNewHeap, Oid OIDOldHeap, Oid OIDOldIndex, bool verbose,
			   bool *pSwapToastByContent, TransactionId *pFreezeXid, MultiXactId *pCutoffMulti)
{
	Relation NewHeap, OldHeap, OldIndex;
	Relation relRelation;
	HeapTuple reltup;
	Form_pg_class relform;
	TupleDesc PG_USED_FOR_ASSERTS_ONLY oldTupDesc;
	TupleDesc newTupDesc;
	int natts;
	Datum *values;
	bool *isnull;
	TransactionId OldestXmin;
	TransactionId FreezeXid;
	MultiXactId MultiXactCutoff;
	bool use_sort;
	double num_tuples = 0, tups_vacuumed = 0, tups_recently_dead = 0;
	BlockNumber num_pages;
	int elevel = verbose ? INFO : DEBUG2;
	PGRUsage ru0;
	pg_rusage_init(&ru0);

	/*
	 * Open the relations we need.
	 */
	NewHeap = table_open(OIDNewHeap, AccessExclusiveLock);
	OldHeap = table_open(OIDOldHeap, ExclusiveLock);

	if (OidIsValid(OIDOldIndex))
		OldIndex = index_open(OIDOldIndex, ExclusiveLock);
	else
		OldIndex = NULL;

	/*
	 * Their tuple descriptors should be exactly alike, but here we only need
	 * assume that they have the same number of columns.
	 */
	oldTupDesc = RelationGetDescr(OldHeap);
	newTupDesc = RelationGetDescr(NewHeap);
	Assert(newTupDesc->natts == oldTupDesc->natts);

	/* Preallocate values/isnull arrays */
	natts = newTupDesc->natts;
	values = (Datum *) palloc(natts * sizeof(Datum));
	isnull = (bool *) palloc(natts * sizeof(bool));

	/*
	 * If the OldHeap has a toast table, get lock on the toast table to keep
	 * it from being vacuumed.  This is needed because autovacuum processes
	 * toast tables independently of their main tables, with no lock on the
	 * latter.  If an autovacuum were to start on the toast table after we
	 * compute our OldestXmin below, it would use a later OldestXmin, and then
	 * possibly remove as DEAD toast tuples belonging to main tuples we think
	 * are only RECENTLY_DEAD.  Then we'd fail while trying to copy those
	 * tuples.
	 *
	 * We don't need to open the toast relation here, just lock it.  The lock
	 * will be held till end of transaction.
	 */
	if (OldHeap->rd_rel->reltoastrelid)
		LockRelationOid(OldHeap->rd_rel->reltoastrelid, ExclusiveLock);

	/* use_wal off requires smgr_targblock be initially invalid */
	Assert(RelationGetTargetBlock(NewHeap) == InvalidBlockNumber);

	/*
	 * If both tables have TOAST tables, perform toast swap by content.  It is
	 * possible that the old table has a toast table but the new one doesn't,
	 * if toastable columns have been dropped.  In that case we have to do
	 * swap by links.  This is okay because swap by content is only essential
	 * for system catalogs, and we don't support schema changes for them.
	 */
	if (OldHeap->rd_rel->reltoastrelid && NewHeap->rd_rel->reltoastrelid)
	{
		*pSwapToastByContent = true;

		/*
		 * When doing swap by content, any toast pointers written into NewHeap
		 * must use the old toast table's OID, because that's where the toast
		 * data will eventually be found.  Set this up by setting rd_toastoid.
		 * This also tells toast_save_datum() to preserve the toast value
		 * OIDs, which we want so as not to invalidate toast pointers in
		 * system catalog caches, and to avoid making multiple copies of a
		 * single toast value.
		 *
		 * Note that we must hold NewHeap open until we are done writing data,
		 * since the relcache will not guarantee to remember this setting once
		 * the relation is closed.  Also, this technique depends on the fact
		 * that no one will try to read from the NewHeap until after we've
		 * finished writing it and swapping the rels --- otherwise they could
		 * follow the toast pointers to the wrong place.  (It would actually
		 * work for values copied over from the old toast table, but not for
		 * any values that we toast which were previously not toasted.)
		 */
		NewHeap->rd_toastoid = OldHeap->rd_rel->reltoastrelid;
	}
	else
		*pSwapToastByContent = false;

	/*
	 * Compute xids used to freeze and weed out dead tuples and multixacts.
	 * Since we're going to rewrite the whole table anyway, there's no reason
	 * not to be aggressive about this.
	 */
	vacuum_set_xid_limits(OldHeap,
						  0,
						  0,
						  0,
						  0,
						  &OldestXmin,
						  &FreezeXid,
						  NULL,
						  &MultiXactCutoff,
						  NULL);

	/*
	 * FreezeXid will become the table's new relfrozenxid, and that mustn't go
	 * backwards, so take the max.
	 */
	if (TransactionIdPrecedes(FreezeXid, OldHeap->rd_rel->relfrozenxid))
		FreezeXid = OldHeap->rd_rel->relfrozenxid;

	/*
	 * MultiXactCutoff, similarly, shouldn't go backwards either.
	 */
	if (MultiXactIdPrecedes(MultiXactCutoff, OldHeap->rd_rel->relminmxid))
		MultiXactCutoff = OldHeap->rd_rel->relminmxid;

	/* return selected values to caller */
	*pFreezeXid = FreezeXid;
	*pCutoffMulti = MultiXactCutoff;

	/*
	 * We know how to use a sort to duplicate the ordering of a btree index,
	 * and will use seqscan-and-sort for that.  Otherwise, always use an
	 * indexscan for other indexes or plain seqscan if no index is supplied.
	 */
	if (OldIndex != NULL && OldIndex->rd_rel->relam == BTREE_AM_OID)
		use_sort = true;
	else
		use_sort = false;

	/* Log what we're doing */
	if (OldIndex != NULL && !use_sort)
		ereport(elevel,
				(errmsg("reordering \"%s.%s\" using index scan on \"%s\"",
						get_namespace_name(RelationGetNamespace(OldHeap)),
						RelationGetRelationName(OldHeap),
						RelationGetRelationName(OldIndex))));
	else if (use_sort)
		ereport(elevel,
				(errmsg("reordering \"%s.%s\" using sequential scan and sort",
						get_namespace_name(RelationGetNamespace(OldHeap)),
						RelationGetRelationName(OldHeap))));
	else
		ereport(ERROR,
				(errmsg("tried to use a reorder without an index \"%s.%s\"",
						get_namespace_name(RelationGetNamespace(OldHeap)),
						RelationGetRelationName(OldHeap))));

	table_relation_copy_for_cluster(OldHeap,
									NewHeap,
									OldIndex,
									use_sort,
									OldestXmin,
									&FreezeXid,
									&MultiXactCutoff,
									&num_tuples,
									&tups_vacuumed,
									&tups_recently_dead);

	/* Reset rd_toastoid just to be tidy --- it shouldn't be looked at again */
	NewHeap->rd_toastoid = InvalidOid;

	num_pages = RelationGetNumberOfBlocks(NewHeap);

	/* Log what we did */
	ereport(elevel,
			(errmsg("\"%s\": found %.0f removable, %.0f nonremovable row versions in %u pages",
					RelationGetRelationName(OldHeap),
					tups_vacuumed,
					num_tuples,
					RelationGetNumberOfBlocks(OldHeap)),
			 errdetail("%.0f dead row versions cannot be removed yet.\n"
					   "%s.",
					   tups_recently_dead,
					   pg_rusage_show(&ru0))));

	/* Clean up */
	pfree(values);
	pfree(isnull);

	if (OldIndex != NULL)
		index_close(OldIndex, NoLock);
	table_close(OldHeap, NoLock);
	table_close(NewHeap, NoLock);

	/* Update pg_class to reflect the correct values of pages and tuples. */
	relRelation = table_open(RelationRelationId, RowExclusiveLock);

	reltup = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(OIDNewHeap));
	if (!HeapTupleIsValid(reltup))
		elog(ERROR, "cache lookup failed for relation %u", OIDNewHeap);
	relform = (Form_pg_class) GETSTRUCT(reltup);

	relform->relpages = num_pages;
	relform->reltuples = num_tuples;

	/* Don't update the stats for pg_class.  See swap_relation_files. */
	Assert(OIDOldHeap != RelationRelationId);
	CacheInvalidateRelcacheByTuple(reltup);

	/* Clean up. */
	heap_freetuple(reltup);
	table_close(relRelation, RowExclusiveLock);

	/* Make the update visible */
	CommandCounterIncrement();
}

/*
 * Remove the transient table that was built by make_new_heap, and finish
 * cleaning up (including rebuilding all indexes on the old heap).
 *
 * NB: new_index_oids must be in the same order as RelationGetIndexList
 *
 */
static void
finish_heap_swaps(Oid OIDOldHeap, Oid OIDNewHeap, List *old_index_oids, List *new_index_oids,
				  bool swap_toast_by_content, bool is_internal, TransactionId frozenXid,
				  MultiXactId cutoffMulti, Oid wait_id)
{
	ObjectAddress object;
	Relation oldHeapRel;
	ListCell *old_index_cell;
	ListCell *new_index_cell;
	int config_change;

#ifdef DEBUG

	/*
	 * For debug purposes we serialize against wait_id if it exists, this
	 * allows us to "pause" reorder immediately before swapping in the new
	 * table
	 */
	if (OidIsValid(wait_id))
	{
		Relation waiter = table_open(wait_id, AccessExclusiveLock);

		table_close(waiter, AccessExclusiveLock);
	}
#endif

	/*
	 * There's a risk of deadlock if some other process is also trying to
	 * upgrade their lock in the same manner as us, at this time. Since our
	 * transaction has performed a large amount of work, and only needs to be
	 * run once per chunk, we do not want to abort it due to this deadlock. To
	 * prevent abort we set our `deadlock_timeout` to a large value in the
	 * expectation that the other process will timeout and abort first.
	 * Currently we set `deadlock_timeout` to 1 hour, as this should be longer
	 * than any other normal process, while still allowing the system to make
	 * progress in the event of a real deadlock. As this is the last lock we
	 * grab, and the setting is local to our transaction we do not bother
	 * changing the guc back.
	 */
	config_change = set_config_option("deadlock_timeout",
									  REORDER_ACCESS_EXCLUSIVE_DEADLOCK_TIMEOUT,
									  PGC_SUSET,
									  PGC_S_SESSION,
									  GUC_ACTION_LOCAL,
									  true,
									  0,
									  false);

	if (config_change == 0)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("deadlock_timeout guc does not exist.")));
	else if (config_change < 0)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("could not set deadlock_timeout guc.")));

	oldHeapRel = table_open(OIDOldHeap, AccessExclusiveLock);

	/*
	 * All predicate locks on the tuples or pages are about to be made
	 * invalid, because we move tuples around.  Promote them to relation
	 * locks.  Predicate locks on indexes will be promoted when they are
	 * reindexed.
	 */
	TransferPredicateLocksToHeapRelation(oldHeapRel);

	/*
	 * Swap the contents of the heap relations (including any toast tables).
	 * Also set old heap's relfrozenxid to frozenXid.
	 */
	swap_relation_files(OIDOldHeap,
						OIDNewHeap,
						swap_toast_by_content,
						is_internal,
						frozenXid,
						cutoffMulti);

	/* Swap the contents of the indexes */
	Assert(list_length(old_index_oids) == list_length(new_index_oids));
	forboth (old_index_cell, old_index_oids, new_index_cell, new_index_oids)
	{
		Oid old_index_oid = lfirst_oid(old_index_cell);
		Oid new_index_oid = lfirst_oid(new_index_cell);

		swap_relation_files(old_index_oid,
							new_index_oid,
							swap_toast_by_content,
							true,
							frozenXid,
							cutoffMulti);
	}
	table_close(oldHeapRel, NoLock);

	CommandCounterIncrement();

	/* Destroy new heap with old filenode */
	object.classId = RelationRelationId;
	object.objectId = OIDNewHeap;
	object.objectSubId = 0;

	/*
	 * The new relation is local to our transaction and we know nothing
	 * depends on it, so DROP_RESTRICT should be OK.
	 */
	performDeletion(&object, DROP_RESTRICT, PERFORM_DELETION_INTERNAL);

	/* performDeletion does CommandCounterIncrement at end */

	/*
	 * At this point, everything is kosher except that, if we did toast swap
	 * by links, the toast table's name corresponds to the transient table.
	 * The name is irrelevant to the backend because it's referenced by OID,
	 * but users looking at the catalogs could be confused.  Rename it to
	 * prevent this problem.
	 *
	 * Note no lock required on the relation, because we already hold an
	 * exclusive lock on it.
	 */
	if (!swap_toast_by_content)
	{
		Relation newrel;

		newrel = table_open(OIDOldHeap, NoLock);
		if (OidIsValid(newrel->rd_rel->reltoastrelid))
		{
			Oid toastidx;
			char NewToastName[NAMEDATALEN];

			/* Get the associated valid index to be renamed */
			toastidx = toast_get_valid_index(newrel->rd_rel->reltoastrelid, AccessShareLock);

			/* rename the toast table ... */
			snprintf(NewToastName, NAMEDATALEN, "pg_toast_%u", OIDOldHeap);
			RenameRelationInternal(newrel->rd_rel->reltoastrelid, NewToastName, true, false);

			/* ... and its valid index too. */
			snprintf(NewToastName, NAMEDATALEN, "pg_toast_%u_index", OIDOldHeap);

			RenameRelationInternal(toastidx, NewToastName, true, true);
		}
		table_close(newrel, NoLock);
	}

	/* it's not a catalog table, clear any missing attribute settings */
	{
		Relation newrel;

		newrel = table_open(OIDOldHeap, NoLock);
		RelationClearMissing(newrel);
		table_close(newrel, NoLock);
	}
}

/*
 * Swap the physical files of two given relations.
 *
 * We swap the physical identity (reltablespace, relfilenode) while keeping the
 * same logical identities of the two relations.  relpersistence is also
 * swapped, which is critical since it determines where buffers live for each
 * relation.
 *
 * We can swap associated TOAST data in either of two ways: recursively swap
 * the physical content of the toast tables (and their indexes), or swap the
 * TOAST links in the given relations' pg_class entries. The latter is the only
 * way to handle cases in which a toast table is added or removed altogether.
 *
 * Additionally, the first relation is marked with relfrozenxid set to
 * frozenXid.  It seems a bit ugly to have this here, but the caller would
 * have to do it anyway, so having it here saves a heap_update.  Note: in
 * the swap-toast-links case, we assume we don't need to change the toast
 * table's relfrozenxid: the new version of the toast table should already
 * have relfrozenxid set to RecentXmin, which is good enough.
 *
 */
static void
swap_relation_files(Oid r1, Oid r2, bool swap_toast_by_content, bool is_internal,
					TransactionId frozenXid, MultiXactId cutoffMulti)
{
	Relation relRelation;
	HeapTuple reltup1, reltup2;
	Form_pg_class relform1, relform2;
	Oid relfilenode1, relfilenode2;
	Oid swaptemp;
	char swptmpchr;

	/* We need writable copies of both pg_class tuples. */
	relRelation = table_open(RelationRelationId, RowExclusiveLock);

	reltup1 = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(r1));
	if (!HeapTupleIsValid(reltup1))
		elog(ERROR, "cache lookup failed for relation %u", r1);
	relform1 = (Form_pg_class) GETSTRUCT(reltup1);

	reltup2 = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(r2));
	if (!HeapTupleIsValid(reltup2))
		elog(ERROR, "cache lookup failed for relation %u", r2);
	relform2 = (Form_pg_class) GETSTRUCT(reltup2);

	relfilenode1 = relform1->relfilenode;
	relfilenode2 = relform2->relfilenode;

	if (!OidIsValid(relfilenode1) || !OidIsValid(relfilenode2))
		elog(ERROR, "cannot reorder mapped relation \"%s\".", NameStr(relform1->relname));

	/* swap relfilenodes, reltablespaces, relpersistence */

	swaptemp = relform1->relfilenode;
	relform1->relfilenode = relform2->relfilenode;
	relform2->relfilenode = swaptemp;

	swaptemp = relform1->reltablespace;
	relform1->reltablespace = relform2->reltablespace;
	relform2->reltablespace = swaptemp;

	swptmpchr = relform1->relpersistence;
	relform1->relpersistence = relform2->relpersistence;
	relform2->relpersistence = swptmpchr;

	/* Also swap toast links, if we're swapping by links */
	if (!swap_toast_by_content)
	{
		swaptemp = relform1->reltoastrelid;
		relform1->reltoastrelid = relform2->reltoastrelid;
		relform2->reltoastrelid = swaptemp;
	}

	/* set rel1's frozen Xid and minimum MultiXid */
	if (relform1->relkind != RELKIND_INDEX)
	{
		Assert(TransactionIdIsNormal(frozenXid));
		relform1->relfrozenxid = frozenXid;
		Assert(MultiXactIdIsValid(cutoffMulti));
		relform1->relminmxid = cutoffMulti;
	}

	/* swap size statistics too, since new rel has freshly-updated stats */
	{
		int32 swap_pages;
		float4 swap_tuples;
		int32 swap_allvisible;

		swap_pages = relform1->relpages;
		relform1->relpages = relform2->relpages;
		relform2->relpages = swap_pages;

		swap_tuples = relform1->reltuples;
		relform1->reltuples = relform2->reltuples;
		relform2->reltuples = swap_tuples;

		swap_allvisible = relform1->relallvisible;
		relform1->relallvisible = relform2->relallvisible;
		relform2->relallvisible = swap_allvisible;
	}

	/* Update the tuples in pg_class. */
	{
		CatalogIndexState indstate;
		indstate = CatalogOpenIndexes(relRelation);
		CatalogTupleUpdateWithInfo(relRelation, &reltup1->t_self, reltup1, indstate);
		CatalogTupleUpdateWithInfo(relRelation, &reltup2->t_self, reltup2, indstate);
		CatalogCloseIndexes(indstate);
	}

	/*
	 * Post alter hook for modified relations. The change to r2 is always
	 * internal, but r1 depends on the invocation context.
	 */
	InvokeObjectPostAlterHookArg(RelationRelationId, r1, 0, InvalidOid, is_internal);
	InvokeObjectPostAlterHookArg(RelationRelationId, r2, 0, InvalidOid, true);

	/*
	 * If we have toast tables associated with the relations being swapped,
	 * deal with them too.
	 */
	if (relform1->reltoastrelid || relform2->reltoastrelid)
	{
		if (swap_toast_by_content)
		{
			if (relform1->reltoastrelid && relform2->reltoastrelid)
			{
				/* Recursively swap the contents of the toast tables */
				swap_relation_files(relform1->reltoastrelid,
									relform2->reltoastrelid,
									swap_toast_by_content,
									is_internal,
									frozenXid,
									cutoffMulti);
			}
			else
			{
				/* caller messed up */
				elog(ERROR, "cannot swap toast files by content when there's only one");
			}
		}
		else
		{
			/*
			 * We swapped the ownership links, so we need to change dependency
			 * data to match.
			 *
			 * NOTE: it is possible that only one table has a toast table.
			 *
			 * NOTE: at present, a TOAST table's only dependency is the one on
			 * its owning table.  If more are ever created, we'd need to use
			 * something more selective than deleteDependencyRecordsFor() to
			 * get rid of just the link we want.
			 */
			ObjectAddress baseobject, toastobject;
			long count;

			/*
			 * The original code disallowed this case for system catalogs. We
			 * don't allow reordering system catalogs, but Assert anyway
			 */
			Assert(!IsSystemClass(r1, relform1));

			/* Delete old dependencies */
			if (relform1->reltoastrelid)
			{
				count =
					deleteDependencyRecordsFor(RelationRelationId, relform1->reltoastrelid, false);
				if (count != 1)
					elog(ERROR, "expected one dependency record for TOAST table, found %ld", count);
			}
			if (relform2->reltoastrelid)
			{
				count =
					deleteDependencyRecordsFor(RelationRelationId, relform2->reltoastrelid, false);
				if (count != 1)
					elog(ERROR, "expected one dependency record for TOAST table, found %ld", count);
			}

			/* Register new dependencies */
			baseobject.classId = RelationRelationId;
			baseobject.objectSubId = 0;
			toastobject.classId = RelationRelationId;
			toastobject.objectSubId = 0;

			if (relform1->reltoastrelid)
			{
				baseobject.objectId = r1;
				toastobject.objectId = relform1->reltoastrelid;
				recordDependencyOn(&toastobject, &baseobject, DEPENDENCY_INTERNAL);
			}

			if (relform2->reltoastrelid)
			{
				baseobject.objectId = r2;
				toastobject.objectId = relform2->reltoastrelid;
				recordDependencyOn(&toastobject, &baseobject, DEPENDENCY_INTERNAL);
			}
		}
	}

	/*
	 * If we're swapping two toast tables by content, do the same for their
	 * valid index. The swap can actually be safely done only if the relations
	 * have indexes.
	 */
	if (swap_toast_by_content && relform1->relkind == RELKIND_TOASTVALUE &&
		relform2->relkind == RELKIND_TOASTVALUE)
	{
		Oid toastIndex1, toastIndex2;

		/* Get valid index for each relation */
		toastIndex1 = toast_get_valid_index(r1, AccessExclusiveLock);
		toastIndex2 = toast_get_valid_index(r2, AccessExclusiveLock);

		swap_relation_files(toastIndex1,
							toastIndex2,
							swap_toast_by_content,
							is_internal,
							InvalidTransactionId,
							InvalidMultiXactId);
	}

	/* Clean up. */
	heap_freetuple(reltup1);
	heap_freetuple(reltup2);

	table_close(relRelation, RowExclusiveLock);

	/*
	 * Close both relcache entries' smgr links.  We need this kludge because
	 * both links will be invalidated during upcoming CommandCounterIncrement.
	 * Whichever of the rels is the second to be cleared will have a dangling
	 * reference to the other's smgr entry.  Rather than trying to avoid this
	 * by ordering operations just so, it's easiest to close the links first.
	 * (Fortunately, since one of the entries is local in our transaction,
	 * it's sufficient to clear out our own relcache this way; the problem
	 * cannot arise for other backends when they see our update on the
	 * non-transient relation.)
	 *
	 * Caution: the placement of this step interacts with the decision to
	 * handle toast rels by recursion.  When we are trying to rebuild pg_class
	 * itself, the smgr close on pg_class must happen after all accesses in
	 * this function.
	 */
	RelationCloseSmgrByOid(r1);
	RelationCloseSmgrByOid(r2);
}
