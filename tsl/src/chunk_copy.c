/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <foreign/foreign.h>
#include <catalog/pg_foreign_server.h>
#include <catalog/pg_foreign_table.h>
#include <catalog/dependency.h>
#include <catalog/namespace.h>
#include <access/htup_details.h>
#include <access/xact.h>
#include <nodes/makefuncs.h>
#include <utils/acl.h>
#include <utils/builtins.h>
#include <utils/syscache.h>
#include <utils/inval.h>
#include <utils/tuplestore.h>
#include <utils/palloc.h>
#include <utils/memutils.h>
#include <utils/snapmgr.h>
#include <executor/executor.h>
#include <parser/parse_func.h>
#include <funcapi.h>
#include <miscadmin.h>
#include <fmgr.h>

#if USE_ASSERT_CHECKING
#include <funcapi.h>
#endif

#include <compat.h>
#include <chunk_data_node.h>
#include <extension.h>
#include <errors.h>
#include <error_utils.h>
#include <hypertable_cache.h>

#include "chunk.h"
#include "chunk_api.h"
#include "chunk_copy.h"
#include "data_node.h"
#include "remote/dist_commands.h"
#include "dist_util.h"

#define CCS_INIT "init"
#define CCS_CREATE_EMPTY_CHUNK "create_empty_chunk"
#define CCS_CREATE_PUBLICATION "create_publication"
#define CCS_CREATE_REPLICATION_SLOT "create_replication_slot"
#define CCS_CREATE_SUBSCRIPTION "create_subscription"
#define CCS_SYNC_START "sync_start"
#define CCS_SYNC "sync"
#define CCS_DROP_PUBLICATION "drop_publication"
#define CCS_DROP_SUBSCRIPTION "drop_subscription"
#define CCS_ATTACH_CHUNK "attach_chunk"
#define CCS_DELETE_CHUNK "delete_chunk"

typedef struct ChunkCopyStage ChunkCopyStage;
typedef struct ChunkCopy ChunkCopy;

typedef void (*chunk_copy_stage_func)(ChunkCopy *);

struct ChunkCopyStage
{
	const char *name;
	chunk_copy_stage_func function;
	/* todo: abort function */
};

/* To track a chunk move or copy activity */
struct ChunkCopy
{
	/* catalog data */
	FormData_chunk_copy_activity fd;
	/* current stage being executed */
	const ChunkCopyStage *stage;
	/* chunk to copy */
	Chunk *chunk;
	/* from/to foreign servers */
	ForeignServer *src_server;
	ForeignServer *dst_server;
	/* temporary memory context */
	MemoryContext mcxt;
};

static HeapTuple
chunk_copy_activity_make_tuple(const FormData_chunk_copy_activity *fd, TupleDesc desc)
{
	Datum values[Natts_chunk_copy_activity];
	bool nulls[Natts_chunk_copy_activity] = { false };
	memset(values, 0, sizeof(values));
	values[AttrNumberGetAttrOffset(Anum_chunk_copy_activity_id)] = Int32GetDatum(fd->id);
	values[AttrNumberGetAttrOffset(Anum_chunk_copy_activity_operation_id)] =
		NameGetDatum(&fd->operation_id);
	values[AttrNumberGetAttrOffset(Anum_chunk_copy_activity_backend_pid)] =
		Int32GetDatum(fd->backend_pid);
	values[AttrNumberGetAttrOffset(Anum_chunk_copy_activity_completed_stage)] =
		NameGetDatum(&fd->completed_stage);
	values[AttrNumberGetAttrOffset(Anum_chunk_copy_activity_time_start)] =
		TimestampTzGetDatum(fd->time_start);
	values[AttrNumberGetAttrOffset(Anum_chunk_copy_activity_chunk_id)] =
		Int32GetDatum(fd->chunk_id);
	values[AttrNumberGetAttrOffset(Anum_chunk_copy_activity_source_node_name)] =
		NameGetDatum(&fd->source_node_name);
	values[AttrNumberGetAttrOffset(Anum_chunk_copy_activity_dest_node_name)] =
		NameGetDatum(&fd->dest_node_name);
	values[AttrNumberGetAttrOffset(Anum_chunk_copy_activity_delete_on_src_node)] =
		BoolGetDatum(fd->delete_on_src_node);
	return heap_form_tuple(desc, values, nulls);
}

static void
chunk_copy_activity_insert_rel(Relation rel, const FormData_chunk_copy_activity *fd)
{
	CatalogSecurityContext sec_ctx;
	HeapTuple new_tuple;

	new_tuple = chunk_copy_activity_make_tuple(fd, RelationGetDescr(rel));

	ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);
	ts_catalog_insert(rel, new_tuple);
	ts_catalog_restore_user(&sec_ctx);
	heap_freetuple(new_tuple);
}

static void
chunk_copy_activity_insert(const FormData_chunk_copy_activity *fd)
{
	Catalog *catalog;
	Relation rel;

	catalog = ts_catalog_get();
	rel = table_open(catalog_get_table_id(catalog, CHUNK_COPY_ACTIVITY), RowExclusiveLock);

	chunk_copy_activity_insert_rel(rel, fd);
	table_close(rel, RowExclusiveLock);
}

static ScanTupleResult
chunk_copy_activity_tuple_update(TupleInfo *ti, void *data)
{
	ChunkCopy *cc = data;
	Datum values[Natts_chunk_copy_activity];
	bool nulls[Natts_chunk_copy_activity];
	CatalogSecurityContext sec_ctx;
	bool should_free;
	HeapTuple tuple = ts_scanner_fetch_heap_tuple(ti, false, &should_free);
	HeapTuple new_tuple;

	heap_deform_tuple(tuple, ts_scanner_get_tupledesc(ti), values, nulls);

	/* We only update the "completed_stage" field */
	Assert(NULL != cc->stage);
	values[AttrNumberGetAttrOffset(Anum_chunk_copy_activity_completed_stage)] =
		DirectFunctionCall1(namein, CStringGetDatum((cc->stage->name)));

	new_tuple = heap_form_tuple(ts_scanner_get_tupledesc(ti), values, nulls);
	ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);
	ts_catalog_update_tid(ti->scanrel, ts_scanner_get_tuple_tid(ti), new_tuple);
	ts_catalog_restore_user(&sec_ctx);
	heap_freetuple(new_tuple);

	if (should_free)
		heap_freetuple(tuple);

	return SCAN_DONE;
}

static int
chunk_copy_activity_scan_update_by_id(int32 id, tuple_found_func tuple_found, void *data,
									  LOCKMODE lockmode)
{
	Catalog *catalog = ts_catalog_get();
	ScanKeyData scankey[1];
	ScannerCtx scanctx = {
		.table = catalog_get_table_id(catalog, CHUNK_COPY_ACTIVITY),
		.index = catalog_get_index(catalog, CHUNK_COPY_ACTIVITY, CHUNK_COPY_ACTIVITY_PKEY_IDX),
		.nkeys = 1,
		.limit = 1,
		.scankey = scankey,
		.data = data,
		.tuple_found = tuple_found,
		.lockmode = lockmode,
		.scandirection = ForwardScanDirection,
	};

	ScanKeyInit(&scankey[0],
				Anum_chunk_copy_activity_pkey_idx_id,
				BTEqualStrategyNumber,
				F_INT4EQ,
				Int32GetDatum(id));

	return ts_scanner_scan(&scanctx);
}

static void
chunk_copy_activity_update(ChunkCopy *cc)
{
	NameData application_name;

	snprintf(application_name.data,
			 sizeof(application_name.data),
			 "%s:%s",
			 cc->fd.operation_id.data,
			 cc->stage->name);

	pgstat_report_appname(application_name.data);

	chunk_copy_activity_scan_update_by_id(cc->fd.id,
										  chunk_copy_activity_tuple_update,
										  cc,
										  RowExclusiveLock);
}

static ScanTupleResult
chunk_copy_activity_tuple_delete(TupleInfo *ti, void *data)
{
	CatalogSecurityContext sec_ctx;

	ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);
	ts_catalog_delete_tid(ti->scanrel, ts_scanner_get_tuple_tid(ti));
	ts_catalog_restore_user(&sec_ctx);

	return SCAN_CONTINUE;
}

static int
chunk_copy_activity_delete_by_id(int32 id)
{
	Catalog *catalog = ts_catalog_get();
	ScanKeyData scankey[1];
	ScannerCtx scanctx = {
		.table = catalog_get_table_id(catalog, CHUNK_COPY_ACTIVITY),
		.index = catalog_get_index(catalog, CHUNK_COPY_ACTIVITY, CHUNK_COPY_ACTIVITY_PKEY_IDX),
		.nkeys = 1,
		.limit = 1,
		.scankey = scankey,
		.data = NULL,
		.tuple_found = chunk_copy_activity_tuple_delete,
		.lockmode = RowExclusiveLock,
		.scandirection = ForwardScanDirection,
	};

	ScanKeyInit(&scankey[0],
				Anum_chunk_copy_activity_pkey_idx_id,
				BTEqualStrategyNumber,
				F_INT4EQ,
				Int32GetDatum(id));

	return ts_scanner_scan(&scanctx);
}

static void
chunk_copy_init(ChunkCopy *cc, Oid chunk_relid, const char *src_node, const char *dst_node,
				bool delete_on_src_node)
{
	Hypertable *ht;
	Cache *hcache;
	MemoryContext old, mcxt;

	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 (errmsg("must be superuser to copy/move chunk to data node"))));

	if (dist_util_membership() != DIST_MEMBER_ACCESS_NODE)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("function must be run on the access node only")));

	/*
	 * The chunk and foreign server info needs to be on a memory context
	 * that will survive moving to a new transaction for each stage
	 */
	mcxt = AllocSetContextCreate(PortalContext, "chunk move activity", ALLOCSET_DEFAULT_SIZES);
	old = MemoryContextSwitchTo(mcxt);
	cc->mcxt = mcxt;
	cc->chunk = ts_chunk_get_by_relid(chunk_relid, true);
	cc->stage = NULL;

	/* It has to be a foreign table chunk */
	if (cc->chunk->relkind != RELKIND_FOREIGN_TABLE)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("\"%s\" is not a valid remote chunk", get_rel_name(chunk_relid))));

	/* It has to be an uncompressed chunk, we query the status field on the AN for this */
	if (ts_chunk_is_compressed(cc->chunk))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("\"%s\" is a compressed remote chunk. Chunk copy/move not supported"
						" currently on compressed chunks",
						get_rel_name(chunk_relid))));

	ht = ts_hypertable_cache_get_cache_and_entry(cc->chunk->hypertable_relid,
												 CACHE_FLAG_NONE,
												 &hcache);

	ts_hypertable_permissions_check(ht->main_table_relid, GetUserId());

	if (!hypertable_is_distributed(ht))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("hypertable \"%s\" is not distributed",
						get_rel_name(ht->main_table_relid))));

	cc->src_server = data_node_get_foreign_server(src_node, ACL_USAGE, true, false);
	Assert(NULL != cc->src_server);

	cc->dst_server = data_node_get_foreign_server(dst_node, ACL_USAGE, true, false);
	Assert(NULL != cc->dst_server);

	/* Ensure that source and destination data nodes are not the same */
	if (cc->src_server == cc->dst_server)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("source and destination data node match")));

	/* Check that src_node is a valid DN and that chunk exists on it */
	if (!ts_chunk_has_data_node(cc->chunk, src_node))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("chunk \"%s\" does not exist on source data node \"%s\"",
						get_rel_name(chunk_relid),
						src_node)));

	/* Check that dst_node is a valid DN and that chunk does not exist on it */
	if (ts_chunk_has_data_node(cc->chunk, dst_node))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("chunk \"%s\" already exists on destination data node \"%s\"",
						get_rel_name(chunk_relid),
						dst_node)));

	/*
	 * Populate the FormData_chunk_copy_activity structure for use by various stages
	 *
	 * Get the operation id for this chunk move/copy activity. The naming
	 * convention is "ts_copy_seq-id_chunk-id and it can
	 * get truncated due to NAMEDATALEN restrictions
	 */
	cc->fd.id = ts_catalog_table_next_seq_id(ts_catalog_get(), CHUNK_COPY_ACTIVITY);
	snprintf(cc->fd.operation_id.data,
			 sizeof(cc->fd.operation_id.data),
			 "ts_copy_%d_%d",
			 cc->fd.id,
			 cc->chunk->fd.id);
	cc->fd.backend_pid = MyProcPid;
	namestrcpy(&cc->fd.completed_stage, CCS_INIT);
	cc->fd.time_start = GetCurrentTimestamp();
	cc->fd.chunk_id = cc->chunk->fd.id;
	namestrcpy(&cc->fd.source_node_name, src_node);
	namestrcpy(&cc->fd.dest_node_name, dst_node);
	cc->fd.delete_on_src_node = delete_on_src_node;

	/* Persist the entry in the catalog */
	chunk_copy_activity_insert(&cc->fd);

	ts_cache_release(hcache);
	MemoryContextSwitchTo(old);

	/* Commit to get out of starting transaction */
	PopActiveSnapshot();
	CommitTransactionCommand();
}

static void
chunk_copy_cleanup(ChunkCopy *cc)
{
	/* Done using this long lived memory context */
	MemoryContextDelete(cc->mcxt);

	/* Start a transaction for the final outer transaction */
	StartTransactionCommand();

	/* All steps complete, delete this ccd entry from the catalog now */
	chunk_copy_activity_delete_by_id(cc->fd.id);
}

static void
chunk_copy_stage_create_empty_chunk(ChunkCopy *cc)
{
	/* Create an empty chunk table on the dst_node */
	Cache *hcache;
	Hypertable *ht;

	ht = ts_hypertable_cache_get_cache_and_entry(cc->chunk->hypertable_relid,
												 CACHE_FLAG_NONE,
												 &hcache);

	chunk_api_call_create_empty_chunk_table(ht, cc->chunk, NameStr(cc->fd.dest_node_name));

	ts_cache_release(hcache);
}

static void
chunk_copy_stage_create_publication(ChunkCopy *cc)
{
	const char *cmd;

	/* Create publication on the source data node */
	cmd = psprintf("CREATE PUBLICATION %s FOR TABLE %s",
				   NameStr(cc->fd.operation_id),
				   quote_qualified_identifier(NameStr(cc->chunk->fd.schema_name),
											  NameStr(cc->chunk->fd.table_name)));

	/* Create the publication in autocommit mode */
	ts_dist_cmd_run_on_data_nodes(cmd, list_make1(NameStr(cc->fd.source_node_name)), true);
}

static void
chunk_copy_stage_create_replication_slot(ChunkCopy *cc)
{
	const char *cmd;

	/*
	 * CREATE SUBSCRIPTION from a database within the same database cluster will hang,
	 * create the replication slot separately before creating the subscription
	 */
	cmd = psprintf("SELECT pg_create_logical_replication_slot('%s', 'pgoutput')",
				   NameStr(cc->fd.operation_id));

	ts_dist_cmd_run_on_data_nodes(cmd, list_make1(NameStr(cc->fd.source_node_name)), true);
}

static void
chunk_copy_stage_create_subscription(ChunkCopy *cc)
{
	const char *cmd;
	const char *connection_string;

	/* Prepare connection string to the source node */
	connection_string = remote_connection_get_connstr(NameStr(cc->fd.source_node_name));

	cmd = psprintf("CREATE SUBSCRIPTION %s CONNECTION '%s' PUBLICATION %s"
				   " WITH (create_slot = false, enabled = false)",
				   NameStr(cc->fd.operation_id),
				   connection_string,
				   NameStr(cc->fd.operation_id));
	ts_dist_cmd_run_on_data_nodes(cmd, list_make1(NameStr(cc->fd.dest_node_name)), true);
}

static void
chunk_copy_stage_sync_start(ChunkCopy *cc)
{
	const char *cmd;

	/* Start data transfer on the destination node */
	cmd = psprintf("ALTER SUBSCRIPTION %s ENABLE", NameStr(cc->fd.operation_id));
	ts_dist_cmd_run_on_data_nodes(cmd, list_make1(NameStr(cc->fd.dest_node_name)), true);
}

static void
chunk_copy_stage_sync(ChunkCopy *cc)
{
	char *cmd;

	/*
	 * Transaction blocks run in REPEATABLE READ mode in the connection pool.
	 * However this wait_subscription_sync procedure needs to refresh the subcription
	 * sync status data and hence needs a READ COMMITTED transaction isolation
	 * level for that.
	 */
	cmd = psprintf("SET transaction_isolation TO 'READ COMMITTED'");
	ts_dist_cmd_run_on_data_nodes(cmd, list_make1(NameStr(cc->fd.dest_node_name)), true);
	pfree(cmd);

	/* Wait until data transfer finishes in its own transaction */
	cmd = psprintf("CALL _timescaledb_internal.wait_subscription_sync(%s, %s)",
				   quote_literal_cstr(NameStr(cc->chunk->fd.schema_name)),
				   quote_literal_cstr(NameStr(cc->chunk->fd.table_name)));

	ts_dist_cmd_run_on_data_nodes(cmd, list_make1(NameStr(cc->fd.dest_node_name)), true);
	pfree(cmd);
}

static void
chunk_copy_stage_drop_subscription(ChunkCopy *cc)
{
	char *cmd;

	/* Stop data transfer on the destination node */
	cmd = psprintf("ALTER SUBSCRIPTION %s DISABLE", NameStr(cc->fd.operation_id));
	ts_dist_cmd_run_on_data_nodes(cmd, list_make1(NameStr(cc->fd.dest_node_name)), true);
	pfree(cmd);

	/* Disassociate the subscription from the replication slot first */
	cmd = psprintf("ALTER SUBSCRIPTION %s SET (slot_name = NONE)", NameStr(cc->fd.operation_id));
	ts_dist_cmd_run_on_data_nodes(cmd, list_make1(NameStr(cc->fd.dest_node_name)), true);
	pfree(cmd);

	/* Drop the subscription now */
	cmd = psprintf("DROP SUBSCRIPTION %s", NameStr(cc->fd.operation_id));
	ts_dist_cmd_run_on_data_nodes(cmd, list_make1(NameStr(cc->fd.dest_node_name)), true);
	pfree(cmd);
}

static void
chunk_copy_stage_drop_publication(ChunkCopy *cc)
{
	const char *cmd;

	cmd = psprintf("DROP PUBLICATION %s", NameStr(cc->fd.operation_id));
	ts_dist_cmd_run_on_data_nodes(cmd, list_make1(NameStr(cc->fd.source_node_name)), true);
}

static void
chunk_copy_stage_attach_chunk(ChunkCopy *cc)
{
	Cache *hcache;
	Hypertable *ht;
	ChunkDataNode *chunk_data_node;
	const char *remote_chunk_name;
	Chunk *chunk = cc->chunk;

	ht = ts_hypertable_cache_get_cache_and_entry(chunk->hypertable_relid, CACHE_FLAG_NONE, &hcache);

	/* Check that the hypertable is already attached to this data node */
	data_node_hypertable_get_by_node_name(ht, cc->dst_server->servername, true);

	chunk_data_node = palloc0(sizeof(ChunkDataNode));

	chunk_data_node->fd.chunk_id = chunk->fd.id;
	chunk_data_node->fd.node_chunk_id = -1; /* below API will fill it up */
	namestrcpy(&chunk_data_node->fd.node_name, cc->dst_server->servername);
	chunk_data_node->foreign_server_oid = cc->dst_server->serverid;

	remote_chunk_name = psprintf("%s.%s",
								 quote_identifier(chunk->fd.schema_name.data),
								 quote_identifier(chunk->fd.table_name.data));

	chunk_api_create_on_data_nodes(chunk, ht, remote_chunk_name, list_make1(chunk_data_node));

	/* All ok, update the AN chunk metadata to add this data node to it */
	chunk->data_nodes = lappend(chunk->data_nodes, chunk_data_node);

	/* persist this association in the metadata */
	ts_chunk_data_node_insert(chunk_data_node);

	ts_cache_release(hcache);
}

static void
chunk_copy_stage_delete_chunk(ChunkCopy *cc)
{
	if (!cc->fd.delete_on_src_node)
		return;

	chunk_api_call_chunk_drop_replica(cc->chunk,
									  NameStr(cc->fd.source_node_name),
									  cc->src_server->serverid);
}

static const ChunkCopyStage chunk_copy_stages[] = {

	/* Create empty chunk table on the dst node */
	{ CCS_CREATE_EMPTY_CHUNK, chunk_copy_stage_create_empty_chunk },

	/* Setup logical replication between nodes */
	{ CCS_CREATE_PUBLICATION, chunk_copy_stage_create_publication },
	{ CCS_CREATE_REPLICATION_SLOT, chunk_copy_stage_create_replication_slot },
	{ CCS_CREATE_SUBSCRIPTION, chunk_copy_stage_create_subscription },

	/* Begin data transfer and wait for completion */
	{ CCS_SYNC_START, chunk_copy_stage_sync_start },
	{ CCS_SYNC, chunk_copy_stage_sync },

	/* Cleanup */
	{ CCS_DROP_PUBLICATION, chunk_copy_stage_drop_publication },
	{ CCS_DROP_SUBSCRIPTION, chunk_copy_stage_drop_subscription },

	/* Attach chunk to the hypertable on the dst_node */
	{ CCS_ATTACH_CHUNK, chunk_copy_stage_attach_chunk },

	/* Maybe delete chunk from the src_node (move operation) */
	{ CCS_DELETE_CHUNK, chunk_copy_stage_delete_chunk },

	/* Done */
	{ NULL, NULL }
};

static void
chunk_copy_execute(ChunkCopy *cc)
{
	const ChunkCopyStage *stage;

	/*
	 * Execute each copy stage in a separate transaction. The below will employ
	 * 2PC by default. This can be later optimized to use 1PC since only one
	 * datanode is involved in most of the stages.
	 */
	for (stage = &chunk_copy_stages[0]; stage->name != NULL; stage++)
	{
		StartTransactionCommand();

		cc->stage = stage;
		cc->stage->function(cc);

		/* Mark current stage as completed and update the catalog */
		chunk_copy_activity_update(cc);

		CommitTransactionCommand();
	}
}

void
chunk_copy(Oid chunk_relid, const char *src_node, const char *dst_node, bool delete_on_src_node)
{
	ChunkCopy cc;
	const MemoryContext oldcontext = CurrentMemoryContext;

	/* Populate copy structure and insert initial catalog entry */
	chunk_copy_init(&cc, chunk_relid, src_node, dst_node, delete_on_src_node);

	/* Execute chunk copy in separate stages */
	PG_TRY();
	{
		chunk_copy_execute(&cc);
	}
	PG_CATCH();
	{
		/* Include chunk copy id to the error message */
		ErrorData *edata;
		MemoryContextSwitchTo(oldcontext);
		edata = CopyErrorData();
		edata->hint =
			psprintf("chunk copy operation id: %d (%s).", cc.fd.id, NameStr(cc.fd.operation_id));
		FlushErrorState();
		ReThrowError(edata);
	}
	PG_END_TRY();

	/* Cleanup and delete the catalog entry */
	chunk_copy_cleanup(&cc);
}
