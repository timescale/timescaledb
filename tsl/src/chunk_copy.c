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
#include <executor/spi.h>

#ifdef USE_ASSERT_CHECKING
#include <funcapi.h>
#endif

#include <compat/compat.h>
#include "ts_catalog/chunk_data_node.h"
#include <extension.h>
#include <errors.h>
#include <error_utils.h>
#include <hypertable_cache.h>

#include "chunk.h"
#include "chunk_api.h"
#include "chunk_copy.h"
#include "data_node.h"
#include "debug_point.h"
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
	chunk_copy_stage_func function_cleanup;
};

/* To track a chunk move or copy activity */
struct ChunkCopy
{
	/* catalog data */
	FormData_chunk_copy_operation fd;
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
chunk_copy_operation_make_tuple(const FormData_chunk_copy_operation *fd, TupleDesc desc)
{
	Datum values[Natts_chunk_copy_operation];
	bool nulls[Natts_chunk_copy_operation] = { false };
	memset(values, 0, sizeof(values));
	values[AttrNumberGetAttrOffset(Anum_chunk_copy_operation_operation_id)] =
		NameGetDatum(&fd->operation_id);
	values[AttrNumberGetAttrOffset(Anum_chunk_copy_operation_backend_pid)] =
		Int32GetDatum(fd->backend_pid);
	values[AttrNumberGetAttrOffset(Anum_chunk_copy_operation_completed_stage)] =
		NameGetDatum(&fd->completed_stage);
	values[AttrNumberGetAttrOffset(Anum_chunk_copy_operation_time_start)] =
		TimestampTzGetDatum(fd->time_start);
	values[AttrNumberGetAttrOffset(Anum_chunk_copy_operation_chunk_id)] =
		Int32GetDatum(fd->chunk_id);
	values[AttrNumberGetAttrOffset(Anum_chunk_copy_operation_source_node_name)] =
		NameGetDatum(&fd->source_node_name);
	values[AttrNumberGetAttrOffset(Anum_chunk_copy_operation_dest_node_name)] =
		NameGetDatum(&fd->dest_node_name);
	values[AttrNumberGetAttrOffset(Anum_chunk_copy_operation_delete_on_src_node)] =
		BoolGetDatum(fd->delete_on_src_node);
	return heap_form_tuple(desc, values, nulls);
}

static void
chunk_copy_operation_insert_rel(Relation rel, const FormData_chunk_copy_operation *fd)
{
	CatalogSecurityContext sec_ctx;
	HeapTuple new_tuple;

	new_tuple = chunk_copy_operation_make_tuple(fd, RelationGetDescr(rel));

	ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);
	ts_catalog_insert(rel, new_tuple);
	ts_catalog_restore_user(&sec_ctx);
	heap_freetuple(new_tuple);
}

static void
chunk_copy_operation_insert(const FormData_chunk_copy_operation *fd)
{
	Catalog *catalog;
	Relation rel;

	catalog = ts_catalog_get();
	rel = table_open(catalog_get_table_id(catalog, CHUNK_COPY_OPERATION), RowExclusiveLock);

	chunk_copy_operation_insert_rel(rel, fd);
	table_close(rel, RowExclusiveLock);
}

static ScanTupleResult
chunk_copy_operation_tuple_update(TupleInfo *ti, void *data)
{
	ChunkCopy *cc = data;
	Datum values[Natts_chunk_copy_operation];
	bool nulls[Natts_chunk_copy_operation];
	CatalogSecurityContext sec_ctx;
	bool should_free;
	HeapTuple tuple = ts_scanner_fetch_heap_tuple(ti, false, &should_free);
	HeapTuple new_tuple;

	heap_deform_tuple(tuple, ts_scanner_get_tupledesc(ti), values, nulls);

	/* We only update the "completed_stage" field */
	Assert(NULL != cc->stage);
	values[AttrNumberGetAttrOffset(Anum_chunk_copy_operation_completed_stage)] =
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
chunk_copy_operation_scan_update_by_id(const char *operation_id, tuple_found_func tuple_found,
									   void *data, LOCKMODE lockmode)
{
	Catalog *catalog = ts_catalog_get();
	ScanKeyData scankey[1];
	ScannerCtx scanctx = {
		.table = catalog_get_table_id(catalog, CHUNK_COPY_OPERATION),
		.index = catalog_get_index(catalog, CHUNK_COPY_OPERATION, CHUNK_COPY_OPERATION_PKEY_IDX),
		.nkeys = 1,
		.limit = 1,
		.scankey = scankey,
		.data = data,
		.tuple_found = tuple_found,
		.lockmode = lockmode,
		.scandirection = ForwardScanDirection,
	};

	ScanKeyInit(&scankey[0],
				Anum_chunk_copy_operation_idx_operation_id,
				BTEqualStrategyNumber,
				F_NAMEEQ,
				CStringGetDatum(operation_id));

	return ts_scanner_scan(&scanctx);
}

static void
chunk_copy_operation_update(ChunkCopy *cc)
{
	NameData application_name;

	snprintf(application_name.data,
			 sizeof(application_name.data),
			 "%s:%s",
			 cc->fd.operation_id.data,
			 cc->stage->name);

	pgstat_report_appname(application_name.data);

	chunk_copy_operation_scan_update_by_id(NameStr(cc->fd.operation_id),
										   chunk_copy_operation_tuple_update,
										   cc,
										   RowExclusiveLock);
}

static ScanTupleResult
chunk_copy_operation_tuple_delete(TupleInfo *ti, void *data)
{
	CatalogSecurityContext sec_ctx;

	ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);
	ts_catalog_delete_tid(ti->scanrel, ts_scanner_get_tuple_tid(ti));
	ts_catalog_restore_user(&sec_ctx);

	return SCAN_CONTINUE;
}

static int
chunk_copy_operation_delete_by_id(const char *operation_id)
{
	Catalog *catalog = ts_catalog_get();
	ScanKeyData scankey[1];
	ScannerCtx scanctx = {
		.table = catalog_get_table_id(catalog, CHUNK_COPY_OPERATION),
		.index = catalog_get_index(catalog, CHUNK_COPY_OPERATION, CHUNK_COPY_OPERATION_PKEY_IDX),
		.nkeys = 1,
		.limit = 1,
		.scankey = scankey,
		.data = NULL,
		.tuple_found = chunk_copy_operation_tuple_delete,
		.lockmode = RowExclusiveLock,
		.scandirection = ForwardScanDirection,
	};

	ScanKeyInit(&scankey[0],
				Anum_chunk_copy_operation_idx_operation_id,
				BTEqualStrategyNumber,
				F_NAMEEQ,
				CStringGetDatum(operation_id));

	return ts_scanner_scan(&scanctx);
}

static void
chunk_copy_setup(ChunkCopy *cc, Oid chunk_relid, const char *src_node, const char *dst_node,
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
	 * Populate the FormData_chunk_copy_operation structure for use by various stages
	 *
	 * The operation_id will be populated in the chunk_copy_stage_init function.
	 */
	cc->fd.backend_pid = MyProcPid;
	namestrcpy(&cc->fd.completed_stage, CCS_INIT);
	cc->fd.time_start = GetCurrentTimestamp();
	cc->fd.chunk_id = cc->chunk->fd.id;
	namestrcpy(&cc->fd.source_node_name, src_node);
	namestrcpy(&cc->fd.dest_node_name, dst_node);
	cc->fd.delete_on_src_node = delete_on_src_node;

	ts_cache_release(hcache);
	MemoryContextSwitchTo(old);

	/* Commit to get out of starting transaction. This will also pop active
	 * snapshots. */
	SPI_commit();
}

static void
chunk_copy_finish(ChunkCopy *cc)
{
	/* Done using this long lived memory context */
	MemoryContextDelete(cc->mcxt);

	/* Start a transaction for the final outer transaction */
	SPI_start_transaction();
}

static void
chunk_copy_stage_init(ChunkCopy *cc)
{
	int32 id;

	/*
	 * Get the operation id for this chunk move/copy activity. The naming
	 * convention is "ts_copy_seq-id_chunk-id".
	 */
	id = ts_catalog_table_next_seq_id(ts_catalog_get(), CHUNK_COPY_OPERATION);
	snprintf(cc->fd.operation_id.data,
			 sizeof(cc->fd.operation_id.data),
			 "ts_copy_%d_%d",
			 id,
			 cc->chunk->fd.id);

	/* Persist the Formdata entry in the catalog */
	chunk_copy_operation_insert(&cc->fd);
}

static void
chunk_copy_stage_init_cleanup(ChunkCopy *cc)
{
	/* Failure in initial stages, delete this entry from the catalog */
	chunk_copy_operation_delete_by_id(NameStr(cc->fd.operation_id));
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
chunk_copy_stage_create_empty_chunk_cleanup(ChunkCopy *cc)
{
	/*
	 * Drop the chunk table on the dst_node. We use the API instead of just
	 * "DROP TABLE" because some metadata cleanup might also be needed
	 */
	chunk_api_call_chunk_drop_replica(cc->chunk,
									  NameStr(cc->fd.dest_node_name),
									  cc->dst_server->serverid);
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

	/* Create the publication */
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
chunk_copy_stage_create_replication_slot_cleanup(ChunkCopy *cc)
{
	char *cmd;
	DistCmdResult *dist_res;
	PGresult *res;

	/* Check if the slot exists on the source data node */
	cmd = psprintf("SELECT 1 FROM pg_catalog.pg_replication_slots WHERE slot_name = '%s'",
				   NameStr(cc->fd.operation_id));
	dist_res =
		ts_dist_cmd_invoke_on_data_nodes(cmd, list_make1(NameStr(cc->fd.source_node_name)), true);
	res = ts_dist_cmd_get_result_by_node_name(dist_res, NameStr(cc->fd.source_node_name));

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_EXCEPTION), errmsg("%s", PQresultErrorMessage(res))));

	/* Drop replication slot on the source data node only if it exists */
	if (PQntuples(res) != 0)
	{
		cmd = psprintf("SELECT pg_drop_replication_slot('%s')", NameStr(cc->fd.operation_id));
		ts_dist_cmd_run_on_data_nodes(cmd, list_make1(NameStr(cc->fd.source_node_name)), true);
	}

	ts_dist_cmd_close_response(dist_res);
}

static void
chunk_copy_stage_create_publication_cleanup(ChunkCopy *cc)
{
	char *cmd;
	DistCmdResult *dist_res;
	PGresult *res;

	/*
	 * Check if the replication slot exists and clean it up if so. This might
	 * happen if there's a failure in the create_replication_slot stage but
	 * PG might end up creating the slot even though we issued a ROLLBACK
	 */
	chunk_copy_stage_create_replication_slot_cleanup(cc);

	/* Check if the publication exists on the source data node */
	cmd = psprintf("SELECT 1 FROM pg_catalog.pg_publication WHERE pubname = '%s'",
				   NameStr(cc->fd.operation_id));
	dist_res =
		ts_dist_cmd_invoke_on_data_nodes(cmd, list_make1(NameStr(cc->fd.source_node_name)), true);
	res = ts_dist_cmd_get_result_by_node_name(dist_res, NameStr(cc->fd.source_node_name));

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_EXCEPTION), errmsg("%s", PQresultErrorMessage(res))));

	/* Drop publication on the source node only if it exists */
	if (PQntuples(res) != 0)
	{
		cmd = psprintf("DROP PUBLICATION %s", NameStr(cc->fd.operation_id));

		/* Drop the publication */
		ts_dist_cmd_run_on_data_nodes(cmd, list_make1(NameStr(cc->fd.source_node_name)), true);
	}

	ts_dist_cmd_close_response(dist_res);
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
chunk_copy_stage_create_subscription_cleanup(ChunkCopy *cc)
{
	char *cmd;
	DistCmdResult *dist_res;
	PGresult *res;

	/* Check if the subscription exists on the destination data node */
	cmd = psprintf("SELECT 1 FROM pg_catalog.pg_subscription WHERE subname = '%s'",
				   NameStr(cc->fd.operation_id));
	dist_res =
		ts_dist_cmd_invoke_on_data_nodes(cmd, list_make1(NameStr(cc->fd.dest_node_name)), true);
	res = ts_dist_cmd_get_result_by_node_name(dist_res, NameStr(cc->fd.dest_node_name));

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_EXCEPTION), errmsg("%s", PQresultErrorMessage(res))));

	/* Cleanup only if the subscription exists */
	if (PQntuples(res) != 0)
	{
		List *nodes = list_make1(NameStr(cc->fd.dest_node_name));

		/* Disassociate the subscription from the replication slot first */
		cmd =
			psprintf("ALTER SUBSCRIPTION %s SET (slot_name = NONE)", NameStr(cc->fd.operation_id));
		ts_dist_cmd_run_on_data_nodes(cmd, nodes, true);

		/* Drop the subscription now */
		pfree(cmd);
		cmd = psprintf("DROP SUBSCRIPTION %s", NameStr(cc->fd.operation_id));
		ts_dist_cmd_run_on_data_nodes(cmd, nodes, true);
	}

	ts_dist_cmd_close_response(dist_res);
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
chunk_copy_stage_sync_start_cleanup(ChunkCopy *cc)
{
	char *cmd;
	DistCmdResult *dist_res;
	PGresult *res;

	/* Check if the subscription exists on the destination data node */
	cmd = psprintf("SELECT 1 FROM pg_catalog.pg_subscription WHERE subname = '%s'",
				   NameStr(cc->fd.operation_id));
	dist_res =
		ts_dist_cmd_invoke_on_data_nodes(cmd, list_make1(NameStr(cc->fd.dest_node_name)), true);
	res = ts_dist_cmd_get_result_by_node_name(dist_res, NameStr(cc->fd.dest_node_name));

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_EXCEPTION), errmsg("%s", PQresultErrorMessage(res))));

	/* Alter subscription only if it exists */
	if (PQntuples(res) != 0)
	{
		/* Stop data transfer on the destination node */
		cmd = psprintf("ALTER SUBSCRIPTION %s DISABLE", NameStr(cc->fd.operation_id));
		ts_dist_cmd_run_on_data_nodes(cmd, list_make1(NameStr(cc->fd.dest_node_name)), true);
	}

	ts_dist_cmd_close_response(dist_res);
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
	char *cmd;

	cmd = psprintf("SELECT pg_drop_replication_slot('%s')", NameStr(cc->fd.operation_id));
	ts_dist_cmd_run_on_data_nodes(cmd, list_make1(NameStr(cc->fd.source_node_name)), true);

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
	/* Initial Marker */
	{ CCS_INIT, chunk_copy_stage_init, chunk_copy_stage_init_cleanup },

	/*
	 * Create empty chunk table on the dst node.
	 * The corresponding cleanup function should just delete this empty chunk.
	 */
	{ CCS_CREATE_EMPTY_CHUNK,
	  chunk_copy_stage_create_empty_chunk,
	  chunk_copy_stage_create_empty_chunk_cleanup },

	/*
	 * Setup logical replication between nodes.
	 * The corresponding cleanup functions should drop the subscription and
	 * remove the replication slot followed by dropping of the publication on
	 * the source data node.
	 */
	{ CCS_CREATE_PUBLICATION,
	  chunk_copy_stage_create_publication,
	  chunk_copy_stage_create_publication_cleanup },
	{ CCS_CREATE_REPLICATION_SLOT,
	  chunk_copy_stage_create_replication_slot,
	  chunk_copy_stage_create_replication_slot_cleanup },
	{ CCS_CREATE_SUBSCRIPTION,
	  chunk_copy_stage_create_subscription,
	  chunk_copy_stage_create_subscription_cleanup },

	/*
	 * Begin data transfer and wait for completion.
	 * The corresponding cleanup function should just disable the subscription so
	 * that earlier steps above can drop the subcription/publication cleanly.
	 */
	{ CCS_SYNC_START, chunk_copy_stage_sync_start, chunk_copy_stage_sync_start_cleanup },
	{ CCS_SYNC, chunk_copy_stage_sync, NULL },

	/*
	 * Cleanup. Nothing else required via the cleanup functions.
	 */
	{ CCS_DROP_SUBSCRIPTION, chunk_copy_stage_drop_subscription, NULL },
	{ CCS_DROP_PUBLICATION, chunk_copy_stage_drop_publication, NULL },

	/*
	 * Attach chunk to the hypertable on the dst_node.
	 * The operation has succeeded from the destination data node perspective.
	 * No cleanup required here.
	 */
	{ CCS_ATTACH_CHUNK, chunk_copy_stage_attach_chunk, NULL },

	/*
	 * Maybe delete chunk from the src_node (move operation).
	 * Again, everything ok, so no cleanup required, we probably shouldn't be
	 * seeing this entry in the catalog table because the operation has succeeded.
	 */
	{ CCS_DELETE_CHUNK, chunk_copy_stage_delete_chunk, NULL },

	/* Done Marker */
	{ NULL, NULL, NULL }
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
		SPI_start_transaction();

		cc->stage = stage;
		cc->stage->function(cc);

		/* Mark current stage as completed and update the catalog */
		chunk_copy_operation_update(cc);

		DEBUG_ERROR_INJECTION(stage->name);

		SPI_commit();
	}
}

void
chunk_copy(Oid chunk_relid, const char *src_node, const char *dst_node, bool delete_on_src_node)
{
	ChunkCopy cc;
	const MemoryContext oldcontext = CurrentMemoryContext;

	/* Populate copy structure */
	chunk_copy_setup(&cc, chunk_relid, src_node, dst_node, delete_on_src_node);

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
		edata->detail = psprintf("Chunk copy operation id: %s.", NameStr(cc.fd.operation_id));
		FlushErrorState();
		ReThrowError(edata);
	}
	PG_END_TRY();

	/* Finish up and delete the catalog entry */
	chunk_copy_finish(&cc);
}

static ScanTupleResult
chunk_copy_operation_tuple_found(TupleInfo *ti, void *const data)
{
	ChunkCopy **cc = data;

	*cc = STRUCT_FROM_SLOT(ti->slot, ti->mctx, ChunkCopy, FormData_chunk_copy_operation);
	return SCAN_CONTINUE;
}

static ChunkCopy *
chunk_copy_operation_get(const char *operation_id)
{
	ScanKeyData scankeys[1];
	ChunkCopy *cc = NULL;
	int indexid;
	MemoryContext old, mcxt;

	/* Objects need to be in long lived context */
	mcxt =
		AllocSetContextCreate(PortalContext, "chunk copy cleanup activity", ALLOCSET_DEFAULT_SIZES);
	old = MemoryContextSwitchTo(mcxt);

	if (operation_id != NULL)
	{
		ScanKeyInit(&scankeys[0],
					Anum_chunk_copy_operation_idx_operation_id,
					BTEqualStrategyNumber,
					F_NAMEEQ,
					CStringGetDatum(operation_id));
		indexid = CHUNK_COPY_OPERATION_PKEY_IDX;
	}
	else
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid chunk copy operation identifier")));

	ts_catalog_scan_one(CHUNK_COPY_OPERATION,
						indexid,
						scankeys,
						1,
						chunk_copy_operation_tuple_found,
						AccessShareLock,
						CHUNK_COPY_OPERATION_TABLE_NAME,
						&cc);

	/*
	 * If a valid entry is returned then fill up the rest of the fields in the
	 * ChunkCopy structure
	 */
	if (cc)
	{
		cc->mcxt = mcxt;
		cc->chunk = ts_chunk_get_by_id(cc->fd.chunk_id, true);
		cc->stage = NULL;

		/* No other sanity checks need to be performed since they were done earlier */

		/* Setup the src_node */
		cc->src_server =
			data_node_get_foreign_server(NameStr(cc->fd.source_node_name), ACL_USAGE, true, false);
		Assert(NULL != cc->src_server);

		/* Setup the dst_node */
		cc->dst_server =
			data_node_get_foreign_server(NameStr(cc->fd.dest_node_name), ACL_USAGE, true, false);
		Assert(NULL != cc->dst_server);
	}

	MemoryContextSwitchTo(old);

	if (cc == NULL)
		/* No entry found, long lived context not required */
		MemoryContextDelete(mcxt);

	return cc;
}

static void
chunk_copy_cleanup_internal(ChunkCopy *cc, int stage_idx)
{
	bool first = true;

	/* Cleanup each copy stage in a separate transaction */
	do
	{
		SPI_start_transaction();

		cc->stage = &chunk_copy_stages[stage_idx];
		if (cc->stage->function_cleanup)
			cc->stage->function_cleanup(cc);

		/* Mark stage as cleaned up and update the catalog */
		if (!first && stage_idx != 0)
			chunk_copy_operation_update(cc);
		else
			first = false;

		SPI_commit();
	} while (--stage_idx >= 0);
}

void
chunk_copy_cleanup(const char *operation_id)
{
	ChunkCopy *cc;
	const MemoryContext oldcontext = CurrentMemoryContext;
	const ChunkCopyStage *stage;
	bool found = false;
	int stage_idx;

	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 (errmsg("must be superuser to cleanup a chunk copy operation"))));

	if (dist_util_membership() != DIST_MEMBER_ACCESS_NODE)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("function must be run on the access node only")));

	cc = chunk_copy_operation_get(operation_id);

	if (cc == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid chunk copy operation identifier. Entry not found")));

	/* Identify the last completed stage for this activity. */
	stage_idx = 0;
	for (stage = &chunk_copy_stages[stage_idx]; stage->name != NULL;
		 stage = &chunk_copy_stages[++stage_idx])
	{
		if (namestrcmp(&cc->fd.completed_stage, stage->name) == 0)
		{
			found = true;
			break;
		}
	}

	/* should always find an entry, add ereport to quell compiler warning */
	Assert(found == true);
	if (!found)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("stage '%s' not found for copy chunk cleanup",
						NameStr(cc->fd.completed_stage))));

	/* Commit to get out of starting transaction, this will also pop active
	 * snapshots. */
	SPI_commit();

	/* Run the corresponding cleanup steps to roll back the activity. */
	PG_TRY();
	{
		chunk_copy_cleanup_internal(cc, stage_idx);
	}
	PG_CATCH();
	{
		/* Include chunk copy id to the error message */
		ErrorData *edata;
		MemoryContextSwitchTo(oldcontext);
		edata = CopyErrorData();
		edata->detail = psprintf("While cleaning up chunk copy operation id: %s.",
								 NameStr(cc->fd.operation_id));
		FlushErrorState();
		ReThrowError(edata);
	}
	PG_END_TRY();

	/* Finish up and delete the catalog entry */
	chunk_copy_finish(cc);
}
