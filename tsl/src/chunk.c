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
#include "data_node.h"
#include "deparse.h"
#include "remote/dist_commands.h"
#include "dist_util.h"

static bool
chunk_match_data_node_by_server(const Chunk *chunk, const ForeignServer *server)
{
	bool server_found = false;
	ListCell *lc;

	foreach (lc, chunk->data_nodes)
	{
		ChunkDataNode *cdn = lfirst(lc);

		if (cdn->foreign_server_oid == server->serverid)
		{
			server_found = true;
			break;
		}
	}

	return server_found;
}

static bool
chunk_set_foreign_server(Chunk *chunk, ForeignServer *new_server)
{
	Relation ftrel;
	HeapTuple tuple;
	HeapTuple copy;
	Datum values[Natts_pg_foreign_table];
	bool nulls[Natts_pg_foreign_table];
	CatalogSecurityContext sec_ctx;
	Oid old_server_id;
	long updated;

	if (!chunk_match_data_node_by_server(chunk, new_server))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("chunk \"%s\" does not exist on data node \"%s\"",
						get_rel_name(chunk->table_id),
						new_server->servername)));

	tuple = SearchSysCache1(FOREIGNTABLEREL, ObjectIdGetDatum(chunk->table_id));

	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("chunk \"%s\" is not a foreign table", get_rel_name(chunk->table_id))));

	ftrel = table_open(ForeignTableRelationId, RowExclusiveLock);

	heap_deform_tuple(tuple, RelationGetDescr(ftrel), values, nulls);

	old_server_id =
		DatumGetObjectId(values[AttrNumberGetAttrOffset(Anum_pg_foreign_table_ftserver)]);

	if (old_server_id == new_server->serverid)
	{
		table_close(ftrel, RowExclusiveLock);
		ReleaseSysCache(tuple);
		return false;
	}

	values[AttrNumberGetAttrOffset(Anum_pg_foreign_table_ftserver)] =
		ObjectIdGetDatum(new_server->serverid);

	copy = heap_form_tuple(RelationGetDescr(ftrel), values, nulls);

	ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);
	ts_catalog_update_tid(ftrel, &tuple->t_self, copy);
	ts_catalog_restore_user(&sec_ctx);

	table_close(ftrel, RowExclusiveLock);
	heap_freetuple(copy);
	ReleaseSysCache(tuple);
	/* invalidate foreign table cache */
	CacheInvalidateRelcacheByRelid(ForeignTableRelationId);
	/* update dependencies between foreign table and foreign server */
	updated = changeDependencyFor(RelationRelationId,
								  chunk->table_id,
								  ForeignServerRelationId,
								  old_server_id,
								  new_server->serverid);
	if (updated != 1)
		elog(ERROR, "could not update data node for chunk \"%s\"", get_rel_name(chunk->table_id));

	/* make changes visible */
	CommandCounterIncrement();

	return true;
}

void
chunk_update_foreign_server_if_needed(int32 chunk_id, Oid existing_server_id)
{
	ListCell *lc;
	ChunkDataNode *new_server = NULL;
	Chunk *chunk = ts_chunk_get_by_id(chunk_id, true);
	ForeignTable *foreign_table = NULL;

	Assert(chunk->relkind == RELKIND_FOREIGN_TABLE);
	foreign_table = GetForeignTable(chunk->table_id);

	/* no need to update since foreign table doesn't reference server we try to remove */
	if (existing_server_id != foreign_table->serverid)
		return;

	Assert(list_length(chunk->data_nodes) > 1);

	foreach (lc, chunk->data_nodes)
	{
		new_server = lfirst(lc);
		if (new_server->foreign_server_oid != existing_server_id)
			break;
	}
	Assert(new_server != NULL);

	chunk_set_foreign_server(chunk, GetForeignServer(new_server->foreign_server_oid));
}

Datum
chunk_set_default_data_node(PG_FUNCTION_ARGS)
{
	Oid chunk_relid = PG_ARGISNULL(0) ? InvalidOid : PG_GETARG_OID(0);
	const char *node_name = PG_ARGISNULL(1) ? NULL : PG_GETARG_CSTRING(1);
	ForeignServer *server;
	Chunk *chunk;

	if (!OidIsValid(chunk_relid))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid chunk: cannot be NULL")));

	chunk = ts_chunk_get_by_relid(chunk_relid, false);

	if (NULL == chunk)
		ereport(ERROR,
				(errcode(ERRCODE_TS_CHUNK_NOT_EXIST),
				 errmsg("relation \"%s\" is not a chunk", get_rel_name(chunk_relid))));

	ts_hypertable_permissions_check(chunk->hypertable_relid, GetUserId());

	server = data_node_get_foreign_server(node_name, ACL_USAGE, true, false);

	Assert(NULL != server);

	PG_RETURN_BOOL(chunk_set_foreign_server(chunk, server));
}

/*
 * Invoke drop_chunks via fmgr so that the call can be deparsed and sent to
 * remote data nodes.
 *
 * Given that drop_chunks is an SRF, and has pseudo parameter types, we need
 * to provide a FuncExpr with type information for the deparser.
 *
 * Returns the number of dropped chunks.
 */
int
chunk_invoke_drop_chunks(Oid relid, Datum older_than, Datum older_than_type)
{
	EState *estate;
	ExprContext *econtext;
	FuncExpr *fexpr;
	List *args = NIL;
	int i, num_results = 0;
	SetExprState *state;
	Oid restype;
	Oid func_oid;
	Const *argarr[DROP_CHUNKS_NARGS] = {
		makeConst(REGCLASSOID,
				  -1,
				  InvalidOid,
				  sizeof(relid),
				  ObjectIdGetDatum(relid),
				  false,
				  false),
		makeConst(older_than_type,
				  -1,
				  InvalidOid,
				  get_typlen(older_than_type),
				  older_than,
				  false,
				  get_typbyval(older_than_type)),
		makeNullConst(older_than_type, -1, InvalidOid),
		castNode(Const, makeBoolConst(false, true)),
	};
	Oid type_id[DROP_CHUNKS_NARGS] = { REGCLASSOID, ANYOID, ANYOID, BOOLOID };
	char *const schema_name = ts_extension_schema_name();
	List *const fqn = list_make2(makeString(schema_name), makeString(DROP_CHUNKS_FUNCNAME));

	StaticAssertStmt(lengthof(type_id) == lengthof(argarr),
					 "argarr and type_id should have matching lengths");

	func_oid = LookupFuncName(fqn, lengthof(type_id), type_id, false);
	Assert(func_oid); /* LookupFuncName should not return an invalid OID */

	/* Prepare the function expr with argument list */
	get_func_result_type(func_oid, &restype, NULL);

	for (i = 0; i < lengthof(argarr); i++)
		args = lappend(args, argarr[i]);

	fexpr = makeFuncExpr(func_oid, restype, args, InvalidOid, InvalidOid, COERCE_EXPLICIT_CALL);
	fexpr->funcretset = true;

	/* Execute the SRF */
	estate = CreateExecutorState();
	econtext = CreateExprContext(estate);
	state = ExecInitFunctionResultSet(&fexpr->xpr, econtext, NULL);

	while (true)
	{
		ExprDoneCond isdone;
		bool isnull;

		ExecMakeFunctionResultSet(state, econtext, estate->es_query_cxt, &isnull, &isdone);

		if (isdone == ExprEndResult)
			break;

		if (!isnull)
			num_results++;
	}

	/* Cleanup */
	FreeExprContext(econtext, false);
	FreeExecutorState(estate);

	return num_results;
}

static bool
chunk_is_distributed(const Chunk *chunk)
{
	return chunk->relkind == RELKIND_FOREIGN_TABLE;
}

Datum
chunk_create_replica_table(PG_FUNCTION_ARGS)
{
	Oid chunk_relid;
	const char *data_node_name;
	const Chunk *chunk;
	const Hypertable *ht;
	const ForeignServer *server;
	Cache *hcache = ts_hypertable_cache_pin();

	TS_PREVENT_FUNC_IF_READ_ONLY();

	GETARG_NOTNULL_OID(chunk_relid, 0, "chunk");
	GETARG_NOTNULL_NULLABLE(data_node_name, 1, "data node name", CSTRING);

	chunk = ts_chunk_get_by_relid(chunk_relid, false);
	if (chunk == NULL)
	{
		const char *rel_name = get_rel_name(chunk_relid);
		if (rel_name == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("oid \"%u\" is not a chunk", chunk_relid)));
		else
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("relation \"%s\" is not a chunk", rel_name)));
	}
	if (!chunk_is_distributed(chunk))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("chunk \"%s\" doesn't belong to a distributed hypertable",
						get_rel_name(chunk_relid))));

	ht = ts_hypertable_cache_get_entry(hcache, chunk->hypertable_relid, CACHE_FLAG_NONE);
	ts_hypertable_permissions_check(ht->main_table_relid, GetUserId());

	/* Check the given data node exists */
	server = data_node_get_foreign_server(data_node_name, ACL_USAGE, true, false);
	/* Find if hypertable is attached to the data node and return an error otherwise */
	data_node_hypertable_get_by_node_name(ht, data_node_name, true);

	if (chunk_match_data_node_by_server(chunk, server))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("chunk \"%s\" already exists on data node \"%s\"",
						get_rel_name(chunk_relid),
						data_node_name)));

	chunk_api_call_create_empty_chunk_table(ht, chunk, data_node_name);

	ts_cache_release(hcache);

	PG_RETURN_VOID();
}

static void
chunk_api_call_chunk_drop_replica(const Chunk *chunk, const char *node_name, Oid serverid)
{
	const char *drop_cmd;
	List *data_nodes;

	/*
	 * Drop chunk on the data node using a regular "DROP TABLE".
	 * Note that CASCADE is not required as it takes care of dropping compressed
	 * chunk (if any).
	 *
	 * If there are any other non-TimescaleDB objects attached to this table due
	 * to some manual user activity then they should be dropped by the user
	 * before invoking this function.
	 */

	drop_cmd = psprintf("DROP TABLE %s.%s",
						quote_identifier(chunk->fd.schema_name.data),
						quote_identifier(chunk->fd.table_name.data));
	data_nodes = list_make1((char *) node_name);
	ts_dist_cmd_run_on_data_nodes(drop_cmd, data_nodes, true);

	/*
	 * This chunk might have this data node as primary, change that association
	 * if so. Then delete the chunk_id and node_name association.
	 */
	chunk_update_foreign_server_if_needed(chunk->fd.id, serverid);
	ts_chunk_data_node_delete_by_chunk_id_and_node_name(chunk->fd.id, node_name);
}

void
chunk_api_call_chunk_drop_replica_wrapper(ChunkCopyData *ccd, Oid serverid, bool transactional)
{
	if (transactional)
		StartTransactionCommand();

	chunk_api_call_chunk_drop_replica(ccd->chunk, ccd->src_node, serverid);

	if (transactional)
	{
		NameData application_name;

		snprintf(application_name.data,
				 sizeof(application_name.data),
				 "chunk_dropped_srcdn:%s",
				 ccd->operation_id.data);
		pgstat_report_appname(application_name.data);

		ccd->completed_stage = "chunk_dropped_srcdn";
		/* it gets committed as part of the overall transaction */
		chunk_copy_activity_update(ccd);

		CommitTransactionCommand();
	}
}

/*
 * chunk_drop_replica:
 *
 * This function drops a chunk on a specified data node. It then
 * removes the metadata about the association of the chunk to this
 * data node on the access node.
 */
Datum
chunk_drop_replica(PG_FUNCTION_ARGS)
{
	Oid chunk_relid = PG_ARGISNULL(0) ? InvalidOid : PG_GETARG_OID(0);
	const char *node_name = PG_ARGISNULL(1) ? NULL : NameStr(*PG_GETARG_NAME(1));
	ForeignServer *server;
	Chunk *chunk;

	TS_PREVENT_FUNC_IF_READ_ONLY();

	if (!OidIsValid(chunk_relid))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("invalid chunk relation")));

	chunk = ts_chunk_get_by_relid(chunk_relid, false);

	if (NULL == chunk)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid chunk relation"),
				 errdetail("Object with OID %u is not a chunk relation", chunk_relid)));

	/* It has to be a foreign table chunk */
	if (chunk->relkind != RELKIND_FOREIGN_TABLE)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("\"%s\" is not a valid remote chunk", get_rel_name(chunk_relid))));

	server = data_node_get_foreign_server(node_name, ACL_USAGE, true, false);
	Assert(NULL != server);

	/* Early abort on missing permissions */
	ts_hypertable_permissions_check(chunk_relid, GetUserId());

	if (!ts_chunk_has_data_node(chunk, node_name))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("chunk \"%s\" does not exist on data node \"%s\"",
						get_rel_name(chunk_relid),
						node_name)));

	/*
	 * There should be at least one surviving replica after the deletion here.
	 *
	 * We could fetch the corresponding hypertable and check its
	 * replication_factor. But the user of this function is using it
	 * to move chunk from one data node to another and is well aware of
	 * the replication_factor requirements
	 */
	if (list_length(chunk->data_nodes) <= 1)
		ereport(ERROR,
				(errcode(ERRCODE_TS_INSUFFICIENT_NUM_DATA_NODES),
				 errmsg("cannot drop the last chunk replica"),
				 errdetail("Dropping the last chunk replica could lead to data loss.")));

	chunk_api_call_chunk_drop_replica(chunk, node_name, server->serverid);

	PG_RETURN_VOID();
}

static void
chunk_copy_data_prepare(ChunkCopyData *ccd, bool transactional)
{
	const char *cmd;
	const char *connection_string;
	NameData application_name;

	if (transactional)
		StartTransactionCommand();

	/* create publication on the source data node */
	cmd = psprintf("CREATE PUBLICATION %s FOR TABLE %s",
				   NameStr(ccd->operation_id),
				   quote_qualified_identifier(NameStr(ccd->chunk->fd.schema_name),
											  NameStr(ccd->chunk->fd.table_name)));

	/* create the publication in autocommit mode */
	ts_dist_cmd_run_on_data_nodes(cmd, list_make1((char *) ccd->src_node), false);

	/* track the progress in the catalog */
	if (transactional)
	{
		snprintf(application_name.data,
				 sizeof(application_name.data),
				 "publication_created:%s",
				 ccd->operation_id.data);
		pgstat_report_appname(application_name.data);

		Assert(ccd->id != 0);
		ccd->completed_stage = "publication_created";
		chunk_copy_activity_update(ccd);

		CommitTransactionCommand();
	}

	/* create subscription on the destination data node in a new transaction block */
	if (transactional)
		StartTransactionCommand();

	/*
	 * CREATE SUBSCRIPTION from a database within the same database cluster will hang,
	 * create the replication slot separately before creating the subscription
	 */
	cmd = psprintf("SELECT pg_create_logical_replication_slot('%s', 'pgoutput')",
				   NameStr(ccd->operation_id));
	ts_dist_cmd_run_on_data_nodes(cmd, list_make1((char *) ccd->src_node), false);

	if (transactional)
	{
		snprintf(application_name.data,
				 sizeof(application_name.data),
				 "replslot_created:%s",
				 ccd->operation_id.data);
		pgstat_report_appname(application_name.data);

		Assert(ccd->id != 0);
		ccd->completed_stage = "replslot_created";
		chunk_copy_activity_update(ccd);

		CommitTransactionCommand();
	}

	if (transactional)
		StartTransactionCommand();
	/* prepare connection string to the source node */
	connection_string = remote_connection_get_connstr(ccd->src_node);

	cmd = psprintf("CREATE SUBSCRIPTION %s CONNECTION '%s' PUBLICATION %s"
				   " WITH (create_slot = false, enabled = false)",
				   NameStr(ccd->operation_id),
				   connection_string,
				   NameStr(ccd->operation_id));
	ts_dist_cmd_run_on_data_nodes(cmd, list_make1((char *) ccd->dst_node), false);

	if (transactional)
	{
		snprintf(application_name.data,
				 sizeof(application_name.data),
				 "subscription_created:%s",
				 ccd->operation_id.data);
		pgstat_report_appname(application_name.data);

		Assert(ccd->id != 0);
		ccd->completed_stage = "subscription_created";
		chunk_copy_activity_update(ccd);

		CommitTransactionCommand();
	}
}

static void
chunk_copy_data_perform(ChunkCopyData *ccd, bool transactional)
{
	const char *cmd;
	NameData application_name;

	if (transactional)
		StartTransactionCommand();

	/* start data transfer on the destination node */
	cmd = psprintf("ALTER SUBSCRIPTION %s ENABLE", NameStr(ccd->operation_id));
	ts_dist_cmd_run_on_data_nodes(cmd, list_make1((char *) ccd->dst_node), false);

	if (transactional)
	{
		snprintf(application_name.data,
				 sizeof(application_name.data),
				 "subscription_syncing:%s",
				 ccd->operation_id.data);
		pgstat_report_appname(application_name.data);

		Assert(ccd->id != 0);
		ccd->completed_stage = "subscription_syncing";
		chunk_copy_activity_update(ccd);

		CommitTransactionCommand();
	}

	/* wait until data transfer finishes in its own transaction */
	if (transactional)
		StartTransactionCommand();

	cmd = psprintf("CALL _timescaledb_internal.wait_subscription_sync(%s, %s)",
				   quote_literal_cstr(NameStr(ccd->chunk->fd.schema_name)),
				   quote_literal_cstr(NameStr(ccd->chunk->fd.table_name)));
	ts_dist_cmd_run_on_data_nodes(cmd, list_make1((char *) ccd->dst_node), false);

	if (transactional)
	{
		snprintf(application_name.data,
				 sizeof(application_name.data),
				 "subscription_syncdone:%s",
				 ccd->operation_id.data);
		pgstat_report_appname(application_name.data);

		Assert(ccd->id != 0);
		ccd->completed_stage = "subscription_syncdone";
		chunk_copy_activity_update(ccd);

		CommitTransactionCommand();
	}
}

static void
chunk_copy_data_end(ChunkCopyData *ccd, bool transactional)
{
	const char *cmd;
	NameData application_name;

	if (transactional)
		StartTransactionCommand();

	cmd = psprintf("DROP SUBSCRIPTION %s", NameStr(ccd->operation_id));
	ts_dist_cmd_run_on_data_nodes(cmd, list_make1((char *) ccd->dst_node), false);

	if (transactional)
	{
		snprintf(application_name.data,
				 sizeof(application_name.data),
				 "subscription_drop:%s",
				 ccd->operation_id.data);
		pgstat_report_appname(application_name.data);

		Assert(ccd->id != 0);
		ccd->completed_stage = "subscription_drop";
		chunk_copy_activity_update(ccd);

		CommitTransactionCommand();
	}

	/* Drop publication in its own transaction */
	if (transactional)
		StartTransactionCommand();

	cmd = psprintf("DROP PUBLICATION %s", NameStr(ccd->operation_id));
	ts_dist_cmd_run_on_data_nodes(cmd, list_make1((char *) ccd->src_node), false);
	if (transactional)
	{
		snprintf(application_name.data,
				 sizeof(application_name.data),
				 "publication_drop:%s",
				 ccd->operation_id.data);
		pgstat_report_appname(application_name.data);

		Assert(ccd->id != 0);
		ccd->completed_stage = "publication_drop";
		chunk_copy_activity_update(ccd);

		CommitTransactionCommand();
	}
}

static void
chunk_api_call_copy_chunk_data_wrapper(ChunkCopyData *ccd, bool transactional)
{
	/* setup logical replication publication and subscription */
	chunk_copy_data_prepare(ccd, transactional);

	/* begin data transfer and wait for completion */
	chunk_copy_data_perform(ccd, transactional);

	/* cleanup */
	chunk_copy_data_end(ccd, transactional);
}

Datum
chunk_copy_data(PG_FUNCTION_ARGS)
{
	Oid chunk_relid = PG_ARGISNULL(0) ? InvalidOid : PG_GETARG_OID(0);
	const char *src_node_name = PG_ARGISNULL(1) ? NULL : PG_GETARG_CSTRING(1);
	const char *dst_node_name = PG_ARGISNULL(2) ? NULL : PG_GETARG_CSTRING(2);
	Chunk *chunk = ts_chunk_get_by_relid(chunk_relid, true);
	Cache *hcache = ts_hypertable_cache_pin();
	Hypertable *ht =
		ts_hypertable_cache_get_entry(hcache, chunk->hypertable_relid, CACHE_FLAG_NONE);
	ChunkCopyData ccd;

	TS_PREVENT_FUNC_IF_READ_ONLY();

	PreventInTransactionBlock(true, get_func_name(FC_FN_OID(fcinfo)));

	if (src_node_name == NULL || dst_node_name == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("data node name cannot be NULL")));

	if (dist_util_membership() != DIST_MEMBER_ACCESS_NODE)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("function must be run on the access node only")));

	if (!hypertable_is_distributed(ht))
		ereport(ERROR,
				(errcode(ERRCODE_TS_HYPERTABLE_NOT_DISTRIBUTED),
				 errmsg("hypertable \"%s\" is not distributed",
						get_rel_name(chunk->hypertable_relid))));

	ts_cache_release(hcache);

	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 (errmsg("must be superuser to copy chunk to data node"))));

	ccd.id = 0; /* no id assigned since we don't want to persist it */
	ccd.chunk = chunk;
	ccd.src_node = src_node_name;
	ccd.dst_node = dst_node_name;

	/*
	 * Prepare shared operation id name for publication and subscription
	 *
	 * Get the operation id for this chunk move/copy activity. The naming
	 * convention is "ts_copy_seq-id_chunk-id_table-name_schema-name and it can
	 * get truncated due to NAMEDATALEN restrictions
	 */
	snprintf(ccd.operation_id.data,
			 sizeof(ccd.operation_id.data),
			 "ts_copy_%d_%s_%s",
			 chunk->fd.id,
			 NameStr(chunk->fd.table_name),
			 NameStr(chunk->fd.schema_name));

	/* perform all the steps for the copy chunk */
	chunk_api_call_copy_chunk_data_wrapper(&ccd, false);

	PG_RETURN_VOID();
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
chunk_copy_activity_insert_rel(Relation rel, ChunkCopyData *ccd)
{
	TupleDesc desc = RelationGetDescr(rel);
	Datum values[Natts_chunk_copy_activity];
	bool nulls[Natts_chunk_copy_activity] = { false };
	CatalogSecurityContext sec_ctx;

	memset(values, 0, sizeof(values));
	values[AttrNumberGetAttrOffset(Anum_chunk_copy_activity_id)] = Int32GetDatum(ccd->id);
	values[AttrNumberGetAttrOffset(Anum_chunk_copy_activity_operation_id)] =
		NameGetDatum(&ccd->operation_id);
	values[AttrNumberGetAttrOffset(Anum_chunk_copy_activity_backend_pid)] =
		Int32GetDatum(MyProcPid);
	values[AttrNumberGetAttrOffset(Anum_chunk_copy_activity_completed_stage)] =
		CStringGetTextDatum(ccd->completed_stage);
	values[AttrNumberGetAttrOffset(Anum_chunk_copy_activity_time_start)] =
		TimestampTzGetDatum(GetCurrentTimestamp());
	values[AttrNumberGetAttrOffset(Anum_chunk_copy_activity_chunk_id)] =
		Int32GetDatum(ccd->chunk->fd.id);
	values[AttrNumberGetAttrOffset(Anum_chunk_copy_activity_source_node_name)] =
		DirectFunctionCall1(namein, CStringGetDatum(ccd->src_node));
	values[AttrNumberGetAttrOffset(Anum_chunk_copy_activity_dest_node_name)] =
		DirectFunctionCall1(namein, CStringGetDatum(ccd->dst_node));
	values[AttrNumberGetAttrOffset(Anum_chunk_copy_activity_delete_on_src_node)] =
		BoolGetDatum(ccd->delete_on_src_node);

	/* nulls is FALSE above, insert the row now */
	ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);
	ts_catalog_insert_values(rel, desc, values, nulls);
	ts_catalog_restore_user(&sec_ctx);
}

static void
chunk_copy_activity_insert(ChunkCopyData *ccd)
{
	Catalog *catalog;
	Relation rel;

	catalog = ts_catalog_get();
	rel = table_open(catalog_get_table_id(catalog, CHUNK_COPY_ACTIVITY), RowExclusiveLock);

	chunk_copy_activity_insert_rel(rel, ccd);
	table_close(rel, RowExclusiveLock);
}

static ScanTupleResult
chunk_copy_activity_tuple_update(TupleInfo *ti, void *data)
{
	ChunkCopyData *ccd = data;
	Datum values[Natts_chunk_copy_activity];
	bool nulls[Natts_chunk_copy_activity];
	CatalogSecurityContext sec_ctx;
	bool should_free;
	HeapTuple tuple = ts_scanner_fetch_heap_tuple(ti, false, &should_free);
	HeapTuple new_tuple;

	heap_deform_tuple(tuple, ts_scanner_get_tupledesc(ti), values, nulls);

	/* We only update the "stage" field */
	values[AttrNumberGetAttrOffset(Anum_chunk_copy_activity_completed_stage)] =
		CStringGetTextDatum(ccd->completed_stage);

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

int
chunk_copy_activity_update(ChunkCopyData *ccd)
{
	return chunk_copy_activity_scan_update_by_id(ccd->id,
												 chunk_copy_activity_tuple_update,
												 ccd,
												 RowExclusiveLock);
}

void
chunk_perform_distributed_copy(Oid chunk_relid, bool verbose, const char *src_node,
							   const char *dst_node, bool delete_on_src_node)
{
	Chunk *chunk;
	Cache *hcache;
	Hypertable *ht;
	ForeignServer *src_server, *dst_server;
	MemoryContext old, mcxt;
	NameData operation_id;
	ChunkCopyData ccd;

	/*
	 * The chunk and foreign server info needs to be on a memory context
	 * that will survive moving to a new transaction for each stage
	 */
	mcxt = AllocSetContextCreate(PortalContext, "chunk move activity", ALLOCSET_DEFAULT_SIZES);

	old = MemoryContextSwitchTo(mcxt);

	chunk = ts_chunk_get_by_relid(chunk_relid, true);

	/* It has to be a foreign table chunk */
	if (chunk->relkind != RELKIND_FOREIGN_TABLE)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("\"%s\" is not a valid remote chunk", get_rel_name(chunk_relid))));

	/* It has to be an uncompressed chunk, we query the status field on the AN for this */
	if (ts_chunk_is_compressed(chunk))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("\"%s\" is a compressed remote chunk. Chunk copy/move not supported"
						" currently on compressed chunks",
						get_rel_name(chunk_relid))));

	ht = ts_hypertable_cache_get_cache_and_entry(chunk->hypertable_relid, CACHE_FLAG_NONE, &hcache);

	ts_hypertable_permissions_check(ht->main_table_relid, GetUserId());

	if (!hypertable_is_distributed(ht))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("hypertable \"%s\" is not distributed",
						get_rel_name(ht->main_table_relid))));

	/* check that src_node is a valid DN and that chunk exists on it */
	src_server = data_node_get_foreign_server(src_node, ACL_USAGE, true, false);
	Assert(NULL != src_server);

	if (!ts_chunk_has_data_node(chunk, src_node))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("chunk \"%s\" does not exist on source data node \"%s\"",
						get_rel_name(chunk_relid),
						src_node)));

	/* check that dst_node is a valid DN and that chunk does not exist on it */
	dst_server = data_node_get_foreign_server(dst_node, ACL_USAGE, true, false);
	Assert(NULL != dst_server);

	if (ts_chunk_has_data_node(chunk, dst_node))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("chunk \"%s\" already exists on destination data node \"%s\"",
						get_rel_name(chunk_relid),
						dst_node)));

	/*
	 * Populate the ChunkCopyData structure for use by various stages
	 *
	 * Get the operation id for this chunk move/copy activity. The naming
	 * convention is "ts_copy_seq-id_chunk-id_table-name_schema-name and it can
	 * get truncated due to NAMEDATALEN restrictions
	 */
	ccd.id = ts_catalog_table_next_seq_id(ts_catalog_get(), CHUNK_COPY_ACTIVITY);
	ccd.chunk = chunk;
	snprintf(ccd.operation_id.data,
			 sizeof(operation_id.data),
			 "ts_copy_%d_%d_%s_%s",
			 ccd.id,
			 chunk->fd.id,
			 NameStr(chunk->fd.table_name),
			 NameStr(chunk->fd.schema_name));
	ccd.src_node = pstrdup(src_node);
	ccd.dst_node = pstrdup(dst_node);
	ccd.delete_on_src_node = delete_on_src_node;
	/* each step below should modify this field according to their actions */
	ccd.completed_stage = "initial";

	/*
	 * Step 0: Persist the entry in the catalog
	 */
	chunk_copy_activity_insert(&ccd);

	ts_cache_release(hcache);
	MemoryContextSwitchTo(old);

	/* Commit to get out of starting transaction */
	PopActiveSnapshot();
	CommitTransactionCommand();

	/*
	 * All sanity checks are done. Start with the actual process
	 *
	 * Step 1: Create an empty chunk table on the dst_node.
	 */
	chunk_api_call_create_empty_chunk_table_wrapper(&ccd, true);

	/*
	 * Step 2: Copy the data from the src_node chunk to this newly created
	 * dst_node chunk.
	 */
	chunk_api_call_copy_chunk_data_wrapper(&ccd, true);

	/*
	 * Step 3: Attach this chunk to the hypertable on the dst_node.
	 */
	chunk_api_call_chunk_attach_replica_wrapper(dst_server, &ccd, true);

	/*
	 * Step 4: Remove this chunk from the src_node to complete the move. But
	 * only if explicitly told to do so.
	 */
	if (ccd.delete_on_src_node)
		chunk_api_call_chunk_drop_replica_wrapper(&ccd, src_server->serverid, true);

	/* done using this long lived memory context */
	MemoryContextReset(mcxt);

	/* start a transaction for the final outer transaction */
	StartTransactionCommand();
	/* All steps complete, delete this ccd entry from the catalog now */
	chunk_copy_activity_delete_by_id(ccd.id);
}
