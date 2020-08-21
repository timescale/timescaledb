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
#include <utils/syscache.h>
#include <utils/inval.h>
#include <utils/tuplestore.h>
#include <utils/palloc.h>
#include <utils/memutils.h>
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

#include "chunk.h"
#include "data_node.h"
#include "deparse.h"
#include "remote/dist_commands.h"

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
	ListCell *lc;
	bool new_server_found = false;

	foreach (lc, chunk->data_nodes)
	{
		ChunkDataNode *cdn = lfirst(lc);

		if (cdn->foreign_server_oid == new_server->serverid)
		{
			new_server_found = true;
			break;
		}
	}

	if (!new_server_found)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("chunk \"%s\" does not exist on server \"%s\"",
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
