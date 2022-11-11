/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <postgres.h>
#include <access/htup_details.h>
#include <access/xact.h>
#include <catalog/pg_foreign_server.h>
#include <catalog/pg_foreign_table.h>
#include <catalog/dependency.h>
#include <catalog/namespace.h>
#include <foreign/foreign.h>
#include <nodes/makefuncs.h>
#include <nodes/parsenodes.h>
#include <storage/lmgr.h>
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
#include <storage/lmgr.h>
#include <funcapi.h>
#include <miscadmin.h>
#include <fmgr.h>
#ifdef USE_ASSERT_CHECKING
#include <funcapi.h>
#endif

#include <compat/compat.h>
#include <extension.h>
#include <errors.h>
#include <error_utils.h>
#include <hypertable_cache.h>
#include "hypercube.h"

#include "chunk.h"
#include "chunk_api.h"
#include "data_node.h"
#include "deparse.h"
#include "debug_point.h"
#include "dist_util.h"
#include "remote/dist_commands.h"
#include "ts_catalog/chunk_data_node.h"
#include "utils.h"

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
chunk_set_foreign_server(const Chunk *chunk, const ForeignServer *new_server)
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

/*
 * Change the data node used to query a chunk.
 *
 * Either switch "away" from using the given data node or switch to using it
 * (depending on the "available" parameter). The function will only switch
 * back to using the data node if it is the determined primary/default data
 * node for the chunk according to the partitioning configuration.
 *
 * Return true if the chunk's data node was changed or no change was
 * needed. Return false if a change should have been made but wasn't possible
 * (due to, e.g., lack of replica chunks).
 */
bool
chunk_update_foreign_server_if_needed(const Chunk *chunk, Oid data_node_id, bool available)
{
	ForeignTable *foreign_table = NULL;
	ForeignServer *server = NULL;
	bool should_switch_data_node = false;
	ListCell *lc;

	Assert(chunk->relkind == RELKIND_FOREIGN_TABLE);
	foreign_table = GetForeignTable(chunk->table_id);

	/* Cannot switch to other data node if only one or none assigned */
	if (list_length(chunk->data_nodes) < 2)
		return false;

	/* Nothing to do if the chunk table already has the requested data node set */
	if ((!available && data_node_id != foreign_table->serverid) ||
		(available && data_node_id == foreign_table->serverid))
		return true;

	if (available)
	{
		/* Switch to using the given data node, but only on chunks where the
		 * given node is the "default" according to partitioning */
		Cache *htcache = ts_hypertable_cache_pin();
		const Hypertable *ht =
			ts_hypertable_cache_get_entry(htcache, chunk->hypertable_relid, CACHE_FLAG_NONE);
		const Dimension *dim = hyperspace_get_closed_dimension(ht->space, 0);

		if (dim != NULL)
		{
			/* For space-partitioned tables, use the current partitioning
			 * configuration in that dimension (dimension partition) as a
			 * template for picking the query data node */
			const DimensionSlice *slice =
				ts_hypercube_get_slice_by_dimension_id(chunk->cube, dim->fd.id);
			unsigned int i;

			Assert(dim->dimension_partitions);

			for (i = 0; i < dim->dimension_partitions->num_partitions; i++)
			{
				const DimensionPartition *dp = dim->dimension_partitions->partitions[i];

				/* Match the chunk with the dimension partition. Count as a
				 * match if the start of chunk is within the range of the
				 * partition. This captures both the case when the chunk
				 * aligns perfectly with the partition and when it is bigger
				 * or smaller (due to a previous partitioning change). */
				if (slice->fd.range_start >= dp->range_start &&
					slice->fd.range_start <= dp->range_end)
				{
					ListCell *lc;

					/* Use the data node for queries if it is the first
					 * available data node in the partition's list (i.e., the
					 * default choice) */
					foreach (lc, dp->data_nodes)
					{
						const char *node_name = lfirst(lc);
						server = GetForeignServerByName(node_name, false);

						if (ts_data_node_is_available_by_server(server))
						{
							should_switch_data_node = (server->serverid == data_node_id);
							break;
						}
					}
				}
			}
		}
		else
		{
			/* For hypertables without a space partition, use the data node
			 * assignment logic to figure out whether to use the data node as
			 * query data node. The "default" query data node is the first in
			 * the list. The chunk assign logic only returns available data
			 * nodes. */
			List *datanodes = ts_hypertable_assign_chunk_data_nodes(ht, chunk->cube);
			const char *node_name = linitial(datanodes);
			server = GetForeignServerByName(node_name, false);

			if (server->serverid == data_node_id)
				should_switch_data_node = true;
		}

		ts_cache_release(htcache);
	}
	else
	{
		/* Switch "away" from using the given data node. Pick the first
		 * "available" data node referenced by the chunk */
		foreach (lc, chunk->data_nodes)
		{
			const ChunkDataNode *cdn = lfirst(lc);

			if (cdn->foreign_server_oid != data_node_id)
			{
				server = GetForeignServer(cdn->foreign_server_oid);

				if (ts_data_node_is_available_by_server(server))
				{
					should_switch_data_node = true;
					break;
				}
			}
		}
	}

	if (should_switch_data_node)
	{
		Assert(server != NULL);
		chunk_set_foreign_server(chunk, server);
	}

	return should_switch_data_node;
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
	int num_results = 0;
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

	for (size_t i = 0; i < lengthof(argarr); i++)
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

/* Data in a frozen chunk cannot be modified. So any operation
 * that rewrites data for a frozen chunk will be blocked.
 * Note that a frozen chunk can still be dropped.
 */
Datum
chunk_freeze_chunk(PG_FUNCTION_ARGS)
{
	Oid chunk_relid = PG_ARGISNULL(0) ? InvalidOid : PG_GETARG_OID(0);
	TS_PREVENT_FUNC_IF_READ_ONLY();
	Chunk *chunk = ts_chunk_get_by_relid(chunk_relid, true);
	Assert(chunk != NULL);
	if (chunk->relkind == RELKIND_FOREIGN_TABLE)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("operation not supported on distributed chunk or foreign table \"%s\"",
						get_rel_name(chunk_relid))));
	}
	if (ts_chunk_is_frozen(chunk))
		PG_RETURN_BOOL(true);
	/* get Share lock. will wait for other concurrent transactions that are
	 * modifying the chunk. Does not block SELECTs on the chunk.
	 * Does not block other DDL on the chunk table.
	 */
	DEBUG_WAITPOINT("freeze_chunk_before_lock");
	LockRelationOid(chunk_relid, ShareLock);
	bool ret = ts_chunk_set_frozen(chunk);
	PG_RETURN_BOOL(ret);
}

Datum
chunk_unfreeze_chunk(PG_FUNCTION_ARGS)
{
	Oid chunk_relid = PG_ARGISNULL(0) ? InvalidOid : PG_GETARG_OID(0);
	TS_PREVENT_FUNC_IF_READ_ONLY();
	Chunk *chunk = ts_chunk_get_by_relid(chunk_relid, true);
	Assert(chunk != NULL);
	if (chunk->relkind == RELKIND_FOREIGN_TABLE)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("operation not supported on distributed chunk or foreign table \"%s\"",
						get_rel_name(chunk_relid))));
	}
	if (!ts_chunk_is_frozen(chunk))
		PG_RETURN_BOOL(true);
	/* This is a previously frozen chunk. Only selects are permitted on this chunk.
	 * This changes the status in the catalog to allow previously blocked operations.
	 */
	bool ret = ts_chunk_unset_frozen(chunk);
	PG_RETURN_BOOL(ret);
}

static List *
chunk_id_list_create(ArrayType *array)
{
	/* create a sorted list of chunk ids from array */
	ArrayIterator it;
	Datum id_datum;
	List *id_list = NIL;
	bool isnull;

	it = array_create_iterator(array, 0, NULL);
	while (array_iterate(it, &id_datum, &isnull))
	{
		if (isnull)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("chunks array arguments cannot be NULL")));
		id_list = lappend_int(id_list, DatumGetInt32(id_datum));
	}
	array_free_iterator(it);

	(void) list_sort_compat(id_list, list_int_cmp_compat);
	return id_list;
}

static List *
chunk_id_list_exclusive_right_merge_join(const List *an_list, const List *dn_list)
{
	/*
	 * merge join two sorted list and return only values which exclusively
	 * exists in the right target (dn_list list)
	 */
	List *result = NIL;
	const ListCell *l = list_head(an_list);
	const ListCell *r = list_head(dn_list);
	for (;;)
	{
		if (l && r)
		{
			int compare = list_int_cmp_compat(l, r);
			if (compare == 0)
			{
				/* l = r */
				l = lnext_compat(an_list, l);
				r = lnext_compat(dn_list, r);
			}
			else if (compare < 0)
			{
				/* l < r */
				/* chunk exists only on the access node */
				l = lnext_compat(an_list, l);
			}
			else
			{
				/* l > r */
				/* chunk exists only on the data node */
				result = lappend_int(result, lfirst_int(r));
				r = lnext_compat(dn_list, r);
			}
		}
		else if (l)
		{
			/* chunk exists only on the access node */
			l = lnext_compat(an_list, l);
		}
		else if (r)
		{
			/* chunk exists only on the data node */
			result = lappend_int(result, lfirst_int(r));
			r = lnext_compat(dn_list, r);
		}
		else
		{
			break;
		}
	}
	return result;
}

/*
 * chunk_drop_stale_chunks:
 *
 * This function drops chunks on a specified data node if those chunks are
 * not known by the access node (chunks array).
 *
 * This function is intended to be used on the access node and data node.
 */
void
ts_chunk_drop_stale_chunks(const char *node_name, ArrayType *chunks_array)
{
	DistUtilMembershipStatus membership;

	/* execute according to the node membership */
	membership = dist_util_membership();
	if (membership == DIST_MEMBER_ACCESS_NODE)
	{
		StringInfo cmd = makeStringInfo();
		bool first = true;

		if (node_name == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("node_name argument cannot be NULL")));
		if (chunks_array != NULL)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("chunks argument cannot be used on the access node")));

		/* get an exclusive lock on the chunks catalog table to prevent new chunk
		 * creation during this operation */
		LockRelationOid(ts_catalog_get()->tables[CHUNK].id, AccessExclusiveLock);

		/* generate query to execute drop_stale_chunks() on the data node */
		appendStringInfo(cmd, "SELECT _timescaledb_internal.drop_stale_chunks(NULL, array[");

		/* scan for chunks that reference the given data node */
		ScanIterator it = ts_chunk_data_nodes_scan_iterator_create(CurrentMemoryContext);
		ts_chunk_data_nodes_scan_iterator_set_node_name(&it, node_name);
		ts_scanner_foreach(&it)
		{
			TupleTableSlot *slot = ts_scan_iterator_slot(&it);
			bool PG_USED_FOR_ASSERTS_ONLY isnull = false;
			int32 node_chunk_id;

			node_chunk_id =
				DatumGetInt32(slot_getattr(slot, Anum_chunk_data_node_node_chunk_id, &isnull));
			Assert(!isnull);

			appendStringInfo(cmd, "%s%d", first ? "" : ",", node_chunk_id);
			first = false;
		}
		ts_scan_iterator_close(&it);

		appendStringInfo(cmd, "]::integer[])");

		/* execute command on the data node */
		ts_dist_cmd_run_on_data_nodes(cmd->data, list_make1((char *) node_name), true);
	}
	else if (membership == DIST_MEMBER_DATA_NODE)
	{
		List *an_chunk_id_list = NIL;
		List *dn_chunk_id_list = NIL;
		List *dn_chunk_id_list_stale = NIL;
		ListCell *lc;
		Cache *htcache;

		if (node_name != NULL)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("node_name argument cannot be used on the data node")));

		if (chunks_array == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("chunks argument cannot be NULL")));

		/* get a sorted list of chunk ids from the supplied chunks id argument array */
		an_chunk_id_list = chunk_id_list_create(chunks_array);

		/* get a local sorted list of chunk ids */
		dn_chunk_id_list = ts_chunk_get_all_chunk_ids(RowExclusiveLock);

		/* merge join two sorted list and get chunk ids which exists locally */
		dn_chunk_id_list_stale =
			chunk_id_list_exclusive_right_merge_join(an_chunk_id_list, dn_chunk_id_list);

		/* drop stale chunks */
		htcache = ts_hypertable_cache_pin();
		foreach (lc, dn_chunk_id_list_stale)
		{
			const Chunk *chunk = ts_chunk_get_by_id(lfirst_int(lc), false);
			Hypertable *ht;

			/* chunk might be already dropped by previous drop, if the chunk was compressed */
			if (chunk == NULL)
				continue;

			/* ensure that we drop only chunks related to distributed hypertables */
			ht = ts_hypertable_cache_get_entry(htcache, chunk->hypertable_relid, CACHE_FLAG_NONE);
			if (hypertable_is_distributed_member(ht))
				ts_chunk_drop(chunk, DROP_RESTRICT, DEBUG1);
		}
		ts_cache_release(htcache);
	}
	else
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("current server is not an access node or data node")));
	}
}

Datum
chunk_drop_stale_chunks(PG_FUNCTION_ARGS)
{
	char *node_name = PG_ARGISNULL(0) ? NULL : NameStr(*PG_GETARG_NAME(0));
	ArrayType *chunks_array = PG_ARGISNULL(1) ? NULL : PG_GETARG_ARRAYTYPE_P(1);

	TS_PREVENT_FUNC_IF_READ_ONLY();

	ts_chunk_drop_stale_chunks(node_name, chunks_array);
	PG_RETURN_VOID();
}
/*
 * Update and refresh the DN list for a given chunk. We remove metadata for this chunk
 * for unavailable DNs
 */
void
chunk_update_stale_metadata(Chunk *new_chunk, List *chunk_data_nodes)
{
	List *serveroids = NIL, *removeoids = NIL;
	ChunkDataNode *cdn;
	ListCell *lc;

	/* check that at least one data node is available for this chunk on the AN */
	if (chunk_data_nodes == NIL)
		ereport(ERROR,
				(errcode(ERRCODE_TS_INSUFFICIENT_NUM_DATA_NODES),
				 (errmsg("insufficient number of available data nodes"),
				  errhint("Increase the number of available data nodes on hypertable "
						  "\"%s\".",
						  get_rel_name(new_chunk->hypertable_relid)))));

	foreach (lc, chunk_data_nodes)
	{
		cdn = lfirst(lc);
		serveroids = lappend_oid(serveroids, cdn->foreign_server_oid);
	}

	foreach (lc, new_chunk->data_nodes)
	{
		cdn = lfirst(lc);

		/*
		 * check if this DN is a part of chunk_data_nodes. If not
		 * found in chunk_data_nodes, then we need to remove this
		 * chunk id to node name mapping and also update the primary
		 * foreign server if necessary. It's possible that this metadata
		 * might have been already cleared earlier in which case the
		 * data_nodes list for the chunk will be the same as the
		 * "serveroids" list and no unnecesary metadata update function
		 * calls will occur.
		 */
		if (!list_member_oid(serveroids, cdn->foreign_server_oid))
		{
			chunk_update_foreign_server_if_needed(new_chunk, cdn->foreign_server_oid, false);
			ts_chunk_data_node_delete_by_chunk_id_and_node_name(cdn->fd.chunk_id,
																NameStr(cdn->fd.node_name));

			removeoids = lappend_oid(removeoids, cdn->foreign_server_oid);
		}
	}

	/* remove entries from new_chunk->data_nodes matching removeoids */
	foreach (lc, removeoids)
	{
		ListCell *l;
		Oid serveroid = lfirst_oid(lc);

		/* this contrived code to ensure PG12+ compatible in-place list delete */
		foreach (l, new_chunk->data_nodes)
		{
			cdn = lfirst(l);

			if (cdn->foreign_server_oid == serveroid)
			{
				new_chunk->data_nodes = list_delete_ptr(new_chunk->data_nodes, cdn);
				break;
			}
		}
	}
}
