/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <executor/executor.h>
#include <parser/parse_relation.h>
#include <parser/parsetree.h>
#include <nodes/plannodes.h>
#include <commands/explain.h>
#include <foreign/fdwapi.h>
#include <storage/lmgr.h>
#include <storage/lockdefs.h>
#include <utils/rel.h>
#include <fmgr.h>
#include <miscadmin.h>
#include <guc.h>

#include <remote/async.h>
#include <remote/stmt_params.h>
#include <remote/connection.h>
#include <remote/dist_txn.h>
#include <remote/utils.h>
#include <remote/tuplefactory.h>
#include "ts_catalog/chunk_data_node.h"

#include <nodes/chunk_dispatch/chunk_insert_state.h>

#include "scan_plan.h"
#include "modify_exec.h"
#include "modify_plan.h"
#include "tsl/src/chunk.h"

/*
 * This enum describes what's kept in the fdw_private list for a ModifyTable
 * node referencing a timescaledb_fdw foreign table.  We store:
 *
 * 1) INSERT/UPDATE/DELETE statement text to be sent to the data node
 * 2) Integer list of target attribute numbers for INSERT/UPDATE
 *	  (NIL for a DELETE)
 * 3) Boolean flag showing if the remote query has a RETURNING clause
 * 4) Integer list of attribute numbers retrieved by RETURNING, if any
 */
enum FdwModifyPrivateIndex
{
	/* SQL statement to execute remotely (as a String node) */
	FdwModifyPrivateUpdateSql,
	/* Integer list of target attribute numbers for INSERT/UPDATE */
	FdwModifyPrivateTargetAttnums,
	/* has-returning flag (as an integer Value node) */
	FdwModifyPrivateHasReturning,
	/* Integer list of attribute numbers retrieved by RETURNING */
	FdwModifyPrivateRetrievedAttrs,
	/* The data nodes for the current chunk */
	FdwModifyPrivateDataNodes,
	/* Insert state for the current chunk */
	FdwModifyPrivateChunkInsertState,
};

typedef struct TsFdwDataNodeState
{
	TSConnectionId id;
	/* for remote query execution */
	TSConnection *conn;	  /* connection for the scan */
	PreparedStmt *p_stmt; /* prepared statement handle, if created */
} TsFdwDataNodeState;

/*
 * Execution state of a foreign insert/update/delete operation.
 */
typedef struct TsFdwModifyState
{
	Relation rel;						  /* relcache entry for the foreign table */
	AttConvInMetadata *att_conv_metadata; /* attribute datatype conversion metadata for converting
											 result to tuples */

	/* extracted fdw_private data */
	char *query;		/* text of INSERT/UPDATE/DELETE command */
	List *target_attrs; /* list of target attribute numbers */
	bool has_returning; /* is there a RETURNING clause? */
	TupleFactory *tupfactory;

	AttrNumber ctid_attno; /* attnum of input resjunk ctid column */

	bool prepared;
	int num_data_nodes;		 /* number of "available" datanodes */
	int num_all_data_nodes;	 /* number of all datanodes assigned to this "rel" */
	List *stale_data_nodes;	 /* DNs marked stale for this chunk */
	StmtParams *stmt_params; /* prepared statement paremeters */
	TsFdwDataNodeState data_nodes[FLEXIBLE_ARRAY_MEMBER];
} TsFdwModifyState;

#define TS_FDW_MODIFY_STATE_SIZE(num_data_nodes)                                                   \
	(sizeof(TsFdwModifyState) + (sizeof(TsFdwDataNodeState) * (num_data_nodes)))

static void
initialize_fdw_data_node_state(TsFdwDataNodeState *fdw_data_node, TSConnectionId id)
{
	fdw_data_node->id = id;
	fdw_data_node->conn = remote_dist_txn_get_connection(id, REMOTE_TXN_USE_PREP_STMT);
	fdw_data_node->p_stmt = NULL;
}

/*
 * create_foreign_modify
 *		Construct an execution state of a foreign insert/update/delete
 *		operation
 */
static TsFdwModifyState *
create_foreign_modify(EState *estate, Relation rel, CmdType operation, Oid check_as_user,
					  Plan *subplan, char *query, List *target_attrs, bool has_returning,
					  List *retrieved_attrs, List *server_id_list)
{
	TsFdwModifyState *fmstate;
	TupleDesc tupdesc = RelationGetDescr(rel);
	ListCell *lc;
	Oid user_id = OidIsValid(check_as_user) ? check_as_user : GetUserId();
	int i = 0;
	int num_data_nodes, num_all_data_nodes;
	int32 hypertable_id = ts_chunk_get_hypertable_id_by_reloid(rel->rd_id);
	List *all_replicas = NIL, *avail_replicas = NIL;

	if (hypertable_id == INVALID_HYPERTABLE_ID)
	{
		num_data_nodes = num_all_data_nodes = 1;
	}
	else
	{
		int32 chunk_id = ts_chunk_get_id_by_relid(rel->rd_id);

		all_replicas = ts_chunk_data_node_scan_by_chunk_id(chunk_id, CurrentMemoryContext);
		avail_replicas = ts_chunk_data_node_scan_by_chunk_id_filter(chunk_id, CurrentMemoryContext);
		num_all_data_nodes = list_length(all_replicas);
	}

	/*
	 * Identify which user to do the remote access as.  This should match what
	 * ExecCheckRTEPerms() does.
	 */

	if (NIL != server_id_list)
	{
		/*
		 * This is either (1) an INSERT on a hypertable chunk, or (2) an
		 * UPDATE or DELETE on a chunk. In the former case (1), the data nodes
		 * were passed on from the INSERT path via the chunk insert state, and
		 * in the latter case (2), the data nodes were resolved at planning time
		 * in the FDW planning callback.
		 */

		fmstate =
			(TsFdwModifyState *) palloc0(TS_FDW_MODIFY_STATE_SIZE(list_length(server_id_list)));
		foreach (lc, server_id_list)
		{
			Oid server_id = lfirst_oid(lc);
			TSConnectionId id = remote_connection_id(server_id, user_id);

			initialize_fdw_data_node_state(&fmstate->data_nodes[i++], id);
		}
		num_data_nodes = list_length(server_id_list);
		Assert(num_data_nodes == list_length(avail_replicas));
	}
	else
	{
		/*
		 * If there is no chunk insert state and no data nodes from planning,
		 * this is an INSERT, UPDATE, or DELETE on a standalone foreign table.
		 *
		 * If it's a regular foreign table then we must get the data node from
		 * the foreign table's metadata.
		 *
		 * Otherwise, we use the list of "available" DNs from earlier
		 */
		if (hypertable_id == INVALID_HYPERTABLE_ID)
		{
			ForeignTable *table = GetForeignTable(rel->rd_id);
			TSConnectionId id = remote_connection_id(table->serverid, user_id);

			Assert(num_data_nodes == 1 && num_all_data_nodes == 1);
			fmstate = (TsFdwModifyState *) palloc0(TS_FDW_MODIFY_STATE_SIZE(num_data_nodes));
			initialize_fdw_data_node_state(&fmstate->data_nodes[0], id);
		}
		else
		{
			/* we use only the available replicas */
			fmstate =
				(TsFdwModifyState *) palloc0(TS_FDW_MODIFY_STATE_SIZE(list_length(avail_replicas)));
			foreach (lc, avail_replicas)
			{
				ChunkDataNode *node = lfirst(lc);
				TSConnectionId id = remote_connection_id(node->foreign_server_oid, user_id);

				initialize_fdw_data_node_state(&fmstate->data_nodes[i++], id);
			}
			num_data_nodes = list_length(avail_replicas);
		}
	}

	/* Set up remote query information. */
	fmstate->rel = rel;
	fmstate->query = query;
	fmstate->target_attrs = target_attrs;
	fmstate->has_returning = has_returning;
	fmstate->prepared = false; /* PREPARE will happen later */
	fmstate->num_data_nodes = num_data_nodes;
	fmstate->num_all_data_nodes = num_all_data_nodes;

	/* Prepare for input conversion of RETURNING results. */
	if (fmstate->has_returning)
		fmstate->att_conv_metadata = data_format_create_att_conv_in_metadata(tupdesc, false);

	if (operation == CMD_UPDATE || operation == CMD_DELETE)
	{
		Assert(subplan != NULL);

		/* Find the ctid resjunk column in the subplan's result */
		fmstate->ctid_attno = ExecFindJunkAttributeInTlist(subplan->targetlist, "ctid");
		if (!AttributeNumberIsValid(fmstate->ctid_attno))
			elog(ERROR, "could not find junk ctid column");
	}

	fmstate->stmt_params = stmt_params_create(fmstate->target_attrs,
											  operation == CMD_UPDATE || operation == CMD_DELETE,
											  tupdesc,
											  1);

	fmstate->tupfactory = tuplefactory_create_for_rel(rel, retrieved_attrs);

	return fmstate;
}

/*
 * Convert a relation's attribute numbers to the corresponding numbers for
 * another relation.
 *
 * Conversions are necessary when, e.g., a (new) chunk's attribute numbers do
 * not match the root table's numbers after a column has been removed.
 */
static List *
convert_attrs(TupleConversionMap *map, List *attrs)
{
	List *new_attrs = NIL;
	ListCell *lc;

	foreach (lc, attrs)
	{
		AttrNumber attnum = lfirst_int(lc);
		int i;

		for (i = 0; i < map->outdesc->natts; i++)
		{
			if (map->attrMap->attnums[i] == attnum)
			{
				new_attrs = lappend_int(new_attrs, AttrOffsetGetAttrNumber(i));
				break;
			}
		}

		/* Assert that we found the attribute */
		Assert(i != map->outdesc->natts);
	}

	Assert(list_length(attrs) == list_length(new_attrs));

	return new_attrs;
}

static List *
get_chunk_server_id_list(const List *chunk_data_nodes)
{
	List *list = NIL;
	ListCell *lc;

	foreach (lc, chunk_data_nodes)
	{
		ChunkDataNode *cdn = lfirst(lc);

		list = lappend_oid(list, cdn->foreign_server_oid);
	}

	return list;
}

void
fdw_begin_foreign_modify(PlanState *pstate, ResultRelInfo *rri, CmdType operation,
						 List *fdw_private, Plan *subplan)
{
	TsFdwModifyState *fmstate;
	EState *estate = pstate->state;
	char *query;
	List *target_attrs;
	bool has_returning;
	List *retrieved_attrs;
	List *server_id_list = NIL;
	ChunkInsertState *cis = NULL;
	RangeTblEntry *rte;

	/* Deconstruct fdw_private data. */
	query = strVal(list_nth(fdw_private, FdwModifyPrivateUpdateSql));
	target_attrs = (List *) list_nth(fdw_private, FdwModifyPrivateTargetAttnums);
	has_returning = intVal(list_nth(fdw_private, FdwModifyPrivateHasReturning));
	retrieved_attrs = (List *) list_nth(fdw_private, FdwModifyPrivateRetrievedAttrs);

	/* Find RTE. */
	rte = rt_fetch(rri->ri_RangeTableIndex, estate->es_range_table);

	Assert(NULL != rte);

	if (list_length(fdw_private) > FdwModifyPrivateDataNodes)
	{
		List *data_nodes = (List *) list_nth(fdw_private, FdwModifyPrivateDataNodes);
		ListCell *lc;

		foreach (lc, data_nodes)
			server_id_list = lappend_oid(server_id_list, lfirst_oid(lc));
	}

	if (list_length(fdw_private) > FdwModifyPrivateChunkInsertState)
	{
		cis = (ChunkInsertState *) list_nth(fdw_private, FdwModifyPrivateChunkInsertState);

		/*
		 * A chunk may have different attribute numbers than the root relation
		 * that we planned the attribute lists for
		 */
		if (NULL != cis->hyper_to_chunk_map)
		{
			/*
			 * Convert the target attributes (the inserted or updated
			 * attributes)
			 */
			target_attrs = convert_attrs(cis->hyper_to_chunk_map, target_attrs);

			/*
			 * Convert the retrieved attributes, if there is a RETURNING
			 * statement
			 */
			if (NIL != retrieved_attrs)
				retrieved_attrs = convert_attrs(cis->hyper_to_chunk_map, retrieved_attrs);
		}

		/*
		 * If there's a chunk insert state, then it has the authoritative
		 * data node list.
		 */
		server_id_list = get_chunk_server_id_list(cis->chunk_data_nodes);
	}

	/* Construct an execution state. */
#if PG16_LT
	Oid checkAsUser = rte->checkAsUser;
#else
	RTEPermissionInfo *perminfo = getRTEPermissionInfo(estate->es_rteperminfos, rte);
	Oid checkAsUser = perminfo->checkAsUser;
#endif
	fmstate = create_foreign_modify(estate,
									rri->ri_RelationDesc,
									operation,
									checkAsUser,
									subplan,
									query,
									target_attrs,
									has_returning,
									retrieved_attrs,
									server_id_list);

	rri->ri_FdwState = fmstate;
}

static PreparedStmt *
prepare_foreign_modify_data_node(TsFdwModifyState *fmstate, TsFdwDataNodeState *fdw_data_node)
{
	AsyncRequest *req;

	Assert(NULL == fdw_data_node->p_stmt);

	req = async_request_send_prepare(fdw_data_node->conn,
									 fmstate->query,
									 stmt_params_num_params(fmstate->stmt_params));

	Assert(NULL != req);

	/*
	 * Async request interface doesn't seem to allow waiting for multiple
	 * prepared statements in an AsyncRequestSet. Should fix async API
	 */
	return async_request_wait_prepared_statement(req);
}

/*
 * prepare_foreign_modify
 *		Establish a prepared statement for execution of INSERT/UPDATE/DELETE
 */
static void
prepare_foreign_modify(TsFdwModifyState *fmstate)
{
	int i;

	for (i = 0; i < fmstate->num_data_nodes; i++)
	{
		TsFdwDataNodeState *fdw_data_node = &fmstate->data_nodes[i];

		fdw_data_node->p_stmt = prepare_foreign_modify_data_node(fmstate, fdw_data_node);
	}

	fmstate->prepared = true;
}

/*
 * store_returning_result
 *		Store the result of a RETURNING clause
 *
 * On error, be sure to release the PGresult on the way out.  Callers do not
 * have PG_TRY blocks to ensure this happens.
 */
static void
store_returning_result(TsFdwModifyState *fmstate, TupleTableSlot *slot, PGresult *res)
{
	PG_TRY();
	{
		HeapTuple newtup =
			tuplefactory_make_tuple(fmstate->tupfactory, res, 0, PQbinaryTuples(res));

		/* tuple will be deleted when it is cleared from the slot */
		ExecStoreHeapTuple(newtup, slot, true);
	}
	PG_CATCH();
	{
		if (res)
			PQclear(res);
		PG_RE_THROW();
	}
	PG_END_TRY();
}

static int
response_type(AttConvInMetadata *att_conv_metadata)
{
	if (!ts_guc_enable_connection_binary_data)
		return FORMAT_TEXT;
	return att_conv_metadata == NULL || att_conv_metadata->binary ? FORMAT_BINARY : FORMAT_TEXT;
}

static void
fdw_chunk_update_stale_metadata(TsFdwModifyState *fmstate)
{
	List *all_data_nodes;
	Relation rel = fmstate->rel;

	if (fmstate->num_all_data_nodes == fmstate->num_data_nodes)
		return;

	if (fmstate->num_all_data_nodes > fmstate->num_data_nodes)
	{
		Chunk *chunk = ts_chunk_get_by_relid(rel->rd_id, true);
		/* get filtered list */
		List *serveroids = get_chunk_data_nodes(rel->rd_id);
		ListCell *lc;
		bool chunk_is_locked = false;

		Assert(list_length(serveroids) == fmstate->num_data_nodes);

		all_data_nodes = ts_chunk_data_node_scan_by_chunk_id(chunk->fd.id, CurrentMemoryContext);
		Assert(list_length(all_data_nodes) == fmstate->num_all_data_nodes);

		foreach (lc, all_data_nodes)
		{
			ChunkDataNode *cdn = lfirst(lc);
			/*
			 * check if this DN is a part of serveroids. If not
			 * found in serveroids, then we need to remove this
			 * chunk id to node name mapping and also update the primary
			 * foreign server if necessary. It's possible that this metadata
			 * might have been already cleared earlier but we have no way of
			 * knowing that here.
			 */
			if (!list_member_oid(serveroids, cdn->foreign_server_oid) &&
				!list_member_oid(fmstate->stale_data_nodes, cdn->foreign_server_oid))
			{
				if (!chunk_is_locked)
				{
					LockRelationOid(chunk->table_id, ShareUpdateExclusiveLock);
					chunk_is_locked = true;
				}

				chunk_update_foreign_server_if_needed(chunk, cdn->foreign_server_oid, false);
				ts_chunk_data_node_delete_by_chunk_id_and_node_name(cdn->fd.chunk_id,
																	NameStr(cdn->fd.node_name));

				/* append this DN serveroid to the list of DNs marked stale for this chunk */
				fmstate->stale_data_nodes =
					lappend_oid(fmstate->stale_data_nodes, cdn->foreign_server_oid);
			}
		}
	}
}

TupleTableSlot *
fdw_exec_foreign_insert(TsFdwModifyState *fmstate, EState *estate, TupleTableSlot *slot,
						TupleTableSlot *planslot)
{
	StmtParams *params = fmstate->stmt_params;
	AsyncRequestSet *reqset;
	AsyncResponseResult *rsp;
	int n_rows = -1;
	int i;

	if (!fmstate->prepared)
		prepare_foreign_modify(fmstate);

	reqset = async_request_set_create();

	stmt_params_convert_values(params, slot, NULL);

	for (i = 0; i < fmstate->num_data_nodes; i++)
	{
		TsFdwDataNodeState *fdw_data_node = &fmstate->data_nodes[i];
		AsyncRequest *req = NULL;
		int type = response_type(fmstate->att_conv_metadata);
		req = async_request_send_prepared_stmt_with_params(fdw_data_node->p_stmt, params, type);
		Assert(NULL != req);
		async_request_set_add(reqset, req);
	}

	while ((rsp = async_request_set_wait_any_result(reqset)))
	{
		PGresult *res = async_response_result_get_pg_result(rsp);

		if (PQresultStatus(res) != (fmstate->has_returning ? PGRES_TUPLES_OK : PGRES_COMMAND_OK))
			async_response_report_error((AsyncResponse *) rsp, ERROR);

		/*
		 * If we insert into multiple replica chunks, we should only return
		 * the results from the first one
		 */
		if (n_rows == -1)
		{
			/* Check number of rows affected, and fetch RETURNING tuple if any */
			if (fmstate->has_returning)
			{
				n_rows = PQntuples(res);

				if (n_rows > 0)
					store_returning_result(fmstate, slot, res);
			}
			else
				n_rows = atoi(PQcmdTuples(res));
		}

		/* And clean up */
		async_response_result_close(rsp);
		stmt_params_reset(params);
	}

	/*
	 * Currently no way to do a deep cleanup of all request in the request
	 * set. The worry here is that since this runs in a per-chunk insert state
	 * memory context, the async API will accumulate a lot of cruft during
	 * inserts
	 */
	pfree(reqset);

	/*
	 * If rows are affected on DNs and a DN was excluded because of being
	 * "unavailable" then we need to update metadata on the AN to mark
	 * this chunk as "stale" for that "unavailable" DN
	 */
	if (n_rows > 0 && fmstate->num_all_data_nodes > fmstate->num_data_nodes)
		fdw_chunk_update_stale_metadata(fmstate);

	/* Return NULL if nothing was inserted on the remote end */
	return (n_rows > 0) ? slot : NULL;
}

/*
 * Execute either an UPDATE or DELETE.
 */
TupleTableSlot *
fdw_exec_foreign_update_or_delete(TsFdwModifyState *fmstate, EState *estate, TupleTableSlot *slot,
								  TupleTableSlot *planslot, ModifyCommand cmd)
{
	StmtParams *params = fmstate->stmt_params;
	AsyncRequestSet *reqset;
	AsyncResponseResult *rsp;
	Datum datum;
	bool is_null;
	int n_rows = -1;
	int i;

	/* Set up the prepared statement on the data node, if we didn't yet */
	if (!fmstate->prepared)
		prepare_foreign_modify(fmstate);

	/* Get the ctid that was passed up as a resjunk column */
	datum = ExecGetJunkAttribute(planslot, fmstate->ctid_attno, &is_null);

	/* shouldn't ever get a null result... */
	if (is_null)
		elog(ERROR, "ctid is NULL");

	stmt_params_convert_values(params,
							   (cmd == UPDATE_CMD ? slot : NULL),
							   (ItemPointer) DatumGetPointer(datum));
	reqset = async_request_set_create();

	for (i = 0; i < fmstate->num_data_nodes; i++)
	{
		AsyncRequest *req = NULL;
		TsFdwDataNodeState *fdw_data_node = &fmstate->data_nodes[i];
		int type = response_type(fmstate->att_conv_metadata);
		req = async_request_send_prepared_stmt_with_params(fdw_data_node->p_stmt, params, type);

		Assert(NULL != req);

		async_request_attach_user_data(req, fdw_data_node);
		async_request_set_add(reqset, req);
	}

	while ((rsp = async_request_set_wait_any_result(reqset)))
	{
		PGresult *res = async_response_result_get_pg_result(rsp);

		if (PQresultStatus(res) != (fmstate->has_returning ? PGRES_TUPLES_OK : PGRES_COMMAND_OK))
			remote_result_elog(res, ERROR);

		/*
		 * If we update multiple replica chunks, we should only return the
		 * results from the first one.
		 */
		if (n_rows == -1)
		{
			/* Check number of rows affected, and fetch RETURNING tuple if any */
			if (fmstate->has_returning)
			{
				n_rows = PQntuples(res);

				if (n_rows > 0)
					store_returning_result(fmstate, slot, res);
			}
			else
				n_rows = atoi(PQcmdTuples(res));
		}

		/* And clean up */
		async_response_result_close(rsp);
	}

	/*
	 * Currently no way to do a deep cleanup of all request in the request
	 * set. The worry here is that since this runs in a per-chunk insert state
	 * memory context, the async API will accumulate a lot of cruft during
	 * inserts
	 */
	pfree(reqset);
	stmt_params_reset(params);

	/*
	 * If rows are affected on DNs and a DN was excluded because of being
	 * "unavailable" then we need to update metadata on the AN to mark
	 * this chunk as "stale" for that "unavailable" DN
	 */
	if (n_rows > 0 && fmstate->num_all_data_nodes > fmstate->num_data_nodes)
		fdw_chunk_update_stale_metadata(fmstate);

	/* Return NULL if nothing was updated on the remote end */
	return (n_rows > 0) ? slot : NULL;
}

/*
 * finish_foreign_modify
 *		Release resources for a foreign insert/update/delete operation
 */
void
fdw_finish_foreign_modify(TsFdwModifyState *fmstate)
{
	int i;

	Assert(fmstate != NULL);

	for (i = 0; i < fmstate->num_data_nodes; i++)
	{
		TsFdwDataNodeState *fdw_data_node = &fmstate->data_nodes[i];

		/* If we created a prepared statement, destroy it */
		if (NULL != fdw_data_node->p_stmt)
		{
			prepared_stmt_close(fdw_data_node->p_stmt);
			fdw_data_node->p_stmt = NULL;
		}

		fdw_data_node->conn = NULL;
	}

	stmt_params_free(fmstate->stmt_params);
}

void
fdw_explain_modify(PlanState *ps, ResultRelInfo *rri, List *fdw_private, int subplan_index,
				   ExplainState *es)
{
	if (es->verbose)
	{
		const char *sql = strVal(list_nth(fdw_private, FdwModifyPrivateUpdateSql));

		ExplainPropertyText("Remote SQL", sql, es);
	}
}
