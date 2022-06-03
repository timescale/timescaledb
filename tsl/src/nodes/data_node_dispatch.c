/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <parser/parsetree.h>
#include <nodes/execnodes.h>
#include <nodes/extensible.h>
#include <nodes/makefuncs.h>
#include <nodes/nodeFuncs.h>
#include <nodes/plannodes.h>
#include <executor/executor.h>
#include <executor/nodeModifyTable.h>
#include <utils/lsyscache.h>
#include <utils/builtins.h>
#include <utils/hsearch.h>
#include <utils/tuplestore.h>
#include <utils/memutils.h>
#include <funcapi.h>
#include <miscadmin.h>

#include "ts_catalog/chunk_data_node.h"

#include <nodes/chunk_dispatch_plan.h>
#include <nodes/chunk_dispatch_state.h>
#include <nodes/chunk_insert_state.h>
#include <hypertable_cache.h>
#include <compat/compat.h>
#include <guc.h>

#include "data_node_dispatch.h"
#include "fdw/scan_exec.h"
#include "fdw/deparse.h"
#include "remote/utils.h"
#include "remote/dist_txn.h"
#include "remote/async.h"
#include "remote/data_format.h"
#include "remote/tuplefactory.h"

#define TUPSTORE_MEMSIZE_KB work_mem
#define TUPSTORE_FLUSH_THRESHOLD ts_guc_max_insert_batch_size

typedef struct DataNodeDispatchPath
{
	CustomPath cpath;
	ModifyTablePath *mtpath;
	Index hypertable_rti; /* range table index of Hypertable */
	int subplan_index;
} DataNodeDispatchPath;

/*
 * DataNodeDispatch dispatches tuples to data nodes using batching. It inserts
 * itself below a ModifyTable node in the plan and subsequent execution tree,
 * like so:
 *
 *          --------------------   Set "direct modify plans" to
 *          | HypertableInsert |   signal ModifyTable to only
 *          --------------------   handle returning projection.
 *                   |
 *            ----------------     resultRelInfo->ri_usesFdwDirectModify
 *            |  ModifyTable |     should be TRUE. Handle returning projection.
 *            ----------------
 *                   ^
 *                   | RETURNING tuple or nothing
 *          --------------------
 *          | DataNodeDispatch |    Batch and send tuples to data nodes.
 *          --------------------
 *                   ^
 *                   | Chunk-routed tuple
 *           -----------------
 *           | ChunkDispatch |     Route tuple to chunk.
 *           -----------------     Set es_result_relation.
 *                   ^
 *                   | tuple
 *             --------------
 *             | ValuesScan |     VALUES ('2019-02-23 13:43', 1, 8.9),
 *             --------------            ('2019-02-23 13:46', 2, 1.5);
 *
 *
 * Data node dispatching uses the state machine outlined below:
 *
 * READ: read tuples from the subnode and save in per-node stores until one
 * of them reaches FLUSH_THRESHOLD and then move to FLUSH. If a NULL-tuple is
 * read before the threshold is reached, move to LAST_FLUSH. In case of
 * replication, tuples are split across a primary and a replica tuple store.
 *
 * FLUSH: flush the tuples for the data nodes that reached
 * FLUSH_THRESHOLD.
 *
 * LAST_FLUSH: flush tuples for all data nodes.
 *
 * RETURNING: if there is a RETURNING clause, return the inserted tuples
 * one-by-one from all flushed nodes. When no more tuples remain, move to
 * READ again or DONE if the previous state was LAST_FLUSH. Note, that in case
 * of replication, the tuples are split across a primary and a replica tuple
 * store for each data node. only tuples in the primary tuple store are
 * returned. It is implicitly assumed that the primary tuples are sent on a
 * connection before the replica tuples and thus the data node will also return
 * the primary tuples first (in order of insertion).
 *
 *   read
 *    ==
 *  thresh     -------------
 *      -----> |   FLUSH   |--->----
 *      |      -------------       |       prev_state
 *      |                          |           ==
 *  --------                 ------------- LAST_FLUSH ----------
 *  | READ | <-------------- | RETURNING | ---------> |  DONE  |
 *  --------                 -------------            ----------
 *      |                          ^
 *      |     --------------       |
 *      ----> | LAST_FLUSH | -------
 * read == 0  --------------
 *
 *
 * Potential optimizations
 * =======================
 *
 * - Tuples from both the primary and the replica tuple store are flushed with
 *   a RETURNING clause when such a clause is available. However, tuples from
 *   the replica store need not be returned, so using a separate prepared
 *   statement without RETURNING for the replica store would save bandwidth.
 *
 * - Better asynchronous behavior. When reading tuples, a flush happens as
 *   soon as a tuple store is filled instead of continuing to fill until more
 *   stores can be flushed. Further, after flushing tuples for a data node, the
 *   code immediately waits for a response instead of doing other work while
 *   waiting.
 *
 * - Currently, there is one "global" state machine for the
 *   DataNodeDispatchState executor node. Turning this into per-node state
 *   machines might make the code more asynchronous and/or amenable to
 *   parallel mode support.
 *
 * - COPY instead of INSERT. When there's no RETURNING clause, it is more
 *   efficient to COPY data rather than using a prepared statement.
 *
 * - Binary instead of text format. Send tuples in binary format instead of
 *   text to save bandwidth and reduce latency.
 */
typedef enum DispatchState
{
	SD_READ,
	SD_FLUSH,
	SD_LAST_FLUSH,
	SD_RETURNING,
	SD_DONE,
} DispatchState;

typedef struct DataNodeDispatchState
{
	CustomScanState cstate;
	DispatchState prevstate; /* Previous state in state machine */
	DispatchState state;	 /* Current state in state machine */
	Relation rel;			 /* The (local) relation we're inserting into */
	bool set_processed;		 /* Indicates whether to set the number or processed tuples */
	DeparsedInsertStmt stmt; /* Partially deparsed insert statement */
	const char *sql_stmt;	/* Fully deparsed insert statement */
	TupleFactory *tupfactory;
	List *target_attrs;		  /* The attributes to send to remote data nodes */
	List *responses;		  /* List of responses to process in RETURNING state */
	HTAB *nodestates;		  /* Hashtable of per-nodestate (tuple stores) */
	MemoryContext mcxt;		  /* Memory context for per-node state */
	MemoryContext batch_mcxt; /* Memory context for batches of data */
	int64 num_tuples;		  /* Total number of tuples flushed each round */
	int64 next_tuple;		  /* Next tuple to return to the parent node when in
							   * RETURNING state */
	int replication_factor;   /* > 1 if we replicate tuples across data nodes */
	StmtParams *stmt_params;  /* Parameters to send with statement. Format can be binary or text */
	int flush_threshold;	  /* Batch size used for this dispatch state */
	TupleTableSlot *batch_slot; /* Slot used for sending tuples to data
								 * nodes. Note that this needs to be a
								 * MinimalTuple slot, so we cannot use the
								 * standard ScanSlot in the ScanState because
								 * CustomNode sets it up to be a
								 * VirtualTuple. */
} DataNodeDispatchState;

/*
 * Plan metadata list indexes.
 */
enum CustomScanPrivateIndex
{
	CustomScanPrivateSql,
	CustomScanPrivateTargetAttrs,
	CustomScanPrivateDeparsedInsertStmt,
	CustomScanPrivateSetProcessed,
	CustomScanPrivateFlushThreshold
};

#define HAS_RETURNING(sds) ((sds)->stmt.returning != NULL)

/*
 * DataNodeState for each data node.
 *
 * Tuples destined for a data node are batched in a tuple store until dispatched
 * using a "flush". A flush happens using the prepared (insert) statement,
 * which can only be used to send a "full" batch of tuples as the number of
 * rows in the statement is predefined. Thus, a flush only happens when the
 * tuple store reaches the predefined size. Once the last tuple is read from
 * the subnode, a final flush occurs. In that case, a flush is "partial" (less
 * than the predefined amount). A partial flush cannot use the prepared
 * statement, since the number of rows do not match, and therefore a one-time
 * statement is created for the last insert.
 *
 * Note that, since we use one DataNodeState per connection, we
 * could technically have multiple DataNodeStates per data node.
 */
typedef struct DataNodeState
{
	TSConnectionId id; /* Must be first */
	TSConnection *conn;
	Tuplestorestate *primary_tupstore; /* Tuples this data node is primary
										* for. These tuples are returned when
										* RETURNING is specified. */
	Tuplestorestate *replica_tupstore; /* Tuples this data node is a replica
										* for. These tuples are NOT returned
										* when RETURNING is specified. */
	PreparedStmt *pstmt;			   /* Prepared statement to use in the FLUSH state */
	int num_tuples_sent;			   /* Number of tuples sent in the FLUSH or LAST_FLUSH states */
	int num_tuples_inserted;		   /* Number of tuples inserted (returned in result)
										* during the FLUSH or LAST_FLUSH states */
	int next_tuple;					   /* The next tuple to return in the RETURNING state */
	TupleTableSlot *slot;
} DataNodeState;

#define NUM_STORED_TUPLES(ss)                                                                      \
	(tuplestore_tuple_count((ss)->primary_tupstore) +                                              \
	 ((ss)->replica_tupstore != NULL ? tuplestore_tuple_count((ss)->replica_tupstore) : 0))

static void
data_node_state_init(DataNodeState *ss, DataNodeDispatchState *sds, TSConnectionId id)
{
	MemoryContext old = MemoryContextSwitchTo(sds->mcxt);

	memset(ss, 0, sizeof(DataNodeState));
	ss->id = id;
	ss->primary_tupstore = tuplestore_begin_heap(false, false, TUPSTORE_MEMSIZE_KB);
	if (sds->replication_factor > 1)
		ss->replica_tupstore = tuplestore_begin_heap(false, false, TUPSTORE_MEMSIZE_KB);
	else
		ss->replica_tupstore = NULL;

	ss->conn = remote_dist_txn_get_connection(id, REMOTE_TXN_USE_PREP_STMT);
	ss->pstmt = NULL;
	ss->next_tuple = 0;
	ss->num_tuples_sent = 0;
	ss->num_tuples_inserted = 0;

	MemoryContextSwitchTo(old);
}

static DataNodeState *
data_node_state_get_or_create(DataNodeDispatchState *sds, TSConnectionId id)
{
	DataNodeState *ss;
	bool found;

	ss = hash_search(sds->nodestates, &id, HASH_ENTER, &found);

	if (!found)
		data_node_state_init(ss, sds, id);

	return ss;
}

static void
data_node_state_clear_primary_store(DataNodeState *ss)
{
	tuplestore_clear(ss->primary_tupstore);
	Assert(tuplestore_tuple_count(ss->primary_tupstore) == 0);
	ss->next_tuple = 0;
}

static void
data_node_state_clear_replica_store(DataNodeState *ss)
{
	if (NULL == ss->replica_tupstore)
		return;

	tuplestore_clear(ss->replica_tupstore);
	Assert(tuplestore_tuple_count(ss->replica_tupstore) == 0);
}

static void
data_node_state_close(DataNodeState *ss)
{
	if (NULL != ss->pstmt)
		prepared_stmt_close(ss->pstmt);

	tuplestore_end(ss->primary_tupstore);

	if (NULL != ss->replica_tupstore)
		tuplestore_end(ss->replica_tupstore);
}

static void
data_node_dispatch_begin(CustomScanState *node, EState *estate, int eflags)
{
	DataNodeDispatchState *sds = (DataNodeDispatchState *) node;
	CustomScan *cscan = castNode(CustomScan, node->ss.ps.plan);
#if PG14_LT
	ResultRelInfo *rri = estate->es_result_relation_info;
#else
	ResultRelInfo *rri = linitial_node(ResultRelInfo, estate->es_opened_result_relations);
#endif
	Relation rel = rri->ri_RelationDesc;
	TupleDesc tupdesc = RelationGetDescr(rel);
	Plan *subplan = linitial(cscan->custom_plans);
	Cache *hcache = ts_hypertable_cache_pin();
	Hypertable *ht = ts_hypertable_cache_get_entry(hcache, rel->rd_id, CACHE_FLAG_NONE);
	PlanState *ps;
	MemoryContext mcxt =
		AllocSetContextCreate(estate->es_query_cxt, "DataNodeState", ALLOCSET_SMALL_SIZES);
	HASHCTL hctl = {
		.keysize = sizeof(TSConnectionId),
		.entrysize = sizeof(DataNodeState),
		.hcxt = mcxt,
	};
	List *available_nodes = ts_hypertable_get_available_data_nodes(ht, true);

	Assert(NULL != ht);
	Assert(hypertable_is_distributed(ht));
	Assert(NIL != available_nodes);

	ps = ExecInitNode(subplan, estate, eflags);

	node->custom_ps = list_make1(ps);
	sds->state = SD_READ;
	sds->rel = rel;
	sds->replication_factor = ht->fd.replication_factor;
	sds->sql_stmt = strVal(list_nth(cscan->custom_private, CustomScanPrivateSql));
	sds->target_attrs = list_nth(cscan->custom_private, CustomScanPrivateTargetAttrs);
	sds->set_processed = intVal(list_nth(cscan->custom_private, CustomScanPrivateSetProcessed));
	sds->flush_threshold = intVal(list_nth(cscan->custom_private, CustomScanPrivateFlushThreshold));

	sds->mcxt = mcxt;
	sds->batch_mcxt = AllocSetContextCreate(mcxt, "DataNodeDispatch batch", ALLOCSET_SMALL_SIZES);
	sds->nodestates = hash_create("DataNodeDispatch tuple stores",
								  list_length(available_nodes),
								  &hctl,
								  HASH_ELEM | HASH_CONTEXT | HASH_BLOBS);

	deparsed_insert_stmt_from_list(&sds->stmt,
								   list_nth(cscan->custom_private,
											CustomScanPrivateDeparsedInsertStmt));
	/* Setup output functions to generate string values for each target attribute */
	sds->stmt_params = stmt_params_create(sds->target_attrs, false, tupdesc, sds->flush_threshold);

	if (HAS_RETURNING(sds))
		sds->tupfactory = tuplefactory_create_for_rel(rel, sds->stmt.retrieved_attrs);

	/* The tuplestores that hold batches of tuples only allow MinimalTuples so
	 * we need a dedicated slot for getting tuples from the stores since the
	 * CustomScan's ScanTupleSlot is a VirtualTuple. */
	sds->batch_slot = MakeSingleTupleTableSlot(tupdesc, &TTSOpsMinimalTuple);

	ts_cache_release(hcache);
}

/*
 * Store the result of a RETURNING clause.
 */
static void
store_returning_result(DataNodeDispatchState *sds, int row, TupleTableSlot *slot, PGresult *res)
{
	PG_TRY();
	{
		HeapTuple newtup = tuplefactory_make_tuple(sds->tupfactory, res, row, PQbinaryTuples(res));

		/* We need to force the tuple into the slot since it is not of the
		 * right type (conversion to the right type will happen if
		 * necessary) */
		ExecForceStoreHeapTuple(newtup, slot, true);
	}
	PG_CATCH();
	{
		if (res)
			PQclear(res);
		PG_RE_THROW();
	}
	PG_END_TRY();
}

static const char *state_names[] = {
	[SD_READ] = "READ",			  [SD_FLUSH] = "FLUSH", [SD_LAST_FLUSH] = "LAST_FLUSH",
	[SD_RETURNING] = "RETURNING", [SD_DONE] = "DONE",
};

/*
 * Move the state machine to a new state.
 */
static void
data_node_dispatch_set_state(DataNodeDispatchState *sds, DispatchState new_state)
{
	Assert(sds->state != new_state);

	elog(DEBUG2,
		 "DataNodeDispatchState: %s -> %s",
		 state_names[sds->state],
		 state_names[new_state]);

#ifdef USE_ASSERT_CHECKING

	switch (new_state)
	{
		case SD_READ:
			Assert(sds->state == SD_RETURNING);
			break;
		case SD_FLUSH:
		case SD_LAST_FLUSH:
			Assert(sds->state == SD_READ);
			break;
		case SD_RETURNING:
			Assert(sds->state == SD_FLUSH || sds->state == SD_LAST_FLUSH);
			break;
		case SD_DONE:
			Assert(sds->state == SD_RETURNING);
	}

#endif

	sds->prevstate = sds->state;
	sds->state = new_state;
}

static PreparedStmt *
prepare_data_node_insert_stmt(DataNodeDispatchState *sds, TSConnection *conn, int total_params)
{
	AsyncRequest *req;
	PreparedStmt *stmt;
	MemoryContext oldcontext = MemoryContextSwitchTo(sds->mcxt);

	req = async_request_send_prepare(conn, sds->sql_stmt, total_params);
	Assert(req != NULL);
	stmt = async_request_wait_prepared_statement(req);
	MemoryContextSwitchTo(oldcontext);

	return stmt;
}

/*
 * Send a batch of tuples to a data node.
 *
 * All stored tuples for the given data node are sent on the node's
 * connection. If in FLUSH state (i.e., sending a predefined amount of
 * tuples), use a prepared statement, or construct a custom statement if in
 * LAST_FLUSH state.
 *
 * If there's a RETURNING clause, we reset the read pointer for the store,
 * since the original tuples need to be returned along with the
 * node-returned ones. If there is no RETURNING clause, simply clear the
 * store.
 */
static AsyncRequest *
send_batch_to_data_node(DataNodeDispatchState *sds, DataNodeState *ss)
{
	TupleTableSlot *slot = sds->batch_slot;
	AsyncRequest *req;
	const char *sql_stmt;
	int response_type = FORMAT_TEXT;

	Assert(sds->state == SD_FLUSH || sds->state == SD_LAST_FLUSH);
	Assert(NUM_STORED_TUPLES(ss) <= sds->flush_threshold);
	Assert(NUM_STORED_TUPLES(ss) > 0);

	ss->num_tuples_sent = 0;

	while (
		tuplestore_gettupleslot(ss->primary_tupstore, true /* forward */, false /* copy */, slot))
	{
		/* get following parameters from slot */
		stmt_params_convert_values(sds->stmt_params, slot, NULL);
		ss->num_tuples_sent++;
	}

	if (NULL != ss->replica_tupstore)
	{
		while (tuplestore_gettupleslot(ss->replica_tupstore,
									   true /* forward */,
									   false /* copy */,
									   slot))
		{
			/* get following parameters from slot */
			stmt_params_convert_values(sds->stmt_params, slot, NULL);
			ss->num_tuples_sent++;
		}
	}

	Assert(ss->num_tuples_sent == NUM_STORED_TUPLES(ss));
	Assert(ss->num_tuples_sent == stmt_params_converted_tuples(sds->stmt_params));

	if (HAS_RETURNING(sds) && tuplefactory_is_binary(sds->tupfactory))
		response_type = FORMAT_BINARY;
	else if (ts_guc_enable_connection_binary_data)
		response_type = FORMAT_BINARY;

	/* Send tuples */
	switch (sds->state)
	{
		case SD_FLUSH:
			/* Lazy initialize the prepared statement */
			if (NULL == ss->pstmt)
				ss->pstmt =
					prepare_data_node_insert_stmt(sds,
												  ss->conn,
												  stmt_params_total_values(sds->stmt_params));

			Assert(ss->num_tuples_sent == sds->flush_threshold);
			req = async_request_send_prepared_stmt_with_params(ss->pstmt,
															   sds->stmt_params,
															   response_type);
			break;
		case SD_LAST_FLUSH:
			sql_stmt = deparsed_insert_stmt_get_sql(&sds->stmt,
													stmt_params_converted_tuples(sds->stmt_params));
			Assert(sql_stmt != NULL);
			Assert(ss->num_tuples_sent < sds->flush_threshold);
			req =
				async_request_send_with_params(ss->conn, sql_stmt, sds->stmt_params, response_type);
			break;
		default:
			elog(ERROR, "unexpected data node dispatch state %s", state_names[sds->state]);
			Assert(false);
			break;
	}

	Assert(NULL != req);
	async_request_attach_user_data(req, ss);

	sds->num_tuples += tuplestore_tuple_count(ss->primary_tupstore);

	/* If there's a RETURNING clause, we need to also return the inserted
	   tuples in
	   rri->ri_projectReturning->pi_exprContext->ecxt_scantuple */
	if (HAS_RETURNING(sds))
		tuplestore_rescan(ss->primary_tupstore);
	else
		data_node_state_clear_primary_store(ss);

	/* No tuples are returned from the replica store so safe to clear */
	data_node_state_clear_replica_store(ss);

	/* Since we're done with current batch, reset params so they are safe to use in the next
	 * batch/node */
	stmt_params_reset(sds->stmt_params);

	return req;
}

/*
 * Check if we should flush tuples stored for a data node.
 *
 * There are two cases when this happens:
 *
 * 1. State is SD_FLUSH and the flush threshold is reached.
 * 2. State is SD_LAST_FLUSH and there are tuples to send.
 */
static bool
should_flush_data_node(DataNodeDispatchState *sds, DataNodeState *ss)
{
	int64 num_tuples = NUM_STORED_TUPLES(ss);

	Assert(sds->state == SD_FLUSH || sds->state == SD_LAST_FLUSH);

	if (sds->state == SD_FLUSH)
	{
		if (num_tuples >= sds->flush_threshold)
			return true;
		return false;
	}

	return num_tuples > 0;
}

/*
 * Flush the tuples of data nodes that have a full batch.
 */
static AsyncRequestSet *
flush_data_nodes(DataNodeDispatchState *sds)
{
	AsyncRequestSet *reqset = NULL;
	DataNodeState *ss;
	HASH_SEQ_STATUS hseq;

	Assert(sds->state == SD_FLUSH || sds->state == SD_LAST_FLUSH);

	hash_seq_init(&hseq, sds->nodestates);

	for (ss = hash_seq_search(&hseq); ss != NULL; ss = hash_seq_search(&hseq))
	{
		if (should_flush_data_node(sds, ss))
		{
			AsyncRequest *req = send_batch_to_data_node(sds, ss);

			if (NULL != req)
			{
				if (NULL == reqset)
					reqset = async_request_set_create();

				async_request_set_add(reqset, req);
			}
		}
	}

	return reqset;
}

/*
 * Wait for responses from data nodes after INSERT.
 *
 * In case of RETURNING, return a list of responses, otherwise NIL.
 */
static List *
await_all_responses(DataNodeDispatchState *sds, AsyncRequestSet *reqset)
{
	AsyncResponseResult *rsp;
	List *results = NIL;

	sds->next_tuple = 0;

	while ((rsp = async_request_set_wait_any_result(reqset)))
	{
		DataNodeState *ss = async_response_result_get_user_data(rsp);
		PGresult *res = async_response_result_get_pg_result(rsp);
		ExecStatusType status = PQresultStatus(res);
		bool report_error = true;

		switch (status)
		{
			case PGRES_TUPLES_OK:
				if (!HAS_RETURNING(sds))
					break;

				results = lappend(results, rsp);
				ss->num_tuples_inserted = PQntuples(res);
				Assert(sds->stmt.do_nothing || (ss->num_tuples_inserted == ss->num_tuples_sent));
				report_error = false;
				break;
			case PGRES_COMMAND_OK:
				if (HAS_RETURNING(sds))
					break;

				ss->num_tuples_inserted = atoi(PQcmdTuples(res));
				async_response_result_close(rsp);
				Assert(sds->stmt.do_nothing || (ss->num_tuples_inserted == ss->num_tuples_sent));
				report_error = false;
				break;
			default:
				break;
		}

		if (report_error)
			async_response_report_error((AsyncResponse *) rsp, ERROR);

		/* Unless there is an ON CONFLICT clause, the number of tuples
		 * returned should greater than zero and be the same as the number of
		 * tuples sent.  */
		Assert(sds->stmt.do_nothing || ss->num_tuples_inserted > 0);
		ss->next_tuple = 0;
	}

	return results;
}

/*
 * Read tuples from the child scan node.
 *
 * Read until there's a NULL tuple or we've filled a data node's batch. Ideally,
 * we'd continue to read more tuples to fill other data nodes' batches, but since
 * the next tuple might be for the same node that has the full batch, we
 * risk overfilling. This could be mitigated by using two tuple stores per
 * data node (current and next batch) and alternate between them. But that also
 * increases memory requirements and complicates the code, so that's left as a
 * future optimization.
 *
 * Return the number of tuples read.
 */
static int64
handle_read(DataNodeDispatchState *sds)
{
	PlanState *substate = linitial(sds->cstate.custom_ps);
	ChunkDispatchState *cds = (ChunkDispatchState *) substate;
	EState *estate = sds->cstate.ss.ps.state;
#if PG14_LT
	ResultRelInfo *rri_saved = estate->es_result_relation_info;
#endif
	int64 num_tuples_read = 0;

	Assert(sds->state == SD_READ);

	/* If we are reading new tuples, we either do it for the first batch or we
	 * finished a previous batch. In either case, reset the batch memory
	 * context so that we release the memory after each batch is finished. */
	MemoryContextReset(sds->batch_mcxt);

	/* Read tuples from the subnode until flush */
	while (sds->state == SD_READ)
	{
		TupleTableSlot *slot = ExecProcNode(substate);

		if (TupIsNull(slot))
			data_node_dispatch_set_state(sds, SD_LAST_FLUSH);
		else
		{
			/* The previous node should have routed the tuple to the right
			 * chunk and set the corresponding result relation. The FdwState
			 * should also point to the chunk's insert state. */
			ResultRelInfo *rri = cds->rri;
			ChunkInsertState *cis = rri->ri_FdwState;
			TriggerDesc *trigdesc = rri->ri_TrigDesc;
			ListCell *lc;
			bool primary_data_node = true;
			MemoryContext oldcontext;
			TupleDesc rri_desc = RelationGetDescr(rri->ri_RelationDesc);

			if (NULL != rri->ri_projectReturning && rri_desc->constr &&
				rri_desc->constr->has_generated_stored)
				ExecComputeStoredGeneratedCompat(rri, estate, slot, CMD_INSERT);

			Assert(NULL != cis);

			oldcontext = MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));

			/* While we could potentially support triggers on frontend nodes,
			 * the triggers should exists also on the remote node and will be
			 * executed there. For new, the safest bet is to avoid triggers on
			 * the frontend. */
			if (trigdesc && (trigdesc->trig_insert_after_row || trigdesc->trig_insert_before_row))
				elog(ERROR, "cannot insert into remote chunk with row triggers");

			/* Total count */
			num_tuples_read++;

			foreach (lc, cis->chunk_data_nodes)
			{
				ChunkDataNode *cdn = lfirst(lc);
				TSConnectionId id = remote_connection_id(cdn->foreign_server_oid, cis->user_id);
				DataNodeState *ss = data_node_state_get_or_create(sds, id);

				/* This will store one copy of the tuple per data node, which is
				 * a bit inefficient. Note that we put the tuple in the
				 * primary store for the first data node, but the replica store
				 * for all other data nodes. This is to be able to know which
				 * tuples to return in a RETURNING statement. */
				if (primary_data_node)
					tuplestore_puttupleslot(ss->primary_tupstore, slot);
				else
					tuplestore_puttupleslot(ss->replica_tupstore, slot);

				/* Once one data node has reached the batch size, we stop
				 * reading. */
				if (sds->state != SD_FLUSH && NUM_STORED_TUPLES(ss) >= sds->flush_threshold)
					data_node_dispatch_set_state(sds, SD_FLUSH);

				primary_data_node = false;
			}

			MemoryContextSwitchTo(oldcontext);
		}
	}

#if PG14_LT
	estate->es_result_relation_info = rri_saved;
#endif

	return num_tuples_read;
}

/*
 * Flush all data nodes and move to the RETURNING state.
 *
 * Note that future optimizations could do this more asynchronously by doing
 * other work until responses are available (e.g., one could start to fill the
 * next batch while waiting for a response). However, the async API currently
 * doesn't expose a way to check for a response without blocking and
 * interleaving different tasks would also complicate the state machine.
 */
static void
handle_flush(DataNodeDispatchState *sds)
{
	MemoryContext oldcontext;
	AsyncRequestSet *reqset;

	Assert(sds->state == SD_FLUSH || sds->state == SD_LAST_FLUSH);

	/* Save the requests and responses in the batch memory context since they
	 * need to survive across several iterations of the executor loop when
	 * there is a RETURNING clause. The batch memory context is cleared the
	 * next time we read a batch. */
	oldcontext = MemoryContextSwitchTo(sds->batch_mcxt);
	reqset = flush_data_nodes(sds);

	if (NULL != reqset)
	{
		sds->responses = await_all_responses(sds, reqset);
		pfree(reqset);
	}

	data_node_dispatch_set_state(sds, SD_RETURNING);
	MemoryContextSwitchTo(oldcontext);
}

/*
 * Get a tuple when there's a RETURNING clause.
 */
static TupleTableSlot *
get_returning_tuple(DataNodeDispatchState *sds)
{
	ChunkDispatchState *cds = (ChunkDispatchState *) linitial(sds->cstate.custom_ps);
	ResultRelInfo *rri = cds->rri;
	TupleTableSlot *res_slot = sds->batch_slot;
	TupleTableSlot *slot = sds->cstate.ss.ss_ScanTupleSlot;
	ExprContext *econtext;

	Assert(NULL != rri->ri_projectReturning);

	econtext = rri->ri_projectReturning->pi_exprContext;

	/*
	 * Store a RETURNING tuple.  If HAS_RETURNING() is false, just emit a
	 * dummy tuple.  (has_returning is false when the local query is of the
	 * form "UPDATE/DELETE .. RETURNING 1" for example.)
	 */
	if (!HAS_RETURNING(sds))
	{
		Assert(NIL == sds->responses);
		ExecStoreAllNullTuple(slot);
		res_slot = slot;
	}
	else
	{
		while (NIL != sds->responses)
		{
			AsyncResponseResult *rsp = linitial(sds->responses);
			DataNodeState *ss = async_response_result_get_user_data(rsp);
			PGresult *res = async_response_result_get_pg_result(rsp);
			int64 num_tuples_to_return = tuplestore_tuple_count(ss->primary_tupstore);
			bool last_tuple;
			bool got_tuple = false;

			if (num_tuples_to_return > 0)
			{
				MemoryContext oldcontext;

				last_tuple = (ss->next_tuple + 1) == num_tuples_to_return;

				Assert(ss->next_tuple < ss->num_tuples_inserted);
				Assert(ss->next_tuple < num_tuples_to_return);

				oldcontext = MemoryContextSwitchTo(econtext->ecxt_per_query_memory);
				store_returning_result(sds, ss->next_tuple, slot, res);

				/* Get the next tuple from the store. If it is the last tuple, we
				 * need to make a copy since we will clear the store before
				 * returning. */
				got_tuple = tuplestore_gettupleslot(ss->primary_tupstore,
													true /* forward */,
													last_tuple /* copy */,
													res_slot);

				MemoryContextSwitchTo(oldcontext);
				Assert(got_tuple);
				Assert(!TupIsNull(res_slot));
				ss->next_tuple++;
			}
			else
				last_tuple = true;

			if (last_tuple)
			{
				sds->responses = list_delete_first(sds->responses);
				async_response_result_close(rsp);
				data_node_state_clear_primary_store(ss);
			}

			if (got_tuple)
				break;
		}
	}

	econtext->ecxt_scantuple = slot;

	return res_slot;
}

/*
 * Get the next tuple slot to return when there's a RETURNING
 * clause. Otherwise, return an empty slot.
 */
static TupleTableSlot *
handle_returning(DataNodeDispatchState *sds)
{
	EState *estate = sds->cstate.ss.ps.state;
	ChunkDispatchState *cds = (ChunkDispatchState *) linitial(sds->cstate.custom_ps);
	ResultRelInfo *rri = cds->rri;
	TupleTableSlot *slot = sds->cstate.ss.ss_ScanTupleSlot;
	bool done = false;
	MemoryContext oldcontext;

	Assert(sds->state == SD_RETURNING);
	oldcontext = MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));

	/*
	 * When all chunks are pruned rri will be NULL and there is nothing to do.
	 * Without returning projection, nothing to do here either.
	 */
	if (!rri || !rri->ri_projectReturning)
	{
		Assert(!HAS_RETURNING(sds));
		Assert(NIL == sds->responses);
		done = true;

		if (sds->set_processed)
			estate->es_processed += sds->num_tuples;
	}

	/* If we've processed all tuples, then we're also done */
	if (sds->next_tuple >= sds->num_tuples)
		done = true;

	if (done)
	{
		sds->next_tuple = 0;
		sds->num_tuples = 0;

		slot = ExecClearTuple(slot);

		if (sds->prevstate == SD_LAST_FLUSH)
			data_node_dispatch_set_state(sds, SD_DONE);
		else
			data_node_dispatch_set_state(sds, SD_READ);
	}
	else
	{
		slot = get_returning_tuple(sds);
		Assert(!TupIsNull(slot));
		sds->next_tuple++;

		if (sds->set_processed)
			estate->es_processed++;
	}

	MemoryContextSwitchTo(oldcontext);

	return slot;
}

/*
 * Execute the remote INSERT.
 *
 * This is called every time the parent asks for a new tuple. Read the child
 * scan node and buffer until there's a full batch, then flush by sending to
 * data node(s). If there's a returning statement, we return the flushed tuples
 * one-by-one, or continue reading more tuples from the child until there's a
 * NULL tuple.
 */
static TupleTableSlot *
data_node_dispatch_exec(CustomScanState *node)
{
	DataNodeDispatchState *sds = (DataNodeDispatchState *) node;
	TupleTableSlot *slot = NULL;
	bool done = false;

#if PG14_LT
	/* Initially, the result relation should always match the hypertable.  */
	Assert(node->ss.ps.state->es_result_relation_info->ri_RelationDesc->rd_id == sds->rel->rd_id);
#endif

	/* Read tuples and flush until there's either something to return or no
	 * more tuples to read */
	while (!done)
	{
		switch (sds->state)
		{
			case SD_READ:
				handle_read(sds);
				break;
			case SD_FLUSH:
			case SD_LAST_FLUSH:
				handle_flush(sds);
				break;
			case SD_RETURNING:
				slot = handle_returning(sds);
				/* If a tuple was read, return it and wait to get called again */
				done = !TupIsNull(slot);
				break;
			case SD_DONE:
				done = true;
				Assert(TupIsNull(slot));
				break;
		}
	}

#if PG14_LT
	/* Tuple routing in the ChunkDispatchState subnode sets the result
	 * relation to a chunk when routing, but the read handler should have
	 * ensured the result relation is reset. */
	Assert(node->ss.ps.state->es_result_relation_info->ri_RelationDesc->rd_id == sds->rel->rd_id);
	Assert(node->ss.ps.state->es_result_relation_info->ri_usesFdwDirectModify);
#endif

	return slot;
}

static void
data_node_dispatch_rescan(CustomScanState *node)
{
	/* Cannot rescan and start from the beginning since we might already have
	 * sent data to remote nodes */
	elog(ERROR, "cannot restart inserts to remote nodes");
}

static void
data_node_dispatch_end(CustomScanState *node)
{
	DataNodeDispatchState *sds = (DataNodeDispatchState *) node;
	DataNodeState *ss;
	HASH_SEQ_STATUS hseq;

	hash_seq_init(&hseq, sds->nodestates);

	for (ss = hash_seq_search(&hseq); ss != NULL; ss = hash_seq_search(&hseq))
		data_node_state_close(ss);

	hash_destroy(sds->nodestates);
	ExecDropSingleTupleTableSlot(sds->batch_slot);
	ExecEndNode(linitial(node->custom_ps));
}

static void
data_node_dispatch_explain(CustomScanState *node, List *ancestors, ExplainState *es)
{
	DataNodeDispatchState *sds = (DataNodeDispatchState *) node;

	ExplainPropertyInteger("Batch size", NULL, sds->flush_threshold, es);

	/*
	 * Add remote query, when VERBOSE option is specified.
	 */
	if (es->verbose)
	{
		const char *explain_sql =
			deparsed_insert_stmt_get_sql_explain(&sds->stmt, sds->flush_threshold);

		ExplainPropertyText("Remote SQL", explain_sql, es);
	}
}

static CustomExecMethods data_node_dispatch_state_methods = {
	.CustomName = "DataNodeDispatchState",
	.BeginCustomScan = data_node_dispatch_begin,
	.EndCustomScan = data_node_dispatch_end,
	.ExecCustomScan = data_node_dispatch_exec,
	.ReScanCustomScan = data_node_dispatch_rescan,
	.ExplainCustomScan = data_node_dispatch_explain,
};

/* Only allocate the custom scan state. Initialize in the begin handler. */
static Node *
data_node_dispatch_state_create(CustomScan *cscan)
{
	DataNodeDispatchState *sds;

	sds = (DataNodeDispatchState *) newNode(sizeof(DataNodeDispatchState), T_CustomScanState);
	sds->cstate.methods = &data_node_dispatch_state_methods;

	return (Node *) sds;
}

static CustomScanMethods data_node_dispatch_plan_methods = {
	.CustomName = "DataNodeDispatch",
	.CreateCustomScanState = data_node_dispatch_state_create,
};

static List *
get_insert_attrs(Relation rel)
{
	TupleDesc tupdesc = RelationGetDescr(rel);
	List *attrs = NIL;
	int i;

	for (i = 0; i < tupdesc->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc, i);

		if (attr->attisdropped || attr->attgenerated != '\0')
			continue;

		attrs = lappend_int(attrs, AttrOffsetGetAttrNumber(i));
	}

	return attrs;
}

/*
 * Plan a remote INSERT on a hypertable.
 *
 * Create the metadata needed for a remote INSERT. This mostly involves
 * deparsing the INSERT statement.
 *
 * Return the metadata as a list of Nodes that can be saved in a prepared
 * statement.
 */
static List *
plan_remote_insert(PlannerInfo *root, DataNodeDispatchPath *sdpath)
{
	ModifyTablePath *mtpath = sdpath->mtpath;
	OnConflictAction onconflict =
		mtpath->onconflict == NULL ? ONCONFLICT_NONE : mtpath->onconflict->action;
	List *returning_lists = sdpath->mtpath->returningLists;
	RangeTblEntry *rte = planner_rt_fetch(sdpath->hypertable_rti, root);
	Relation rel;
	DeparsedInsertStmt stmt;
	const char *sql;
	List *target_attrs = NIL;
	List *returning_list = NIL;
	bool do_nothing = false;
	int flush_threshold;

	/*
	 * Core code already has some lock on each rel being planned, so we can
	 * use NoLock here.
	 */
	rel = table_open(rte->relid, NoLock);

	/*
	 * Extract the relevant RETURNING list if any.
	 */
	if (NIL != returning_lists)
		returning_list = (List *) list_nth(returning_lists, sdpath->subplan_index);

	/*
	 * ON CONFLICT DO UPDATE and DO NOTHING case with inference specification
	 * should have already been rejected in the optimizer, as presently there
	 * is no way to recognize an arbiter index on a foreign table.  Only DO
	 * NOTHING is supported without an inference specification.
	 */
	if (onconflict == ONCONFLICT_NOTHING)
		do_nothing = true;
	else if (onconflict != ONCONFLICT_NONE)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("ON CONFLICT DO UPDATE not supported"
						" on distributed hypertables")));

	/*
	 * Construct the SQL command string matching the fixed batch size. We also
	 * save the partially deparsed SQL command so that we can easily create
	 * one with less value parameters later for flushing a partially filled
	 * batch.
	 */
	target_attrs = get_insert_attrs(rel);

	deparse_insert_stmt(&stmt,
						rte,
						sdpath->hypertable_rti,
						rel,
						target_attrs,
						do_nothing,
						returning_list);

	/* Set suitable flush threshold value that takes into account the max number
	 * of prepared statement arguments */
	flush_threshold =
		stmt_params_validate_num_tuples(list_length(target_attrs), TUPSTORE_FLUSH_THRESHOLD);

	sql = deparsed_insert_stmt_get_sql(&stmt, flush_threshold);

	table_close(rel, NoLock);

	return list_make5(makeString((char *) sql),
					  target_attrs,
					  deparsed_insert_stmt_to_list(&stmt),
					  makeInteger(sdpath->mtpath->canSetTag),
					  makeInteger(flush_threshold));
}

static Plan *
data_node_dispatch_plan_create(PlannerInfo *root, RelOptInfo *rel, struct CustomPath *best_path,
							   List *tlist, List *clauses, List *custom_plans)
{
	DataNodeDispatchPath *sdpath = (DataNodeDispatchPath *) best_path;
	CustomScan *cscan = makeNode(CustomScan);
	Plan *subplan;

	Assert(list_length(custom_plans) == 1);

	subplan = linitial(custom_plans);
	cscan->methods = &data_node_dispatch_plan_methods;
	cscan->custom_plans = custom_plans;
	cscan->scan.scanrelid = 0;
	cscan->scan.plan.targetlist = tlist;
	cscan->custom_scan_tlist = subplan->targetlist;
	cscan->custom_private = plan_remote_insert(root, sdpath);

	return &cscan->scan.plan;
}

static CustomPathMethods data_node_dispatch_path_methods = {
	.CustomName = "DataNodeDispatchPath",
	.PlanCustomPath = data_node_dispatch_plan_create,
};

Path *
data_node_dispatch_path_create(PlannerInfo *root, ModifyTablePath *mtpath, Index hypertable_rti,
							   int subplan_index)
{
	DataNodeDispatchPath *sdpath = palloc0(sizeof(DataNodeDispatchPath));
	Path *subpath = ts_chunk_dispatch_path_create(root, mtpath, hypertable_rti, subplan_index);

	/* Copy costs, etc. from the subpath */
	memcpy(&sdpath->cpath.path, subpath, sizeof(Path));

	sdpath->cpath.path.type = T_CustomPath;
	sdpath->cpath.path.pathtype = T_CustomScan;
	sdpath->cpath.custom_paths = list_make1(subpath);
	sdpath->cpath.methods = &data_node_dispatch_path_methods;
	sdpath->mtpath = mtpath;
	sdpath->hypertable_rti = hypertable_rti;
	sdpath->subplan_index = subplan_index;

	return &sdpath->cpath.path;
}
