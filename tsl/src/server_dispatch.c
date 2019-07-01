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
#include <nodes/relation.h>
#include <executor/executor.h>
#include <utils/lsyscache.h>
#include <utils/builtins.h>
#include <utils/hsearch.h>
#include <utils/tuplestore.h>
#include <utils/memutils.h>
#include <funcapi.h>
#include <miscadmin.h>

#include <chunk_dispatch_plan.h>
#include <chunk_dispatch_state.h>
#include <chunk_insert_state.h>
#include <hypertable_cache.h>
#include <compat.h>
#include <guc.h>

#include "fdw/scan_exec.h"
#include "fdw/deparse.h"
#include "remote/utils.h"
#include "remote/dist_txn.h"
#include "remote/async.h"
#include "server_dispatch.h"
#include "remote/data_format.h"
#include "remote/tuplefactory.h"

#define TUPSTORE_MEMSIZE_KB work_mem
#define TUPSTORE_FLUSH_THRESHOLD ts_guc_max_insert_batch_size

typedef struct ServerDispatchPath
{
	CustomPath cpath;
	ModifyTablePath *mtpath;
	Index hypertable_rti; /* range table index of Hypertable */
	int subplan_index;
} ServerDispatchPath;

/*
 * ServerDispatch dispatches tuples to servers using batching. It inserts
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
 *           ------------------
 *           | ServerDispatch |    Batch and send tuples to servers.
 *           ------------------
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
 * Server dispatching uses the state machine outlined below:
 *
 * READ: read tuples from the subnode and save in per-server stores until one
 * of them reaches FLUSH_THRESHOLD and then move to FLUSH. If a NULL-tuple is
 * read before the threshold is reached, move to LAST_FLUSH. In case of
 * replication, tuples are split across a primary and a replica tuple store.
 *
 * FLUSH: flush the tuples for the server that reached
 * FLUSH_THRESHOLD.
 *
 * LAST_FLUSH: flush tuples for all servers.
 *
 * RETURNING: if there is a RETURNING clause, return the inserted tuples
 * one-by-one from all flushed servers. When no more tuples remain, move to
 * READ again or DONE if the previous state was LAST_FLUSH. Note, that in case
 * of replication, the tuples are split across a primary and a replica tuple
 * store for each server. only tuples in the primary tuple store are
 * returned. It is implicitly assumed that the primary tuples are sent on a
 * connection before the replica tuples and thus the server will also return
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
 *   stores can be flushed. Further, after flushing tuples for a server, the
 *   code immediately waits for a response instead of doing other work while
 *   waiting.
 *
 * - Currently, there is one "global" state machine for the
 *   ServerDispatchState executor node. Turning this into per-server state
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

typedef struct ServerDispatchState
{
	CustomScanState cstate;
	DispatchState prevstate; /* Previous state in state machine */
	DispatchState state;	 /* Current state in state machine */
	Relation rel;			 /* The (local) relation we're inserting into */
	Oid userid;				 /* User performing INSERT */
	bool set_processed;		 /* Indicates whether to set the number or processed tuples */
	DeparsedInsertStmt stmt; /* Partially deparsed insert statement */
	const char *sql_stmt;	/* Fully deparsed insert statement */
	TupleFactory *tupfactory;
	List *target_attrs;		 /* The attributes to send to remote servers */
	List *responses;		 /* List of responses to process in RETURNING state */
	HTAB *serverstates;		 /* Hashtable of per-server state (tuple stores) */
	MemoryContext mcxt;		 /* Memory context for per-server state */
	int64 num_tuples;		 /* Total number of tuples flushed each round */
	int64 next_tuple;		 /* Next tuple to return to the parent node when in
							  * RETURNING state */
	int replication_factor;  /* > 1 if we replicate tuples across servers */
	StmtParams *stmt_params; /* Parameters to send with statement. Format can be binary or text */
} ServerDispatchState;

/*
 * Plan metadata list indexes.
 */
enum CustomScanPrivateIndex
{
	CustomScanPrivateSql,
	CustomScanPrivateTargetAttrs,
	CustomScanPrivateDeparsedInsertStmt,
	CustomScanPrivateSetProcessed,
	CustomScanPrivateUserId,
};

#define HAS_RETURNING(sds) ((sds)->stmt.returning != NULL)

/*
 * ServerState for each server.
 *
 * Tuples destined for a server are batched in a tuple store until dispatched
 * using a "flush". A flush happens using the prepared (insert) statement,
 * which can only be used to send a "full" batch of tuples as the number of
 * rows in the statement is predefined. Thus, a flush only happens when the
 * tuple store reaches the predefined size. Once the last tuple is read from
 * the subnode, a final flush occurs. In that case, a flush is "partial" (less
 * than the predefined amount). A partial flush cannot use the prepared
 * statement, since the number of rows do not match, and therefore a one-time
 * statement is created for the last insert.
 *
 * Note that, since we use one ServerState per connection/user-mapping, we
 * could technically have multiple ServerStates per foreign server.
 */
typedef struct ServerState
{
	Oid umid; /* Must be first */
	TSConnection *conn;
	Tuplestorestate *primary_tupstore; /* Tuples this server is primary
										* for. These tuples are returned when
										* RETURNING is specified. */
	Tuplestorestate *replica_tupstore; /* Tuples this server is a replica
										* for. This tuples are NOT returned
										* when RETURNING is specified. */
	PreparedStmt *pstmt;			   /* Prepared statement to use in the FLUSH state */
	int num_tuples_sent;			   /* Number of tuples sent in the FLUSH or LAST_FLUSH states */
	int num_tuples_inserted;		   /* Number of tuples inserted (returned in result)
										* during the FLUSH or LAST_FLUSH states */
	int next_tuple;					   /* The next tuple to return in the RETURNING state */
} ServerState;

#define NUM_STORED_TUPLES(ss)                                                                      \
	(tuplestore_tuple_count((ss)->primary_tupstore) +                                              \
	 ((ss)->replica_tupstore != NULL ? tuplestore_tuple_count((ss)->replica_tupstore) : 0))

static void
server_state_init(ServerState *ss, ServerDispatchState *sds, UserMapping *um)
{
	MemoryContext old = MemoryContextSwitchTo(sds->mcxt);

	ss->umid = um->umid;
	ss->primary_tupstore = tuplestore_begin_heap(false, false, TUPSTORE_MEMSIZE_KB);
	if (sds->replication_factor > 1)
		ss->replica_tupstore = tuplestore_begin_heap(false, false, TUPSTORE_MEMSIZE_KB);
	else
		ss->replica_tupstore = NULL;
	ss->conn = remote_dist_txn_get_connection(um, REMOTE_TXN_USE_PREP_STMT);
	ss->pstmt = NULL;
	ss->next_tuple = 0;
	ss->num_tuples_sent = 0;
	ss->num_tuples_inserted = 0;
	MemoryContextSwitchTo(old);
}

static ServerState *
server_state_get_or_create(ServerDispatchState *sds, UserMapping *um)
{
	ServerState *ss;
	bool found;

	ss = hash_search(sds->serverstates, &um->umid, HASH_ENTER, &found);

	if (!found)
		server_state_init(ss, sds, um);

	return ss;
}

static void
server_state_clear_primary_store(ServerState *ss)
{
	tuplestore_clear(ss->primary_tupstore);
	Assert(tuplestore_tuple_count(ss->primary_tupstore) == 0);
	ss->next_tuple = 0;
}

static void
server_state_clear_replica_store(ServerState *ss)
{
	if (NULL == ss->replica_tupstore)
		return;

	tuplestore_clear(ss->replica_tupstore);
	Assert(tuplestore_tuple_count(ss->replica_tupstore) == 0);
}

static void
server_state_close(ServerState *ss)
{
	if (NULL != ss->pstmt)
		prepared_stmt_close(ss->pstmt);

	tuplestore_end(ss->primary_tupstore);

	if (NULL != ss->replica_tupstore)
		tuplestore_end(ss->replica_tupstore);
}

static void
server_dispatch_begin(CustomScanState *node, EState *estate, int eflags)
{
	ServerDispatchState *sds = (ServerDispatchState *) node;
	CustomScan *cscan = (CustomScan *) node->ss.ps.plan;
	ResultRelInfo *rri = estate->es_result_relation_info;
	Relation rel = rri->ri_RelationDesc;
	TupleDesc tupdesc = RelationGetDescr(rel);
	Plan *subplan = linitial(cscan->custom_plans);
	Cache *hcache = ts_hypertable_cache_pin();
	Hypertable *ht = ts_hypertable_cache_get_entry(hcache, rel->rd_id);
	PlanState *ps;
	MemoryContext mcxt =
		AllocSetContextCreate(estate->es_query_cxt, "ServerState", ALLOCSET_SMALL_SIZES);
	HASHCTL hctl = {
		.keysize = sizeof(Oid),
		.entrysize = sizeof(ServerState),
		.hcxt = mcxt,
	};
	List *available_servers = ts_hypertable_get_available_servers(ht, true);

	Assert(NULL != ht);
	Assert(NIL != available_servers);
	Assert(ht->fd.replication_factor >= 1);

	ps = ExecInitNode(subplan, estate, eflags);

	node->custom_ps = list_make1(ps);
	sds->state = SD_READ;
	sds->rel = rel;
	sds->replication_factor = ht->fd.replication_factor;
	sds->sql_stmt = strVal(list_nth(cscan->custom_private, CustomScanPrivateSql));
	sds->target_attrs = list_nth(cscan->custom_private, CustomScanPrivateTargetAttrs);
	sds->userid = intVal(list_nth(cscan->custom_private, CustomScanPrivateUserId));
	sds->set_processed = intVal(list_nth(cscan->custom_private, CustomScanPrivateSetProcessed));
	sds->mcxt = mcxt;
	sds->serverstates = hash_create("ServerDispatch tuple stores",
									list_length(available_servers),
									&hctl,
									HASH_ELEM | HASH_CONTEXT | HASH_BLOBS);

	deparsed_insert_stmt_from_list(&sds->stmt,
								   list_nth(cscan->custom_private,
											CustomScanPrivateDeparsedInsertStmt));
	sds->stmt_params =
		stmt_params_create(sds->target_attrs, false, tupdesc, TUPSTORE_FLUSH_THRESHOLD);

	if (HAS_RETURNING(sds))
		sds->tupfactory = tuplefactory_create_for_rel(rel, sds->stmt.retrieved_attrs);

	ts_cache_release(hcache);
}

/*
 * Store the result of a RETURNING clause.
 */
static void
store_returning_result(ServerDispatchState *sds, int row, TupleTableSlot *slot, PGresult *res)
{
	PG_TRY();
	{
		HeapTuple newtup = tuplefactory_make_tuple(sds->tupfactory, res, row);

		/* tuple will be deleted when it is cleared from the slot */
		ExecStoreTuple(newtup, slot, InvalidBuffer, true);
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
server_dispatch_set_state(ServerDispatchState *sds, DispatchState new_state)
{
	Assert(sds->state != new_state);

	elog(DEBUG2, "ServerDispatchState: %s -> %s", state_names[sds->state], state_names[new_state]);

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
prepare_server_insert_stmt(ServerDispatchState *sds, TSConnection *conn, int total_params)
{
	AsyncRequest *req;

	req = async_request_send_prepare(conn, sds->sql_stmt, total_params);
	Assert(req != NULL);

	return async_request_wait_prepared_statement(req);
}

/*
 * Send a batch of tuples to a server.
 *
 * All stored tuples for the given server are sent on the server's
 * connection. If in FLUSH state (i.e., sending a predefined amount of
 * tuples), use a prepared statement, or construct a custom statement if in
 * LAST_FLUSH state.
 *
 * If there's a RETURNING clause, we reset the read pointer for the store,
 * since the original tuples need to be returned along with the
 * server-returned ones. If there is no RETURNING clause, simply clear the
 * store.
 */
static AsyncRequest *
send_batch_to_server(ServerDispatchState *sds, ServerState *ss)
{
	TupleTableSlot *slot;
	AsyncRequest *req;
	const char *sql_stmt;
	int response_type = FORMAT_TEXT;

	Assert(sds->state == SD_FLUSH || sds->state == SD_LAST_FLUSH);
	Assert(NUM_STORED_TUPLES(ss) <= TUPSTORE_FLUSH_THRESHOLD);
	Assert(NUM_STORED_TUPLES(ss) > 0);

	slot = sds->cstate.ss.ss_ScanTupleSlot;

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
				ss->pstmt = prepare_server_insert_stmt(sds,
													   ss->conn,
													   stmt_params_total_values(sds->stmt_params));
			Assert(ss->num_tuples_sent == TUPSTORE_FLUSH_THRESHOLD);
			req = async_request_send_prepared_stmt_with_params(ss->pstmt,
															   sds->stmt_params,
															   response_type);
			break;
		case SD_LAST_FLUSH:
			sql_stmt = deparsed_insert_stmt_get_sql(&sds->stmt,
													stmt_params_converted_tuples(sds->stmt_params));
			Assert(sql_stmt != NULL);
			Assert(ss->num_tuples_sent < TUPSTORE_FLUSH_THRESHOLD);
			req =
				async_request_send_with_params(ss->conn, sql_stmt, sds->stmt_params, response_type);
			break;
		default:
			elog(ERROR, "unexpected server dispatch state %s", state_names[sds->state]);
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
		server_state_clear_primary_store(ss);

	/* No tuples are returned from the replica store so safe to clear */
	server_state_clear_replica_store(ss);

	/* Since we're done with current batch, reset params so they are safe to use in the next
	 * batch/server */
	stmt_params_reset(sds->stmt_params);

	return req;
}

/*
 * Check if we should flush tuples stored for a server.
 *
 * There are two cases when this happens:
 *
 * 1. State is SD_FLUSH and the flush threshold is reached.
 * 2. State is SD_LAST_FLUSH and there are tuples to send.
 */
static bool
should_flush_server(ServerDispatchState *sds, ServerState *ss)
{
	int64 num_tuples = NUM_STORED_TUPLES(ss);

	Assert(sds->state == SD_FLUSH || sds->state == SD_LAST_FLUSH);

	if (sds->state == SD_FLUSH)
	{
		if (num_tuples >= TUPSTORE_FLUSH_THRESHOLD)
			return true;
		return false;
	}

	return num_tuples > 0;
}

/*
 * Flush the tuples of servers that have a full batch.
 */
static AsyncRequestSet *
flush_servers(ServerDispatchState *sds)
{
	AsyncRequestSet *reqset = NULL;
	ServerState *ss;
	HASH_SEQ_STATUS hseq;

	Assert(sds->state == SD_FLUSH || sds->state == SD_LAST_FLUSH);

	hash_seq_init(&hseq, sds->serverstates);

	for (ss = hash_seq_search(&hseq); ss != NULL; ss = hash_seq_search(&hseq))
	{
		if (should_flush_server(sds, ss))
		{
			AsyncRequest *req = send_batch_to_server(sds, ss);

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
 * Wait for responses from servers after INSERT.
 *
 * In case of RETURNING, return a list of responses, otherwise NIL.
 */
static List *
await_all_responses(ServerDispatchState *sds, AsyncRequestSet *reqset)
{
	AsyncResponseResult *rsp;
	List *results = NIL;

	sds->next_tuple = 0;

	while ((rsp = async_request_set_wait_any_result(reqset)))
	{
		ServerState *ss = async_response_result_get_user_data(rsp);
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
 * Read until there's a NULL tuple or we've filled a server's batch. Ideally,
 * we'd continue to read more tuples to fill other servers' batches, but since
 * the next tuple might be for the same server that has the full batch, we
 * risk overfilling. This could be mitigated by using two tuple stores per
 * server (current and next batch) and alternate between them. But that also
 * increases memory requirements and complicates the code, so that's left as a
 * future optimization.
 *
 * Return the number of tuples read.
 */
static int64
handle_read(ServerDispatchState *sds)
{
	PlanState *substate = linitial(sds->cstate.custom_ps);
	EState *estate = sds->cstate.ss.ps.state;
	ResultRelInfo *rri_saved = estate->es_result_relation_info;
	int64 num_tuples_read = 0;

	Assert(sds->state == SD_READ);

	/* Read tuples from the subnode until flush */
	while (sds->state == SD_READ)
	{
		TupleTableSlot *slot = ExecProcNode(substate);

		if (TupIsNull(slot))
			server_dispatch_set_state(sds, SD_LAST_FLUSH);
		else
		{
			/* The previous node should have routed the tuple to the right
			 * chunk and set the corresponding result relation. The FdwState
			 * should also point to the chunk's insert state. */
			ResultRelInfo *rri = estate->es_result_relation_info;
			ChunkInsertState *cis = rri->ri_FdwState;
			TriggerDesc *trigdesc = rri->ri_TrigDesc;
			ListCell *lc;
			bool primary_server = true;

			Assert(NULL != cis);

			/* While we could potentially support triggers on frontend nodes,
			 * the triggers should exists also on the remote node and will be
			 * executed there. For new, the safest bet is to avoid triggers on
			 * the frontend. */
			if (trigdesc && (trigdesc->trig_insert_after_row || trigdesc->trig_insert_before_row))
				elog(ERROR, "cannot insert into remote chunk with row triggers");

			/* Total count */
			num_tuples_read++;

			foreach (lc, cis->usermappings)
			{
				UserMapping *um = lfirst(lc);
				ServerState *ss = server_state_get_or_create(sds, um);

				/* This will store one copy of the tuple per server, which is
				 * a bit inefficient. Note that we put the tuple in the
				 * primary store for the first server, but the replica store
				 * for all other servers. This is to be able to know which
				 * tuples to return in a RETURNING statement. */
				if (primary_server)
					tuplestore_puttupleslot(ss->primary_tupstore, slot);
				else
					tuplestore_puttupleslot(ss->replica_tupstore, slot);

				/* Once one server has reached the batch size, we stop
				 * reading. */
				if (sds->state != SD_FLUSH && NUM_STORED_TUPLES(ss) >= TUPSTORE_FLUSH_THRESHOLD)
					server_dispatch_set_state(sds, SD_FLUSH);

				primary_server = false;
			}
		}
	}

	estate->es_result_relation_info = rri_saved;

	return num_tuples_read;
}

/*
 * Flush all servers and move to the RETURNING state.
 *
 * Note that future optimizations could do this more asynchronously by doing
 * other work until responses are available (e.g., one could start to fill the
 * next batch while waiting for a response). However, the async API currently
 * doesn't expose a way to check for a response without blocking and
 * interleaving different tasks would also complicate the state machine.
 */
static void
handle_flush(ServerDispatchState *sds)
{
	AsyncRequestSet *reqset;

	Assert(sds->state == SD_FLUSH || sds->state == SD_LAST_FLUSH);

	reqset = flush_servers(sds);

	if (NULL != reqset)
	{
		sds->responses = await_all_responses(sds, reqset);
		pfree(reqset);
	}

	server_dispatch_set_state(sds, SD_RETURNING);
}

/*
 * Get a tuple when there's a RETURNING clause.
 */
static TupleTableSlot *
get_returning_tuple(ServerDispatchState *sds)
{
	EState *estate = sds->cstate.ss.ps.state;
	ResultRelInfo *rri = estate->es_result_relation_info;
	TupleTableSlot *res_slot = estate->es_trig_tuple_slot;
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
		TupleDesc res_tupdesc = RelationGetDescr(rri->ri_RelationDesc);

		if (res_slot->tts_tupleDescriptor != res_tupdesc)
			ExecSetSlotDescriptor(res_slot, res_tupdesc);

		while (NIL != sds->responses)
		{
			AsyncResponseResult *rsp = linitial(sds->responses);
			ServerState *ss = async_response_result_get_user_data(rsp);
			PGresult *res = async_response_result_get_pg_result(rsp);
			int64 num_tuples_to_return = tuplestore_tuple_count(ss->primary_tupstore);
			bool last_tuple;
			bool got_tuple = false;

			if (num_tuples_to_return > 0)
			{
				last_tuple = (ss->next_tuple + 1) == num_tuples_to_return;

				Assert(ss->next_tuple < ss->num_tuples_inserted);
				Assert(ss->next_tuple < num_tuples_to_return);

				store_returning_result(sds, ss->next_tuple, slot, res);

				/* Get the next tuple from the store. If it is the last tuple, we
				 * need to make a copy since we will clear the store before
				 * returning. */
				got_tuple = tuplestore_gettupleslot(ss->primary_tupstore,
													true /* forward */,
													last_tuple /* copy */,
													res_slot);

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
				server_state_clear_primary_store(ss);
			}

			if (got_tuple)
				break;
		}
	}

	econtext->ecxt_scantuple = res_slot;

	return slot;
}

/*
 * Get the next tuple slot to return when there's a RETURNING
 * clause. Otherwise, return an empty slot.
 */
static TupleTableSlot *
handle_returning(ServerDispatchState *sds)
{
	EState *estate = sds->cstate.ss.ps.state;
	ResultRelInfo *rri = estate->es_result_relation_info;
	TupleTableSlot *slot = sds->cstate.ss.ss_ScanTupleSlot;
	bool done = false;

	Assert(sds->state == SD_RETURNING);

	/* No returning projection, which means we are done */
	if (NULL == rri->ri_projectReturning)
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
			server_dispatch_set_state(sds, SD_DONE);
		else
			server_dispatch_set_state(sds, SD_READ);
	}
	else
	{
		slot = get_returning_tuple(sds);
		Assert(!TupIsNull(slot));
		sds->next_tuple++;

		if (sds->set_processed)
			estate->es_processed++;
	}

	return slot;
}

/*
 * Execute the remote INSERT.
 *
 * This is called every time the parent asks for a new tuple. Read the child
 * scan node and buffer until there's a full batch, then flush by sending to
 * server(s). If there's a returning statement, we return the flushed tuples
 * one-by-one, or continue reading more tuples from the child until there's a
 * NULL tuple.
 */
static TupleTableSlot *
server_dispatch_exec(CustomScanState *node)
{
	ServerDispatchState *sds = (ServerDispatchState *) node;
	ExprContext *econtext = node->ss.ps.ps_ExprContext;
	MemoryContext oldcontext;
	TupleTableSlot *slot = NULL;
	bool done = false;

	/* Initially, the result relation should always match the hypertable.  */
	Assert(node->ss.ps.state->es_result_relation_info->ri_RelationDesc->rd_id == sds->rel->rd_id);

	oldcontext = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);

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

	/* Tuple routing in the ChunkDispatchState subnode sets the result
	 * relation to a chunk when routing, but the read handler should have
	 * ensured the result relation is reset. */
	Assert(node->ss.ps.state->es_result_relation_info->ri_RelationDesc->rd_id == sds->rel->rd_id);
	Assert(node->ss.ps.state->es_result_relation_info->ri_usesFdwDirectModify);
	MemoryContextSwitchTo(oldcontext);

	return slot;
}

static void
server_dispatch_rescan(CustomScanState *node)
{
	/* Cannot rescan and start from the beginning since we might already have
	 * sent data to remote nodes */
	elog(ERROR, "cannot restart inserts to remote nodes");
}

static void
server_dispatch_end(CustomScanState *node)
{
	ServerDispatchState *sds = (ServerDispatchState *) node;
	ServerState *ss;
	HASH_SEQ_STATUS hseq;

	hash_seq_init(&hseq, sds->serverstates);

	for (ss = hash_seq_search(&hseq); ss != NULL; ss = hash_seq_search(&hseq))
		server_state_close(ss);

	hash_destroy(sds->serverstates);
	ExecEndNode(linitial(node->custom_ps));
}

static void
server_dispatch_explain(CustomScanState *node, List *ancestors, ExplainState *es)
{
	ServerDispatchState *sds = (ServerDispatchState *) node;

	ExplainPropertyIntegerCompat("Batch size", NULL, TUPSTORE_FLUSH_THRESHOLD, es);

	/*
	 * Add remote query, when VERBOSE option is specified.
	 */
	if (es->verbose)
	{
		const char *explain_sql =
			deparsed_insert_stmt_get_sql_explain(&sds->stmt, TUPSTORE_FLUSH_THRESHOLD);

		ExplainPropertyText("Remote SQL", explain_sql, es);
	}
}

static CustomExecMethods server_dispatch_state_methods = {
	.CustomName = "ServerDispatchState",
	.BeginCustomScan = server_dispatch_begin,
	.EndCustomScan = server_dispatch_end,
	.ExecCustomScan = server_dispatch_exec,
	.ReScanCustomScan = server_dispatch_rescan,
	.ExplainCustomScan = server_dispatch_explain,
};

/* Only allocate the custom scan state. Initialize in the begin handler. */
static Node *
server_dispatch_state_create(CustomScan *cscan)
{
	ServerDispatchState *sds;

	sds = (ServerDispatchState *) newNode(sizeof(ServerDispatchState), T_CustomScanState);
	sds->cstate.methods = &server_dispatch_state_methods;

	return (Node *) sds;
}

static CustomScanMethods server_dispatch_plan_methods = {
	.CustomName = "ServerDispatch",
	.CreateCustomScanState = server_dispatch_state_create,
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

		if (!attr->attisdropped)
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
plan_remote_insert(PlannerInfo *root, ServerDispatchPath *sdpath)
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
	Oid userid;

	/*
	 * Core code already has some lock on each rel being planned, so we can
	 * use NoLock here.
	 */
	rel = heap_open(rte->relid, NoLock);

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
		elog(ERROR, "unexpected ON CONFLICT specification: %d", (int) onconflict);

	userid = OidIsValid(rte->checkAsUser) ? rte->checkAsUser : GetUserId();

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

	sql = deparsed_insert_stmt_get_sql(&stmt, TUPSTORE_FLUSH_THRESHOLD);

	heap_close(rel, NoLock);

	return list_make5(makeString((char *) sql),
					  target_attrs,
					  deparsed_insert_stmt_to_list(&stmt),
					  makeInteger(sdpath->mtpath->canSetTag),
					  makeInteger(userid));
}

static Plan *
server_dispatch_plan_create(PlannerInfo *root, RelOptInfo *rel, struct CustomPath *best_path,
							List *tlist, List *clauses, List *custom_plans)
{
	ServerDispatchPath *sdpath = (ServerDispatchPath *) best_path;
	CustomScan *cscan = makeNode(CustomScan);

	Assert(list_length(custom_plans) == 1);

	cscan->methods = &server_dispatch_plan_methods;
	cscan->custom_plans = custom_plans;
	cscan->scan.scanrelid = 0;
	cscan->scan.plan.targetlist = tlist;
	cscan->custom_scan_tlist = tlist;
	cscan->custom_private = plan_remote_insert(root, sdpath);

	return &cscan->scan.plan;
}

static CustomPathMethods server_dispatch_path_methods = {
	.CustomName = "ServerDispatchPath",
	.PlanCustomPath = server_dispatch_plan_create,
};

Path *
server_dispatch_path_create(PlannerInfo *root, ModifyTablePath *mtpath, Index hypertable_rti,
							int subplan_index)
{
	ServerDispatchPath *sdpath = palloc0(sizeof(ServerDispatchPath));
	Path *subpath = ts_chunk_dispatch_path_create(root, mtpath, hypertable_rti, subplan_index);

	/* Copy costs, etc. from the subpath */
	memcpy(&sdpath->cpath.path, subpath, sizeof(Path));

	sdpath->cpath.path.type = T_CustomPath;
	sdpath->cpath.path.pathtype = T_CustomScan;
	sdpath->cpath.custom_paths = list_make1(subpath);
	sdpath->cpath.methods = &server_dispatch_path_methods;
	sdpath->mtpath = mtpath;
	sdpath->hypertable_rti = hypertable_rti;
	sdpath->subplan_index = subplan_index;

	return &sdpath->cpath.path;
}
