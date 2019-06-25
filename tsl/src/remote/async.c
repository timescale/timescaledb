/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <postgres.h>
#include <nodes/pg_list.h>
#include <libpq-fe.h>
#include <storage/latch.h>
#include <miscadmin.h>
#include <pgstat.h>
#include <fmgr.h>
#include <utils/lsyscache.h>
#include <catalog/pg_type.h>
#include <nodes/relation.h>
#include <access/tuptoaster.h>

#include "async.h"
#include "connection.h"
#include "utils.h"

#define MAX_ASYNC_TIMEOUT_MS 60000

/**
 * State machine for AsyncRequest:
 *
 *   +-------------+           +--------------+        +--------------+
 *   |             |           |              |        |              |
 *   |  DEFERRED   +---------->+  EXECUTING   +------->+   COMPLETED  |
 *   |             |           |              |        |              |
 *   +-------------+           +--------------+        +--------------+
 *
 **/

typedef enum AsyncRequestState
{
	DEFERRED,
	EXECUTING,
	COMPLETED,
} AsyncRequestState;

typedef struct AsyncRequest
{
	const char *sql;
	TSConnection *conn;
	AsyncRequestState state;
	const char *stmt_name;
	int prep_stmt_params;
	void *user_data; /* custom data saved with the request */
	StmtParams *params;
	int res_format; /* text or binary */
} AsyncRequest;

typedef struct PreparedStmt
{
	const char *sql;
	TSConnection *conn;
	const char *stmt_name;
	int n_params;
} PreparedStmt;

/* It is often useful to get the request along with the result in the response */

typedef struct AsyncResponse
{
	AsyncResponseType type;
} AsyncResponse;

typedef struct AsyncResponseResult
{
	AsyncResponse base;
	PGresult *result;
	AsyncRequest *request;
} AsyncResponseResult;

typedef struct AsyncResponseCommunicationError
{
	AsyncResponse base;
	AsyncRequest *request;
} AsyncResponseCommunicationError;

typedef struct AsyncRequestSet
{
	List *requests;
} AsyncRequestSet;

static AsyncRequest *
async_request_create(TSConnection *conn, const char *sql, const char *stmt_name,
					 int prep_stmt_params, StmtParams *stmt_params, int res_format)
{
	AsyncRequest *req;
	if (conn == NULL)
		elog(ERROR, "can't create AsyncRequest with NULL connection");
	req = palloc(sizeof(AsyncRequest));
	*req = (AsyncRequest){
		.conn = conn,
		.state = DEFERRED,
		.sql = pstrdup(sql),
		.stmt_name = stmt_name,
		.params = stmt_params,
		.prep_stmt_params = prep_stmt_params,
		.res_format = res_format,
	};
	return req;
}

static void
async_request_set_state(AsyncRequest *req, AsyncRequestState new_state)
{
	if (req->state != DEFERRED)
		Assert(req->state != new_state);

#ifdef USE_ASSERT_CHECKING
	switch (new_state)
	{
		case DEFERRED:
			/* initial state */
			Assert(req->state == DEFERRED);
			break;
		case EXECUTING:
			Assert(req->state == DEFERRED);
			break;
		case COMPLETED:
			Assert(req->state == EXECUTING);
	}
#endif
	req->state = new_state;
}

/* Send a request. In case there is an ongoing request for the connection,
   we will not send the request but set its status to DEFERRED.
   Getting a response from DEFERRED AsyncRequest will try sending it if
   the connection is not in use.

   Note that we can only send one sql statement per request.
   This is because we use `PQsendQueryParams` which uses the extended query protocol
   instead of the simple one. The extended protocol does not support multiple
   statements. In the future we can use a `PQsendQuery` variant for queries without parameters,
   which can support multiple statements because it uses the simple protocol. But this is
   an optimization for another time.
*/
static AsyncRequest *
async_request_send_internal(AsyncRequest *req, int elevel)
{
	if (req->state != DEFERRED)
		elog(ERROR, "can't send async request in state \"%d\"", req->state);
	if (remote_connection_is_processing(req->conn))
		return req;
	if (req->stmt_name)
	{
		/*
		 * We intentionally do not specify parameter types here, but leave the
		 * data node to derive them by default.  This avoids possible problems
		 * with the data node using different type OIDs than we do.  All of
		 * the prepared statements we use in this module are simple enough that
		 * the data node will make the right choices.
		 */
		if (!PQsendPrepare(remote_connection_get_pg_conn(req->conn),
						   req->stmt_name,
						   req->sql,
						   req->prep_stmt_params,
						   NULL))
		{
			/*
			 * null is fine to pass down as the res, the connection error message
			 * will get through
			 */
			remote_connection_report_error(elevel, NULL, req->conn, false, req->sql);
			return NULL;
		}
	}
	else
	{
		if (0 == PQsendQueryParams(remote_connection_get_pg_conn(req->conn),
								   req->sql,
								   stmt_params_total_values(req->params),
								   /* param types - see note above */ NULL,
								   stmt_params_values(req->params),
								   stmt_params_lengths(req->params),
								   stmt_params_formats(req->params),
								   req->res_format))
		{
			/*
			 * null is fine to pass down as the res, the connection error message
			 * will get through
			 */
			remote_connection_report_error(elevel, NULL, req->conn, false, req->sql);
			return NULL;
		}
	}
	async_request_set_state(req, EXECUTING);
	remote_connection_set_processing(req->conn, true);
	return req;
}

AsyncRequest *
async_request_send_with_stmt_params_elevel_res_format(TSConnection *conn, const char *sql_statement,
													  StmtParams *params, int elevel,
													  int res_format)
{
	AsyncRequest *req = async_request_create(conn, sql_statement, NULL, 0, params, res_format);
	req = async_request_send_internal(req, elevel);
	return req;
}

AsyncRequest *
async_request_send_prepare(TSConnection *conn, const char *sql, int n_params)
{
	AsyncRequest *req;
	size_t stmt_name_len = NAMEDATALEN;
	char *stmt_name = palloc(sizeof(char) * stmt_name_len);
	int written;

	/* Construct name we'll use for the prepared statement. */
	written =
		snprintf(stmt_name, stmt_name_len, "ts_prep_%u", remote_connection_get_prep_stmt_number());

	if (written < 0 || written >= stmt_name_len)
		elog(ERROR, "cannot create prepared statement name");

	req = async_request_create(conn, sql, stmt_name, n_params, NULL, FORMAT_TEXT);
	req = async_request_send_internal(req, ERROR);

	return req;
}

extern AsyncRequest *
async_request_send_prepared_stmt(PreparedStmt *stmt, const char *const *param_values)
{
	AsyncRequest *req =
		async_request_create(stmt->conn,
							 stmt->sql,
							 NULL,
							 stmt->n_params,
							 stmt_params_create_from_values((const char **) param_values,
															stmt->n_params),
							 FORMAT_TEXT);
	return async_request_send_internal(req, ERROR);
}

AsyncRequest *
async_request_send_prepared_stmt_with_params(PreparedStmt *stmt, StmtParams *params, int res_format)
{
	AsyncRequest *req =
		async_request_create(stmt->conn, stmt->sql, NULL, stmt->n_params, params, res_format);
	return async_request_send_internal(req, ERROR);
}

/* Set user data. Often it is useful to attach data with a request so
   that it can later be fetched from the response. */
void
async_request_attach_user_data(AsyncRequest *req, void *user_data)
{
	req->user_data = user_data;
}

static AsyncResponseResult *
async_response_result_create(AsyncRequest *req, PGresult *res)
{
	AsyncResponseResult *ares = palloc(sizeof(AsyncResponseResult));

	*ares = (AsyncResponseResult){
		.base = { .type = RESPONSE_RESULT },
		.request = req,
		.result = res,
	};

	return ares;
}

static AsyncResponseCommunicationError *
async_response_communication_error_create(AsyncRequest *req)
{
	AsyncResponseCommunicationError *ares = palloc(sizeof(AsyncResponseCommunicationError));

	*ares = (AsyncResponseCommunicationError){
		.base = { .type = RESPONSE_COMMUNICATION_ERROR },
		.request = req,
	};

	return ares;
}

static AsyncResponse *
async_response_timeout_create()
{
	AsyncResponse *ares = palloc(sizeof(AsyncResponse));

	*ares = (AsyncResponse){
		.type = RESPONSE_TIMEOUT,
	};

	return ares;
}

void
async_response_result_close(AsyncResponseResult *res)
{
	PQclear(res->result);
	pfree(res);
}

/* Closes the async response. Note that `async_response_report_error` does this automatically. */
void
async_response_close(AsyncResponse *res)
{
	switch (res->type)
	{
		case RESPONSE_RESULT:
			async_response_result_close((AsyncResponseResult *) res);
			break;
		default:
			pfree(res);
			break;
	}
}

AsyncResponseType
async_response_get_type(AsyncResponse *res)
{
	return res->type;
}

/* get the user data attached to the corresponding request */
void *
async_response_result_get_user_data(AsyncResponseResult *res)
{
	return res->request->user_data;
}

PGresult *
async_response_result_get_pg_result(AsyncResponseResult *res)
{
	return res->result;
}

AsyncRequest *
async_response_result_get_request(AsyncResponseResult *res)
{
	return res->request;
}
void
async_response_report_error(AsyncResponse *res, int elevel)
{
	PG_TRY();
	{
		switch (res->type)
		{
			case RESPONSE_RESULT:
			{
				AsyncResponseResult *response_result = (AsyncResponseResult *) res;

				remote_connection_report_error(elevel,
											   response_result->result,
											   response_result->request->conn,
											   false,
											   response_result->request->sql);
			}
			break;
			case RESPONSE_COMMUNICATION_ERROR:
			{
				AsyncResponseCommunicationError *response_ce =
					(AsyncResponseCommunicationError *) res;

				remote_connection_report_error(elevel,
											   NULL,
											   response_ce->request->conn,
											   false,
											   response_ce->request->sql);
			}
			break;
			case RESPONSE_TIMEOUT:
				elog(elevel, "async operation timed out");
		}
	}
	PG_CATCH();
	{
		async_response_close(res);
		PG_RE_THROW();
	}
	PG_END_TRY();
	async_response_close(res);
}

/* This is a convenience function to wait for a single result from a request.
 * This function requires that the request is for a single sql-statement.
 */
AsyncResponseResult *
async_request_wait_any_result(AsyncRequest *req)
{
	AsyncRequestSet set = {};
	AsyncResponseResult *result;

	async_request_set_add(&set, req);

	result = async_request_set_wait_any_result(&set);

	Assert(result != NULL);

	/* Make sure to drain the connection */
	if (NULL != async_request_set_wait_any_result(&set))
		elog(ERROR, "request must be for one sql statement");

	return result;
}

AsyncResponseResult *
async_request_wait_ok_result(AsyncRequest *req)
{
	AsyncResponseResult *res = async_request_wait_any_result(req);

	if (PQresultStatus(res->result) != PGRES_COMMAND_OK &&
		PQresultStatus(res->result) != PGRES_TUPLES_OK)
	{
		async_response_report_error(&res->base, ERROR);
		Assert(false);
	}

	return res;
}

void
async_request_wait_ok_command(AsyncRequest *req)
{
	AsyncResponseResult *res = async_request_wait_any_result(req);

	if (PQresultStatus(res->result) != PGRES_COMMAND_OK)
	{
		async_response_report_error(&res->base, ERROR);
		Assert(false);
	}

	async_response_result_close(res);
}

PreparedStmt *
async_request_wait_prepared_statement(AsyncRequest *request)
{
	AsyncResponseResult *result;
	PreparedStmt *prep;

	Assert(request->stmt_name != NULL);

	result = async_request_wait_ok_result(request);
	prep = async_response_result_generate_prepared_stmt(result);
	async_response_result_close(result);

	return prep;
}

AsyncRequestSet *
async_request_set_create()
{
	return palloc0(sizeof(AsyncRequestSet));
}

void
async_request_set_add(AsyncRequestSet *set, AsyncRequest *req)
{
	set->requests = list_append_unique(set->requests, req);
}

static AsyncResponse *
get_single_response_nonblocking(AsyncRequestSet *set)
{
	ListCell *lc;

	foreach (lc, set->requests)
	{
		AsyncRequest *req = lfirst(lc);
		PGconn *pg_conn = remote_connection_get_pg_conn(req->conn);

		if (remote_connection_is_processing(req->conn) && req->state == DEFERRED)
			elog(ERROR, "can't get a response because previous request hasn't completed");

		if (req->state == DEFERRED)
			async_request_send_internal(req, ERROR);

		if (req->state != EXECUTING)
			elog(ERROR, "can't get a response for a request that's not currently executing");

		if (!PQisBusy(pg_conn))
		{
			PGresult *res = PQgetResult(pg_conn);

			if (NULL == res)
			{
				/*
				 * NULL return means query is complete
				 */
				set->requests = list_delete_ptr(set->requests, req);
				remote_connection_set_processing(req->conn, false);
				async_request_set_state(req, COMPLETED);
				/* set changed so rerun function */
				return get_single_response_nonblocking(set);
			}
			return &async_response_result_create(req, res)->base;
		}
	}
	return NULL;
}

/*
 * wait_to_consume_data waits until data is recieved and put into buffers
 * so that it can be recieved without blocking by `get_single_response_nonblocking`
 * or similar.
 *
 * Returns NULL on success or an "error" AsyncResponse
 */
static AsyncResponse *
wait_to_consume_data(AsyncRequestSet *set, int elevel, TimestampTz end_time)
{
	/*
	 * Looks like there is no good way to modify a WaitEventSet so we have to
	 * make a new one, otherwise we can't turn off wait events
	 */
	WaitEventSet *we_set;
	ListCell *lc;
	int rc;
	WaitEvent event;
	uint32 wait_event_info = PG_WAIT_EXTENSION;
	AsyncRequest *wait_req;
	AsyncResponse *result;
	long timeout_ms = -1L;

	Assert(list_length(set->requests) > 0);

	if (end_time != TS_NO_TIMEOUT)
	{
		TimestampTz now = GetCurrentTimestamp();
		long secs;
		int microsecs;

		if (now >= end_time)
			return async_response_timeout_create();

		TimestampDifference(now, end_time, &secs, &microsecs);

		/* To protect against clock skew, limit sleep to one minute. */
		timeout_ms = Min(MAX_ASYNC_TIMEOUT_MS, secs * 1000 + (microsecs / 1000));
	}

	we_set = CreateWaitEventSet(CurrentMemoryContext, list_length(set->requests) + 1);

	/* TODO optimize single-member set with WaitLatchOrSocket? */

	/* always wait for my latch */
	AddWaitEventToSet(we_set, WL_LATCH_SET, PGINVALID_SOCKET, (Latch *) MyLatch, NULL);

	foreach (lc, set->requests)
	{
		AsyncRequest *req = lfirst(lc);

		AddWaitEventToSet(we_set,
						  WL_SOCKET_READABLE,
						  PQsocket(remote_connection_get_pg_conn(req->conn)),
						  NULL,
						  req);
	}

	while (true)
	{
		wait_req = NULL;
		rc = WaitEventSetWait(we_set, timeout_ms, &event, 1, wait_event_info);

		if (rc == 0)
		{
			/* often an error, not always */
			elog(elevel, "unexpected timeout");
			result = async_response_timeout_create();
			break;
		}

		CHECK_FOR_INTERRUPTS();

		if (event.events & WL_LATCH_SET)
			ResetLatch(MyLatch);
		else if (event.events & WL_SOCKET_READABLE)
		{
			wait_req = event.user_data;
			Assert(wait_req != NULL);

			if (0 == PQconsumeInput(remote_connection_get_pg_conn(wait_req->conn)))
			{
				/* This is often an error but not always */
				remote_connection_report_error(elevel, NULL, wait_req->conn, false, wait_req->sql);

				/* remove connection from set */
				set->requests = list_delete_ptr(set->requests, wait_req);
				result = &async_response_communication_error_create(wait_req)->base;
				break;
			}
			result = NULL;
			break;
		}
		else
			elog(FATAL, "unexpected event");
	}

	FreeWaitEventSet(we_set);
	return result;
}

/* Return NULL when nothing more to do in set */
AsyncResponse *
async_request_set_wait_any_response_deadline(AsyncRequestSet *set, int elevel, TimestampTz endtime)
{
	AsyncResponse *response;

	while (true)
	{
		response = get_single_response_nonblocking(set);
		if (response != NULL)
			return response;

		if (list_length(set->requests) == 0)
			/* nothing to wait on anymore */
			return NULL;

		response = wait_to_consume_data(set, elevel, endtime);
		if (response != NULL)
			return response;
	}
}

AsyncResponseResult *
async_request_set_wait_any_result(AsyncRequestSet *set)
{
	AsyncResponse *res = async_request_set_wait_any_response(set, ERROR);

	if (res == NULL)
		return NULL;

	if (RESPONSE_RESULT != res->type)
	{
		async_response_report_error(res, ERROR);
		Assert(false);
	}

	return (AsyncResponseResult *) res;
}

AsyncResponseResult *
async_request_set_wait_ok_result(AsyncRequestSet *set)
{
	AsyncResponseResult *response_result = async_request_set_wait_any_result(set);

	if (response_result == NULL)
		return NULL;

	if (PQresultStatus(response_result->result) != PGRES_TUPLES_OK &&
		PQresultStatus(response_result->result) != PGRES_COMMAND_OK)
	{
		async_response_report_error(&response_result->base, ERROR);
		Assert(false);
	}

	return response_result;
}

void
async_request_set_wait_all_ok_commands(AsyncRequestSet *set)
{
	AsyncResponseResult *ar;

	while ((ar = async_request_set_wait_ok_result(set)))
	{
		if (PQresultStatus(async_response_result_get_pg_result(ar)) != PGRES_COMMAND_OK)
			elog(ERROR, "unexpected tuple recieved while expecting a command");
		async_response_result_close(ar);
	}
}

void
prepared_stmt_close(PreparedStmt *stmt)
{
	char sql[64] = { '\0' };
	int ret;

	ret = snprintf(sql, sizeof(sql), "DEALLOCATE %s", stmt->stmt_name);

	if (ret < 0 || ret >= sizeof(sql))
		elog(ERROR, "could not create deallocate statement");

	async_request_wait_ok_command(async_request_send(stmt->conn, sql));
}

/* Request must have been generated by async_request_send_prepare() */
PreparedStmt *
async_response_result_generate_prepared_stmt(AsyncResponseResult *result)
{
	PreparedStmt *prep = palloc(sizeof(PreparedStmt));

	if (PQresultStatus(result->result) != PGRES_COMMAND_OK)
		async_response_report_error(&result->base, ERROR);

	*prep = (PreparedStmt){
		.conn = result->request->conn,
		.sql = result->request->sql,
		.stmt_name = result->request->stmt_name,
		.n_params = result->request->prep_stmt_params,
	};

	return prep;
}
