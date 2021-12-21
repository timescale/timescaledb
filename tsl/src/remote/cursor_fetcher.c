/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
/*
 * This file contains source code that was copied and/or modified from
 * the PostgreSQL database, which is licensed under the open-source
 * PostgreSQL License. Please see the NOTICE at the top level
 * directory for a copy of the PostgreSQL License.
 */
#include <postgres.h>
#include <lib/stringinfo.h>
#include <utils/rel.h>
#include <utils/guc.h>

#include "utils.h"
#include "async.h"
#include "stmt_params.h"
#include "tuplefactory.h"
#include "cursor_fetcher.h"
#include "data_fetcher.h"

/*
 * Cursor for fetching data from a data node.
 *
 * The cursor fetcher splits the query result into multiple fetches, which
 * allows multiplexing on-going sub-queries on the same connection without
 * having to fetch the full result for each sub-query in one go (thus not
 * over-running memory).
 *
 * When a query consists of multiple subqueries that fetch data from the same
 * data nodes, and the sub-queries are joined using, e.g., a nested loop, then
 * a CURSOR is necessary to run the two sub-queries over the same connection.
 *
 * The downside of using a CURSOR, however, is that the plan on the remote
 * node cannot execute in parallel.
 *
 * https://www.postgresql.org/docs/current/when-can-parallel-query-be-used.html
 */
typedef struct CursorFetcher
{
	DataFetcher state;
	unsigned int id;
	char fetch_stmt[64];	  /* cursor fetch statement */
	AsyncRequest *create_req; /* a request to create cursor */
} CursorFetcher;

static void cursor_fetcher_send_fetch_request(DataFetcher *df);
static int cursor_fetcher_fetch_data(DataFetcher *df);
static void cursor_fetcher_set_fetch_size(DataFetcher *df, int fetch_size);
static void cursor_fetcher_set_tuple_memcontext(DataFetcher *df, MemoryContext mctx);
static HeapTuple cursor_fetcher_get_next_tuple(DataFetcher *df);
static HeapTuple cursor_fetcher_get_tuple(DataFetcher *df, int row);
static void cursor_fetcher_rewind(DataFetcher *df);
static void cursor_fetcher_close(DataFetcher *df);

static DataFetcherFuncs funcs = {
	.send_fetch_request = cursor_fetcher_send_fetch_request,
	.fetch_data = cursor_fetcher_fetch_data,
	.set_fetch_size = cursor_fetcher_set_fetch_size,
	.set_tuple_mctx = cursor_fetcher_set_tuple_memcontext,
	.get_next_tuple = cursor_fetcher_get_next_tuple,
	.get_tuple = cursor_fetcher_get_tuple,
	.rewind = cursor_fetcher_rewind,
	.close = cursor_fetcher_close,
};

static void
cursor_create_req(CursorFetcher *cursor)
{
	AsyncRequest *volatile req = NULL;
	StringInfoData buf;
	MemoryContext oldcontext;

	initStringInfo(&buf);
	appendStringInfo(&buf, "DECLARE c%u CURSOR FOR\n%s", cursor->id, cursor->state.stmt);
	oldcontext = MemoryContextSwitchTo(cursor->state.req_mctx);

	PG_TRY();
	{
		if (NULL == cursor->state.stmt_params)
			req = async_request_send(cursor->state.conn, buf.data);
		else
			req = async_request_send_with_params(cursor->state.conn,
												 buf.data,
												 cursor->state.stmt_params,
												 FORMAT_TEXT);

		Assert(NULL != req);

		cursor->create_req = req;
		pfree(buf.data);
	}
	PG_CATCH();
	{
		if (NULL != req)
			pfree(req);

		PG_RE_THROW();
	}
	PG_END_TRY();
	MemoryContextSwitchTo(oldcontext);
}

/*
 * Complete ongoing cursor create request if needed and return cursor.
 * If cursor is in async mode then we will dispatch a request to fetch data.
 */
static void
cursor_fetcher_wait_until_open(DataFetcher *df)
{
	CursorFetcher *cursor = cast_fetcher(CursorFetcher, df);

	if (cursor->state.open)
	{
		Assert(cursor->create_req == NULL);
		/* nothing to do */
		return;
	}

	if (cursor->create_req == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_CURSOR_STATE),
				 errmsg("invalid cursor state"),
				 errdetail("Cannot wait on unsent cursor request.")));

	async_request_wait_ok_command(cursor->create_req);
	cursor->state.open = true;
	pfree(cursor->create_req);
	cursor->create_req = NULL;
}

static CursorFetcher *
remote_cursor_init_with_params(TSConnection *conn, Relation rel, TupleDesc tupdesc, ScanState *ss,
							   List *retrieved_attrs, const char *stmt, StmtParams *params)
{
	CursorFetcher *cursor = palloc0(sizeof(CursorFetcher));

	data_fetcher_init(&cursor->state, conn, stmt, params, rel, ss, retrieved_attrs);
	cursor->state.type = CursorFetcherType;
	/* Assign a unique ID for my cursor */
	cursor->id = remote_connection_get_cursor_number();
	cursor->create_req = NULL;
	/* send a request to DECLARE cursor  */
	cursor_create_req(cursor);
	cursor->state.funcs = &funcs;
	cursor_fetcher_wait_until_open(&cursor->state);

	return cursor;
}

DataFetcher *
cursor_fetcher_create_for_rel(TSConnection *conn, Relation rel, List *retrieved_attrs,
							  const char *stmt, StmtParams *params)
{
	CursorFetcher *cursor;

	Assert(NULL != rel);

	cursor = remote_cursor_init_with_params(conn,
											rel,
											RelationGetDescr(rel),
											NULL,
											retrieved_attrs,
											stmt,
											params);
	return &cursor->state;
}

DataFetcher *
cursor_fetcher_create_for_scan(TSConnection *conn, ScanState *ss, List *retrieved_attrs,
							   const char *stmt, StmtParams *params)
{
	Scan *scan = (Scan *) ss->ps.plan;
	TupleDesc tupdesc;
	Relation rel;
	CursorFetcher *cursor;

	Assert(NULL != ss);

	/*
	 * Get info we'll need for converting data fetched from the data node
	 * into local representation and error reporting during that process.
	 */
	if (scan->scanrelid > 0)
	{
		rel = ss->ss_currentRelation;
		tupdesc = RelationGetDescr(rel);
	}
	else
	{
		rel = NULL;
		tupdesc = ss->ss_ScanTupleSlot->tts_tupleDescriptor;
	}

	cursor = remote_cursor_init_with_params(conn, rel, tupdesc, ss, retrieved_attrs, stmt, params);
	return &cursor->state;
}

static void
cursor_fetcher_set_fetch_size(DataFetcher *df, int fetch_size)
{
	CursorFetcher *cursor = cast_fetcher(CursorFetcher, df);

	data_fetcher_set_fetch_size(&cursor->state, fetch_size);
	snprintf(cursor->fetch_stmt,
			 sizeof(cursor->fetch_stmt),
			 "FETCH %u FROM c%u",
			 fetch_size,
			 cursor->id);
}

static void
cursor_fetcher_set_tuple_memcontext(DataFetcher *df, MemoryContext mctx)
{
	CursorFetcher *cursor = cast_fetcher(CursorFetcher, df);

	data_fetcher_set_tuple_mctx(&cursor->state, mctx);
}

/*
 * Send async req to fetch data from cursor.
 */
static void
cursor_fetcher_send_fetch_request(DataFetcher *df)
{
	AsyncRequest *volatile req = NULL;
	MemoryContext oldcontext;
	CursorFetcher *cursor = cast_fetcher(CursorFetcher, df);

	Assert(cursor->state.open);

	if (cursor->state.data_req != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_CURSOR_STATE),
				 errmsg("invalid cursor state"),
				 errdetail("Cannot fetch new data while previous request is ongoing.")));

	PG_TRY();
	{
		TSConnection *conn = cursor->state.conn;

		/* We use a separate mem context because batch mem context is getting reset once we fetch
		 * new batch and here we need our async request to survive */
		oldcontext = MemoryContextSwitchTo(cursor->state.req_mctx);

		if (tuplefactory_is_binary(cursor->state.tf))
			req = async_request_send_binary(conn, cursor->fetch_stmt);
		else
			req = async_request_send(conn, cursor->fetch_stmt);

		Assert(NULL != req);
		cursor->state.data_req = req;
	}
	PG_CATCH();
	{
		if (NULL != req)
			pfree(req);

		PG_RE_THROW();
	}
	PG_END_TRY();

	MemoryContextSwitchTo(oldcontext);
}

/*
 * Retrieve data from ongoing async fetch request
 */
static int
cursor_fetcher_fetch_data_complete(CursorFetcher *cursor)
{
	AsyncResponseResult *volatile response = NULL;
	MemoryContext oldcontext;
	int numrows = 0;
	int format = 0;

	Assert(cursor != NULL);
	Assert(cursor->state.data_req != NULL);

	Assert(cursor->state.open);
	data_fetcher_validate(&cursor->state);

	/*
	 * We'll store the tuples in the batch_mctx.  First, flush the previous
	 * batch.
	 */
	cursor->state.tuples = NULL;
	MemoryContextReset(cursor->state.batch_mctx);

	PG_TRY();
	{
		PGresult *res;
		int i;

		oldcontext = MemoryContextSwitchTo(cursor->state.req_mctx);

		response = async_request_wait_any_result(cursor->state.data_req);
		Assert(NULL != response);

		res = async_response_result_get_pg_result(response);
		format = PQbinaryTuples(res);

		MemoryContextSwitchTo(cursor->state.batch_mctx);

		/* On error, report the original query, not the FETCH. */
		if (PQresultStatus(res) != PGRES_TUPLES_OK)
		{
			/* remote_result_elog will call PQclear() on the result, so need
			 * to mark the response as NULL to avoid double PQclear() */
			pfree(response);
			response = NULL;
			remote_result_elog(res, ERROR);
		}

		/* Convert the data into HeapTuples */
		numrows = PQntuples(res);
		cursor->state.tuples = (HeapTuple *) palloc0(numrows * sizeof(HeapTuple));
		cursor->state.num_tuples = numrows;
		cursor->state.next_tuple_idx = 0;

		/* Allow creating tuples in alternative memory context if user has set
		 * it explicitly, otherwise same as batch_mctx */
		MemoryContextSwitchTo(cursor->state.tuple_mctx);

		for (i = 0; i < numrows; i++)
			cursor->state.tuples[i] = tuplefactory_make_tuple(cursor->state.tf, res, i, format);

		tuplefactory_reset_mctx(cursor->state.tf);
		MemoryContextSwitchTo(cursor->state.batch_mctx);

		/* Update batch count to indicate we are no longer in the first
		 * batch. When we are on the second batch or greater, a rewind of the
		 * cursor needs to refetch the first batch. If we are still in the
		 * first batch, however, a rewind can be done by simply resetting the
		 * tuple index to 0 within the batch. */
		if (cursor->state.batch_count < 2)
			cursor->state.batch_count++;

		/* Must be EOF if we didn't get as many tuples as we asked for. */
		cursor->state.eof = (numrows < cursor->state.fetch_size);

		pfree(cursor->state.data_req);
		cursor->state.data_req = NULL;

		async_response_result_close(response);
		response = NULL;
	}
	PG_CATCH();
	{
		if (NULL != cursor->state.data_req)
		{
			pfree(cursor->state.data_req);
			cursor->state.data_req = NULL;
		}

		if (NULL != response)
			async_response_result_close(response);

		PG_RE_THROW();
	}
	PG_END_TRY();

	MemoryContextSwitchTo(oldcontext);

	return numrows;
}

static int
cursor_fetcher_fetch_data(DataFetcher *df)
{
	CursorFetcher *cursor = cast_fetcher(CursorFetcher, df);

	if (cursor->state.eof)
		return 0;

	if (!cursor->state.open)
		cursor_fetcher_wait_until_open(df);

	if (cursor->state.data_req == NULL)
		cursor_fetcher_send_fetch_request(df);

	return cursor_fetcher_fetch_data_complete(cursor);
}

static HeapTuple
cursor_fetcher_get_tuple(DataFetcher *df, int row)
{
	CursorFetcher *cursor = cast_fetcher(CursorFetcher, df);

	return data_fetcher_get_tuple(&cursor->state, row);
}

static HeapTuple
cursor_fetcher_get_next_tuple(DataFetcher *df)
{
	CursorFetcher *cursor = cast_fetcher(CursorFetcher, df);

	return data_fetcher_get_next_tuple(&cursor->state);
}

static void
remote_cursor_exec_cmd(CursorFetcher *cursor, const char *sql)
{
	AsyncRequest *req;

	/*
	 * We don't use a PG_TRY block here, so be careful not to throw error
	 * without releasing the PGresult.
	 */
	req = async_request_send(cursor->state.conn, sql);
	Assert(NULL != req);
	async_request_wait_ok_command(req);
	pfree(req);

	data_fetcher_reset(&cursor->state);
}

static void
cursor_fetcher_rewind(DataFetcher *df)
{
	CursorFetcher *cursor = cast_fetcher(CursorFetcher, df);
	/* We need to make sure that cursor is opened */
	cursor_fetcher_wait_until_open(df);

	if (cursor->state.batch_count > 1)
	{
		char sql[64];

		Assert(cursor->state.eof || cursor->state.data_req != NULL);

		if (!cursor->state.eof)
			async_request_discard_response(cursor->state.data_req);
		/* We are beyond the first fetch, so need to rewind the remote end */
		snprintf(sql, sizeof(sql), "MOVE BACKWARD ALL IN c%u", cursor->id);
		remote_cursor_exec_cmd(cursor, sql);
	}
	else
	{
		/* We have done zero or one fetch, so we can simply re-read what we
		 * have in memory, if anything */
		cursor->state.next_tuple_idx = 0;
	}
}

static void
cursor_fetcher_close(DataFetcher *df)
{
	CursorFetcher *cursor = cast_fetcher(CursorFetcher, df);
	char sql[64];

	if (!cursor->state.open && cursor->create_req != NULL)
	{
		async_request_discard_response(cursor->create_req);
		return;
	}

	if (!cursor->state.eof && cursor->state.data_req != NULL)
		async_request_discard_response(cursor->state.data_req);

	snprintf(sql, sizeof(sql), "CLOSE c%u", cursor->id);
	cursor->state.open = false;
	remote_cursor_exec_cmd(cursor, sql);
}
