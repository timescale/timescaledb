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

#include "utils.h"
#include "async.h"
#include "stmt_params.h"
#include "tuplefactory.h"
#include "cursor.h"

#define DEFAULT_FETCH_SIZE 100

/*
 * Cursor for fetching data from a data node.
 */
typedef struct Cursor
{
	unsigned int id;
	TSConnection *conn;
	Relation rel; /* Relation we're scanning with this cursor. Can be NULL
				   * for, e.g., JOINs */
	TupleDesc tupdesc;
	TupleFactory *tf;
	MemoryContext batch_mctx; /* Stores batches of fetched tuples */
	MemoryContext tuple_mctx;
	const char *stmt;
	unsigned int fetch_size;
	char fetch_stmt[64];
	HeapTuple *tuples; /* array of currently-retrieved tuples */
	int num_tuples;	/* # of tuples in array */
	int next_tuple;	/* index of next one to return */

	/* batch-level state, for optimizing rewinds and avoiding useless fetch */
	int fetch_ct_2; /* Min(# of fetches done, 2) */
	bool isopen;
	bool eof;
} Cursor;

static void
cursor_open(Cursor *cursor, StmtParams *params)
{
	AsyncRequest *req;
	StringInfoData buf;

	initStringInfo(&buf);
	appendStringInfo(&buf, "DECLARE c%u CURSOR FOR\n%s", cursor->id, cursor->stmt);

	if (NULL == params)
		req = async_request_send(cursor->conn, buf.data);
	else
		req = async_request_send_with_params(cursor->conn, buf.data, params, FORMAT_TEXT);

	Assert(NULL != req);
	async_request_wait_ok_command(req);
	pfree(req);
	pfree(buf.data);
	cursor->isopen = true;
}

static Cursor *
remote_cursor_init_with_params(Cursor *cursor, TSConnection *conn, Relation rel, TupleDesc tupdesc,
							   ScanState *ss, List *retrieved_attrs, const char *stmt,
							   StmtParams *params)
{
	MemSet(cursor, 0, sizeof(Cursor));

	/* Assign a unique ID for my cursor */
	cursor->id = remote_connection_get_cursor_number();
	cursor->tuples = NULL;
	cursor->num_tuples = 0;
	cursor->next_tuple = 0;
	cursor->eof = false;
	cursor->isopen = false;
	cursor->conn = conn;
	cursor->stmt = pstrdup(stmt);
	cursor->rel = rel;
	cursor->tupdesc = tupdesc;
	cursor->tf = (rel == NULL) ? tuplefactory_create_for_scan(ss, retrieved_attrs) :
								 tuplefactory_create_for_rel(rel, retrieved_attrs);
	cursor->batch_mctx =
		AllocSetContextCreate(CurrentMemoryContext, "cursor tuple data", ALLOCSET_DEFAULT_SIZES);
	cursor->tuple_mctx = cursor->batch_mctx;

	remote_cursor_set_fetch_size(cursor, DEFAULT_FETCH_SIZE);

	cursor_open(cursor, params);

	return cursor;
}

Cursor *
remote_cursor_create_for_rel(TSConnection *conn, Relation rel, List *retrieved_attrs,
							 const char *stmt, StmtParams *params)
{
	Assert(NULL != rel);
	return remote_cursor_init_with_params(palloc0(sizeof(Cursor)),
										  conn,
										  rel,
										  RelationGetDescr(rel),
										  NULL,
										  retrieved_attrs,
										  stmt,
										  params);
}

Cursor *
remote_cursor_create_for_scan(TSConnection *conn, ScanState *ss, List *retrieved_attrs,
							  const char *stmt, StmtParams *params)
{
	Scan *scan = (Scan *) ss->ps.plan;
	TupleDesc tupdesc;
	Relation rel;

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

	return remote_cursor_init_with_params(palloc0(sizeof(Cursor)),
										  conn,
										  rel,
										  tupdesc,
										  ss,
										  retrieved_attrs,
										  stmt,
										  params);
}

bool
remote_cursor_set_fetch_size(Cursor *cursor, unsigned int fetch_size)
{
	if (cursor->fetch_size == fetch_size)
		return false;

	cursor->fetch_size = fetch_size;

	snprintf(cursor->fetch_stmt,
			 sizeof(cursor->fetch_stmt),
			 "FETCH %u FROM c%u",
			 fetch_size,
			 cursor->id);

	return true;
}

void
remote_cursor_set_tuple_memcontext(Cursor *cursor, MemoryContext mctx)
{
	Assert(mctx != NULL);
	cursor->tuple_mctx = mctx;
}

int
remote_cursor_fetch_data(Cursor *cursor)
{
	AsyncRequest *volatile req = NULL;
	AsyncResponseResult *volatile rsp = NULL;
	MemoryContext oldcontext;
	int numrows = 0;

	if (!cursor->isopen)
		ereport(ERROR, (errcode(ERRCODE_INVALID_CURSOR_STATE), errmsg("cursor is closed")));

	/*
	 * We'll store the tuples in the batch_mctx.  First, flush the previous
	 * batch.
	 */
	cursor->tuples = NULL;
	MemoryContextReset(cursor->batch_mctx);
	oldcontext = MemoryContextSwitchTo(cursor->batch_mctx);

	/* PGresult must be released before leaving this function. */
	PG_TRY();
	{
		TSConnection *conn = cursor->conn;
		PGresult *res;
		int i;

		if (tuplefactory_is_binary(cursor->tf))
			req = async_request_send_binary(conn, cursor->fetch_stmt);
		else
			req = async_request_send(conn, cursor->fetch_stmt);

		Assert(NULL != req);
		rsp = async_request_wait_any_result(req);
		Assert(NULL != rsp);

		res = async_response_result_get_pg_result(rsp);

		/* On error, report the original query, not the FETCH. */
		if (PQresultStatus(res) != PGRES_TUPLES_OK)
			remote_connection_report_error(ERROR, res, conn, false, cursor->stmt);

		/* Convert the data into HeapTuples */
		numrows = PQntuples(res);
		cursor->tuples = (HeapTuple *) palloc0(numrows * sizeof(HeapTuple));
		cursor->num_tuples = numrows;
		cursor->next_tuple = 0;

		/* Allow creating tuples in alternative memory context if user has set
		 * it explicitly, otherwise same as batch_mctx */
		MemoryContextSwitchTo(cursor->tuple_mctx);

		for (i = 0; i < numrows; i++)
			cursor->tuples[i] = tuplefactory_make_tuple(cursor->tf, res, i);

		MemoryContextSwitchTo(cursor->batch_mctx);

		/* Update fetch_ct_2 */
		if (cursor->fetch_ct_2 < 2)
			cursor->fetch_ct_2++;

		/* Must be EOF if we didn't get as many tuples as we asked for. */
		cursor->eof = (numrows < cursor->fetch_size);

		pfree(req);
		async_response_result_close(rsp);
		req = NULL;
		rsp = NULL;
	}
	PG_CATCH();
	{
		if (NULL != req)
			pfree(req);

		if (NULL != rsp)
			async_response_result_close(rsp);

		PG_RE_THROW();
	}
	PG_END_TRY();

	MemoryContextSwitchTo(oldcontext);

	return numrows;
}

HeapTuple
remote_cursor_get_tuple(Cursor *cursor, int row)
{
	if (row >= cursor->num_tuples)
	{
		/* No point in another fetch if we already detected EOF, though. */
		if (cursor->eof || remote_cursor_fetch_data(cursor) == 0)
			return NULL;

		/* More data was fetched so need to reset row index */
		row = 0;
		Assert(row == cursor->next_tuple);
	}

	Assert(cursor->tuples != NULL);
	Assert(row >= 0 && row < cursor->num_tuples);

	return cursor->tuples[row];
}

HeapTuple
remote_cursor_get_next_tuple(Cursor *cursor)
{
	HeapTuple tuple = remote_cursor_get_tuple(cursor, cursor->next_tuple);

	if (NULL != tuple)
		cursor->next_tuple++;

	Assert(cursor->next_tuple <= cursor->num_tuples);

	return tuple;
}

static void
remote_cursor_exec_cmd(Cursor *cursor, const char *sql)
{
	AsyncRequest *req;

	/*
	 * We don't use a PG_TRY block here, so be careful not to throw error
	 * without releasing the PGresult.
	 */
	req = async_request_send(cursor->conn, sql);
	Assert(NULL != req);
	async_request_wait_ok_command(req);
	pfree(req);

	/* Now force a fresh FETCH. */
	cursor->tuples = NULL;
	cursor->num_tuples = 0;
	cursor->next_tuple = 0;
	cursor->fetch_ct_2 = 0;
}

void
remote_cursor_rewind(Cursor *cursor)
{
	if (cursor->fetch_ct_2 > 1)
	{
		char sql[64];

		/* We are beyond the first fetch, so need to rewind the remote end */
		snprintf(sql, sizeof(sql), "MOVE BACKWARD ALL IN c%u", cursor->id);
		remote_cursor_exec_cmd(cursor, sql);
	}
	else
	{
		/* We have done zero or one fetch, so we can simply re-read what we
		 * have in memory, if anything */
		cursor->next_tuple = 0;
	}
}

void
remote_cursor_close(Cursor *cursor)
{
	char sql[64];

	snprintf(sql, sizeof(sql), "CLOSE c%u", cursor->id);
	cursor->isopen = false;
	remote_cursor_exec_cmd(cursor, sql);
}
