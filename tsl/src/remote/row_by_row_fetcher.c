/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <postgres.h>

#include "row_by_row_fetcher.h"
#include "tuplefactory.h"
#include "async.h"

typedef struct RowByRowFetcher
{
	DataFetcher state;

	/* Data for virtual tuples of the current retrieved batch. */
	Datum *batch_values;
	bool *batch_nulls;
} RowByRowFetcher;

static void row_by_row_fetcher_send_fetch_request(DataFetcher *df);
static void row_by_row_fetcher_reset(RowByRowFetcher *fetcher);
static int row_by_row_fetcher_fetch_data(DataFetcher *df);
static void row_by_row_fetcher_set_fetch_size(DataFetcher *df, int fetch_size);
static void row_by_row_fetcher_set_tuple_memcontext(DataFetcher *df, MemoryContext mctx);
static void row_by_row_fetcher_store_next_tuple(DataFetcher *df, TupleTableSlot *slot);
static void row_by_row_fetcher_rescan(DataFetcher *df);
static void row_by_row_fetcher_close(DataFetcher *df);

static DataFetcherFuncs funcs = {
	.send_fetch_request = row_by_row_fetcher_send_fetch_request,
	.fetch_data = row_by_row_fetcher_fetch_data,
	.set_fetch_size = row_by_row_fetcher_set_fetch_size,
	.set_tuple_mctx = row_by_row_fetcher_set_tuple_memcontext,
	.store_next_tuple = row_by_row_fetcher_store_next_tuple,
	.rewind = row_by_row_fetcher_rescan,
	.close = row_by_row_fetcher_close,
};

static void
row_by_row_fetcher_set_fetch_size(DataFetcher *df, int fetch_size)
{
	RowByRowFetcher *fetcher = cast_fetcher(RowByRowFetcher, df);
	data_fetcher_set_fetch_size(&fetcher->state, fetch_size);
}

static void
row_by_row_fetcher_set_tuple_memcontext(DataFetcher *df, MemoryContext mctx)
{
	RowByRowFetcher *fetcher = cast_fetcher(RowByRowFetcher, df);
	data_fetcher_set_tuple_mctx(&fetcher->state, mctx);
}

static void
row_by_row_fetcher_reset(RowByRowFetcher *fetcher)
{
	fetcher->state.open = false;
	data_fetcher_reset(&fetcher->state);
}

static void
row_by_row_fetcher_send_fetch_request(DataFetcher *df)
{
	AsyncRequest *volatile req = NULL;
	MemoryContext oldcontext;
	RowByRowFetcher *fetcher = cast_fetcher(RowByRowFetcher, df);

	if (fetcher->state.open)
	{
		/* data request has already been sent */
		Assert(fetcher->state.data_req != NULL);
		return;
	}

	/* make sure to have a clean state */
	row_by_row_fetcher_reset(fetcher);

	PG_TRY();
	{
		oldcontext = MemoryContextSwitchTo(fetcher->state.req_mctx);

		req = async_request_send_with_stmt_params_elevel_res_format(fetcher->state.conn,
																	fetcher->state.stmt,
																	fetcher->state.stmt_params,
																	ERROR,
																	tuplefactory_is_binary(
																		fetcher->state.tf) ?
																		FORMAT_BINARY :
																		FORMAT_TEXT);
		Assert(NULL != req);

		if (!async_request_set_single_row_mode(req))
			ereport(ERROR,
					(errcode(ERRCODE_CONNECTION_FAILURE),
					 errmsg("could not set single-row mode on connection to \"%s\"",
							remote_connection_node_name(fetcher->state.conn)),
					 errdetail("The aborted statement is: %s.", fetcher->state.stmt),
					 errhint(
						 "Row-by-row fetching of data is not supported together with sub-queries."
						 " Use cursor fetcher instead.")));

		fetcher->state.data_req = req;
		fetcher->state.open = true;
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
 * Process response for ongoing async request
 */
static int
row_by_row_fetcher_complete(RowByRowFetcher *fetcher)
{
	/* Marked as volatile since it's modified in PG_TRY used in PG_CATCH */
	AsyncResponseResult *volatile response = NULL;
	MemoryContext oldcontext;
	AsyncRequestSet *fetch_req_wrapper = async_request_set_create();

	Assert(fetcher->state.open);
	Assert(fetcher->state.data_req != NULL);

	data_fetcher_validate(&fetcher->state);

	async_request_set_add(fetch_req_wrapper, fetcher->state.data_req);

	/*
	 * We'll store the tuples in the batch_mctx.  First, flush the previous
	 * batch.
	 */
	MemoryContextReset(fetcher->state.batch_mctx);
	oldcontext = MemoryContextSwitchTo(fetcher->state.batch_mctx);
	const int nattrs = tuplefactory_get_nattrs(fetcher->state.tf);
	const int total = nattrs * fetcher->state.fetch_size;
	fetcher->batch_nulls = palloc(sizeof(bool) * total);
	for (int i = 0; i < total; i++)
	{
		fetcher->batch_nulls[i] = true;
	}
	fetcher->batch_values = palloc0(sizeof(Datum) * total);

	PG_TRY();
	{
		int i;

		for (i = 0; i < fetcher->state.fetch_size; i++)
		{
			PGresult *res;

			MemoryContextSwitchTo(fetcher->state.req_mctx);

			response = async_request_set_wait_any_result(fetch_req_wrapper);
			if (NULL == response)
				elog(ERROR, "unexpected NULL response");

			/* Make sure to drain the connection only if we've retrieved complete result set */
			if (async_response_get_type((AsyncResponse *) response) == RESPONSE_RESULT &&
				NULL != async_request_set_wait_any_result(fetch_req_wrapper))
				elog(ERROR, "request must be for one sql statement");

			res = async_response_result_get_pg_result(response);

			if (!(PQresultStatus(res) == PGRES_SINGLE_TUPLE ||
				  PQresultStatus(res) == PGRES_TUPLES_OK))
			{
				/* remote_result_elog will call PQclear() on the result, so
				 * need to mark the response as NULL to avoid double
				 * PQclear() */
				pfree(response);
				response = NULL;
				remote_result_elog(res, ERROR);
			}

			if (PQresultStatus(res) == PGRES_TUPLES_OK)
			{
				/* fetched all the data */
				Assert(PQntuples(res) == 0);

				fetcher->state.eof = true;
				async_response_result_close(response);
				response = NULL;
				break;
			}

			Assert(PQresultStatus(res) == PGRES_SINGLE_TUPLE);
			/* Allow creating tuples in alternative memory context if user has set
			 * it explicitly, otherwise same as batch_mctx */
			MemoryContextSwitchTo(fetcher->state.tuple_mctx);

			PG_USED_FOR_ASSERTS_ONLY ItemPointer ctid =
				tuplefactory_make_virtual_tuple(fetcher->state.tf,
												res,
												0,
												PQbinaryTuples(res),
												&fetcher->batch_values[i * nattrs],
												&fetcher->batch_nulls[i * nattrs]);

			/*
			 * This fetcher uses virtual tuples that can't hold ctid, so if we're
			 * receiving a ctid here, we're doing something wrong.
			 */
			Assert(ctid == NULL);

			async_response_result_close(response);
			response = NULL;
		}
		/* We need to manually reset the context since we've turned off per tuple reset */
		tuplefactory_reset_mctx(fetcher->state.tf);

		fetcher->state.num_tuples = i;
		fetcher->state.next_tuple_idx = 0;
		/* Must be EOF if we didn't get as many tuples as we asked for. */
		if (fetcher->state.num_tuples < fetcher->state.fetch_size)
		{
			Assert(fetcher->state.eof);
		}

		fetcher->state.batch_count++;

		if (fetcher->state.eof)
		{
			pfree(fetcher->state.data_req);
			fetcher->state.data_req = NULL;
		}
	}
	PG_CATCH();
	{
		if (NULL != fetcher->state.data_req)
		{
			pfree(fetcher->state.data_req);
			fetcher->state.data_req = NULL;
		}

		if (NULL != response)
			async_response_result_close(response);

		PG_RE_THROW();
	}
	PG_END_TRY();

	MemoryContextSwitchTo(oldcontext);
	pfree(fetch_req_wrapper);

	return fetcher->state.num_tuples;
}

static int
row_by_row_fetcher_fetch_data(DataFetcher *df)
{
	RowByRowFetcher *fetcher = cast_fetcher(RowByRowFetcher, df);

	if (fetcher->state.eof)
		return 0;

	if (!fetcher->state.open)
		row_by_row_fetcher_send_fetch_request(df);

	return row_by_row_fetcher_complete(fetcher);
}

static void
row_by_row_fetcher_store_tuple(DataFetcher *df, int row, TupleTableSlot *slot)
{
	RowByRowFetcher *fetcher = cast_fetcher(RowByRowFetcher, df);

	ExecClearTuple(slot);

	if (row >= df->num_tuples)
	{
		if (df->eof || df->funcs->fetch_data(df) == 0)
		{
			return;
		}

		row = 0;
		Assert(row == df->next_tuple_idx);
	}

	Assert(fetcher->batch_values != NULL);
	Assert(fetcher->batch_nulls != NULL);
	Assert(row >= 0 && row < df->num_tuples);

	const int nattrs = tuplefactory_get_nattrs(fetcher->state.tf);
	slot->tts_values = &fetcher->batch_values[nattrs * row];
	slot->tts_isnull = &fetcher->batch_nulls[nattrs * row];
	ExecStoreVirtualTuple(slot);
}

static void
row_by_row_fetcher_store_next_tuple(DataFetcher *df, TupleTableSlot *slot)
{
	row_by_row_fetcher_store_tuple(df, df->next_tuple_idx, slot);

	if (!TupIsNull(slot))
		df->next_tuple_idx++;

	Assert(df->next_tuple_idx <= df->num_tuples);
}

DataFetcher *
row_by_row_fetcher_create_for_scan(TSConnection *conn, const char *stmt, StmtParams *params,
								   TupleFactory *tf)
{
	RowByRowFetcher *fetcher = palloc0(sizeof(RowByRowFetcher));

	data_fetcher_init(&fetcher->state, conn, stmt, params, tf);
	fetcher->state.type = RowByRowFetcherType;
	fetcher->state.funcs = &funcs;

	return &fetcher->state;
}

static void
row_by_row_fetcher_close(DataFetcher *df)
{
	RowByRowFetcher *fetcher = cast_fetcher(RowByRowFetcher, df);

	Assert(fetcher->state.open);

	if (fetcher->state.data_req != NULL)
	{
		async_request_discard_response(fetcher->state.data_req);
		pfree(fetcher->state.data_req);
		fetcher->state.data_req = NULL;
	}
	row_by_row_fetcher_reset(fetcher);
}

static void
row_by_row_fetcher_rescan(DataFetcher *df)
{
	RowByRowFetcher *fetcher = cast_fetcher(RowByRowFetcher, df);

	if (fetcher->state.batch_count > 1)
		/* we're over the first batch so we need to close fetcher and restart from clean state */
		row_by_row_fetcher_close(df);
	else
		/* we can reuse current batch of results */
		fetcher->state.next_tuple_idx = 0;
}
