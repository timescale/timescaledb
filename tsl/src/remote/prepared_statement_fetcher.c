/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <postgres.h>

#include "prepared_statement_fetcher.h"
#include "tuplefactory.h"
#include "async.h"

typedef struct PreparedStatementFetcher
{
	DataFetcher state;

	/* Data for virtual tuples of the current retrieved batch. */
	Datum *batch_values;
	bool *batch_nulls;
} PreparedStatementFetcher;

static void prepared_statement_fetcher_send_fetch_request(DataFetcher *df);
static void prepared_statement_fetcher_reset(PreparedStatementFetcher *fetcher);
static int prepared_statement_fetcher_fetch_data(DataFetcher *df);
static void prepared_statement_fetcher_set_fetch_size(DataFetcher *df, int fetch_size);
static void prepared_statement_fetcher_set_tuple_memcontext(DataFetcher *df, MemoryContext mctx);
static void prepared_statement_fetcher_store_next_tuple(DataFetcher *df, TupleTableSlot *slot);
static void prepared_statement_fetcher_rewind(DataFetcher *df);
static void prepared_statement_fetcher_rescan(DataFetcher *df, StmtParams *params);
static void prepared_statement_fetcher_close(DataFetcher *df);

static DataFetcherFuncs funcs = {
	.close = prepared_statement_fetcher_close,
	.fetch_data = prepared_statement_fetcher_fetch_data,
	.rescan = prepared_statement_fetcher_rescan,
	.rewind = prepared_statement_fetcher_rewind,
	.send_fetch_request = prepared_statement_fetcher_send_fetch_request,
	.set_fetch_size = prepared_statement_fetcher_set_fetch_size,
	.set_tuple_mctx = prepared_statement_fetcher_set_tuple_memcontext,
	.store_next_tuple = prepared_statement_fetcher_store_next_tuple,
};

static void
prepared_statement_fetcher_set_fetch_size(DataFetcher *df, int fetch_size)
{
	PreparedStatementFetcher *fetcher = cast_fetcher(PreparedStatementFetcher, df);
	data_fetcher_set_fetch_size(&fetcher->state, fetch_size);
}

static void
prepared_statement_fetcher_set_tuple_memcontext(DataFetcher *df, MemoryContext mctx)
{
	PreparedStatementFetcher *fetcher = cast_fetcher(PreparedStatementFetcher, df);
	data_fetcher_set_tuple_mctx(&fetcher->state, mctx);
}

static void
prepared_statement_fetcher_reset(PreparedStatementFetcher *fetcher)
{
	/* Drain the connection, reporting any errors. */
	TSConnection *conn = fetcher->state.conn;
	PGresult *res;
	while ((res = remote_connection_get_result(conn, TS_NO_TIMEOUT)) != NULL)
	{
		char *sqlstate = PQresultErrorField(res, PG_DIAG_SQLSTATE);
		if (sqlstate != NULL && strcmp(sqlstate, "00000") == 0)
		{
			remote_result_elog(res, ERROR);
		}
		PQclear(res);
	}

	fetcher->state.open = false;
	data_fetcher_reset(&fetcher->state);
}

static void
prepared_statement_fetcher_send_fetch_request(DataFetcher *df)
{
	PreparedStatementFetcher *fetcher = cast_fetcher(PreparedStatementFetcher, df);

	if (fetcher->state.open)
	{
		/* data request has already been sent */
		Assert(fetcher->state.data_req != NULL);
		return;
	}

	/* make sure to have a clean state */
	prepared_statement_fetcher_reset(fetcher);

	TSConnection *conn = fetcher->state.conn;
	if (remote_connection_get_status(conn) != CONN_IDLE)
	{
		elog(ERROR, "unexpected activity on data node connection when sending fetch request");
	}

	PGresult *pgres = remote_connection_get_result(conn, TS_NO_TIMEOUT);
	if (pgres != NULL)
	{
		char *sqlstate = PQresultErrorField(pgres, PG_DIAG_SQLSTATE);
		if (sqlstate != NULL && strcmp(sqlstate, "00000") == 0)
		{
			remote_result_elog(pgres, ERROR);
		}

		elog(ERROR,
			 "unexpected activity on data node connection when sending fetch request "
			 "(PQresultStatus %d)",
			 PQresultStatus(pgres));
	}

	PGconn *pg_conn = remote_connection_get_pg_conn(conn);
	int ret = PQsendQueryPrepared(pg_conn,
								  /* stmtName = */ "",
								  stmt_params_num_params(fetcher->state.stmt_params),
								  stmt_params_values(fetcher->state.stmt_params),
								  stmt_params_lengths(fetcher->state.stmt_params),
								  stmt_params_formats(fetcher->state.stmt_params),
								  tuplefactory_is_binary(fetcher->state.tf) ? FORMAT_BINARY :
																			  FORMAT_TEXT);

	if (ret != 1)
	{
		TSConnectionError err;
		remote_connection_get_error(conn, &err);
		remote_connection_error_elog(&err, ERROR);
	}

	if (!remote_connection_set_single_row_mode(conn))
		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_FAILURE),
				 errmsg("could not set single-row mode on connection to \"%s\"",
						remote_connection_node_name(fetcher->state.conn)),
				 errdetail("The aborted statement is: %s.", fetcher->state.stmt),
				 errhint("Row-by-row fetching of data is not supported together with sub-queries."
						 " Use cursor fetcher instead.")));

	fetcher->state.data_req = (void *) 1;
	fetcher->state.open = true;
}

/*
 * Process response for ongoing async request
 */
static int
prepared_statement_fetcher_complete(PreparedStatementFetcher *fetcher)
{
	MemoryContext oldcontext;

	Assert(fetcher->state.open);
	Assert(fetcher->state.data_req != NULL);

	data_fetcher_validate(&fetcher->state);

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

	TSConnection *conn = fetcher->state.conn;
	PGconn *pg_conn = remote_connection_get_pg_conn(conn);
	if (PQsetnonblocking(pg_conn, 0) != 0)
	{
		remote_connection_elog(conn, ERROR);
	}

	PG_TRY();
	{
		int i;

		for (i = 0; i < fetcher->state.fetch_size; i++)
		{
			PGresult *res;

			res = remote_connection_get_result(conn, TS_NO_TIMEOUT);

			if (!(PQresultStatus(res) == PGRES_SINGLE_TUPLE ||
				  PQresultStatus(res) == PGRES_TUPLES_OK))
			{
				remote_result_elog(res, ERROR);
			}

			if (PQresultStatus(res) == PGRES_TUPLES_OK)
			{
				/* fetched all the data */
				Assert(PQntuples(res) == 0);
				PQclear(res);

				fetcher->state.eof = true;
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

			PQclear(res);
		}
		/* We need to manually reset the context since we've turned off per tuple reset */
		tuplefactory_reset_mctx(fetcher->state.tf);

		fetcher->state.num_tuples = i;
		fetcher->state.next_tuple_idx = 0;
		fetcher->state.batch_count++;

		if (fetcher->state.eof)
		{
			fetcher->state.data_req = NULL;
		}
	}
	PG_CATCH();
	{
		if (NULL != fetcher->state.data_req)
		{
			fetcher->state.data_req = NULL;
		}

		PG_RE_THROW();
	}
	PG_END_TRY();

	MemoryContextSwitchTo(oldcontext);

	return fetcher->state.num_tuples;
}

static int
prepared_statement_fetcher_fetch_data(DataFetcher *df)
{
	PreparedStatementFetcher *fetcher = cast_fetcher(PreparedStatementFetcher, df);

	if (fetcher->state.eof)
		return 0;

	if (!fetcher->state.open)
		prepared_statement_fetcher_send_fetch_request(df);

	return prepared_statement_fetcher_complete(fetcher);
}

static void
prepared_statement_fetcher_store_tuple(DataFetcher *df, int row, TupleTableSlot *slot)
{
	PreparedStatementFetcher *fetcher = cast_fetcher(PreparedStatementFetcher, df);

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
prepared_statement_fetcher_store_next_tuple(DataFetcher *df, TupleTableSlot *slot)
{
	prepared_statement_fetcher_store_tuple(df, df->next_tuple_idx, slot);

	if (!TupIsNull(slot))
		df->next_tuple_idx++;

	Assert(df->next_tuple_idx <= df->num_tuples);
}

DataFetcher *
prepared_statement_fetcher_create_for_scan(TSConnection *conn, const char *stmt, StmtParams *params,
										   TupleFactory *tf)
{
	PreparedStatementFetcher *fetcher = palloc0(sizeof(PreparedStatementFetcher));

	data_fetcher_init(&fetcher->state, conn, stmt, params, tf);
	fetcher->state.type = PreparedStatementFetcherType;
	fetcher->state.funcs = &funcs;

	PGconn *pg_conn = remote_connection_get_pg_conn(conn);
	if (remote_connection_get_status(conn) != CONN_IDLE)
	{
		elog(ERROR,
			 "unexpected activity on data node connection when creating the row-by-row fetcher");
	}

	/*
	 * Force using the generic plan for each execution of the data node query,
	 * because it would be very expensive and pointless to replan it for each
	 * subsequent parameter value.
	 */
	PGresult *res = remote_connection_exec(conn, "SET plan_cache_mode = 'force_generic_plan'");
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		TSConnectionError err;
		remote_connection_get_result_error(res, &err);
		remote_connection_error_elog(&err, ERROR);
	}
	PQclear(res);

	if (1 != PQsendPrepare(pg_conn,
						   /* stmtName = */ "",
						   stmt,
						   stmt_params_num_params(params),
						   /* paramTypes = */ NULL))
	{
		TSConnectionError err;
		remote_connection_get_error(conn, &err);
		remote_connection_error_elog(&err, ERROR);
	}

	res = remote_connection_get_result(conn, TS_NO_TIMEOUT);
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		TSConnectionError err;
		remote_connection_get_result_error(res, &err);
		remote_connection_error_elog(&err, ERROR);
	}

	PQclear(res);

	return &fetcher->state;
}

static void
prepared_statement_fetcher_close(DataFetcher *df)
{
	PreparedStatementFetcher *fetcher = cast_fetcher(PreparedStatementFetcher, df);

	if (fetcher->state.open)
	{
		if (fetcher->state.data_req != NULL)
		{
			fetcher->state.data_req = NULL;
		}
		prepared_statement_fetcher_reset(fetcher);
	}
	else
	{
		Assert(fetcher->state.data_req == NULL);
		Assert(fetcher->state.num_tuples == 0);

#ifdef USE_ASSERT_CHECKING
		TSConnection *conn = fetcher->state.conn;
		PGconn *pg_conn = remote_connection_get_pg_conn(conn);

		Assert(PQtransactionStatus(pg_conn) != PQTRANS_ACTIVE);
#endif
	}

	PGresult *res = remote_connection_exec(fetcher->state.conn, "RESET plan_cache_mode");
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		TSConnectionError err;
		remote_connection_get_result_error(res, &err);
		remote_connection_error_elog(&err, ERROR);
	}
	PQclear(res);
}

static void
prepared_statement_fetcher_rewind(DataFetcher *df)
{
	PreparedStatementFetcher *fetcher = cast_fetcher(PreparedStatementFetcher, df);

	if (fetcher->state.batch_count > 1)
		/* we're over the first batch so we need to reset fetcher and restart from clean state */
		prepared_statement_fetcher_reset(fetcher);
	else
		/* we can reuse current batch of results */
		fetcher->state.next_tuple_idx = 0;
}

static void
prepared_statement_fetcher_rescan(DataFetcher *df, StmtParams *params)
{
	PreparedStatementFetcher *fetcher = cast_fetcher(PreparedStatementFetcher, df);
	prepared_statement_fetcher_reset(fetcher);
	df->stmt_params = params;
}
