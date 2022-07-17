/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <postgres.h>
#include <port/pg_bswap.h>

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

	StringInfoData copy_query;
	initStringInfo(&copy_query);
	appendStringInfo(&copy_query, "copy (%s) to stdout with (format binary)", fetcher->state.stmt);

	PG_TRY();
	{
		oldcontext = MemoryContextSwitchTo(fetcher->state.req_mctx);

		Assert(tuplefactory_is_binary(fetcher->state.tf));
		req = async_request_send_with_stmt_params_elevel_res_format(fetcher->state.conn,
																	copy_query.data,
																	fetcher->state.stmt_params,
																	ERROR,
																	FORMAT_BINARY);
		Assert(NULL != req);

		/*
		 * Single-row mode doesn't really influence the COPY queries, but setting
		 * it here is a convenient way to prevent concurrent COPY requests on the
		 * same connection. This can happen if we have multiple tables on the same
		 * data node and still use the row-by-row fetcher.
		 */
		if (!async_request_set_single_row_mode(req))
		{
			ereport(ERROR,
					(errcode(ERRCODE_CONNECTION_FAILURE),
					 errmsg("could not set single-row mode on connection to \"%s\"",
							remote_connection_node_name(fetcher->state.conn)),
					 errdetail("The aborted statement is: %s.", fetcher->state.stmt),
					 errhint(
						 "Row-by-row fetching of data is not supported together with sub-queries."
						 " Use cursor fetcher instead.")));
		}

		fetcher->state.data_req = req;
		fetcher->state.open = true;

		PGresult *res = PQgetResult(remote_connection_get_pg_conn(fetcher->state.conn));
		if (!res)
		{
			elog(ERROR, "unexpected NULL response when starting COPY mode");
		}
		if (PQresultStatus(res) != PGRES_COPY_OUT)
		{
			elog(ERROR,
				 "unexpected PQresult status %d when starting COPY mode",
				 PQresultStatus(res));
		}

		PQclear(res);
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

static int
copy_data_consume_bytes(StringInfo copy_data, int bytes_to_read)
{
	const int bytes_read = Min(bytes_to_read, copy_data->len - copy_data->cursor);
	copy_data->cursor += bytes_read;
	Assert(copy_data->cursor <= copy_data->len);
	return bytes_read;
}

static char *
copy_data_read_bytes(StringInfo copy_data, int bytes_to_read)
{
	char *result = &copy_data->data[copy_data->cursor];
	const int bytes_read = copy_data_consume_bytes(copy_data, bytes_to_read);

	if (bytes_read != bytes_to_read)
	{
		elog(ERROR,
			 "could not read the requested %d bytes of COPY data, read %d instead",
			 bytes_to_read,
			 bytes_read);
	}

	return result;
}

static int16
copy_data_read_int16(StringInfo copy_data)
{
	char *buf = &copy_data->data[copy_data->cursor];
	char aligned_buf[2];
	if (copy_data_consume_bytes(copy_data, 2) != 2)
	{
		elog(ERROR, "failed to read int16 from COPY data: not enough bytes left");
	}
	if (buf != (const char *) TYPEALIGN(2, buf))
	{
		memcpy(aligned_buf, buf, 2);
		buf = aligned_buf;
	}
	AssertPointerAlignment(buf, 2);
	return (int16) pg_ntoh16(*(uint16 *) buf);
}

static int32
copy_data_read_int32(StringInfo copy_data)
{
	char *buf = &copy_data->data[copy_data->cursor];
	char aligned_buf[4];
	if (copy_data_consume_bytes(copy_data, 4) != 4)
	{
		elog(ERROR, "failed to read int32 from COPY data: not enough bytes left");
	}
	if (buf != (const char *) TYPEALIGN(4, buf))
	{
		memcpy(aligned_buf, buf, 4);
		buf = aligned_buf;
	}
	AssertPointerAlignment(buf, 4);
	return (int32) pg_ntoh32(*(uint32 *) buf);
}

static void
copy_data_check_header(StringInfo copy_data)
{
	static const char required_signature[11] = "PGCOPY\n\377\r\n\0";
	char *actual_signature = copy_data_read_bytes(copy_data, sizeof(required_signature));
	if (memcmp(required_signature, actual_signature, sizeof(required_signature)) != 0)
	{
		elog(ERROR, "wrong COPY data signature");
	}

	int32 flags = copy_data_read_int32(copy_data);
	if (flags != 0)
	{
		elog(ERROR, "wrong COPY flags: %d, should be 0", flags);
	}

	/*
	 * Header extension area length
	 * 32-bit integer, length in bytes of remainder of header, not including
	 * self. Currently, this is zero, and the first tuple follows
	 * immediately. Future changes to the format might allow additional data
	 * to be present in the header. A reader should silently skip over any
	 * header extension data it does not know what to do with.
	 */
	int32 header_extension_length = copy_data_read_int32(copy_data);
	int bytes_read = copy_data_consume_bytes(copy_data, header_extension_length);
	if (bytes_read != header_extension_length)
	{
		elog(ERROR,
			 "failed to read COPY header extension: expected %d bytes, read %d",
			 header_extension_length,
			 bytes_read);
	}
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
	const TupleDesc tupdesc = tuplefactory_get_tupdesc(fetcher->state.tf);
	const List *retrieved_attrs = tuplefactory_get_retrieved_attrs(fetcher->state.tf);
	const int tupdesc_natts = tupdesc->natts;
	const int retrieved_natts = list_length(retrieved_attrs);
	const int total = tupdesc_natts * fetcher->state.fetch_size;
	fetcher->batch_nulls = palloc(sizeof(bool) * total);
	for (int i = 0; i < total; i++)
	{
		fetcher->batch_nulls[i] = true;
	}
	fetcher->batch_values = palloc0(sizeof(Datum) * total);

	PG_TRY();
	{
		int row;

		for (row = 0; row < fetcher->state.fetch_size; row++)
		{
			MemoryContextSwitchTo(fetcher->state.req_mctx);

			StringInfoData copy_data = { 0 };

			copy_data.len = PQgetCopyData(remote_connection_get_pg_conn(fetcher->state.conn),
										  &copy_data.data,
										  /* async = */ false);

			if (copy_data.len == -1)
			{
				/*
				 * According to the docs, this means EOF, but in practice we get
				 * it when there is an error. Check for error and report it.
				 */
				fetcher->state.eof = true;

				PGresult *res = PQgetResult(remote_connection_get_pg_conn(fetcher->state.conn));
				if (res == NULL)
				{
					/* Shouldn't really happen but technically possible. */
					TSConnectionError err;
					remote_connection_get_error(fetcher->state.conn, &err);
					remote_connection_error_elog(&err, ERROR);
				}
				if (PQresultStatus(res) != PGRES_COPY_OUT)
				{
					TSConnectionError err;
					remote_connection_get_result_error(res, &err);
					remote_connection_error_elog(&err, ERROR);
				}
				break;
			}
			else if (copy_data.len == -2)
			{
				/*
				 * Error. The docs say: consult PQerrorMessage() for the reason.
				 * remote_connection_get_error() will do this for us.
				 */
				TSConnectionError err;
				remote_connection_get_error(fetcher->state.conn, &err);
				remote_connection_error_elog(&err, ERROR);
			}

			copy_data.maxlen = copy_data.len;
			Assert(copy_data.cursor == 0);

			if (fetcher->state.batch_count == 0 && row == 0)
			{
				copy_data_check_header(&copy_data);
			}

			const AttConvInMetadata *attconv = tuplefactory_get_attconv(fetcher->state.tf);
			Assert(attconv->binary);
			const int16 natts = copy_data_read_int16(&copy_data);
			if (natts == -1)
			{
				fetcher->state.eof = true;
				break;
			}

			/*
			 * There is also one case where no tupdesc attributes are retrieved.
			 * This is when we do `select count(*) from t`, and
			 * `enable_partitionwise_aggregate` is 0, so the data node queries
			 * become `select null from ...` and we should get 1 NULL attribute
			 * from COPY.
			 */
			int16 expected_natts = Max(1, retrieved_natts);
			if (natts != expected_natts)
			{
				elog(ERROR,
					 "wrong number of attributes for a COPY tuple: expected %d, got %d",
					 expected_natts,
					 natts);
			}

			Datum *values = &fetcher->batch_values[tupdesc_natts * row];
			bool *nulls = &fetcher->batch_nulls[tupdesc_natts * row];
			for (int i = 0; i < tupdesc_natts; i++)
			{
				nulls[i] = true;
			}

			MemoryContextSwitchTo(fetcher->state.tuple_mctx);
			for (int i = 0; i < retrieved_natts; i++)
			{
				const int att = list_nth_int(retrieved_attrs, i) - 1;
				Assert(att >= 0);
				Assert(att < tupdesc_natts);
				const int32 att_bytes = copy_data_read_int32(&copy_data);
				if (att_bytes == -1)
				{
					/*
					 * NULL. From the Postgres docs:
					 * Usually, a receive function should be declared STRICT; if
					 * it is not, it will be called with a NULL first parameter
					 * when reading a NULL input value. The function must still
					 * return NULL in this case, unless it raises an error.
					 * (This case is mainly meant to support domain receive
					 * functions, which might need to reject NULL inputs.)
					 * https://www.postgresql.org/docs/current/sql-createtype.html
					 */
					if (!attconv->conv_funcs[att].fn_strict)
					{
						values[att] = ReceiveFunctionCall(&attconv->conv_funcs[att],
														  NULL,
														  attconv->ioparams[att],
														  attconv->typmods[att]);
					}
					else
					{
						values[att] = PointerGetDatum(NULL);
					}
					nulls[att] = true;
					continue;
				}

				StringInfoData att_data = { 0 };
				att_data.data = copy_data_read_bytes(&copy_data, att_bytes);
				att_data.len = att_bytes;

				values[att] = ReceiveFunctionCall(&attconv->conv_funcs[att],
												  &att_data,
												  attconv->ioparams[att],
												  attconv->typmods[att]);
				nulls[att] = false;
			}

			MemoryContextSwitchTo(fetcher->state.batch_mctx);

			PQfreemem(copy_data.data);
		}

		fetcher->state.num_tuples = row;
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

			int end_res = PQendcopy(remote_connection_get_pg_conn(fetcher->state.conn));
			if (end_res != 0)
			{
				TSConnectionError err;
				remote_connection_get_error(fetcher->state.conn, &err);
				remote_connection_error_elog(&err, ERROR);
			}

			/*
			 * Shouldn't have any activity on the connection after we have
			 * finished COPY. Just double-check.
			 */
			PGresult *pgres = PQgetResult(remote_connection_get_pg_conn(fetcher->state.conn));
			if (pgres)
			{
				TSConnectionError err;
				remote_connection_get_result_error(pgres, &err);
				if (err.msg == NULL)
				{
					err.msg = "internal program error: remaining activity on the data node "
							  "connection after finishing COPY";
				}
				remote_connection_error_elog(&err, ERROR);
			}

			remote_connection_set_status(fetcher->state.conn, CONN_IDLE);
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

	/*
	 * The fetcher state might not be open if the fetcher got initialized but
	 * never executed due to executor constraints.
	 */
	if (fetcher->state.open && fetcher->state.data_req != NULL)
	{
		int end_res = PQendcopy(remote_connection_get_pg_conn(fetcher->state.conn));
		if (end_res != 0)
		{
			TSConnectionError err;
			remote_connection_get_error(fetcher->state.conn, &err);
			remote_connection_error_elog(&err, ERROR);
		}

		/*
		 * Shouldn't have any activity on the connection after we have
		 * finished COPY. Just double-check.
		 */
		PGresult *pgres = PQgetResult(remote_connection_get_pg_conn(fetcher->state.conn));
		if (pgres)
		{
			TSConnectionError err;
			remote_connection_get_result_error(pgres, &err);
			if (err.msg == NULL)
			{
				err.msg = "internal program error: remaining activity on the data node connection "
						  "after finishing COPY";
			}
			remote_connection_error_elog(&err, ERROR);
		}

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
