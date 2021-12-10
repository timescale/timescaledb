/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>

#include "data_fetcher.h"
#include "cursor_fetcher.h"
#include "row_by_row_fetcher.h"
#include "guc.h"
#include "errors.h"

#define DEFAULT_FETCH_SIZE 100

void
data_fetcher_init(DataFetcher *df, TSConnection *conn, const char *stmt, StmtParams *params,
				  Relation rel, ScanState *ss, List *retrieved_attrs)
{
	Assert(df != NULL);
	Assert(stmt != NULL);

	memset(df, 0, sizeof(DataFetcher));
	df->tuples = NULL;
	df->conn = conn;
	df->stmt = pstrdup(stmt);
	df->stmt_params = params;
	if (rel == NULL)
		df->tf = tuplefactory_create_for_scan(ss, retrieved_attrs);
	else
		df->tf = tuplefactory_create_for_rel(rel, retrieved_attrs);

	tuplefactory_set_per_tuple_mctx_reset(df->tf, false);
	df->batch_mctx =
		AllocSetContextCreate(CurrentMemoryContext, "cursor tuple data", ALLOCSET_DEFAULT_SIZES);
	df->tuple_mctx = df->batch_mctx;
	df->req_mctx =
		AllocSetContextCreate(CurrentMemoryContext, "async req/resp", ALLOCSET_DEFAULT_SIZES);
	df->fetch_size = DEFAULT_FETCH_SIZE;
}

void
data_fetcher_validate(DataFetcher *df)
{
	/* ANALYZE command is accessing random tuples so we should never fail here when running ANALYZE
	 */
	if (df->next_tuple_idx != 0 && df->next_tuple_idx < df->num_tuples)
		ereport(ERROR,
				(errcode(ERRCODE_TS_INTERNAL_ERROR),
				 errmsg("invalid cursor state. sql: %s", df->stmt),
				 errhint("Shouldn't fetch new data before consuming existing.")));
}

HeapTuple
data_fetcher_get_tuple(DataFetcher *df, int row)
{
	if (row >= df->num_tuples)
	{
		/* No point in another fetch if we already detected EOF, though. */
		if (df->eof || df->funcs->fetch_data(df) == 0)
			return NULL;

		/* More data was fetched so need to reset row index */
		row = 0;
		Assert(row == df->next_tuple_idx);
	}

	Assert(df->tuples != NULL);
	Assert(row >= 0 && row < df->num_tuples);

	return df->tuples[row];
}

HeapTuple
data_fetcher_get_next_tuple(DataFetcher *df)
{
	HeapTuple tuple = data_fetcher_get_tuple(df, df->next_tuple_idx);

	if (tuple != NULL)
		df->next_tuple_idx++;

	Assert(df->next_tuple_idx <= df->num_tuples);

	return tuple;
}

void
data_fetcher_set_fetch_size(DataFetcher *df, int fetch_size)
{
	df->fetch_size = fetch_size;
}

void
data_fetcher_set_tuple_mctx(DataFetcher *df, MemoryContext mctx)
{
	Assert(mctx != NULL);
	df->tuple_mctx = mctx;
}

void
data_fetcher_reset(DataFetcher *df)
{
	df->tuples = NULL;
	df->num_tuples = 0;
	df->next_tuple_idx = 0;
	df->batch_count = 0;
	df->eof = false;
	MemoryContextReset(df->req_mctx);
	MemoryContextReset(df->batch_mctx);
}

void
data_fetcher_free(DataFetcher *df)
{
	df->funcs->close(df);
	pfree(df);
}
