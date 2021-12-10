/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_TSL_REMOTE_DATA_FETCHER_H
#define TIMESCALEDB_TSL_REMOTE_DATA_FETCHER_H

#include <postgres.h>
#include <access/tupdesc.h>
#include <utils/relcache.h>
#include <nodes/execnodes.h>

#include "connection.h"
#include "stmt_params.h"
#include "guc.h"
#include "tuplefactory.h"

typedef struct DataFetcher DataFetcher;

typedef struct DataFetcherFuncs
{
	/* Send a request for new data. This doesn't read the data itself */
	void (*send_fetch_request)(DataFetcher *data_fetcher);
	/* Read data in response to a fetch request. If no request has been sent,
	 * send it first. */
	int (*fetch_data)(DataFetcher *data_fetcher);
	/* Set the fetch (batch) size */
	void (*set_fetch_size)(DataFetcher *data_fetcher, int fetch_size);
	void (*set_tuple_mctx)(DataFetcher *data_fetcher, MemoryContext mctx);
	HeapTuple (*get_next_tuple)(DataFetcher *data_fetcher);
	HeapTuple (*get_tuple)(DataFetcher *data_fetcher, int row);
	void (*rewind)(DataFetcher *data_fetcher);
	void (*close)(DataFetcher *data_fetcher);
} DataFetcherFuncs;

typedef struct DataFetcher
{
	DataFetcherType type;
	DataFetcherFuncs *funcs;

	TSConnection *conn;
	TupleFactory *tf;

	MemoryContext req_mctx;   /* Stores async request and response */
	MemoryContext batch_mctx; /* Stores batches of fetched tuples */
	MemoryContext tuple_mctx;

	const char *stmt;		 /* sql statement */
	StmtParams *stmt_params; /* sql statement params */

	HeapTuple *tuples;  /* array of currently-retrieved tuples */
	int num_tuples;		/* # of tuples in array */
	int next_tuple_idx; /* index of next one to return */
	int fetch_size;		/* # of tuples to fetch */
	int batch_count;	/* how many batches (parts of result set) we've done */

	bool open;
	bool eof;

	AsyncRequest *data_req; /* a request to fetch data */
} DataFetcher;

void data_fetcher_free(DataFetcher *df);

extern void data_fetcher_init(DataFetcher *df, TSConnection *conn, const char *stmt,
							  StmtParams *params, Relation rel, ScanState *ss,
							  List *retrieved_attrs);

extern HeapTuple data_fetcher_get_tuple(DataFetcher *df, int row);
extern HeapTuple data_fetcher_get_next_tuple(DataFetcher *df);
extern void data_fetcher_set_fetch_size(DataFetcher *df, int fetch_size);
extern void data_fetcher_set_tuple_mctx(DataFetcher *df, MemoryContext mctx);
extern void data_fetcher_validate(DataFetcher *df);
extern void data_fetcher_reset(DataFetcher *df);

#ifdef USE_ASSERT_CHECKING
static inline DataFetcher *
assert_df_type(DataFetcherType type, DataFetcher *df)
{
	Assert(df == NULL || df->type == type);
	return df;
}
#define cast_fetcher(type, dfptr) ((type *) assert_df_type(type##Type, dfptr))
#else
#define cast_fetcher(type, dfptr) ((type *) dfptr)
#endif /* USE_ASSERT_CHECKING */

#endif /* TIMESCALEDB_TSL_REMOTE_DATA_FETCHER_H */
