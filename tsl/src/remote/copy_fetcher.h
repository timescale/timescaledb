/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_TSL_COPY_FETCHER_H
#define TIMESCALEDB_TSL_COPY_FETCHER_H

#include <postgres.h>

#include "data_fetcher.h"

extern DataFetcher *copy_fetcher_create_for_scan(TSConnection *conn, const char *stmt,
												 StmtParams *params, TupleFactory *tf);

#endif /* TIMESCALEDB_TSL_COPY_FETCHER_H */
