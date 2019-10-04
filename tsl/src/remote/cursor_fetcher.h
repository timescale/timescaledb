/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_TSL_CURSOR_FETCHER_H
#define TIMESCALEDB_TSL_CURSOR_FETCHER_H

#include <postgres.h>

#include "data_fetcher.h"

extern DataFetcher *cursor_fetcher_create_for_rel(TSConnection *conn, Relation rel,
												  List *retrieved_attrs, const char *stmt,
												  StmtParams *params);
extern DataFetcher *cursor_fetcher_create_for_scan(TSConnection *conn, ScanState *ss,
												   List *retrieved_attrs, const char *stmt,
												   StmtParams *params, FetchMode mode);

#endif /* TIMESCALEDB_TSL_CURSOR_FETCHER_H */
