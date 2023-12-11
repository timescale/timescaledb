/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

#include <postgres.h>

#include "data_fetcher.h"

extern DataFetcher *prepared_statement_fetcher_create_for_scan(TSConnection *conn, const char *stmt,
															   StmtParams *params,
															   TupleFactory *tf);
