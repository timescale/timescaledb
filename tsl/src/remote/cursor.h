/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_TSL_REMOTE_CURSOR_H
#define TIMESCALEDB_TSL_REMOTE_CURSOR_H

#include <postgres.h>
#include <utils/memutils.h>
#include <access/tupdesc.h>
#include <nodes/relation.h>

#include "connection.h"
#include "stmt_params.h"
#include "data_format.h"

typedef struct Cursor Cursor;

extern Cursor *remote_cursor_create_for_rel(TSConnection *conn, Relation rel, List *retrieved_attrs,
											const char *stmt, StmtParams *params);
extern Cursor *remote_cursor_create_for_scan(TSConnection *conn, ScanState *ss,
											 List *retrieved_attrs, const char *stmt,
											 StmtParams *params);
extern bool remote_cursor_set_fetch_size(Cursor *cursor, unsigned int fetch_size);
extern void remote_cursor_set_tuple_memcontext(Cursor *cursor, MemoryContext mctx);
extern int remote_cursor_fetch_data(Cursor *cursor);
extern HeapTuple remote_cursor_get_next_tuple(Cursor *cursor);
extern HeapTuple remote_cursor_get_tuple(Cursor *cursor, int row);
extern void remote_cursor_close(Cursor *cursor);
extern void remote_cursor_rewind(Cursor *cursor);

#endif /* TIMESCALEDB_TSL_REMOTE_CURSOR_H */
