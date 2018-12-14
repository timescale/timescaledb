/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_HYPERTABLE_SERVER_H
#define TIMESCALEDB_HYPERTABLE_SERVER_H

#include "catalog.h"
#include "export.h"

typedef struct HypertableServer
{
	FormData_hypertable_server fd;
	Oid foreign_server_oid;
} HypertableServer;

extern TSDLLEXPORT List *ts_hypertable_server_scan(int32 hypertable_id, MemoryContext mctx);
extern TSDLLEXPORT int ts_hypertable_server_delete_by_hypertable_id(int32 hypertable_id);
extern TSDLLEXPORT int ts_hypertable_server_delete_by_servername(const char *servername);
extern TSDLLEXPORT void ts_hypertable_server_insert_multi(List *hypertable_servers);

#endif /* TIMESCALEDB_HYPERTABLE_SERVER_H */
