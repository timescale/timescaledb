/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_TABLESPACE_H
#define TIMESCALEDB_TABLESPACE_H

#include <postgres.h>
#include <nodes/parsenodes.h>

#include "catalog.h"

typedef struct Tablespace
{
	FormData_tablespace fd;
	Oid tablespace_oid;
} Tablespace;

typedef struct Tablespaces
{
	int capacity;
	int num_tablespaces;
	Tablespace *tablespaces;
} Tablespaces;

extern Tablespace *ts_tablespaces_add(Tablespaces *tablespaces, FormData_tablespace *form,
									  Oid tspc_oid);
extern bool ts_tablespaces_contain(Tablespaces *tspcs, Oid tspc_oid);
extern Tablespaces *ts_tablespace_scan(int32 hypertable_id);
extern TSDLLEXPORT void ts_tablespace_attach_internal(Name tspcname, Oid hypertable_oid,
													  bool if_not_attached);
extern int ts_tablespace_delete(int32 hypertable_id, const char *tspcname);
extern int ts_tablespace_count_attached(const char *tspcname);
extern void ts_tablespace_validate_revoke(GrantStmt *stmt);
extern void ts_tablespace_validate_revoke_role(GrantRoleStmt *stmt);

#endif /* TIMESCALEDB_TABLESPACE_H */
