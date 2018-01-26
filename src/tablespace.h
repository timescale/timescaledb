#ifndef TIMESCALEDB_TABLESPACE_H
#define TIMESCALEDB_TABLESPACE_H

#include <postgres.h>
#include <nodes/parsenodes.h>

#include "catalog.h"

typedef struct Tablespace
{
	FormData_tablespace fd;
	Oid			tablespace_oid;
} Tablespace;

typedef struct Tablespaces
{
	int			capacity;
	int			num_tablespaces;
	Tablespace *tablespaces;
} Tablespaces;

extern Tablespace *tablespaces_add(Tablespaces *tablespaces, FormData_tablespace *form, Oid tspc_oid);
extern bool tablespaces_delete(Tablespaces *tspcs, Oid tspc_oid);
extern int	tablespaces_clear(Tablespaces *tspcs);
extern bool tablespaces_contain(Tablespaces *tspcs, Oid tspc_oid);
extern Tablespaces *tablespace_scan(int32 hypertable_id);
extern void tablespace_attach_internal(Name tspcname, Oid hypertable_oid, bool if_not_attached);
extern int	tablespace_delete(int32 hypertable_id, const char *tspcname);
extern int	tablespace_count_attached(const char *tspcname);
extern void tablespace_validate_revoke(GrantStmt *stmt);
extern void tablespace_validate_revoke_role(GrantRoleStmt *stmt);

#endif							/* TIMESCALEDB_TABLESPACE_H */
