#ifndef TIMESCALEDB_TABLESPACE_H
#define TIMESCALEDB_TABLESPACE_H

#include <postgres.h>

typedef struct Tablespace
{
	int32		tablespace_id;
	Oid			tablespace_oid;
} Tablespace;

typedef struct Tablespaces
{
	int			capacity;
	int			num_tablespaces;
	Tablespace *tablespaces;
} Tablespaces;

extern Tablespace *tablespaces_add(Tablespaces *tablespaces, int32 tspc_id, Oid tspc_oid);
extern Tablespaces *tablespace_scan(int32 hypertable_id);

#endif   /* TIMESCALEDB_TABLESPACE_H */
