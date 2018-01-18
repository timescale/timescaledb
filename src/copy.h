#ifndef TIMESCALEDB_COPY_H
#define TIMESCALEDB_COPY_H

#include <postgres.h>
#include <nodes/parsenodes.h>

typedef struct Hypertable Hypertable;

Oid			timescaledb_DoCopy(const CopyStmt *stmt, const char *queryString, uint64 *processed, Hypertable *ht);

#endif							/* TIMESCALEDB_COPY_H */
