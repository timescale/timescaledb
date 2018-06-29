#ifndef TIMESCALEDB_COPY_H
#define TIMESCALEDB_COPY_H

#include <postgres.h>
#include <nodes/parsenodes.h>
#include <access/xact.h>
#include <executor/executor.h>
#include <commands/copy.h>

typedef struct Hypertable Hypertable;

void		timescaledb_DoCopy(const CopyStmt *stmt, const char *queryString, uint64 *processed, Hypertable *ht);
void		timescaledb_move_from_table_to_chunks(Hypertable *ht, LOCKMODE lockmode);

#endif							/* TIMESCALEDB_COPY_H */
