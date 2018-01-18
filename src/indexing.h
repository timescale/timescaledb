#ifndef TIMESCALEDB_INDEXING_H
#define TIMESCALEDB_INDEXING_H

#include <postgres.h>
#include <nodes/pg_list.h>
#include <nodes/parsenodes.h>

#include "dimension.h"

extern void indexing_verify_columns(Hyperspace *hs, List *indexelems);
extern void indexing_verify_index(Hyperspace *hs, IndexStmt *stmt);

#endif							/* TIMESCALEDB_INDEXING_H */
