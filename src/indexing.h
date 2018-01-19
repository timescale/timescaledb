#ifndef TIMESCALEDB_INDEXING_H
#define TIMESCALEDB_INDEXING_H

#include <postgres.h>
#include <nodes/pg_list.h>
#include <nodes/parsenodes.h>

#include "dimension.h"

extern void indexing_verify_columns(Hyperspace *hs, List *indexelems);
extern void indexing_verify_index(Hyperspace *hs, IndexStmt *stmt);
extern void indexing_create_and_verify_hypertable_indexes(Hypertable *ht, bool create_default);

#endif							/* TIMESCALEDB_INDEXING_H */
