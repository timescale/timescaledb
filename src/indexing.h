/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_INDEXING_H
#define TIMESCALEDB_INDEXING_H

#include <postgres.h>
#include <nodes/pg_list.h>
#include <nodes/parsenodes.h>

#include "export.h"
#include "dimension.h"

extern void ts_indexing_verify_columns(Hyperspace *hs, List *indexelems);
extern void ts_indexing_verify_index(Hyperspace *hs, IndexStmt *stmt);
extern void ts_indexing_verify_indexes(Hypertable *ht);
extern void ts_indexing_create_default_indexes(Hypertable *ht);
extern ObjectAddress ts_indexing_root_table_create_index(IndexStmt *stmt, const char *queryString,
														 bool is_multitransaction,
														 bool is_distributed);
extern TSDLLEXPORT Oid ts_indexing_find_clustered_index(Oid table_relid);

extern void ts_indexing_mark_as_valid(Oid index_id);
extern bool ts_indexing_mark_as_invalid(Oid index_id);

#endif /* TIMESCALEDB_INDEXING_H */
