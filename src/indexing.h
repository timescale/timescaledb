/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#pragma once

#include <postgres.h>
#include <nodes/parsenodes.h>
#include <nodes/pg_list.h>

#include "dimension.h"
#include "export.h"

extern void ts_indexing_verify_columns(const Hyperspace *hs, const List *indexelems);
extern void ts_indexing_verify_index(const Hyperspace *hs, const IndexStmt *stmt);
extern void ts_indexing_verify_indexes(const Hypertable *ht);
extern void ts_indexing_create_default_indexes(const Hypertable *ht);
extern ObjectAddress ts_indexing_root_table_create_index(IndexStmt *stmt, const char *queryString,
														 bool is_multitransaction);
extern TSDLLEXPORT Oid ts_indexing_find_clustered_index(Oid table_relid);

extern void ts_indexing_mark_as_valid(Oid index_id);
extern bool ts_indexing_mark_as_invalid(Oid index_id);
extern bool TSDLLEXPORT ts_indexing_relation_has_primary_or_unique_index(Relation htrel);
