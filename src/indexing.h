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
#include <utils/builtins.h>

#include "export.h"
#include "catalog.h"
#include "dimension.h"

typedef struct OptionalIndexInfo
{
	FormData_optional_index_info fd;
} OptionalIndexInfo;

extern void ts_indexing_verify_columns(Hyperspace *hs, List *indexelems);
extern void ts_indexing_verify_index(Hyperspace *hs, IndexStmt *stmt);
extern void ts_indexing_verify_indexes(Hypertable *ht);
extern void ts_indexing_create_default_indexes(Hypertable *ht);
extern ObjectAddress ts_indexing_root_table_create_index(IndexStmt *stmt, const char *queryString,
														 bool is_multitransaction);
extern TSDLLEXPORT Oid ts_indexing_find_clustered_index(Oid table_relid);

extern void ts_indexing_mark_as_valid(Oid index_id);
extern bool ts_indexing_mark_as_invalid(Oid index_id);

extern TSDLLEXPORT OptionalIndexInfo *ts_indexing_optional_info_find_by_index_name(Name index_name);
void TSDLLEXPORT ts_indexing_optional_info_insert(OptionalIndexInfo *policy);
bool TSDLLEXPORT ts_indexing_optional_info_delete_by_index_name(Name index_name);

static inline OptionalIndexInfo *
ts_optional_index_info_alloc(char *name)
{
	OptionalIndexInfo *optional_index_info = palloc0(sizeof(*optional_index_info));
	namestrcpy(&optional_index_info->fd.hypertable_index_name, name);
	return optional_index_info;
}

#endif /* TIMESCALEDB_INDEXING_H */
