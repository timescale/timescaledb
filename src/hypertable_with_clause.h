/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_HYPERTABLE_WITH_CLAUSE_H
#define TIMESCALEDB_HYPERTABLE_WITH_CLAUSE_H

#include <postgres.h>
#include <catalog/pg_type.h>
#include "ts_catalog/catalog.h"
#include "chunk.h"
#include "with_clause_parser.h"

extern TSDLLEXPORT void ts_hypertable_create_using_set_clause(Oid relid, const List *defelems);
extern TSDLLEXPORT void ts_hypertable_alter_using_set_clause(Hypertable *ht, const List *defelems);

#endif
