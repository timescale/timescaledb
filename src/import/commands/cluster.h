/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */

/*
 * This file contains source code that was copied and/or modified from
 * the PostgreSQL database, which is licensed under the open-source
 * PostgreSQL License. Please see the NOTICE at the top level
 * directory for a copy of the PostgreSQL License.
 */
#pragma once

#include <postgres.h>
#include <nodes/pg_list.h>
#include <utils/rel.h>

#include "export.h"

extern TSDLLEXPORT void ts_finish_heap_swap(Oid OIDOldHeap, Oid OIDNewHeap, bool is_system_catalog,
											bool swap_toast_by_content, bool check_constraints,
											bool is_internal, bool reindex, TransactionId frozenXid,
											MultiXactId cutoffMulti, char newrelpersistence);
extern TSDLLEXPORT void ts_swap_relation_files(Oid r1, Oid r2, bool target_is_pg_class,
											   bool swap_toast_by_content, bool is_internal,
											   TransactionId frozenXid, MultiXactId cutoffMulti,
											   Oid *mapped_tables);
