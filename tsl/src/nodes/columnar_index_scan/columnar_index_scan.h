/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

#define COLUMNAR_INDEX_SCAN_NAME "ColumnarIndexScan"

#include <postgres.h>
#include <nodes/plannodes.h>

extern void _columnar_index_scan_init(void);
extern Plan *try_insert_columnar_index_scan_node(Plan *plan, List *rtable);
extern Node *columnar_index_scan_state_create(CustomScan *cscan);
