/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

#include <postgres.h>

extern List *fdw_plan_foreign_modify(PlannerInfo *root, ModifyTable *plan, Index result_relation,
									 int subplan_index);
extern List *get_chunk_data_nodes(Oid relid);
