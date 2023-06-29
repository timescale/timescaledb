/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

/*
 * This file contains source code that was copied and/or modified from
 * the PostgreSQL database, which is licensed under the open-source
 * PostgreSQL License. Please see the NOTICE at the top level
 * directory for a copy of the PostgreSQL License.
 */

#pragma once

#include <postgres.h>

#include <commands/explain.h>
#include <nodes/execnodes.h>
#include <nodes/pg_list.h>

void ts_show_scan_qual(List *qual, const char *qlabel, PlanState *planstate, List *ancestors,
					   ExplainState *es);

void ts_show_instrumentation_count(const char *qlabel, int which, PlanState *planstate,
								   ExplainState *es);
