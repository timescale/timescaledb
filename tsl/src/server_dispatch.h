/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_TSL_SERVER_DISPATCH_H
#define TIMESCALEDB_TSL_SERVER_DISPATCH_H

#include <nodes/extensible.h>
#include <nodes/plannodes.h>
#include <nodes/execnodes.h>

#include "fdw/deparse.h"

Path *server_dispatch_path_create(PlannerInfo *root, ModifyTablePath *mtpath, Index hypertable_rti,
								  int subplan_index);

#endif /* TIMESCALEDB_TSL_SERVER_DISPATCH_H */
