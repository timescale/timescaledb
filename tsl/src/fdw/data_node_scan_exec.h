/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_TSL_FDW_DATA_NODE_SCAN_EXEC_H
#define TIMESCALEDB_TSL_FDW_DATA_NODE_SCAN_EXEC_H

#include <postgres.h>
#include <nodes/plannodes.h>

#include "fdw/scan_exec.h"
#include "remote/async.h"

extern Node *data_node_scan_state_create(CustomScan *cscan);

#endif /* TIMESCALEDB_TSL_FDW_DATA_NODE_SCAN_EXEC_H */
