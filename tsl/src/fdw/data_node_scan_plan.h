/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_TSL_FDW_DATA_NODE_SCAN_H
#define TIMESCALEDB_TSL_FDW_DATA_NODE_SCAN_H

#include <postgres.h>
#include <nodes/plannodes.h>
#include <optimizer/cost.h>

#define DATA_NODE_SCAN_PATH_NAME "DataNodeScanPath"

extern void data_node_scan_add_node_paths(PlannerInfo *root, RelOptInfo *hyper_rel);
extern void data_node_scan_create_upper_paths(PlannerInfo *root, UpperRelationKind stage,
											  RelOptInfo *input_rel, RelOptInfo *output_rel,
											  void *extra);

/* Indexes of fields in ForeignScan->custom_private */
typedef enum
{
	DataNodeScanFdwPrivate,
	DataNodeScanSystemcol,
	DataNodeScanFetcherType,
} DataNodeScanPrivateIndex;

#endif /* TIMESCALEDB_TSL_FDW_DATA_NODE_SCAN_H */
