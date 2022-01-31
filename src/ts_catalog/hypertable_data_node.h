/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_HYPERTABLE_DATA_NODE_H
#define TIMESCALEDB_HYPERTABLE_DATA_NODE_H

#include "ts_catalog/catalog.h"
#include "export.h"

typedef struct HypertableDataNode
{
	FormData_hypertable_data_node fd;
	Oid foreign_server_oid;
} HypertableDataNode;

extern TSDLLEXPORT List *ts_hypertable_data_node_scan(int32 hypertable_id, MemoryContext mctx);
extern TSDLLEXPORT List *ts_hypertable_data_node_scan_by_node_name(const char *node_name,
																   MemoryContext mctx);
extern TSDLLEXPORT int ts_hypertable_data_node_delete_by_hypertable_id(int32 hypertable_id);
extern TSDLLEXPORT int ts_hypertable_data_node_delete_by_node_name(const char *node_name);
extern TSDLLEXPORT void ts_hypertable_data_node_insert_multi(List *hypertable_data_nodes);
extern TSDLLEXPORT int
ts_hypertable_data_node_delete_by_node_name_and_hypertable_id(const char *node_name,
															  int32 hypertable_id);
extern TSDLLEXPORT int
ts_hypertable_data_node_update(const HypertableDataNode *hypertable_data_node);

#endif /* TIMESCALEDB_HYPERTABLE_DATA_NODE_H */
