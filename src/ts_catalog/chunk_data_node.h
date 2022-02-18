/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_CHUNK_DATA_NODE_H
#define TIMESCALEDB_CHUNK_DATA_NODE_H

#include "ts_catalog/catalog.h"
#include "export.h"
#include "scan_iterator.h"

typedef struct ChunkDataNode
{
	FormData_chunk_data_node fd;
	Oid foreign_server_oid;
} ChunkDataNode;

extern TSDLLEXPORT List *ts_chunk_data_node_scan_by_chunk_id(int32 chunk_id, MemoryContext mctx);
extern TSDLLEXPORT ChunkDataNode *
ts_chunk_data_node_scan_by_chunk_id_and_node_name(int32 chunk_id, const char *node_name,
												  MemoryContext mctx);
extern TSDLLEXPORT ChunkDataNode *
ts_chunk_data_node_scan_by_remote_chunk_id_and_node_name(int32 chunk_id, const char *node_name,
														 MemoryContext mctx);
extern TSDLLEXPORT void ts_chunk_data_node_insert(const ChunkDataNode *node);
extern void ts_chunk_data_node_insert_multi(List *chunk_data_nodes);
extern int ts_chunk_data_node_delete_by_chunk_id(int32 chunk_id);
extern TSDLLEXPORT int ts_chunk_data_node_delete_by_chunk_id_and_node_name(int32 chunk_id,
																		   const char *node_name);
extern int ts_chunk_data_node_delete_by_node_name(const char *node_name);
extern TSDLLEXPORT List *
ts_chunk_data_node_scan_by_node_name_and_hypertable_id(const char *node_name, int32 hypertable_id,
													   MemoryContext mctx);
extern ScanIterator ts_chunk_data_nodes_scan_iterator_create(MemoryContext result_mcxt);
extern void ts_chunk_data_nodes_scan_iterator_set_chunk_id(ScanIterator *it, int32 chunk_id);

#endif /* TIMESCALEDB_CHUNK_DATA_NODE_H */
