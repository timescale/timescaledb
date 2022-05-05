/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_TSL_CHUNK_COPY_H
#define TIMESCALEDB_TSL_CHUNK_COPY_H

extern void chunk_copy(Oid chunk_relid, const char *src_node, const char *dst_node,
					   const char *op_id, bool delete_on_src_node);
extern void chunk_copy_cleanup(const char *operation_id);

#endif /* TIMESCALEDB_TSL_CHUNK_COPY_H */
