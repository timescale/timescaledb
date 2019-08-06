/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_TSL_DATA_NODE_H
#define TIMESCALEDB_TSL_DATA_NODE_H

#include <foreign/foreign.h>

#include "catalog.h"

/* Used to skip ACL checks */
#define ACL_NO_CHECK N_ACL_RIGHTS

extern ForeignServer *data_node_get_foreign_server(const char *node_name, AclMode mode,
												   bool missing_ok);
extern ForeignServer *data_node_get_foreign_server_by_oid(Oid foreign_server_oid, AclMode mode);

extern Datum data_node_add(PG_FUNCTION_ARGS);
extern Datum data_node_delete(PG_FUNCTION_ARGS);
extern Datum data_node_attach(PG_FUNCTION_ARGS);
extern Datum data_node_detach(PG_FUNCTION_ARGS);
extern Datum data_node_set_block_new_chunks(PG_FUNCTION_ARGS, bool block);
extern List *data_node_get_node_name_list_with_aclcheck(AclMode mode);
extern List *data_node_get_node_name_list(void);
extern List *data_node_array_to_node_name_list_with_aclcheck(ArrayType *noderarr, AclMode mode);
extern List *data_node_array_to_node_name_list(ArrayType *nodearr);
extern List *data_node_oids_to_node_name_list(List *data_node_oids, AclMode mode);
extern Datum data_node_ping(PG_FUNCTION_ARGS);
extern Datum data_node_set_chunk_default_data_node(PG_FUNCTION_ARGS);

/* This should only be used for testing */
extern Datum data_node_add_without_dist_id(PG_FUNCTION_ARGS);

#endif /* TIMESCALEDB_TSL_DATA_NODE_H */
