/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_TSL_DATA_NODE_H
#define TIMESCALEDB_TSL_DATA_NODE_H

#include <foreign/foreign.h>

#include "ts_catalog/catalog.h"
#include "ts_catalog/hypertable_data_node.h"
#include "hypertable.h"
#include "remote/dist_txn.h"

/* Used to skip ACL checks */
#define ACL_NO_CHECK N_ACL_RIGHTS

extern ForeignServer *data_node_get_foreign_server(const char *node_name, AclMode mode,
												   bool fail_on_aclcheck, bool missing_ok);
extern ForeignServer *data_node_get_foreign_server_by_oid(Oid foreign_server_oid, AclMode mode);

extern TSConnection *data_node_get_connection(const char *const data_node,
											  RemoteTxnPrepStmtOption const ps_opt,
											  bool transactional);

extern Datum data_node_add(PG_FUNCTION_ARGS);
extern Datum data_node_delete(PG_FUNCTION_ARGS);
extern Datum data_node_attach(PG_FUNCTION_ARGS);
extern Datum data_node_detach(PG_FUNCTION_ARGS);
extern Datum data_node_alter(PG_FUNCTION_ARGS);
extern Datum data_node_block_new_chunks(PG_FUNCTION_ARGS);
extern Datum data_node_allow_new_chunks(PG_FUNCTION_ARGS);
extern List *data_node_get_node_name_list_with_aclcheck(AclMode mode, bool fail_on_aclcheck);
extern List *data_node_get_filtered_node_name_list(ArrayType *nodearr, AclMode mode,
												   bool fail_on_aclcheck);
extern List *data_node_get_node_name_list(void);
extern void data_node_fail_if_nodes_are_unavailable(void);
extern List *data_node_array_to_node_name_list_with_aclcheck(ArrayType *nodearr, AclMode mode,
															 bool fail_on_aclcheck);
extern List *data_node_array_to_node_name_list(ArrayType *nodearr);
extern List *data_node_oids_to_node_name_list(List *data_node_oids, AclMode mode);
extern void data_node_name_list_check_acl(List *data_node_names, AclMode mode);
extern Datum data_node_ping(PG_FUNCTION_ARGS);

extern HypertableDataNode *data_node_hypertable_get_by_node_name(const Hypertable *ht,
																 const char *node_name,
																 bool attach_check);

/* This should only be used for testing */
extern Datum data_node_add_without_dist_id(PG_FUNCTION_ARGS);

#endif /* TIMESCALEDB_TSL_DATA_NODE_H */
