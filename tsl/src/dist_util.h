/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_TSL_DIST_UTIL_H
#define TIMESCALEDB_TSL_DIST_UTIL_H

#include <postgres.h>
#include <fmgr.h>
#include "hypertable.h"

typedef enum DistUtilMembershipStatus
{
	DIST_MEMBER_NONE,		/* Database doesn't belong to a distributed database */
	DIST_MEMBER_DATA_NODE,  /* Database is a data node */
	DIST_MEMBER_ACCESS_NODE /* Database is an access node */
} DistUtilMembershipStatus;

DistUtilMembershipStatus dist_util_membership(void);
const char *dist_util_membership_str(DistUtilMembershipStatus status);

void dist_util_set_as_access_node(void);
bool dist_util_set_id(Datum dist_id);
Datum dist_util_get_id(void);
bool dist_util_remove_from_db(void);

const char *dist_util_internal_key_name(void);

void dist_util_set_peer_id(Datum dist_id);
bool dist_util_is_access_node_session_on_data_node(void);

Datum dist_util_remote_hypertable_info(PG_FUNCTION_ARGS);
Datum dist_util_remote_chunk_info(PG_FUNCTION_ARGS);
Datum dist_util_remote_compressed_chunk_info(PG_FUNCTION_ARGS);
Datum dist_util_remote_hypertable_index_info(PG_FUNCTION_ARGS);

void validate_data_node_settings(void);
bool dist_util_is_compatible_version(const char *data_node_version, const char *access_node_version,
									 bool *is_old_version);

#endif /* TIMESCALEDB_TSL_DIST_UTIL_H */
