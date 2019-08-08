/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_TSL_DIST_UTIL_H
#define TIMESCALEDB_TSL_DIST_UTIL_H

#include <postgres.h>
#include <fmgr.h>

typedef enum DistUtilMembershipStatus
{
	DIST_MEMBER_NONE,	/* Database doesn't belong to a distributed database */
	DIST_MEMBER_BACKEND, /* Database is a backend node */
	DIST_MEMBER_FRONTEND /* Database is a frontend node */
} DistUtilMembershipStatus;

DistUtilMembershipStatus dist_util_membership(void);

void dist_util_set_as_frontend(void);
bool dist_util_set_id(Datum dist_id);
Datum dist_util_get_id(void);
bool dist_util_remove_from_db(void);

const char *dist_util_internal_key_name(void);

void dist_util_set_peer_id(Datum dist_id);
bool dist_util_is_frontend_session(void);

Datum dist_util_remote_hypertable_info(PG_FUNCTION_ARGS);

void validate_data_node_settings(void);

#endif /* TIMESCALEDB_TSL_CHUNK_API_H */
