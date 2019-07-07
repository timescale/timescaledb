/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_TSL_REMOTE_UTILS_H
#define TIMESCALEDB_TSL_REMOTE_UTILS_H

#include <postgres.h>
#include <foreign/foreign.h>

extern int set_transmission_modes(void);
extern void reset_transmission_modes(int nestlevel);
extern UserMapping *get_user_mapping(Oid userid, Oid serverid, bool missing_ok);

#endif /* TIMESCALEDB_TSL_REMOTE_UTILS_H */
