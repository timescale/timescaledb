/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_TSL_TEST_REMOTE_CONNECTION_H
#define TIMESCALEDB_TSL_TEST_REMOTE_CONNECTION_H
#include <postgres.h>
#include <libpq-fe.h>

extern pid_t remote_connecton_get_remote_pid(PGconn *conn);
extern char *remote_connecton_get_application_name(PGconn *conn);

#endif /* TIMESCALEDB_TSL_TEST_REMOTE_CONNECTION_H */
