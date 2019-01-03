/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TSL_TEST_REMOTE_CONNECTION_H
#define TSL_TEST_REMOTE_CONNECTION_H

#include <postgres.h>
#include <libpq-fe.h>

#include <remote/connection.h>

extern PGconn *get_connection(void);

#endif /* TSL_TEST_REMOTE_CONNECTION_H */
