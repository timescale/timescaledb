/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

#include <postgres.h>
#include <libpq-fe.h>

#include <remote/connection.h>

extern TSConnection *get_connection(void);
extern pid_t remote_connection_get_remote_pid(const TSConnection *conn);
extern char *remote_connection_get_application_name(const TSConnection *conn);
