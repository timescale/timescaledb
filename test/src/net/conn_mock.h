/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_CONN_MOCK_H
#define TIMESCALEDB_CONN_MOCK_H

#include <sys/socket.h>

typedef struct Connection Connection;

extern ssize_t ts_connection_mock_set_recv_buf(Connection *conn, char *buf, size_t buflen);

#endif /* TIMESCALEDB_CONN_MOCK_H */
