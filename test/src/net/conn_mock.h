/*
 * Copyright (c) 2016-2018  Timescale, Inc. All Rights Reserved.
 *
 * This file is licensed under the Apache License,
 * see LICENSE-APACHE at the top level directory.
 */
#ifndef TIMESCALEDB_CONN_MOCK_H
#define TIMESCALEDB_CONN_MOCK_H

#include <sys/socket.h>

typedef struct Connection Connection;

extern ssize_t ts_connection_mock_set_recv_buf(Connection *conn, char *buf, size_t buflen);

#endif							/* TIMESCALEDB_CONN_MOCK_H */
