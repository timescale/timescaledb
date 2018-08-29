#ifndef TIMESCALEDB_CONN_MOCK_H
#define TIMESCALEDB_CONN_MOCK_H

#include <sys/socket.h>

typedef struct Connection Connection;

extern ssize_t connection_mock_set_recv_buf(Connection *conn, char *buf, size_t buflen);

#endif							/* TIMESCALEDB_CONN_MOCK_H */
