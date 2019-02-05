/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_NET_CONN_H
#define TIMESCALEDB_NET_CONN_H

#include <pg_config.h>

typedef struct ConnOps ConnOps;

typedef enum ConnectionType
{
	CONNECTION_PLAIN,
	CONNECTION_SSL,
	CONNECTION_MOCK,
	_CONNECTION_MAX,
} ConnectionType;

typedef struct Connection
{
	ConnectionType type;
#ifdef WIN32
	SOCKET sock;
#else
	int sock;
#endif
	ConnOps *ops;
	int err;
} Connection;

extern Connection *ts_connection_create(ConnectionType type);
extern int ts_connection_connect(Connection *conn, const char *host, const char *servname,
								 int port);
extern ssize_t ts_connection_read(Connection *conn, char *buf, size_t buflen);
extern ssize_t ts_connection_write(Connection *conn, const char *buf, size_t writelen);
extern void ts_connection_close(Connection *conn);
extern void ts_connection_destroy(Connection *conn);
extern int ts_connection_set_timeout_millis(Connection *conn, unsigned long millis);
extern const char *ts_connection_get_and_clear_error(Connection *conn);

/*  Called in init.c */
extern void ts_connection_init(void);
extern void ts_connection_fini(void);

#endif /* TIMESCALEDB_NET_CONN_H */
