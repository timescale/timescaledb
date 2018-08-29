#ifndef TIMESCALEDB_NET_CONN_H
#define TIMESCALEDB_NET_CONN_H

#include <stdio.h>
#include <stdlib.h>
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
	int			sock;
	ConnOps    *ops;
} Connection;

extern Connection *connection_create(ConnectionType type);
extern int	connection_connect(Connection *conn, const char *host, const char *servname, int port);
extern ssize_t connection_read(Connection *conn, char *buf, size_t buflen);
extern ssize_t connection_write(Connection *conn, const char *buf, size_t writelen);
extern void connection_close(Connection *conn);
extern void connection_destroy(Connection *conn);

/*  Called in init.c */
extern void _connection_init(void);
extern void _connection_fini(void);

#endif							/* TIMESCALEDB_NET_CONN_H */
