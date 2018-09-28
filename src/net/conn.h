#ifndef TIMESCALEDB_NET_CONN_H
#define TIMESCALEDB_NET_CONN_H

#include <pg_config.h>

typedef enum ConnectionType ConnectionType;
typedef struct ConnOps ConnOps;
typedef struct Connection Connection;

enum ConnectionType
{
	CONNECTION_PLAIN,
	CONNECTION_SSL,
	CONNECTION_MOCK,
	_CONNECTION_MAX,
};

struct Connection
{
	ConnectionType type;
#ifdef WIN32
	SOCKET		sock;
#else
	int			sock;
#endif
	ConnOps    *ops;
	int			err;
};

extern Connection *connection_create(ConnectionType type);
extern int	connection_connect(Connection *conn, const char *host, const char *servname, int port);
extern ssize_t connection_read(Connection *conn, char *buf, size_t buflen);
extern ssize_t connection_write(Connection *conn, const char *buf, size_t writelen);
extern void connection_close(Connection *conn);
extern void connection_destroy(Connection *conn);
extern int	connection_set_timeout_millis(Connection *conn, unsigned long millis);
extern const char *connection_get_and_clear_error(Connection *conn);

/*  Called in init.c */
extern void _connection_init(void);
extern void _connection_fini(void);

#endif							/* TIMESCALEDB_NET_CONN_H */
