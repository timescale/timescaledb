#ifndef TIMESCALEDB_CONN_PLAIN_H
#define TIMESCALEDB_CONN_PLAIN_H

typedef struct Connection Connection;

extern int	plain_connect(Connection *conn, const char *host, const char *servname, int port);
extern void plain_close(Connection *conn);

#endif							/* TIMESCALEDB_CONN_PLAIN_H */
