#ifndef TIMESCALEDB_CONN_PLAIN_H
#define TIMESCALEDB_CONN_PLAIN_H

typedef struct Connection Connection;

#ifdef WIN32
#define IS_SOCKET_ERROR(err) (err == SOCKET_ERROR)
#else
#define SOCKET_ERROR -1
#define IS_SOCKET_ERROR(err) (err < 0)
#endif

extern int	plain_connect(Connection *conn, const char *host, const char *servname, int port);
extern void plain_close(Connection *conn);
extern int	plain_set_timeout(Connection *conn, unsigned long millis);
extern const char *plain_errmsg(Connection *conn);

#endif							/* TIMESCALEDB_CONN_PLAIN_H */
