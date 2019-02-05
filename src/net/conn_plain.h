/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_CONN_PLAIN_H
#define TIMESCALEDB_CONN_PLAIN_H

typedef struct Connection Connection;

#ifdef WIN32
#define IS_SOCKET_ERROR(err) (err == SOCKET_ERROR)
#else
#define SOCKET_ERROR -1
#define IS_SOCKET_ERROR(err) (err < 0)
#endif

extern int ts_plain_connect(Connection *conn, const char *host, const char *servname, int port);
extern void ts_plain_close(Connection *conn);
extern int ts_plain_set_timeout(Connection *conn, unsigned long millis);
extern const char *ts_plain_errmsg(Connection *conn);

#endif /* TIMESCALEDB_CONN_PLAIN_H */
