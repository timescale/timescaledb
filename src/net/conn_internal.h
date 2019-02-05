/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_CONN_INTERNAL_H
#define TIMESCALEDB_CONN_INTERNAL_H

#include "conn.h"

typedef struct ConnOps
{
	size_t size; /* Size of the connection object */
	int (*init)(Connection *conn);
	int (*connect)(Connection *conn, const char *host, const char *servname, int port);
	void (*close)(Connection *conn);
	ssize_t (*write)(Connection *conn, const char *buf, size_t writelen);
	ssize_t (*read)(Connection *conn, char *buf, size_t readlen);
	int (*set_timeout)(Connection *conn, unsigned long millis);
	const char *(*errmsg)(Connection *conn);
} ConnOps;

extern int ts_connection_register(ConnectionType type, ConnOps *ops);

#endif /* TIMESCALEDB_CONN_INTERNAL_H */
