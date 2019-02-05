/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <pg_config.h>

#include "conn_internal.h"

static ConnOps *conn_ops[_CONNECTION_MAX] = { NULL };

static const char *conn_names[] = {
	[CONNECTION_PLAIN] = "PLAIN",
	[CONNECTION_SSL] = "SSL",
	[CONNECTION_MOCK] = "MOCK",
};

static Connection *
connection_internal_create(ConnectionType type, ConnOps *ops)
{
	Connection *conn = palloc(ops->size);

	if (NULL == conn)
		return NULL;

	memset(conn, 0, ops->size);
	conn->ops = ops;
	conn->type = type;

	return conn;
}

Connection *
ts_connection_create(ConnectionType type)
{
	Connection *conn;

	if (type == _CONNECTION_MAX)
		elog(ERROR, "invalid connection type");

	if (NULL == conn_ops[type])
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("%s connections are not supported", conn_names[type]),
				 errhint("Enable %s support when compiling the extension.", conn_names[type])));

	conn = connection_internal_create(type, conn_ops[type]);

	Assert(NULL != conn);

	if (NULL != conn->ops->init)
		if (conn->ops->init(conn) < 0)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("%s connection could not be initialized", conn_names[type])));

	return conn;
}

/*
 * Connect to a remote endpoint (host, service/port).
 *
 * The connection will be made to the host's service endpoint given by
 * 'servname' (e.g., 'http'), unless a valid port number is given.
 */
int
ts_connection_connect(Connection *conn, const char *host, const char *servname, int port)
{
/* Windows defines 'connect()' as a macro, so we need to undef it here to use it in ops->connect */
#ifdef WIN32
#undef connect
#endif
	return conn->ops->connect(conn, host, servname, port);
}

ssize_t
ts_connection_write(Connection *conn, const char *buf, size_t writelen)
{
	return conn->ops->write(conn, buf, writelen);
}

ssize_t
ts_connection_read(Connection *conn, char *buf, size_t buflen)
{
	return conn->ops->read(conn, buf, buflen);
}

void
ts_connection_close(Connection *conn)
{
	if (NULL != conn->ops)
		conn->ops->close(conn);
}

int
ts_connection_set_timeout_millis(Connection *conn, unsigned long millis)
{
	if (NULL != conn->ops->set_timeout)
		return conn->ops->set_timeout(conn, millis);

	return -1;
}

void
ts_connection_destroy(Connection *conn)
{
	if (conn == NULL)
		return;

	ts_connection_close(conn);
	conn->ops = NULL;
	pfree(conn);
}

int
ts_connection_register(ConnectionType type, ConnOps *ops)
{
	if (type == _CONNECTION_MAX)
		return -1;

	conn_ops[type] = ops;

	return 0;
}

const char *
ts_connection_get_and_clear_error(Connection *conn)
{
	if (NULL != conn->ops->errmsg)
		return conn->ops->errmsg(conn);

	return "unknown connection error";
}
