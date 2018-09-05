#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>
#include <postgres.h>
#include <pg_config.h>

#include "conn_internal.h"
#include "conn_plain.h"

#define DEFAULT_TIMEOUT_SEC	3
#define MAX_PORT 65535

/*  Create socket and connect */
int
plain_connect(Connection *conn, const char *host, const char *servname, int port)
{
	char		strport[6];
	struct addrinfo *ainfo,
				hints = {
		.ai_family = PF_UNSPEC,
		.ai_socktype = SOCK_STREAM,
	};
	struct timeval timeouts = {
		.tv_sec = DEFAULT_TIMEOUT_SEC,
	};
	int			ret;

	if (NULL == servname && (port <= 0 || port > MAX_PORT))
		return -1;

	/* Explicit port given. Use it instead of servname */
	if (port > 0 && port <= MAX_PORT)
	{
		snprintf(strport, sizeof(strport), "%d", port);
		servname = strport;
		hints.ai_flags = AI_NUMERICSERV;
	}

	/* Lookup the endpoint ip address */
	ret = getaddrinfo(host, servname, &hints, &ainfo);

	if (ret < 0)
		return ret;

	ret = socket(ainfo->ai_family, ainfo->ai_socktype, ainfo->ai_protocol);

	if (ret < 0)
		goto out;

	conn->sock = ret;

	/*
	 * Set send / recv timeout so that write and read don't block forever. Set
	 * separately so that one of the actions failing doesn't block the other.
	 */
	ret = setsockopt(conn->sock, SOL_SOCKET, SO_RCVTIMEO, (const char *) &timeouts, sizeof(struct timeval));

	if (ret < 0)
		goto out;

	ret = setsockopt(conn->sock, SOL_SOCKET, SO_SNDTIMEO, (const char *) &timeouts, sizeof(struct timeval));

	if (ret < 0)
		goto out;

	/* connect the socket */
	ret = connect(conn->sock, ainfo->ai_addr, ainfo->ai_addrlen);

out:
	freeaddrinfo(ainfo);

	return ret;
}

static ssize_t
plain_write(Connection *conn, const char *buf, size_t writelen)
{
	int			ret = send(conn->sock, buf, writelen, 0);

	if (ret < 0)
		elog(ERROR, "could not send on a socket");

	return ret;
}

static ssize_t
plain_read(Connection *conn, char *buf, size_t buflen)
{
	int			ret = recv(conn->sock, buf, buflen, 0);

	if (ret < 0)
		elog(ERROR, "could not read from a socket");

	return ret;
}

void
plain_close(Connection *conn)
{
	close(conn->sock);
}

static ConnOps plain_ops = {
	.size = sizeof(Connection),
	.init = NULL,
	.connect = plain_connect,
	.close = plain_close,
	.write = plain_write,
	.read = plain_read,
};

extern void _conn_plain_init(void);
extern void _conn_plain_fini(void);

void
_conn_plain_init(void)
{
	connection_register(CONNECTION_PLAIN, &plain_ops);
}

void
_conn_plain_fini(void)
{
}
