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

/*  Create socket and connect */
int
plain_connect(Connection *conn, const char *host, int port)
{
	struct addrinfo *server_ip;
	struct sockaddr_in serv_info;
	struct sockaddr_in *temp;
	struct timeval timeouts = {
		.tv_sec = DEFAULT_TIMEOUT_SEC,
	};
	int			ret;

	ret = socket(AF_INET, SOCK_STREAM, 0);

	if (ret < 0)
		elog(ERROR, "connection library: could not create a socket");
	else
		conn->sock = ret;

	/*
	 * Set send / recv timeout so that write and read don't block forever. Set
	 * separately so that one of the actions failing doesn't block the other.
	 */
	if (setsockopt(conn->sock, SOL_SOCKET, SO_RCVTIMEO, &timeouts, sizeof(struct timeval)) != 0)
		elog(ERROR, "connection library: could not set recv timeouts on SSL sockets");
	if (setsockopt(conn->sock, SOL_SOCKET, SO_SNDTIMEO, &timeouts, sizeof(struct timeval)) != 0)
		elog(ERROR, "connection library: could not set send timeouts on SSL sockets");

	/* Lookup the endpoint ip address */
	if (getaddrinfo(host, NULL, NULL, &server_ip) < 0 || server_ip == NULL)
		elog(ERROR, "connection library: could not get IP of endpoint");
	memset(&serv_info, 0, sizeof(serv_info));
	serv_info.sin_family = AF_INET;
	serv_info.sin_port = htons(port);
	temp = (struct sockaddr_in *) (server_ip->ai_addr);

	memcpy(&serv_info.sin_addr.s_addr, &temp->sin_addr.s_addr, sizeof(serv_info.sin_addr.s_addr));

	freeaddrinfo(server_ip);

	/* connect the socket */
	ret = connect(conn->sock, (struct sockaddr *) &serv_info, sizeof(serv_info));
	if (ret < 0)
		elog(ERROR, "connection library: could not connect to endpoint");
	return 0;
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
