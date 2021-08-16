/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <unistd.h>
#include <postgres.h>

#include <sys/socket.h>
#include <sys/time.h>

#include "conn_internal.h"
#include "conn_plain.h"
#include "compat/compat.h"
#include "port.h"

#define DEFAULT_TIMEOUT_MSEC 3000
#define MAX_PORT 65535

static void
set_error(int err)
{
#ifdef WIN32
	WSASetLastError(err);
#else
	errno = err;
#endif
}

static int
get_error(void)
{
#ifdef WIN32
	return WSAGetLastError();
#else
	return errno;
#endif
}

/*  Create socket and connect */
int
ts_plain_connect(Connection *conn, const char *host, const char *servname, int port)
{
	char strport[6];
	struct addrinfo *ainfo, hints = {
		.ai_family = AF_UNSPEC,
		.ai_socktype = SOCK_STREAM,
	};
	int ret;

	if (NULL == servname && (port <= 0 || port > MAX_PORT))
	{
		set_error(EINVAL);
		return -1;
	}

	/* Explicit port given. Use it instead of servname */
	if (port > 0 && port <= MAX_PORT)
	{
		snprintf(strport, sizeof(strport), "%d", port);
		servname = strport;
		hints.ai_flags = AI_NUMERICSERV;
	}

	/* Lookup the endpoint ip address */
	ret = getaddrinfo(host, servname, &hints, &ainfo);

	if (ret != 0)
	{
		ret = SOCKET_ERROR;

#ifdef WIN32
		WSASetLastError(WSAHOST_NOT_FOUND);
#else

		/*
		 * The closest match for a name resolution error. Strictly, this error
		 * should not be used here, but to fix we need to support using
		 * gai_strerror()
		 */
		errno = EADDRNOTAVAIL;
#endif
		goto out;
	}

#ifdef WIN32

	/*
	 * PostgreSQL redefines the socket() call on Windows and creates a
	 * non-blocking socket by default. We avoid this by calling WSASocket
	 * directly.
	 */
	conn->sock = WSASocket(ainfo->ai_family,
						   ainfo->ai_socktype,
						   ainfo->ai_protocol,
						   NULL,
						   0,
						   WSA_FLAG_OVERLAPPED);

	if (conn->sock == INVALID_SOCKET)
		ret = SOCKET_ERROR;
#else
	ret = conn->sock = socket(ainfo->ai_family, ainfo->ai_socktype, ainfo->ai_protocol);
#endif

	if (IS_SOCKET_ERROR(ret))
		goto out_addrinfo;

	/*
	 * Set send / recv timeout so that write and read don't block forever. Set
	 * separately so that one of the actions failing doesn't block the other.
	 */
	if (ts_plain_set_timeout(conn, DEFAULT_TIMEOUT_MSEC) < 0)
	{
		ret = SOCKET_ERROR;
		goto out_addrinfo;
	}

#ifdef WIN32
	ret = WSAConnect(conn->sock, ainfo->ai_addr, ainfo->ai_addrlen, NULL, NULL, NULL, NULL);
#else
	/* connect the socket */
	ret = connect(conn->sock, ainfo->ai_addr, ainfo->ai_addrlen);
#endif

out_addrinfo:
	freeaddrinfo(ainfo);

out:
	if (IS_SOCKET_ERROR(ret))
	{
		conn->err = ret;
		return -1;
	}

	return 0;
}

static ssize_t
plain_write(Connection *conn, const char *buf, size_t writelen)
{
	ssize_t ret;
#ifdef WIN32
	DWORD b;
	WSABUF wbuf = {
		.len = writelen,
		.buf = (char *) buf,
	};

	conn->err = WSASend(conn->sock, &wbuf, 1, &b, 0, NULL, NULL);

	if (IS_SOCKET_ERROR(conn->err))
		ret = -1;
	else
		ret = b;
#else
	ret = send(conn->sock, buf, writelen, 0);

	if (ret < 0)
		conn->err = ret;
#endif

	return ret;
}

static ssize_t
plain_read(Connection *conn, char *buf, size_t buflen)
{
	ssize_t ret;
#ifdef WIN32
	DWORD b, flags = 0;
	WSABUF wbuf = {
		.len = buflen,
		.buf = buf,
	};

	conn->err = WSARecv(conn->sock, &wbuf, 1, &b, &flags, NULL, NULL);

	if (IS_SOCKET_ERROR(conn->err))
		ret = -1;
	else
		ret = b;
#else
	ret = recv(conn->sock, buf, buflen, 0);

	if (ret < 0)
		conn->err = ret;
#endif

	return ret;
}

void
ts_plain_close(Connection *conn)
{
#ifdef WIN32
	closesocket(conn->sock);
#else
	close(conn->sock);
#endif
}

int
ts_plain_set_timeout(Connection *conn, unsigned long millis)
{
#ifdef WIN32
	/* Timeout is in milliseconds on Windows */
	DWORD timeout = millis;
	int optlen = sizeof(DWORD);
#else
	/* we deliberately use a long constant here instead of a fixed width one because tv_sec is
	 * declared as a long */
	struct timeval timeout = {
		.tv_sec = millis / 1000L,
		.tv_usec = (millis % 1000L) * 1000L,
	};
	int optlen = sizeof(struct timeval);
#endif

	/*
	 * Set send / recv timeout so that write and read don't block forever. Set
	 * separately so that one of the actions failing doesn't block the other.
	 */
	conn->err = setsockopt(conn->sock, SOL_SOCKET, SO_RCVTIMEO, (const char *) &timeout, optlen);

	if (conn->err != 0)
		return -1;

	conn->err = setsockopt(conn->sock, SOL_SOCKET, SO_SNDTIMEO, (const char *) &timeout, optlen);

	if (conn->err != 0)
		return -1;

	return 0;
}

const char *
ts_plain_errmsg(Connection *conn)
{
	const char *errmsg = "no connection error";

	if (IS_SOCKET_ERROR(conn->err))
		errmsg = strerror(get_error());
	conn->err = 0;

	return errmsg;
}

static ConnOps plain_ops = {
	.size = sizeof(Connection),
	.init = NULL,
	.connect = ts_plain_connect,
	.close = ts_plain_close,
	.write = plain_write,
	.read = plain_read,
	.errmsg = ts_plain_errmsg,
};

extern void _conn_plain_init(void);
extern void _conn_plain_fini(void);

void
_conn_plain_init(void)
{
	/*
	 * WSAStartup is required on Windows before using the Winsock API.
	 * However, PostgreSQL already handles this for us, so it is disabled here
	 * by default. Set WSA_STARTUP_ENABLED to perform this initialization
	 * anyway.
	 */
#if defined(WIN32) && defined(WSA_STARTUP_ENABLED)
	WSADATA wsadata;
	int res;

	res = WSAStartup(MAKEWORD(2, 2), &wsadata);

	if (res != 0)
	{
		elog(ERROR, "WSAStartup failed: %d", res);
		return;
	}
#endif
	ts_connection_register(CONNECTION_PLAIN, &plain_ops);
}

void
_conn_plain_fini(void)
{
#if defined(WIN32) && defined(WSA_STARTUP_ENABLED)
	int ret = WSACleanup();

	if (ret != 0)
		elog(WARNING, "WSACleanup failed");
#endif
}
