#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>
#include <postgres.h>
#include <pg_config.h>

#include "conn.h"

#define DEFAULT_TIMEOUT_SEC	3
#define MOCK_MAX_BUF_SIZE	1024

typedef struct ConnOps
{
	int			(*connect) (Connection *conn, const char *host, int port);
	void		(*close) (Connection *conn);
	ssize_t		(*write) (Connection *conn, const char *buf, size_t writelen);
	ssize_t		(*read) (Connection *conn, char *buf, size_t readlen);
} ConnOps;


/*  Create socket and connect */
static int
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
	 * Set send / recv timeout so that write and read don't block forever.
	 * Set separately so that one of the actions failing doesn't block the other.
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
		elog(ERROR, "connection library: could not send on a socket");
	return ret;
}

static ssize_t
plain_read(Connection *conn, char *buf, size_t buflen)
{
	int			ret = recv(conn->sock, buf, buflen, 0);

	if (ret < 0)
		elog(ERROR, "connection library: could not read from a socket");
	return ret;
}

static void
plain_close(Connection *conn)
{
	close(conn->sock);
}

static ConnOps plain_ops = {
	.connect = plain_connect,
	.close = plain_close,
	.write = plain_write,
	.read = plain_read,
};

static Connection *
connection_internal_create(size_t size, ConnOps *ops)
{
	Connection *conn = malloc(size);

	if (NULL == conn)
		return NULL;

	memset(conn, 0, size);
	conn->ops = ops;

	return conn;
}

static Connection *
connection_create_plain()
{
	return connection_internal_create(sizeof(Connection), &plain_ops);
}

#ifdef USE_OPENSSL

typedef struct SSLConnection
{
	Connection	conn;
	SSL_CTX    *ssl_ctx;
	SSL		   *ssl;
} SSLConnection;


static int
ssl_setup(SSLConnection *conn)
{
	int			ret;

	conn->ssl_ctx = SSL_CTX_new(SSLv23_method());

	if (NULL == conn->ssl_ctx)
		elog(ERROR, "connection library: could not create SSL context");

	SSL_CTX_set_options(conn->ssl_ctx, SSL_OP_NO_SSLv2 | SSL_OP_NO_SSLv3);

	/*
	 * Because we have a blocking socket, we don't want to be bothered with
	 * retries.
	 */
	SSL_CTX_set_mode(conn->ssl_ctx, SSL_MODE_AUTO_RETRY);

	ERR_clear_error();
	/* Clear the SSL error before next SSL_ * call */
	conn->ssl = SSL_new(conn->ssl_ctx);

	if (conn->ssl == NULL)
		elog(ERROR, "connection library: could not create SSL connection");
	ERR_clear_error();

	ret = SSL_set_fd(conn->ssl, conn->conn.sock);
	if (ret == 0)
		elog(ERROR, "connection library: could not associate socket with SSL connection");

	ret = SSL_connect(conn->ssl);
	if (ret <= 0)
		elog(ERROR, "connection library: could not make SSL connection");

	return 0;
}

static int
ssl_connect(Connection *conn, const char *host, int port)
{
	int			ret;

	/* First do the base connection setup */
	ret = plain_connect(conn, host, port);

	if (ret < 0)
		return ret;

	ret = ssl_setup((SSLConnection *) conn);

	if (ret < 0)
		close(conn->sock);

	return ret;
}

static ssize_t
ssl_write(Connection *conn, const char *buf, size_t writelen)
{
	SSLConnection *sslconn = (SSLConnection *) conn;

	int			ret = SSL_write(sslconn->ssl, buf, writelen);

	if (ret < 0)
		elog(ERROR, "connection library: could not SSL_write");
	return ret;
}

static ssize_t
ssl_read(Connection *conn, char *buf, size_t buflen)
{
	SSLConnection *sslconn = (SSLConnection *) conn;

	int			ret = SSL_read(sslconn->ssl, buf, buflen);

	if (ret < 0)
		elog(ERROR, "connection library: could not SSL_read");
	return ret;
}

static void
ssl_close(Connection *conn)
{
	SSLConnection *sslconn = (SSLConnection *) conn;

	if (sslconn->ssl != NULL)
	{
		SSL_free(sslconn->ssl);
		sslconn->ssl = NULL;
	}

	if (sslconn->ssl_ctx != NULL)
	{
		SSL_CTX_free(sslconn->ssl_ctx);
		sslconn->ssl_ctx = NULL;
	}

	plain_close(conn);
}

static ConnOps ssl_ops = {
	.connect = ssl_connect,
	.close = ssl_close,
	.write = ssl_write,
	.read = ssl_read,
};

#endif							/* USE_OPENSSL */


static Connection *
connection_create_ssl()
{
#ifdef USE_OPENSSL
	return connection_internal_create(sizeof(SSLConnection), &ssl_ops);
#else
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("SSL connections are not supported"),
			 errhint("Enable SSL support when compiling the extension.")));
#endif
}

#ifdef DEBUG

typedef struct MockConnection
{
	Connection	conn;
	char		recv_buf[MOCK_MAX_BUF_SIZE];
	int			recv_buf_offset;
	int			recv_buf_len;
} MockConnection;

static int
mock_connect(Connection *conn, const char *host, int port)
{
	return 0;
}

static void
mock_close(Connection *conn)
{
	return;
}

static ssize_t
mock_write(Connection *conn, const char *buf, size_t writelen)
{
	return writelen;
}

static ssize_t
mock_read(Connection *conn, char *buf, size_t readlen)
{
	size_t		bytes_to_read = 0;
	size_t		max = readlen;
	MockConnection *mock = (MockConnection *) conn;

	if (mock->recv_buf_offset >= mock->recv_buf_len)
		return 0;

	if (max >= mock->recv_buf_len - mock->recv_buf_offset)
		max = mock->recv_buf_len - mock->recv_buf_offset;

	/* Now read a random amount */
	while (bytes_to_read == 0)
	{
		bytes_to_read = rand() % (max + 1);
	}
	memcpy(buf, mock->recv_buf + mock->recv_buf_offset, bytes_to_read);
	mock->recv_buf_offset += bytes_to_read;

	return bytes_to_read;
}

static ConnOps mock_ops = {
	.connect = mock_connect,
	.close = mock_close,
	.write = mock_write,
	.read = mock_read,
};

static Connection *
connection_create_mock()
{
	srand(time(0));
	return connection_internal_create(sizeof(MockConnection), &mock_ops);
}

/* Public API */

ssize_t
connection_mock_set_recv_buf(Connection *conn, char *buf, size_t buf_len)
{
	if (buf_len > MOCK_MAX_BUF_SIZE)
		return -1;
	MockConnection *mock = (MockConnection *) conn;

	memcpy(mock->recv_buf, buf, buf_len);
	mock->recv_buf_len = buf_len;
	return mock->recv_buf_len;
}
#endif

Connection *
connection_create(ConnectionType type)
{
	Connection *ret = NULL;

	switch (type)
	{
		case CONNECTION_PLAIN:
			ret = connection_create_plain();
			break;
		case CONNECTION_SSL:
			ret = connection_create_ssl();
			break;
#ifdef DEBUG
		case CONNECTION_MOCK:
			ret = connection_create_mock();
			break;
#endif
	}
	if (ret == NULL)
		return NULL;
	ret->type = type;

	return ret;
}

int
connection_connect(Connection *conn, const char *host, int port)
{
	return conn->ops->connect(conn, host, port);
}

ssize_t
connection_write(Connection *conn, const char *buf, size_t writelen)
{
	int			bytes;

	bytes = conn->ops->write(conn, buf, writelen);

	if (bytes <= 0 || bytes != writelen)
		elog(ERROR, "connection library: could not write");

	return bytes;
}

ssize_t
connection_read(Connection *conn, char *buf, size_t buflen)
{
	return conn->ops->read(conn, buf, buflen);
}

void
connection_close(Connection *conn)
{
	if (NULL != conn->ops)
		conn->ops->close(conn);
}

void
connection_destroy(Connection *conn)
{
	if (conn == NULL)
		return;

	connection_close(conn);
	conn->ops = NULL;
	free(conn);
}

void
_connection_init(void)
{
#ifdef USE_OPENSSL
	SSL_library_init();
	/* Always returns 1 */
	SSL_load_error_strings();
#endif
}

void
_connection_fini(void)
{
#ifdef USE_OPENSSL
	ERR_free_strings();
#endif
}
