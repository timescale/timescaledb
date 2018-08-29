#include <postgres.h>
#include <errno.h>
#include <fcntl.h>
#include <time.h>
#include <unistd.h>
#include <pg_config.h>

#include <openssl/ssl.h>
#include <openssl/err.h>

#include "conn_internal.h"
#include "conn_plain.h"

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
		elog(ERROR, "could not create SSL context");

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
		elog(ERROR, "could not create SSL connection");

	ERR_clear_error();

	ret = SSL_set_fd(conn->ssl, conn->conn.sock);
	if (ret == 0)
		elog(ERROR, "could not associate socket with SSL connection");

	ret = SSL_connect(conn->ssl);
	if (ret <= 0)
		elog(ERROR, "could not make SSL connection");

	return 0;
}

static int
ssl_connect(Connection *conn, const char *host, const char *servname, int port)
{
	int			ret;

	/* First do the base connection setup */
	ret = plain_connect(conn, host, servname, port);

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
		elog(ERROR, "could not SSL_write");

	return ret;
}

static ssize_t
ssl_read(Connection *conn, char *buf, size_t buflen)
{
	SSLConnection *sslconn = (SSLConnection *) conn;

	int			ret = SSL_read(sslconn->ssl, buf, buflen);

	if (ret < 0)
		elog(ERROR, "could not SSL_read");

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
	.size = sizeof(SSLConnection),
	.init = NULL,
	.connect = ssl_connect,
	.close = ssl_close,
	.write = ssl_write,
	.read = ssl_read,
};

extern void _conn_ssl_init(void);
extern void _conn_ssl_fini(void);

void
_conn_ssl_init(void)
{
	SSL_library_init();
	/* Always returns 1 */
	SSL_load_error_strings();
	connection_register(CONNECTION_SSL, &ssl_ops);
}

void
_conn_ssl_fini(void)
{
	ERR_free_strings();
}
