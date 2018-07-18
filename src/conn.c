#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <postgres.h>
#include <pg_config.h>

#include "conn.h"

/*  Implementation of conn functions, including mocks (TODO) */

ConnOps default_ops;

#ifdef USE_OPENSSL
static void
ssl_err_msg(char *response)
{
	unsigned long ecode = ERR_get_error();

	if (ecode == 0)
		strncpy(response, "error during SSL operation, but no message", MAX_RESULT_SIZE);
	else
		strncpy(response, ERR_reason_error_string(ecode), MAX_RESULT_SIZE);
}

static SSL *
setup_ssl(int sockfd, char *response, SSL_CTX **ctx_ptr)
{
	SSL		   *ssl_hi;
	SSL_CTX    *ssl_ctx = SSL_CTX_new(SSLv23_method());

	if (!ssl_ctx)
	{
		elog(LOG, "connection library: could not create SSL context");
		ssl_err_msg(response);
		*ctx_ptr = NULL;
		return NULL;
	}

	SSL_CTX_set_options(ssl_ctx, SSL_OP_NO_SSLv2 | SSL_OP_NO_SSLv3);

	/*
	 * Because we have a blocking socket, we don't want to be bothered with
	 * retries.
	 */
	SSL_CTX_set_mode(ssl_ctx, SSL_MODE_AUTO_RETRY);

	ERR_clear_error();
	/* Clear the SSL error before next SSL_ * call */
	ssl_hi = SSL_new(ssl_ctx);

	if (ssl_hi == NULL)
	{
		elog(LOG, "connection library: could not create SSL connection");
		ssl_err_msg(response);
		SSL_CTX_free(ssl_ctx);
		return NULL;
	}
	ERR_clear_error();

	if (SSL_set_fd(ssl_hi, sockfd) == 0)
	{
		elog(LOG, "connection library: could not associate socket \
	 		with SSL connection");
		ssl_err_msg(response);
		SSL_free(ssl_hi);
		SSL_CTX_free(ssl_ctx);
		return NULL;
	}
	if (SSL_connect(ssl_hi) <= 0)
	{
		elog(LOG, "connection library: could not make SSL connection");
		goto err;
	}
	*ctx_ptr = ssl_ctx;
	return ssl_hi;
err:
	ssl_err_msg(response);
	SSL_free(ssl_hi);
	SSL_CTX_free(ssl_ctx);
	return NULL;
}
#endif

/*  Create socket and connect */
static int
connection_setup(Connection *conn, char *host, int port, char *response)
{
	struct addrinfo *server_ip;
	struct sockaddr_in serv_info;
	struct sockaddr_in *temp;
	struct timeval timeouts = {
		.tv_sec = 3,
	};
	int			sockfd = socket(AF_INET, SOCK_STREAM, 0);

	if (sockfd < 0)
	{
		strncpy(response, strerror(errno), MAX_RESULT_SIZE);
		return sockfd;
	}

	/*
	 * Set send / recv timeout so that write and read don't
	 * block forever
	 */
	if (setsockopt(sockfd, SOL_SOCKET, SO_RCVTIMEO, &timeouts, sizeof(struct timeval)) != 0 ||
		setsockopt(sockfd, SOL_SOCKET, SO_SNDTIMEO, &timeouts, sizeof(struct timeval)) != 0)
		elog(LOG, "connection library: could not set timeouts on SSL sockets");

	/* lookup the ip address */
	if (getaddrinfo(host, NULL, NULL, &server_ip) < 0 || server_ip == NULL)
	{
		elog(LOG, "connection library: could not get IP of endpoint");
		close(sockfd);
		return -1;
	}
	memset(&serv_info, 0, sizeof(serv_info));
	serv_info.sin_family = AF_INET;
	serv_info.sin_port = htons(port);
	temp = (struct sockaddr_in *) (server_ip->ai_addr);

	memcpy(&serv_info.sin_addr.s_addr, &(temp->sin_addr.s_addr), sizeof(serv_info.sin_addr.s_addr));

	freeaddrinfo(server_ip);

	/* connect the socket */
	if (connect(sockfd, (struct sockaddr *) &serv_info, sizeof(serv_info)) < 0)
	{
		elog(LOG, "connection library: could not connect to endpoint");
		close(sockfd);
		return -1;
	}

	conn->sock = sockfd;
	return 0;
}

#ifdef USE_OPENSSL
static int
connection_setup_ssl(Connection *conn, char *host, int port, char *response) {
	SSL		   *temp_ssl;
	int ret;

	// First do the base connection setup
	ret = connection_setup(conn, host, port, response);
	if (ret < 0)
		return ret;

	temp_ssl = setup_ssl(conn->sock, response, &conn->ssl_ctx);
	if (temp_ssl == NULL)
	{
		close(conn->sock);
		return -1;
	}
	conn->ssl = temp_ssl;
	return 0;
}
#endif

static int
connection_write(Connection *conn, char *request, int len, char *response) {
	int			bytes;

	bytes = write(conn->sock, request, len);
	if (bytes <= 0 || bytes != len)
	{
		elog(LOG, "connection library: could not_write");
		strncpy(response, strerror(errno), MAX_RESULT_SIZE);
		return -1;
	}
	
	return bytes;
}

#ifdef USE_OPENSSL
static int
connection_write_ssl(Connection *conn, char *request, int len, char *response)
{
	int			bytes;

	elog(LOG, "about to SSL_write req");

	bytes = SSL_write(conn->ssl, request, len);
	if (bytes <= 0 || bytes != len)
	{
		elog(LOG, "connection library: could not SSL_write");
		ssl_err_msg(response);
		return -1;
	}
	
	return bytes;
}
#endif

static int
connection_read(Connection *conn, char *response, int bytes_to_read) {
	int			bytes;
	int offset = 0;

	while (bytes_to_read > 0)
	{
		bytes = read(conn->sock, response + offset, bytes_to_read);
		if (bytes == 0)
			break;
		if (bytes < 0)
		{
			elog(LOG, "connection library: could not read");
			return -1;
		}
		offset += bytes;
		bytes_to_read -= bytes;
	}
	return offset;
}

#ifdef USE_OPENSSL
static int
connection_read_ssl(Connection *conn, char *response, int bytes_to_read) {
	int			bytes;
	int offset = 0;

	while (bytes_to_read > 0)
	{
		bytes = SSL_read(conn->ssl, response + offset, bytes_to_read);
		if (bytes == 0)
			break;
		if (bytes < 0)
		{
			elog(LOG, "connection library: could not read");
			return -1;
		}
		offset += bytes;
		bytes_to_read -= bytes;
	}
	return offset;
}
#endif

static void
connection_cleanup(Connection *conn)
{
	close(conn->sock);
}

#ifdef USE_OPENSSL
static void
connection_cleanup_ssl(Connection *conn) {
	if (conn->ssl != NULL)
		SSL_free(conn->ssl);
	if (conn->ssl_ctx != NULL)
		SSL_CTX_free(conn->ssl_ctx);

	connection_cleanup(conn);
}
#endif

Connection *connection_init() {
	Connection *ret = malloc(sizeof(Connection));
	if (ret == NULL)
		return ret;

	memset(ret, 0, sizeof(*ret));
	ret->ops = &default_ops;
	return ret;
}

void connection_destroy(Connection *conn) {
	if (conn == NULL)
		return;
	conn->ops = NULL;
	free(conn);
}

void connection_set_ops(Connection *conn, ConnOps *ops) {
	if (ops != NULL)
		conn->ops = ops;
}

ConnOps default_ops = {
	.connect = connection_setup,
	.close = connection_cleanup,
	.write = connection_write,
	.read = connection_read,
};

ConnOps ssl_ops = {
	.connect = connection_setup_ssl,
	.close = connection_cleanup_ssl,
	.write = connection_write_ssl,
	.read = connection_read_ssl,
};

void _connection_library_init(void) {
#ifdef USE_OPENSSL	
	SSL_library_init(); // Always returns 1
	SSL_load_error_strings();
#endif
}

void _connection_library_fini(void) {
#ifdef USE_OPENSSL	
	ERR_free_strings();
#endif
}
