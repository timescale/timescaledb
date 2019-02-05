/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>
#include <postgres.h>
#include <pg_config.h>

#include "conn_internal.h"
#include "conn_mock.h"

#define MOCK_MAX_BUF_SIZE 1024

typedef struct MockConnection
{
	Connection conn;
	char recv_buf[MOCK_MAX_BUF_SIZE];
	int recv_buf_offset;
	int recv_buf_len;
} MockConnection;

static int
mock_connect(Connection *conn, const char *host, const char *servname, int port)
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
	size_t bytes_to_read = 0;
	size_t max = readlen;
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

static int
mock_init(Connection *conn)
{
	srand(time(0));
	return 0;
}

static ConnOps mock_ops = {
	.size = sizeof(MockConnection),
	.init = mock_init,
	.connect = mock_connect,
	.close = mock_close,
	.write = mock_write,
	.read = mock_read,
};

ssize_t
ts_connection_mock_set_recv_buf(Connection *conn, char *buf, size_t buf_len)
{
	MockConnection *mock = (MockConnection *) conn;

	if (buf_len > MOCK_MAX_BUF_SIZE)
		return -1;

	memcpy(mock->recv_buf, buf, buf_len);
	mock->recv_buf_len = buf_len;
	return mock->recv_buf_len;
}

extern void _conn_mock_init(void);
extern void _conn_mock_fini(void);

void
_conn_mock_init(void)
{
	ts_connection_register(CONNECTION_MOCK, &mock_ops);
}

void
_conn_mock_fini(void)
{
}
