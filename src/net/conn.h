#include <stdio.h>
#include <stdlib.h>
#include <pg_config.h>

#ifdef USE_OPENSSL
#include <openssl/ssl.h>
#include <openssl/err.h>
#endif

typedef struct ConnOps ConnOps;

typedef enum ConnectionType
{
	CONNECTION_PLAIN,
	CONNECTION_SSL,
#ifdef DEBUG
	CONNECTION_MOCK,
#endif
} ConnectionType;

typedef struct Connection
{
	ConnectionType type;
	int			sock;
	ConnOps    *ops;
} Connection;

extern Connection *connection_create(ConnectionType type);
extern int	connection_connect(Connection *conn, const char *host, int port);
extern ssize_t connection_read(Connection *conn, char *buf, size_t buflen);
extern ssize_t connection_write(Connection *conn, const char *buf, size_t writelen);
extern void connection_close(Connection *conn);
extern void connection_destroy(Connection *conn);

#ifdef DEBUG
/*  Special functions for a connection mocker */
extern ssize_t connection_mock_set_recv_buf(Connection *conn, char *buf, size_t buflen);
#endif

/*  Called in init.c */
extern void _connection_init(void);
extern void _connection_fini(void);
