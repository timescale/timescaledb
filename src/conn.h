#include <stdio.h>
#include <stdlib.h>
#include <pg_config.h>

#ifdef USE_OPENSSL
#include <openssl/ssl.h>
#include <openssl/err.h>
#endif

#define MAX_RESULT_SIZE	2048

typedef struct Connection Connection;

typedef struct ConnOps
{
	int			(*connect) (Connection *conn, char *host, int port, char *response);
	void		(*close) (Connection *conn);
	/* int (*write_request)(Connection *conn, char *request); */
	int			(*write) ();
	int			(*read) ();
} ConnOps;

typedef struct Connection
{
    int sock;
#ifdef USE_OPENSSL
    SSL_CTX *ssl_ctx;
	SSL *ssl;
#endif
	ConnOps *ops;
} Connection;

extern ConnOps ssl_ops;

Connection *connection_init(void);
void connection_destroy(Connection *conn);
void connection_set_ops(Connection *conn, ConnOps *ops);

// Called in init.c
extern void _connection_library_init(void);
extern void _connection_library_fini(void);
