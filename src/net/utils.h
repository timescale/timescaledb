#ifndef TIMESCALEDB_NET_UTILS_H
#define TIMESCALEDB_NET_UTILS_H
#include <stdio.h>
#include <stdlib.h>
#include <pg_config.h>

#include "conn.h"
#include "http.h"

char	   *send_and_recv_http(Connection *conn, HttpRequest *req);

#endif /* TIMESCALEDB_NET_UTILS_H */
