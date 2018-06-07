/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_TSL_REMOTE_NODE_KILLER_H
#define TIMESCALEDB_TSL_REMOTE_NODE_KILLER_H

#include <postgres.h>
#include <libpq-fe.h>

typedef struct RemoteNodeKiller RemoteNodeKiller;
extern RemoteNodeKiller *remote_node_killer_init(PGconn *conn);
void remote_node_killer_kill(RemoteNodeKiller *rnk);

extern void remote_node_killer_kill_on_event(const char *event);

#endif /* TIMESCALEDB_TSL_REMOTE_NODE_KILLER_H */
