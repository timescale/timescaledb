/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_TSL_REMOTE_NODE_KILLER_H
#define TIMESCALEDB_TSL_REMOTE_NODE_KILLER_H

#include <postgres.h>
#include <libpq-fe.h>

#include "remote/connection.h"
#include "remote/dist_txn.h"

typedef struct RemoteNodeKiller
{
	pid_t pid;
	DistTransactionEvent killevent;
	const TSConnection *conn;
	int num_kills;
} RemoteNodeKiller;

extern void remote_node_killer_init(RemoteNodeKiller *rnk, const TSConnection *conn,
									const DistTransactionEvent event);
void remote_node_killer_kill(RemoteNodeKiller *rnk);

extern void remote_node_killer_kill_on_event(const char *event);

#endif /* TIMESCALEDB_TSL_REMOTE_NODE_KILLER_H */
