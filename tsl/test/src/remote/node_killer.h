/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

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
