/*
 * Copyright (c) 2018  Timescale, Inc. All Rights Reserved.
 *
 * This file is licensed under the Timescale License,
 * see LICENSE-TIMESCALE at the top of the tsl directory.
 */
#ifndef REMOTE_NODE_KILLER_H
#define REMOTE_NODE_KILLER_H

#include <postgres.h>
#include <libpq-fe.h>

typedef struct RemoteNodeKiller RemoteNodeKiller;
extern RemoteNodeKiller *remote_node_killer_init(PGconn *conn);
void remote_node_killer_kill(RemoteNodeKiller *rnk);

extern void remote_node_killer_kill_on_event(const char *event);

#endif /* REMOTE_NODE_KILLER_H */
