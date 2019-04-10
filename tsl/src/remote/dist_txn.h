/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_TSL_REMOTE_DIST_TXN_H
#define TIMESCALEDB_TSL_REMOTE_DIST_TXN_H

#include <postgres.h>
#include <foreign/foreign.h>
#include <libpq-fe.h>

#include "txn.h"

/* Get a remote connection for a distributed txn corresponding to the current local txn. */

extern TSConnection *remote_dist_txn_get_connection(UserMapping *user,
													RemoteTxnPrepStmtOption prep_stmt);

#ifdef DEBUG
extern void (*testing_callback_call_hook)(const char *event);
#endif

void _remote_dist_txn_init(void);
void _remote_dist_txn_fini(void);

#endif /* TIMESCALEDB_TSL_REMOTE_DIST_TXN_H */
