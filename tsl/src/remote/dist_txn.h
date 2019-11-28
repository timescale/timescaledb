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

extern TSConnection *remote_dist_txn_get_connection(TSConnectionId id,
													RemoteTxnPrepStmtOption prep_stmt);

#ifdef DEBUG

typedef enum DistTransactionEvent
{
	DTXN_EVENT_ANY,
	DTXN_EVENT_PRE_COMMIT,
	DTXN_EVENT_WAIT_COMMIT,
	DTXN_EVENT_PRE_ABORT,
	DTXN_EVENT_PRE_PREPARE,
	DTXN_EVENT_WAIT_PREPARE,
	DTXN_EVENT_POST_PREPARE,
	DTXN_EVENT_PRE_COMMIT_PREPARED,
	DTXN_EVENT_WAIT_COMMIT_PREPARED,
	DTXN_EVENT_SUB_XACT_ABORT,
} DistTransactionEvent;

#define MAX_DTXN_EVENT (DTXN_EVENT_SUB_XACT_ABORT + 1)

typedef struct DistTransactionEventHandler
{
	void (*handler)(const DistTransactionEvent event, void *data);
	void *data;
} DistTransactionEventHandler;

extern void remote_dist_txn_set_event_handler(const DistTransactionEventHandler *handler);
extern DistTransactionEvent remote_dist_txn_event_from_name(const char *eventname);
extern const char *remote_dist_txn_event_name(const DistTransactionEvent event);

#endif /* DEBUG */

void _remote_dist_txn_init(void);
void _remote_dist_txn_fini(void);

#endif /* TIMESCALEDB_TSL_REMOTE_DIST_TXN_H */
