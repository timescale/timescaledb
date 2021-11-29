/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <utils/builtins.h>
#include <utils/fmgrprotos.h>
#include <utils/snapmgr.h>
#include <utils/fmgroids.h>
#include <access/xact.h>
#include <access/transam.h>
#include <miscadmin.h>

#include "connection.h"
#include "txn_id.h"

#define GID_SEP "-"

/* The separator is part of the GID prefix */
#define GID_PREFIX "ts-"
/* This is the maximum size of the literal accepted by PREPARE TRANSACTION, etc. */
#define GID_MAX_SIZE 200

#define REMOTE_TXN_ID_VERSION ((uint8) 1)

/* current_pattern: ts-version-xid-server_id-user_id */
#define FMT_PATTERN GID_PREFIX "%hhu" GID_SEP "%u" GID_SEP "%u" GID_SEP "%u"

static char *
remote_txn_id_get_sql(const char *command, RemoteTxnId *id)
{
	StringInfoData sql;

	initStringInfo(&sql);
	appendStringInfoString(&sql, command);
	appendStringInfoSpaces(&sql, 1);
	appendStringInfoString(&sql, quote_literal_cstr(remote_txn_id_out(id)));
	return sql.data;
}

const char *
remote_txn_id_prepare_transaction_sql(RemoteTxnId *id)
{
	return remote_txn_id_get_sql("PREPARE TRANSACTION", id);
}

const char *
remote_txn_id_commit_prepared_sql(RemoteTxnId *id)
{
	return remote_txn_id_get_sql("COMMIT PREPARED", id);
}

const char *
remote_txn_id_rollback_prepared_sql(RemoteTxnId *id)
{
	return remote_txn_id_get_sql("ROLLBACK PREPARED", id);
}

bool
remote_txn_id_matches_prepared_txn(const char *id_string)
{
	if (strncmp(GID_PREFIX, id_string, strlen(GID_PREFIX)) == 0)
		return true;
	return false;
}

RemoteTxnId *
remote_txn_id_in(const char *id_string)
{
	RemoteTxnId *id = palloc0(sizeof(RemoteTxnId));
	char dummy;

	if (sscanf(id_string,
			   FMT_PATTERN "%c",
			   &id->version,
			   &id->xid,
			   &id->id.server_id,
			   &id->id.user_id,
			   &dummy) != 4)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("invalid input syntax for remote transaction ID: '%s'", id_string)));

	if (id->version != REMOTE_TXN_ID_VERSION)
		elog(ERROR, "invalid version for remote transaction ID: %hhu", id->version);

	return id;
}

Datum
remote_txn_id_in_pg(PG_FUNCTION_ARGS)
{
	const char *id_string = PG_GETARG_CSTRING(0);

	PG_RETURN_POINTER(remote_txn_id_in(id_string));
}

const char *
remote_txn_id_out(const RemoteTxnId *id)
{
	char *out = palloc0(sizeof(char) * GID_MAX_SIZE);
	int written;

	written = snprintf(out,
					   GID_MAX_SIZE,
					   FMT_PATTERN,
					   REMOTE_TXN_ID_VERSION,
					   id->xid,
					   id->id.server_id,
					   id->id.user_id);

	if (written < 0 || written >= GID_MAX_SIZE)
		elog(ERROR, "unexpected length when generating a 2pc transaction name: %d", written);

	return out;
}

Datum
remote_txn_id_out_pg(PG_FUNCTION_ARGS)
{
	RemoteTxnId *id = (RemoteTxnId *) PG_GETARG_POINTER(0);

	PG_RETURN_POINTER(remote_txn_id_out(id));
}

RemoteTxnId *
remote_txn_id_create(TransactionId xid, TSConnectionId cid)
{
	RemoteTxnId *id = palloc0(sizeof(RemoteTxnId));

	id->xid = xid;
	id->id = cid;

	return id;
}
