/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <access/tupdesc.h>
#include <catalog/namespace.h>
#include <commands/dbcommands.h>
#include <executor/executor.h>
#include <libpq-fe.h>
#include <lib/stringinfo.h>
#include <miscadmin.h>
#include <parser/parse_type.h>
#include <port/pg_bswap.h>
#include <storage/latch.h>
#include <utils/builtins.h>
#include <utils/lsyscache.h>
#include <utils/palloc.h>

#include "dist_copy.h"
#include "compat/compat.h"
#include "chunk.h"
#include "config.h"
#include "copy.h"
#include "data_node.h"
#include "dimension.h"
#include "dimension_slice.h"
#include "guc.h"
#include "hypercube.h"
#include "hypertable.h"
#include "nodes/chunk_dispatch/chunk_dispatch.h"
#include "nodes/chunk_dispatch/chunk_insert_state.h"
#include "partitioning.h"
#include "remote/connection.h"
#include "remote/connection_cache.h"
#include "remote/dist_txn.h"
#include "ts_catalog/chunk_data_node.h"

#define DEFAULT_PG_DELIMITER '\t'
#define DEFAULT_PG_NULL_VALUE "\\N"

/*
 * Default value for maximum number of rows to send in one CopyData
 * message. The setting can be tweaked at runtime via:
 *
 * ALTER FOREIGN DATA WRAPPER timescaledb_fdw
 * OPTIONS (ADD copy_rows_per_message '200');
 *
 */
#define DEFAULT_COPY_ROWS_PER_MESSAGE 100

/* This contains the information needed to parse a dimension attribute out of a row of text copy
 * data
 */
typedef struct CopyDimensionInfo
{
	const Dimension *dim;
	int corresponding_copy_field;
	Datum default_value;
	FmgrInfo io_func;
	Oid typioparams;
	int32 atttypmod;
} CopyDimensionInfo;

typedef struct DataNodeConnection
{
	TSConnectionId id;
	TSConnection *connection;
	size_t bytes_in_message;
	size_t rows_in_message;
	size_t rows_sent;
	size_t outbuf_size;
	char *outbuf;
} DataNodeConnection;

/* This contains information about connections currently in use by the copy as well as how to create
 * and end the copy command.
 */
typedef struct CopyConnectionState
{
	/*
	 * Cached connections to data nodes.
	 * Why do we need another layer of caching, when there is dist_txn layer
	 * already? The API it provides is one function that "does everything
	 * automatically", namely it's going to stop the COPY each time we request
	 * the connection. This is not something we want to do for each row when
	 * we're trying to do bulk copy.
	 * We can't use the underlying remote_connection_cache directly, because the
	 * remote chunk creation (chunk_api_create_on_data_nodes) would still use
	 * the dist_txn layer. Chunks are created interleaved with the actual COPY
	 * operation, so we would have to somehow maintain these two layers in sync.
	 */
	HTAB *data_node_connections;
	bool using_binary;
	const char *outgoing_copy_cmd;
} CopyConnectionState;

/* This contains the state needed by a non-binary copy operation.
 */
typedef struct TextCopyContext
{
	int ndimensions;
	CopyDimensionInfo *dimensions;
	FmgrInfo *out_functions;
	char delimiter;
	char *null_string;
	char **fields;
	int nfields;
} TextCopyContext;

/* This contains the state needed by a binary copy operation.
 */
typedef struct BinaryCopyContext
{
	ExprContext *econtext;
	FmgrInfo *out_functions;
	Datum *values;
	bool *nulls;
} BinaryCopyContext;

/* This is this high level state needed for an in-progress copy command.
 */
typedef struct RemoteCopyContext
{
	/* Operation data */
	CopyConnectionState connection_state;
	Hypertable *ht;
	Oid user_id;
	List *attnums;
	Point *point;
	void *data_context; /* TextCopyContext or BinaryCopyContext */
	bool binary_operation;
	MemoryContext mctx;	  /* MemoryContext that holds the RemoteCopyContext */
	bool dns_unavailable; /* are some DNs marked as "unavailable"? */
	uint64 num_rows;
	uint32 copy_rows_per_message;
} RemoteCopyContext;

/* From libpq-int.c */
extern int ts_pqPutMsgStart(char msg_type, PGconn *conn);

/*
 * This will create and populate a CopyDimensionInfo struct from the passed in
 * dimensions and values.
 */
static CopyDimensionInfo *
generate_copy_dimensions(const Dimension *dims, int ndimensions, const List *attnums,
						 const Hypertable *ht)
{
	CopyDimensionInfo *result = palloc0(ndimensions * sizeof(CopyDimensionInfo));
	int idx;

	for (idx = 0; idx < ndimensions; ++idx)
	{
		const Dimension *d = &dims[idx];
		CopyDimensionInfo *target = &result[idx];
		int i = 0;
		ListCell *lc;

		foreach (lc, attnums)
		{
			if (lfirst_int(lc) == d->column_attno)
				break;
			++i;
		}

		target->dim = d;

		if (i == attnums->length)
		{
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("unable to use default value for partitioning column \"%s\"",
							NameStr(d->fd.column_name))));
		}
		else
		{
			Relation rel = relation_open(ht->main_table_relid, AccessShareLock);
			TupleDesc rel_desc = RelationGetDescr(rel);
			Form_pg_attribute attribute =
				TupleDescAttr(rel_desc, AttrNumberGetAttrOffset(d->column_attno));
			Oid in_func_oid;

			target->corresponding_copy_field = i;
			getTypeInputInfo(attribute->atttypid, &in_func_oid, &target->typioparams);
			fmgr_info(in_func_oid, &target->io_func);
			target->atttypmod = attribute->atttypmod;

			relation_close(rel, AccessShareLock);
		}
	}

	return result;
}

static Datum
get_copy_dimension_datum(char **fields, CopyDimensionInfo *info)
{
	Datum d;
	if (info->corresponding_copy_field != -1)
	{
		if (fields[info->corresponding_copy_field] == NULL)
		{
			ereport(ERROR,
					(errcode(ERRCODE_NOT_NULL_VIOLATION),
					 errmsg("NULL value in column \"%s\" violates not-null constraint",
							NameStr(info->dim->fd.column_name)),
					 errhint("Columns used for partitioning cannot be NULL")));
		}
		d = InputFunctionCall(&info->io_func,
							  fields[info->corresponding_copy_field],
							  info->typioparams,
							  info->atttypmod);
	}
	else
		d = info->default_value;

	return d;
}

static int64
convert_datum_to_dim_idx(Datum datum, const Dimension *d)
{
	Oid dimtype;

	if (d->partitioning)
		datum = ts_partitioning_func_apply(d->partitioning, InvalidOid, datum);

	switch (d->type)
	{
		case DIMENSION_TYPE_OPEN:
			dimtype =
				(d->partitioning == NULL) ? d->fd.column_type : d->partitioning->partfunc.rettype;

			return ts_time_value_to_internal(datum, dimtype);
		case DIMENSION_TYPE_CLOSED:
			return (int64) DatumGetInt32(datum);
		case DIMENSION_TYPE_ANY:
		default:
			elog(ERROR, "invalid dimension type when inserting tuple");
			return -1;
	}
}

static void
calculate_hyperspace_point_from_fields(char **data, CopyDimensionInfo *dimensions,
									   int num_dimensions, Point *p)
{
	int i;

	p->cardinality = p->num_coords = num_dimensions;

	for (i = 0; i < num_dimensions; ++i)
	{
		Datum datum = get_copy_dimension_datum(data, &dimensions[i]);
		p->coordinates[i] = convert_datum_to_dim_idx(datum, dimensions[i].dim);
	}
}

/*
 * Look up or set up a COPY connection to the data node.
 */
static DataNodeConnection *
get_copy_connection_to_data_node(RemoteCopyContext *context, Oid data_node_oid)
{
	CopyConnectionState *state = &context->connection_state;
	TSConnectionId required_id = remote_connection_id(data_node_oid, context->user_id);
	DataNodeConnection *entry;
	bool found = false;

	entry = hash_search(state->data_node_connections, &required_id, HASH_ENTER, &found);

	if (!found)
	{
		/*
		 * Did not find a cached connection, create a new one and cache it.
		 * The rest of the code using the connection cache in this process has
		 * to take care of exiting the COPY subprotocol if it wants to do
		 * something else like creating a new chunk. Normally this is done under
		 * the hood by the remote connection layer, and the dist_copy layer also
		 * uses faster functions that do this for several connections in
		 * parallel.
		 */
		MemoryContext old = MemoryContextSwitchTo(context->mctx);
		entry->connection = remote_dist_txn_get_connection(required_id, REMOTE_TXN_NO_PREP_STMT);
		entry->id = required_id;
		entry->bytes_in_message = 0;
		entry->rows_in_message = 0;
		entry->rows_sent = 0;
#ifdef TS_DEBUG
		/* Use a small output buffer in tests to make sure we test growing the
		 * buffer */
		entry->outbuf_size = 10 * 1024;
#else
		/* Assume one row is 1k when allocating the output buffer. If not
		 * enough, we will grow it later. */
		entry->outbuf_size = context->copy_rows_per_message * 1024;
#endif

		entry->outbuf = palloc(entry->outbuf_size);
		MemoryContextSwitchTo(old);
	}

	/*
	 * Begin COPY on the connection if needed.
	 */
	TSConnectionStatus status = remote_connection_get_status(entry->connection);
	if (status == CONN_IDLE)
	{
		TSConnectionError err;

		if (!remote_connection_begin_copy(entry->connection,
										  psprintf("%s /* row " INT64_FORMAT " conn %p */",
												   state->outgoing_copy_cmd,
												   context->num_rows,
												   remote_connection_get_pg_conn(
													   entry->connection)),
										  state->using_binary,
										  &err))
		{
			remote_connection_error_elog(&err, ERROR);
		}
	}
	else if (status == CONN_COPY_IN)
	{
		/* Ready to use. */
	}
	else
	{
		elog(ERROR,
			 "wrong status %d for connection to data node %d when performing "
			 "distributed COPY\n",
			 status,
			 required_id.server_id);
	}

	return entry;
}

/*
 * Flush the outgoing buffers on the active data node connections.
 */
static void
flush_active_connections(CopyConnectionState *state)
{
	/*
	 * The connections that we are going to flush on the current iteration.
	 */
	List *to_flush = NIL;
	HASH_SEQ_STATUS status;
	DataNodeConnection *dnc;

	hash_seq_init(&status, state->data_node_connections);

	for (dnc = hash_seq_search(&status); dnc != NULL; dnc = hash_seq_search(&status))
	{
		to_flush = lappend(to_flush, dnc->connection);
	}

	/*
	 * The connections that were busy on the current iteration and that we have
	 * to wait for.
	 */
	List *busy_connections = NIL;
	for (;;)
	{
		CHECK_FOR_INTERRUPTS();

		ListCell *to_flush_cell;
		foreach (to_flush_cell, to_flush)
		{
			TSConnection *conn = lfirst(to_flush_cell);
			PGconn *pg_conn = remote_connection_get_pg_conn(conn);

			if (remote_connection_get_status(conn) != CONN_COPY_IN)
			{
				/*
				 * This functions only makes sense for connections that are
				 * currently doing COPY and therefore are in nonblocking mode.
				 */
				continue;
			}

			/*
			 * This function expects that the COPY processing so far was
			 * successful, so the data connections should be in nonblocking
			 * mode.
			 */
			Assert(PQisnonblocking(pg_conn) == 1);

			/* Write out all the pending buffers. */
			int res = PQflush(pg_conn);
			if (res == -1)
			{
				TSConnectionError err;
				remote_connection_get_error(conn, &err);
				remote_connection_error_elog(&err, ERROR);
			}
			else if (res == 0)
			{
				/* Flushed. */
			}
			else
			{
				/* Busy. */
				Assert(res == 1);
				busy_connections = lappend(busy_connections, conn);
				continue;
			}

			/* Hooray, done with this connection. */
		}

		if (list_length(busy_connections) == 0)
		{
			/* Flushed everything. */
			break;
		}

		/*
		 * Wait for changes on the busy connections.
		 * Postgres API doesn't allow to remove a socket from the wait event,
		 * and it's level-triggered, so we have to recreate the set each time.
		 */
		WaitEventSet *set =
			CreateWaitEventSet(CurrentMemoryContext, list_length(busy_connections) + 1);

		/*
		 * Postmaster-managed callers must handle postmaster death somehow,
		 * as stated by the comments in WaitLatchOrSocket.
		 */
		(void) AddWaitEventToSet(set, WL_EXIT_ON_PM_DEATH, PGINVALID_SOCKET, NULL, NULL);

		/*
		 * Add wait events for each busy connection.
		 */
		ListCell *busy_cell;
		foreach (busy_cell, busy_connections)
		{
			TSConnection *conn = lfirst(busy_cell);
			PGconn *pg_conn = remote_connection_get_pg_conn(conn);
			(void) AddWaitEventToSet(set,
									 /* events = */ WL_SOCKET_WRITEABLE,
									 PQsocket(pg_conn),
									 /* latch = */ NULL,
									 /* user_data = */ NULL);
		}

		/* Wait. */
		WaitEvent occurred[1];
		int wait_result PG_USED_FOR_ASSERTS_ONLY = WaitEventSetWait(set,
																	/* timeout = */ 1000,
																	occurred,
																	/* nevents = */ 1,
																	WAIT_EVENT_COPY_FILE_WRITE);

		/*
		 * The possible results are:
		 * `0` -- Timeout. Just retry the flush, it will report errors in case
		 *        there are any.
		 * `1` -- We have successfully waited for something, we don't care,
		 *        just continue flushing the rest of the list.
		 */
		Assert(wait_result == 0 || wait_result == 1);

		FreeWaitEventSet(set);

		/*
		 * Repeat the procedure for all the connections that were busy.
		 */
		List *tmp = busy_connections;
		busy_connections = to_flush;
		to_flush = tmp;

		busy_connections = list_truncate(busy_connections, 0);
	}
}

/*
 * Flush all active data node connections and end COPY simultaneously, instead
 * of doing this one-by-one in remote_connection_end_copy(). Implies that there
 * were no errors so far. For error handling, use remote_connection_end_copy().
 */
static void
end_copy_on_success(CopyConnectionState *state)
{
	List *to_end_copy = NIL;
	ListCell *lc;
	HASH_SEQ_STATUS status;
	DataNodeConnection *dnc;

	hash_seq_init(&status, state->data_node_connections);

	for (dnc = hash_seq_search(&status); dnc != NULL; dnc = hash_seq_search(&status))
	{
		TSConnection *conn = dnc->connection;

		/*
		 * We expect the connection to be in CONN_COPY_IN status.
		 * What about other statuses?
		 * CONN_IDLE:
		 * The normal distributed insert path (not dist_copy, but
		 * data_node_copy) doesn't reset the connections when it creates
		 * a new chunk. So the connection status will be idle after we
		 * created a new chunk, but it will still be in the list of
		 * active connections. On the other hand, this function isn't called
		 * on the normal insert path, so we shouldn't see this state here.
		 * CONN_PROCESSING:
		 * Not sure what it would mean, probably an internal program error.
		 */
		Assert(remote_connection_get_status(conn) == CONN_COPY_IN);

		PGconn *pg_conn = remote_connection_get_pg_conn(conn);

		/*
		 * This function expects that the COPY processing so far was
		 * successful, so the data connections should be in nonblocking
		 * mode.
		 */
		Assert(PQisnonblocking(pg_conn));

		PGresult *res = PQgetResult(pg_conn);
		if (res == NULL)
		{
			/*
			 * No activity on the connection while we're expecting COPY. This
			 * is probably an internal program error.
			 */
			elog(ERROR,
				 "the connection is expected to be in PGRES_COPY_IN status, but it has no activity "
				 "(when flushing data)");
		}

		if (PQresultStatus(res) != PGRES_COPY_IN)
		{
			char *sqlstate = PQresultErrorField(res, PG_DIAG_SQLSTATE);
			if (sqlstate != NULL && strcmp(sqlstate, "00000") == 0)
			{
				/*
				 * An error has occurred.
				 */
				TSConnectionError err;
				remote_connection_get_result_error(res, &err);
				remote_connection_error_elog(&err, ERROR);
			}

			/*
			 * No erroneous SQLSTATE, but at the same time the connection is
			 * not in PGRES_COPY_IN status. This must be a logic error.
			 */
			elog(ERROR,
				 "the connection is expected to be in PGRES_COPY_IN status, but instead the status "
				 "is %d  (when flushing data)",
				 PQresultStatus(res));
		}

		/* The connection is in PGRES_COPY_IN status, as expected. */
		Assert(res != NULL && PQresultStatus(res) == PGRES_COPY_IN);

		to_end_copy = lappend(to_end_copy, conn);

		if (PQputCopyEnd(pg_conn, NULL) != 1)
		{
			ereport(ERROR,
					(errmsg("could not end remote COPY"),
					 errdetail("%s", PQerrorMessage(pg_conn))));
		}
	}

	flush_active_connections(state);

	/*
	 * Switch the connections back into blocking mode because that's what the
	 * non-COPY code expects.
	 */
	foreach (lc, to_end_copy)
	{
		TSConnection *conn = lfirst(lc);
		PGconn *pg_conn = remote_connection_get_pg_conn(conn);

		if (PQsetnonblocking(pg_conn, 0))
		{
			ereport(ERROR,
					(errmsg("failed to switch the connection into blocking mode"),
					 errdetail("%s", PQerrorMessage(pg_conn))));
		}
	}

	/*
	 * Verify that the copy has successfully finished on each connection.
	 */
	foreach (lc, to_end_copy)
	{
		TSConnection *conn = lfirst(lc);
		PGconn *pg_conn = remote_connection_get_pg_conn(conn);
		PGresult *res = PQgetResult(pg_conn);
		if (res == NULL)
		{
			ereport(ERROR, (errmsg("unexpected NULL result when ending remote COPY")));
		}

		if (PQresultStatus(res) != PGRES_COMMAND_OK)
		{
			TSConnectionError err;
			remote_connection_get_result_error(res, &err);
			remote_connection_error_elog(&err, ERROR);
		}

		res = PQgetResult(pg_conn);
		if (res != NULL)
		{
			ereport(ERROR,
					(errmsg("unexpected non-NULL result %d when ending remote COPY",
							PQresultStatus(res)),
					 errdetail("%s", PQerrorMessage(pg_conn))));
		}
	}

	/*
	 * Mark the connections as idle. If an error occurs before this, the
	 * connections are going to be still marked as CONN_COPY_IN, and the
	 * remote_connection_end_copy() will bring each connection to a valid state.
	 */
	foreach (lc, to_end_copy)
	{
		TSConnection *conn = (TSConnection *) lfirst(lc);
		remote_connection_set_status(conn, CONN_IDLE);
	}

	list_free(to_end_copy);
}

static void
end_copy_on_failure(CopyConnectionState *state)
{
	/* Exit the copy subprotocol. */
	TSConnectionError err = { 0 };
	bool failure = false;
	HASH_SEQ_STATUS status;
	DataNodeConnection *dnc;

	hash_seq_init(&status, state->data_node_connections);

	for (dnc = hash_seq_search(&status); dnc != NULL; dnc = hash_seq_search(&status))
	{
		TSConnection *conn = dnc->connection;

		if (remote_connection_get_status(conn) == CONN_COPY_IN &&
			!remote_connection_end_copy(conn, &err))
		{
			failure = true;
		}
	}

	if (failure)
		remote_connection_error_elog(&err, ERROR);
}

/*
 * Extract a quoted list of identifiers from a DefElem with arg type T_list.
 */
static char *
name_list_to_string(const DefElem *def)
{
	StringInfoData string;
	ListCell *lc;
	bool first = true;

	initStringInfo(&string);

	foreach (lc, (List *) def->arg)
	{
		Node *name = (Node *) lfirst(lc);

		if (!first)
			appendStringInfoString(&string, ", ");
		else
			first = false;

		if (IsA(name, String))
			appendStringInfoString(&string, quote_identifier(strVal(name)));
		else if (IsA(name, A_Star))
			appendStringInfoChar(&string, '*');
		else
			elog(ERROR, "unexpected node type in name list: %d", (int) nodeTag(name));
	}
	return string.data;
}

/*
 * Extract a string value (otherwise uninterpreted) from a DefElem.
 */
static char *
def_get_string(const DefElem *def)
{
	if (def->arg == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR), errmsg("%s requires a parameter", def->defname)));
	switch (nodeTag(def->arg))
	{
		case T_Integer:
			return psprintf("%ld", (long) intVal(def->arg));
		case T_Float:

			/*
			 * T_Float values are kept in string form, so this type cheat
			 * works (and doesn't risk losing precision)
			 */
			return strVal(def->arg);
		case T_String:
			return strVal(def->arg);
		case T_TypeName:
			return TypeNameToString((TypeName *) def->arg);
		case T_List:
			return name_list_to_string(def);
		case T_A_Star:
			return pstrdup("*");
		default:
			elog(ERROR, "unrecognized node type: %d", nodeTag(def->arg));
	}
	return NULL; /* keep compiler quiet */
}

/* These are the only option available for binary copy operations */
static bool
is_supported_binary_option(const char *option)
{
	return strcmp(option, "oids") == 0 || strcmp(option, "freeze") == 0 ||
		   strcmp(option, "encoding") == 0;
}

/* Generate a COPY sql command for sending the data being passed in via 'stmt'
 * to a data node.
 */
static const char *
deparse_copy_cmd(const CopyStmt *stmt, const Hypertable *ht, bool binary)
{
	ListCell *lc;
	StringInfo command = makeStringInfo();

	appendStringInfo(command,
					 "COPY %s ",
					 quote_qualified_identifier(NameStr(ht->fd.schema_name),
												NameStr(ht->fd.table_name)));

	if (stmt->attlist != NULL)
	{
		bool first = true;
		appendStringInfo(command, "(");
		foreach (lc, stmt->attlist)
		{
			if (!first)
				appendStringInfo(command, ", ");
			else
				first = false;

			appendStringInfo(command, "%s", quote_identifier(strVal(lfirst(lc))));
		}
		appendStringInfo(command, ") ");
	}

	appendStringInfo(command, "FROM STDIN");

	if (stmt->options != NIL || binary)
	{
		bool first = true;
		foreach (lc, stmt->options)
		{
			DefElem *defel = lfirst_node(DefElem, lc);
			const char *option = defel->defname;

			/* Ignore text only options for binary copy */
			if (binary && !is_supported_binary_option(option))
				continue;

			if (strcmp(option, "delimiter") == 0 || strcmp(option, "encoding") == 0 ||
				strcmp(option, "escape") == 0 || strcmp(option, "force_not_null") == 0 ||
				strcmp(option, "force_null") == 0 || strcmp(option, "format") == 0 ||
				strcmp(option, "header") == 0 || strcmp(option, "null") == 0 ||
				strcmp(option, "quote") == 0)
			{
				/*
				 * These options are fixed as default for transfer to data nodes
				 * in text format, regardless of how they are set in the input
				 * file.
				 */
				continue;
			}

			if (!first)
			{
				appendStringInfo(command, ", ");
			}
			else
			{
				appendStringInfo(command, " WITH (");
				first = false;
			}

			if (defel->arg == NULL &&
				(strcmp(option, "oids") == 0 || strcmp(option, "freeze") == 0))
			{
				/* boolean options don't require an argument to use default setting */
				appendStringInfo(command, "%s", option);
			}
			else
			{
				/* everything else should pass directly through */
				appendStringInfo(command, "%s %s", option, def_get_string(defel));
			}
		}

		if (binary)
		{
			if (first)
			{
				appendStringInfo(command, " WITH (");
			}
			appendStringInfo(command, "%sFORMAT binary", first ? "" : ", ");
			first = false;
		}
		if (!first)
		{
			appendStringInfo(command, ")");
		}
	}

	return command->data;
}

/*
 * This function checks the options specified for the copy command and makes
 * sure they're supported.  It also determines what delimiter and null
 * encoding are being specified and will use these values when sending data to
 * the data node as they presumably won't conflict with the values being passed.
 * Note that the CopyBegin call will have such validation as checking for
 * duplicate options, this function just checks added constraints for the
 * distributed copy. This call is only needed when sending data in text format
 * to the data node.
 */
static void
validate_options(List *copy_options, char *delimiter, char **null_string)
{
	ListCell *lc;
	bool delimiter_found = false;

	/* Postgres defaults */
	*delimiter = DEFAULT_PG_DELIMITER;
	*null_string = DEFAULT_PG_NULL_VALUE;

	foreach (lc, copy_options)
	{
		const DefElem *defel = lfirst_node(DefElem, lc);

		if (strcmp(defel->defname, "format") == 0)
		{
			const char *fmt;

			Assert(nodeTag(defel->arg) == T_String);
			fmt = strVal(defel->arg);

			if (strcmp(fmt, "binary") == 0)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("remote copy does not support binary input in combination with "
								"text transfer to data nodes"),
						 errhint("Set timescaledb.enable_connection_binary_data to true and "
								 "timescaledb.dist_copy_transfer_format to auto to enable "
								 "binary data transfer.")));
			else if (strcmp(fmt, "csv") == 0 && !delimiter_found)
				*delimiter = ',';
		}
		else if (strcmp(defel->defname, "delimiter") == 0)
		{
			const char *delim_string = def_get_string(defel);

			Assert(strlen(delim_string) == 1);
			*delimiter = delim_string[0];
			delimiter_found = true;
		}
		else if (strcmp(defel->defname, "null") == 0)
			*null_string = def_get_string(defel);
	}
}

static bool
copy_should_send_binary(const CopyStmt *stmt)
{
	bool input_format_binary = false;
	ListCell *lc;
	foreach (lc, stmt->options)
	{
		const DefElem *defel = lfirst_node(DefElem, lc);
		if (strcmp(defel->defname, "format") == 0)
		{
			if (strcmp(def_get_string(defel), "binary") == 0)
			{
				input_format_binary = true;
			}
			break;
		}
	}

	if (ts_guc_dist_copy_transfer_format == DCTF_Auto)
	{
		return input_format_binary && ts_guc_enable_connection_binary_data;
	}

	if (ts_guc_dist_copy_transfer_format == DCTF_Binary)
	{
		if (!ts_guc_enable_connection_binary_data)
		{
			ereport(ERROR,
					(errmsg("the requested binary format for COPY data transfer is disabled by the "
							"settings"),
					 errhint(
						 "Either enable it by setting timescaledb.enable_connection_binary_data "
						 "to true, or use automatic COPY format detection by setting "
						 "timescaledb.dist_copy_transfer_format to 'auto'.")));
		}
		return true;
	}

	return false;
}

/* Populates the passed in pointer with an array of output functions and returns the array size.
 * Note that we size the array to the number of columns in the hypertable for convenience, but only
 * populate the functions for columns used in the copy command.
 */
static int
get_copy_conversion_functions(Oid relid, const List *copy_attnums, FmgrInfo **functions,
							  bool binary)
{
	ListCell *lc;
	Relation rel = relation_open(relid, AccessShareLock);
	TupleDesc tupDesc = RelationGetDescr(rel);

	*functions = palloc0(tupDesc->natts * sizeof(FmgrInfo));
	foreach (lc, copy_attnums)
	{
		int offset = AttrNumberGetAttrOffset(lfirst_int(lc));
		Oid out_func_oid;
		bool isvarlena;
		Form_pg_attribute attr = TupleDescAttr(tupDesc, offset);

		if (binary)
			getTypeBinaryOutputInfo(attr->atttypid, &out_func_oid, &isvarlena);
		else
			getTypeOutputInfo(attr->atttypid, &out_func_oid, &isvarlena);

		fmgr_info(out_func_oid, &((*functions)[offset]));
	}
	relation_close(rel, AccessShareLock);

	return tupDesc->natts;
}

static TextCopyContext *
generate_text_copy_context(const CopyStmt *stmt, const Hypertable *ht, const List *attnums)
{
	TextCopyContext *ctx = palloc0(sizeof(TextCopyContext));

	get_copy_conversion_functions(ht->main_table_relid, attnums, &ctx->out_functions, false);

	ctx->ndimensions = ht->space->num_dimensions;
	validate_options(stmt->options, &ctx->delimiter, &ctx->null_string);
	ctx->dimensions =
		generate_copy_dimensions(ht->space->dimensions, ctx->ndimensions, attnums, ht);
	return ctx;
}

static BinaryCopyContext *
generate_binary_copy_context(ExprContext *econtext, const Hypertable *ht, const List *attnums)
{
	BinaryCopyContext *ctx = palloc0(sizeof(BinaryCopyContext));
	int columns =
		get_copy_conversion_functions(ht->main_table_relid, attnums, &ctx->out_functions, true);

	ctx->econtext = econtext;
	ctx->values = palloc0(columns * sizeof(Datum));
	ctx->nulls = palloc0(columns * sizeof(bool));

	return ctx;
}

static uint32
get_copy_rows_per_message(void)
{
	ForeignDataWrapper *fdw = GetForeignDataWrapperByName(EXTENSION_FDW_NAME, false);
	ListCell *lc;

	foreach (lc, fdw->options)
	{
		DefElem *def = (DefElem *) lfirst(lc);

		if (strcmp(def->defname, "copy_rows_per_message") == 0)
			return strtol(defGetString(def), NULL, 10);
	}

	return DEFAULT_COPY_ROWS_PER_MESSAGE;
}

RemoteCopyContext *
remote_copy_begin(const CopyStmt *stmt, Hypertable *ht, ExprContext *per_tuple_ctx, List *attnums,
				  bool binary_copy)
{
	MemoryContext mctx =
		AllocSetContextCreate(CurrentMemoryContext, "Remote COPY", ALLOCSET_DEFAULT_SIZES);
	RemoteCopyContext *context;
	MemoryContext oldmctx;
	struct HASHCTL hctl = {
		.keysize = sizeof(TSConnectionId),
		.entrysize = sizeof(DataNodeConnection),
		.hcxt = mctx,
	};

	oldmctx = MemoryContextSwitchTo(mctx);
	context = palloc0(sizeof(RemoteCopyContext));
	context->ht = ht;
	context->user_id = GetUserId();
	context->attnums = attnums;
	context->mctx = mctx;
	context->binary_operation = binary_copy;
	context->connection_state.data_node_connections =
		hash_create("COPY connections",
					list_length(ht->data_nodes),
					&hctl,
					HASH_ELEM | HASH_CONTEXT | HASH_BLOBS);
	context->connection_state.using_binary = binary_copy;
	context->connection_state.outgoing_copy_cmd = deparse_copy_cmd(stmt, ht, binary_copy);
	context->dns_unavailable = data_node_some_unavailable();
	context->num_rows = 0;
	context->copy_rows_per_message = get_copy_rows_per_message();
	context->point = palloc0(POINT_SIZE(ht->space->num_dimensions));

	if (binary_copy)
		context->data_context = generate_binary_copy_context(per_tuple_ctx, ht, attnums);
	else
		context->data_context = generate_text_copy_context(stmt, ht, attnums);

	MemoryContextSwitchTo(oldmctx);

	return context;
}

const char *
remote_copy_get_copycmd(RemoteCopyContext *context)
{
	return context->connection_state.outgoing_copy_cmd;
}

/*
 * Functions for escaping values for Postgres text format.
 * See CopyAttributeOutText.
 */
static bool
is_special_character(char c)
{
	return (c == '\b' || c == '\f' || c == '\n' || c == '\r' || c == '\t' || c == '\v' ||
			c == '\\');
}

/*
 * Generate a text row for sending to data nodes, based on text input.
 *
 * The input fields are already in text format, so instead of converting to
 * internal data format and then back to text for output, we simply use the
 * original text. This saves a lot of CPU by avoiding parsing and re-formatting
 * the column values. However, we still need to escape any special characters
 * since the input is unescaped.
 *
 * One caveat here is that skipping conversion is only possible under the
 * assumption that the text encoding used in the client (from which we received
 * the data) is the same as the destination data node's encoding.
 */
static bool
parse_next_text_row(CopyFromState cstate, List *attnums, TextCopyContext *ctx, StringInfo row_data)
{
	if (!NextCopyFromRawFields(cstate, &ctx->fields, &ctx->nfields))
		return false;

	/* check for overflowing/missing fields */
	if (ctx->nfields != list_length(attnums))
	{
		ereport(ERROR,
				(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
				 errmsg("the number of columns doesn't match")));
	}

	/*
	 * Format the raw unquoted unescaped fields into the Postgres text format.
	 * We only have to escape the special characters, mirroring what
	 * CopyAttributeOutText does. We assume here that the following three
	 * encodings are the same:
	 * 1) database encoding on the access node,
	 * 2) database encoding on the data node,
	 * 3) client encoding used by the access node to connect to the data node.
	 * The encoding of the input file is not relevant here, because the raw
	 * fields are already converted to the server encoding.
	 * All encoding supported by Postgres embed ASCII, so we can get away with
	 * single-byte comparisons.
	 */
	const int nfields = ctx->nfields;
	char **restrict fields = ctx->fields;
	char *restrict output = row_data->data;
	int len = row_data->len;
	for (int field = 0; field < nfields; field++)
	{
		const char *restrict src = fields[field];
		/*
		 * The length of the input we'll have to escape is either the length of
		 * the input text, or one for null fields which are written as \N.
		 */
		const int input_len = (src != NULL) ? strlen(src) : 1;
		/*
		 * Calculate how much bytes we might need for output. Each character
		 * might be replaced with two when escaping, plus the field separator,
		 * terminating newline and terminating zero.
		 */
		const int additional_len = (input_len * 2 + 3);
		if (row_data->maxlen < len + additional_len)
		{
			row_data->len = len;
			enlargeStringInfo(row_data, additional_len);
			output = row_data->data;
		}

		/* Add separator. */
		if (field > 0)
		{
			output[len++] = '\t';
			Assert(len <= row_data->maxlen);
		}

		if (src == NULL)
		{
			/* Null value, encoded as \N */
			output[len++] = '\\';
			output[len++] = 'N';
			Assert(len <= row_data->maxlen);
			continue;
		}

		/* The field is not null, replace special characters. */
		for (int pos = 0; pos < input_len; pos++)
		{
			if (unlikely(is_special_character(src[pos])))
			{
				output[len++] = '\\';
				output[len++] = src[pos];
			}
			else
			{
				output[len++] = src[pos];
			}
			Assert(len <= row_data->maxlen);
		}
	}
	/*
	 * Newline.
	 */
	output[len++] = '\n';
	row_data->len = len;
	Assert(len <= row_data->maxlen);

	return true;
}

static void
write_binary_copy_data(Datum *values, bool *nulls, List *attnums, FmgrInfo *out_functions,
					   StringInfo row_data)
{
	uint16 buf16;
	uint32 buf32;
	ListCell *lc;

	buf16 = pg_hton16((uint16) attnums->length);
	appendBinaryStringInfo(row_data, (char *) &buf16, sizeof(buf16));

	foreach (lc, attnums)
	{
		int offset = AttrNumberGetAttrOffset(lfirst_int(lc));

		if (nulls[offset])
		{
			buf32 = pg_hton32((uint32) -1);
			appendBinaryStringInfo(row_data, (char *) &buf32, sizeof(buf32));
		}
		else
		{
			Datum value = values[offset];
			bytea *outputbytes;
			int output_length;

			outputbytes = SendFunctionCall(&out_functions[offset], value);
			output_length = VARSIZE(outputbytes) - VARHDRSZ;
			buf32 = pg_hton32((uint32) output_length);
			appendBinaryStringInfo(row_data, (char *) &buf32, sizeof(buf32));
			appendBinaryStringInfo(row_data, VARDATA(outputbytes), output_length);
		}
	}
}

static bool
parse_next_binary_row(CopyFromState cstate, List *attnums, BinaryCopyContext *ctx,
					  StringInfo row_data)
{
	MemoryContext old = MemoryContextSwitchTo(ctx->econtext->ecxt_per_tuple_memory);
	bool result = NextCopyFrom(cstate, ctx->econtext, ctx->values, ctx->nulls);
	MemoryContextSwitchTo(old);

	if (!result)
		return false;

	write_binary_copy_data(ctx->values, ctx->nulls, attnums, ctx->out_functions, row_data);
	return true;
}

static void
calculate_hyperspace_point_from_binary(Datum *values, bool *nulls, const Hyperspace *space,
									   Point *p)
{
	int i;

	p->cardinality = space->num_dimensions;
	p->num_coords = space->num_dimensions;

	for (i = 0; i < space->num_dimensions; ++i)
	{
		const Dimension *dim = &space->dimensions[i];
		Datum datum = values[dim->column_attno - 1];

		if (nulls[dim->column_attno - 1])
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("value required for partitioning column %s",
							NameStr(dim->fd.column_name))));
		p->coordinates[i] = convert_datum_to_dim_idx(datum, dim);
	}
}

static Point *
read_next_copy_row(RemoteCopyContext *context, CopyFromState cstate, StringInfo row_data)
{
	bool read_row;

	if (context->binary_operation)
		read_row = parse_next_binary_row(cstate, context->attnums, context->data_context, row_data);
	else
		read_row = parse_next_text_row(cstate, context->attnums, context->data_context, row_data);

	if (!read_row)
		return NULL;

	if (context->binary_operation)
	{
		BinaryCopyContext *ctx = context->data_context;
		calculate_hyperspace_point_from_binary(ctx->values,
											   ctx->nulls,
											   context->ht->space,
											   context->point);
	}
	else
	{
		TextCopyContext *ctx = context->data_context;
		calculate_hyperspace_point_from_fields(ctx->fields,
											   ctx->dimensions,
											   ctx->ndimensions,
											   context->point);
	}

	return context->point;
}

/*
 * This function is for successfully finishing the COPY: it tries to flush all
 * the outstanding COPY data to the data nodes. It is sensitive to erroneous
 * state of connections and is going to fail if they are in a wrong state due to
 * other errors. Resetting the connections after a known error should be done
 * with remote_connection_end_copy, not this function.
 */
void
remote_copy_end_on_success(RemoteCopyContext *context)
{
	end_copy_on_success(&context->connection_state);
	MemoryContextDelete(context->mctx);
}

/*
 * Write data to a CopyData message.
 *
 * The function allows writing multiple rows to the same CopyData message to
 * reduce overhead. The CopyData message will automatically be "ended" and
 * flushed when it reaches its max number of rows.
 *
 * Returns 0 on success, -1 on failure and 1 if the CopyData message could not
 * be completely flushed.
 */
static int
write_copy_data(RemoteCopyContext *context, DataNodeConnection *dnc, const char *data, size_t len,
				bool endmsg)
{
	PGconn *conn = remote_connection_get_pg_conn(dnc->connection);

	/*
	 * Check if the message buffer is big enough to fit the additional
	 * data. Otherwise compute the required size and double that.
	 */
	if (dnc->bytes_in_message + len > dnc->outbuf_size)
	{
		char *newbuf;
		size_t newsize = (dnc->bytes_in_message + len) * 2;
		MemoryContext old = MemoryContextSwitchTo(context->mctx);
		newbuf = repalloc(dnc->outbuf, newsize);
		dnc->outbuf = newbuf;
		dnc->outbuf_size = newsize;
		MemoryContextSwitchTo(old);
	}

	memcpy(dnc->outbuf + dnc->bytes_in_message, data, len);
	dnc->bytes_in_message += len;
	dnc->rows_in_message++;
	dnc->rows_sent++;

	if (endmsg || dnc->rows_in_message >= context->copy_rows_per_message)
	{
		int ret = PQputCopyData(conn, dnc->outbuf, dnc->bytes_in_message);

		if (ret == 0)
		{
			/* Not queued, full buffers and could not allocate memory. This is
			 * a very unlikely situation, and it is possible to try to flush
			 * and wait on writeability instead of failing. */
			elog(ERROR, "could not allocate memory for COPY data");
		}
		else if (ret == -1)
		{
			return -1;
		}
		else
		{
			/* Successfully queued */
			Assert(ret == 1);
		}

		/* Clear output buffer and flush */
		dnc->bytes_in_message = 0;
		dnc->rows_in_message = 0;

		return PQflush(conn);
	}

	return 0;
}

/*
 * Flush every pending message on data node connections.
 */
static void
write_copy_data_end(RemoteCopyContext *context)
{
	HASH_SEQ_STATUS status;
	DataNodeConnection *dnc;
	int num_blocked_nodes = 0;

	hash_seq_init(&status, context->connection_state.data_node_connections);

	for (dnc = hash_seq_search(&status); dnc != NULL; dnc = hash_seq_search(&status))
	{
		if (dnc->bytes_in_message > 0)
		{
			PGconn *conn = remote_connection_get_pg_conn(dnc->connection);
			int ret = PQputCopyData(conn, dnc->outbuf, dnc->bytes_in_message);

			if (ret == 0)
			{
				/* Not queued, full buffers and could not allocate memory */
				elog(ERROR, "could not allocate memory for COPY data");
			}
			else if (ret == -1)
			{
				remote_connection_elog(dnc->connection, ERROR);
			}
			else
			{
				/* Successfully queued */
				Assert(ret == 1);
			}

			ret = PQflush(conn);

			switch (ret)
			{
				case -1:
					/* failure */
					remote_connection_elog(dnc->connection, ERROR);
					break;
				case 0:
					/* flushed */
					break;
				default:
					Assert(ret == 1);
					/* partial flush */
					num_blocked_nodes++;
					break;
			}

			dnc->bytes_in_message = 0;
			dnc->rows_in_message = 0;
		}
	}

	if (num_blocked_nodes > 0)
		flush_active_connections(&context->connection_state);
}

static void
send_row_to_data_nodes(RemoteCopyContext *context, List *data_nodes, StringInfo row_data,
					   bool endmsg)
{
	ListCell *lc;
	int num_blocked_nodes = 0;

	foreach (lc, data_nodes)
	{
		ChunkDataNode *chunk_data_node = lfirst(lc);
		/* Find the existing insert state for this data node. */
		DataNodeConnection *dnc =
			get_copy_connection_to_data_node(context, chunk_data_node->foreign_server_oid);
		int ret;

		ret = write_copy_data(context, dnc, row_data->data, row_data->len, endmsg);

		if (ret == -1)
			remote_connection_elog(dnc->connection, ERROR);
		else if (ret == 1)
		{
			/* Could not flush completely */
			num_blocked_nodes++;
		}
		else
		{
			Assert(ret == 0);
		}
	}

	if (num_blocked_nodes > 0)
		flush_active_connections(&context->connection_state);
}

uint64
remote_distributed_copy(const CopyStmt *stmt, CopyChunkState *ccstate, List *attnums)
{
	MemoryContext orig_mctx = CurrentMemoryContext;
	EState *estate = ccstate->estate;
	Hypertable *ht = ccstate->dispatch->hypertable;
	RemoteCopyContext *context;
	uint64 processed;

	context = remote_copy_begin(stmt,
								ht,
								GetPerTupleExprContext(estate),
								attnums,
								copy_should_send_binary(stmt));
	PG_TRY();
	{
		StringInfoData row_data;
		List *chunk_data_nodes = NIL;
		int32 chunk_id = INVALID_CHUNK_ID;
		Chunk *chunk = NULL;

		initStringInfo(&row_data);

		MemoryContextSwitchTo(GetPerTupleMemoryContext(ccstate->estate));

		while (true)
		{
			bool found;
			Point *p;

			Assert(CurrentMemoryContext == GetPerTupleMemoryContext(ccstate->estate));

			CHECK_FOR_INTERRUPTS();
			ResetPerTupleExprContext(ccstate->estate);
			resetStringInfo(&row_data);

			p = read_next_copy_row(context, ccstate->cstate, &row_data);

			if (p == NULL)
				break;

			chunk = ts_hypertable_find_chunk_for_point(ht, p);

			if (chunk == NULL)
			{
				/*
				 * Since at least one connection will switch out of COPY_IN
				 * mode to create the chunk, we need to first end the current
				 * CopyData message and flush the data. Do it on all
				 * connections, even though only one connection might be
				 * affected (unless replication_factor > 1).
				 */
				write_copy_data_end(context);

				/* No need to exit out of the COPY_IN mode in order to create
				 * the chunk on the same connection; it is handled
				 * automatically by remote_dist_txn_get_connection(). */
				chunk = ts_hypertable_create_chunk_for_point(ht, p, &found);
			}
			else
				found = true;

			/*
			 * Get the filtered list of "available" DNs for this chunk but only if it's replicated.
			 * We only fetch the filtered list once. Assuming that inserts will typically go to the
			 * same chunk we should be able to reuse this filtered list a few more times
			 *
			 * The worse case scenario is one in which INSERT1 goes into CHUNK1, INSERT2 goes into
			 * CHUNK2, INSERT3 goes into CHUNK1,... in which case we will end up refreshing the list
			 * everytime
			 *
			 * We will also enter the below loop if we KNOW that any of the DNs has been marked
			 * unavailable before we started this transaction. If not, then we know that every
			 * chunk's datanode list is fine and no stale chunk metadata updates are needed.
			 */
			if (context->dns_unavailable && found && ht->fd.replication_factor > 1)
			{
				MemoryContext oldmctx = MemoryContextSwitchTo(context->mctx);

				if (chunk_id != chunk->fd.id)
					chunk_id = INVALID_CHUNK_ID;

				if (chunk_id == INVALID_CHUNK_ID)
				{
					if (chunk_data_nodes)
						list_free(chunk_data_nodes);
					chunk_data_nodes =
						ts_chunk_data_node_scan_by_chunk_id_filter(chunk->fd.id,
																   CurrentMemoryContext);
					chunk_id = chunk->fd.id;
				}

				Assert(chunk_id == chunk->fd.id);
				Assert(chunk_data_nodes != NIL);
				/*
				 * If the chunk was not created as part of this insert, we need to check whether any
				 * of the chunk's data nodes are currently unavailable and in that case consider the
				 * chunk stale on those data nodes. Do that by removing the AN's chunk-datanode
				 * mapping for the unavailable data nodes.
				 *
				 * Note that the metadata will only get updated once since we assign the chunk's
				 * data_node list to the list of available DNs the first time this
				 * dist_update_stale_chunk_metadata API gets called. So both chunk_data_nodes and
				 * chunk->data_nodes will point to the same list and no subsequent metadata updates
				 * will occur.
				 */
				if (ht->fd.replication_factor > list_length(chunk_data_nodes))
					ts_cm_functions->dist_update_stale_chunk_metadata(chunk, chunk_data_nodes);

				MemoryContextSwitchTo(oldmctx);
			}

			/*
			 * For remote copy, we don't use chunk insert states on the AN.
			 * So we need to explicitly set the chunk as partial when copies
			 * are directed to previously compressed chunks.
			 */
			if (ts_chunk_is_compressed(chunk) && (!ts_chunk_is_partial(chunk)))
				ts_chunk_set_partial(chunk);

			/*
			 * Write the copy data to the data node connections.
			 */
			send_row_to_data_nodes(context, chunk->data_nodes, &row_data, false);
			context->num_rows++;
		}

		/* End the final CopyData messages, if any, and flush. */
		write_copy_data_end(context);
		MemoryContextSwitchTo(orig_mctx);

		if (chunk_data_nodes)
			list_free(chunk_data_nodes);
	}
	PG_CATCH();
	{
		/* If we hit an error, make sure we end our in-progress COPYs */
		end_copy_on_failure(&context->connection_state);
		MemoryContextDelete(context->mctx);
		PG_RE_THROW();
	}
	PG_END_TRY();

	processed = context->num_rows;

	remote_copy_end_on_success(context);

	return processed;
}

/*
 * Send a tuple/row to data nodes.
 *
 * The slot is serialized in text or binary format, depending on setting. The
 * data is already "routed" to the "right" chunk as indicated by the chunk
 * insert state.
 */
void
remote_copy_send_slot(RemoteCopyContext *context, TupleTableSlot *slot, const ChunkInsertState *cis)
{
	ListCell *lc;
	StringInfoData row_data;

	initStringInfo(&row_data);

	/* Pre-materialize all attributes since we will access all of them */
	slot_getallattrs(slot);

	if (context->binary_operation)
	{
		BinaryCopyContext *binctx = context->data_context;

		MemSet(binctx->nulls, 0, list_length(context->attnums) * sizeof(bool));

		foreach (lc, context->attnums)
		{
			AttrNumber attnum = lfirst_int(lc);
			int i = AttrNumberGetAttrOffset(attnum);

			binctx->values[i] = slot_getattr(slot, attnum, &binctx->nulls[i]);
		}

		write_binary_copy_data(binctx->values,
							   binctx->nulls,
							   context->attnums,
							   binctx->out_functions,
							   &row_data);
	}
	else
	{
		TextCopyContext *textctx = context->data_context;
		char delim = textctx->delimiter;

		foreach (lc, context->attnums)
		{
			AttrNumber attnum = lfirst_int(lc);
			bool isnull;
			Datum value;

			if (lc == list_tail(context->attnums))
				delim = '\n';

			value = slot_getattr(slot, attnum, &isnull);

			if (isnull)
				appendStringInfo(&row_data, "%s%c", textctx->null_string, delim);
			else
			{
				int off = AttrNumberGetAttrOffset(attnum);
				const char *output = OutputFunctionCall(&textctx->out_functions[off], value);
				appendStringInfo(&row_data, "%s%c", output, delim);
			}
		}
	}

	PG_TRY();
	{
		send_row_to_data_nodes(context, cis->chunk_data_nodes, &row_data, true);
	}
	PG_CATCH();
	{
		/* If we hit an error, make sure we end our in-progress COPYs */
		end_copy_on_failure(&context->connection_state);
		MemoryContextDelete(context->mctx);
		PG_RE_THROW();
	}
	PG_END_TRY();
}
