/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include "dist_copy.h"
#include "remote/connection_cache.h"
#include "dimension.h"
#include "hypertable.h"
#include <utils/lsyscache.h>
#include "partitioning.h"
#include "chunk.h"
#include "chunk_data_node.h"
#include <miscadmin.h>
#include "remote/dist_txn.h"
#include <utils/builtins.h>
#include <libpq-fe.h>
#include <catalog/namespace.h>
#include <parser/parse_type.h>
#include <executor/executor.h>
#include <access/tupdesc.h>
#include "guc.h"

#define DEFAULT_PG_DELIMITER '\t'
#define DEFAULT_PG_NULL_VALUE "\\N"
#define POSTGRES_BINARY_COPY_SIGNATURE "PGCOPY\n\377\r\n\0"
#define POSTGRES_SIGNATURE_LENGTH 11

/* This will maintain a list of connections associated with a given chunk so we don't have to keep
 * looking them up every time.
 */
typedef struct ChunkConnectionList
{
	Chunk *chunk;
	List *connections;
} ChunkConnectionList;

/* This contains the information needed to parse a dimension attribute out of a row of text copy
 * data
 */
typedef struct CopyDimensionInfo
{
	Dimension *dim;
	int corresponding_copy_field;
	Datum default_value;
	FmgrInfo io_func;
	Oid typioparams;
	int32 atttypmod;
} CopyDimensionInfo;

/* This contains information about connections currently in use by the copy as well as how to create
 * and end the copy command.
 */
typedef struct CopyConnectionState
{
	List *cached_connections;
	List *connections_in_use;
	bool using_binary;
	MemoryContext mctx;
	const char *outgoing_copy_cmd;
} CopyConnectionState;

/* This contains the state needed by a non-binary copy operation.
 */
typedef struct TextCopyContext
{
	MemoryContext orig_context;
	int ndimensions;
	CopyDimensionInfo *dimensions;
	char delimiter;
	char *null_string;
	MemoryContext tuple_context;
	char **fields;
	int nfields;
} TextCopyContext;

/* This contains the state needed by a binary copy operation.
 */
typedef struct BinaryCopyContext
{
	MemoryContext orig_context;
	FmgrInfo *out_functions;
	EState *estate;
	Datum *values;
	bool *nulls;
} BinaryCopyContext;

/* This is this high level state needed for an in-progress copy command.
 */
typedef struct CopyContext
{
	/* Operation data */
	CopyConnectionState connection_state;
	void *data_context; /* TextCopyContext or BinaryCopyContext */
	bool binary_operation;

	/* Data for the current read row */
	StringInfo row_data;
} CopyContext;

/* This will create and populate a CopyDimensionInfo struct from the passed in dimensions and values
 */
static CopyDimensionInfo *
generate_copy_dimensions(Dimension *dims, int ndimensions, List *attnums, Hypertable *ht)
{
	CopyDimensionInfo *result = palloc(ndimensions * sizeof(CopyDimensionInfo));
	int idx;

	for (idx = 0; idx < ndimensions; ++idx)
	{
		Dimension *d = &dims[idx];
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
			if (info->dim->type == DIMENSION_TYPE_OPEN)
				ereport(ERROR,
						(errcode(ERRCODE_NOT_NULL_VIOLATION),
						 errmsg("NULL value in column \"%s\" violates not-null constraint",
								NameStr(info->dim->fd.column_name)),
						 errhint("Columns used for time partitioning cannot be NULL")));

			return 0;
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
convert_datum_to_dim_idx(Datum datum, Dimension *d)
{
	Oid dimtype;

	if (d->partitioning)
		datum = ts_partitioning_func_apply(d->partitioning, datum);

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

static Point *
calculate_hyperspace_point_from_fields(char **data, CopyDimensionInfo *dimensions,
									   int num_dimensions)
{
	Point *p;
	int i;

	p = palloc0(POINT_SIZE(num_dimensions));
	p->cardinality = num_dimensions;
	p->num_coords = num_dimensions;

	for (i = 0; i < num_dimensions; ++i)
	{
		Datum datum = get_copy_dimension_datum(data, &dimensions[i]);
		p->coordinates[i] = convert_datum_to_dim_idx(datum, dimensions[i].dim);
	}

	return p;
}

static void
send_binary_copy_header(PGconn *connection)
{
	StringInfo header = makeStringInfo();
	uint32_t buf = 0;
	int result;

	appendBinaryStringInfo(header,
						   POSTGRES_BINARY_COPY_SIGNATURE,
						   POSTGRES_SIGNATURE_LENGTH);			/* signature */
	appendBinaryStringInfo(header, (char *) &buf, sizeof(buf)); /* flags */
	appendBinaryStringInfo(header, (char *) &buf, sizeof(buf)); /* header extension length */

	result = PQputCopyData(connection, header->data, 19);
	if (result != 1)
		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_FAILURE),
				 errmsg("failed to send data to data server %s", PQhost(connection))));
}

static void
start_remote_copy_on_new_connection(CopyConnectionState *state, TSConnection *connection)
{
	PGconn *pg_conn = remote_connection_get_pg_conn(connection);
	if (PQisnonblocking(pg_conn))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("distributed copy doesn't support non-blocking connections")));

	if (!list_member_ptr(state->connections_in_use, connection))
	{
		PGresult *res = PQexec(pg_conn, state->outgoing_copy_cmd);

		if (PQresultStatus(res) != PGRES_COPY_IN)
			ereport(ERROR,
					(errcode(ERRCODE_CONNECTION_FAILURE),
					 errmsg("unable to start remote COPY on backend data server (%d)",
							PQresultStatus(res))));

		if (state->using_binary)
			send_binary_copy_header(pg_conn);

		state->connections_in_use = lappend(state->connections_in_use, connection);
	}
}

static ChunkConnectionList *
create_connection_list_for_chunk(CopyConnectionState *state, Chunk *chunk)
{
	ChunkConnectionList *chunk_connections;
	ListCell *lc;

	MemoryContext oldcontext = MemoryContextSwitchTo(state->mctx);
	chunk_connections = palloc(sizeof(ChunkConnectionList));
	chunk_connections->chunk = chunk;
	chunk_connections->connections = NIL;
	foreach (lc, chunk->servers)
	{
		ChunkServer *cs = lfirst(lc);
		ForeignServer *fs = GetForeignServerByName(NameStr(cs->fd.server_name), false);
		UserMapping *um = GetUserMapping(GetUserId(), fs->serverid);
		TSConnection *connection = remote_dist_txn_get_connection(um, REMOTE_TXN_NO_PREP_STMT);

		start_remote_copy_on_new_connection(state, connection);
		chunk_connections->connections = lappend(chunk_connections->connections, connection);
	}
	state->cached_connections = lappend(state->cached_connections, chunk_connections);
	MemoryContextSwitchTo(oldcontext);
	return chunk_connections;
}

static int
send_end_binary_copy_data(PGconn *connection)
{
	const uint16_t buf = htons((uint16_t) -1);
	return PQputCopyData(connection, (char *) &buf, sizeof(buf));
}

static void
finish_outstanding_copies(CopyConnectionState *state)
{
	ListCell *lc;
	List *results = NIL;

	foreach (lc, state->connections_in_use)
	{
		TSConnection *conn = lfirst(lc);
		PGconn *pg_conn = remote_connection_get_pg_conn(conn);
		PGresult *res;

		if (state->using_binary)
			if (send_end_binary_copy_data(pg_conn) != 1)
				ereport(ERROR,
						(errcode(ERRCODE_CONNECTION_EXCEPTION),
						 errmsg("%s", PQerrorMessage(pg_conn))));

		if (PQputCopyEnd(pg_conn, NULL) == -1)
			ereport(ERROR,
					(errcode(ERRCODE_CONNECTION_EXCEPTION), errmsg("%s", PQerrorMessage(pg_conn))));

		results = lappend(results, PQgetResult(pg_conn));
		/* Need to get result a second time to move the connection out of copy mode */
		res = PQgetResult(pg_conn);
		Assert(res == NULL);
	}

	foreach (lc, results)
		if (PQresultStatus(lfirst(lc)) != PGRES_COMMAND_OK)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("error during copy completion: %s", PQresultErrorMessage(lfirst(lc)))));
}

static List *
get_connections_for_chunk(CopyConnectionState *state, Chunk *chunk)
{
	ListCell *lc;

	foreach (lc, state->cached_connections)
		if (((ChunkConnectionList *) lfirst(lc))->chunk == chunk)
			return ((ChunkConnectionList *) lfirst(lc))->connections;

	return create_connection_list_for_chunk(state, chunk)->connections;
}

static bool
copy_should_send_binary()
{
	return ts_guc_enable_connection_binary_data;
}

/* Extract a quoted list of identifiers from a DefElem with arg type T_list */
static char *
name_list_to_string(DefElem *def)
{
	StringInfoData string;
	ListCell *lc;
	bool first = true;

	initStringInfo(&string);

	foreach (lc, (List *) def->arg)
	{
		Node *name = (Node *) lfirst(lc);

		if (!first)
			appendStringInfo(&string, ", ");
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
def_get_string(DefElem *def)
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

/* Generate a COPY sql command for sending the data being passed in via 'stmt' to a backend data
 * node.
 */
static const char *
deparse_copy_cmd(const CopyStmt *stmt, Hypertable *ht, bool binary)
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

	if (stmt->options != NULL || binary)
	{
		bool first = true;
		appendStringInfo(command, " WITH (");
		foreach (lc, stmt->options)
		{
			DefElem *defel = lfirst_node(DefElem, lc);
			const char *option = defel->defname;

			/* Ignore text only options for binary copy */
			if (binary && !is_supported_binary_option(option))
				continue;

			if (!first)
				appendStringInfo(command, ", ");
			else
				first = false;

			/* quoted options */
			if (strcmp(option, "delimiter") == 0 || strcmp(option, "null") == 0 ||
				strcmp(option, "quote") == 0 || strcmp(option, "escape") == 0 ||
				strcmp(option, "encoding") == 0)
				appendStringInfo(command, "%s '%s'", option, def_get_string(defel));
			/* options that take columns (note force_quote is only for COPY TO) */
			else if (strcmp(option, "force_not_null") == 0 || strcmp(option, "force_null") == 0)
				appendStringInfo(command, "%s (%s)", option, def_get_string(defel));
			/* boolean options don't require an argument to use default setting */
			else if (defel->arg == NULL &&
					 (strcmp(option, "oids") == 0 || strcmp(option, "freeze") == 0 ||
					  strcmp(option, "header") == 0))
				appendStringInfo(command, "%s", option);
			/* everything else should pass directly through */
			else
				appendStringInfo(command, "%s %s", option, def_get_string(defel));
		}
		if (binary)
			appendStringInfo(command, "%sformat binary", first ? "" : ", ");
		appendStringInfo(command, ")");
	}

	return command->data;
}

/* This function checks the options specified for the copy command and makes sure they're supported.
It also determines what delimiter and null encoding are being specified and will use these values
when sending data to the backend as they presumably won't conflict with the values being passed.
Note that the CopyBegin call will have such validation as checking for duplicate options, this
function just checks added constraints for the distributed copy.  This call is only needed when
sending data in text format to the data backend. */
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
		DefElem *defel = lfirst_node(DefElem, lc);

		if (strcmp(defel->defname, "format") == 0)
		{
			char *fmt;

			Assert(nodeTag(defel->arg) == T_String);
			fmt = strVal(defel->arg);

			if (strcmp(fmt, "binary") == 0)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("remote copy does not support binary data")));
			else if (strcmp(fmt, "csv") == 0 && !delimiter_found)
				*delimiter = ',';
		}
		else if (strcmp(defel->defname, "delimiter") == 0)
		{
			char *delim_string = def_get_string(defel);

			Assert(strlen(delim_string) == 1);
			*delimiter = delim_string[0];
			delimiter_found = true;
		}
		else if (strcmp(defel->defname, "null") == 0)
		{
			*null_string = def_get_string(defel);
		}
	}
}

static TextCopyContext *
generate_text_copy_context(const CopyStmt *stmt, Hypertable *ht, List *attnums)
{
	TextCopyContext *ctx = palloc(sizeof(TextCopyContext));

	ctx->ndimensions = ht->space->num_dimensions;
	validate_options(stmt->options, &ctx->delimiter, &ctx->null_string);
	ctx->dimensions =
		generate_copy_dimensions(ht->space->dimensions, ctx->ndimensions, attnums, ht);
	ctx->tuple_context =
		AllocSetContextCreate(CurrentMemoryContext, "COPY", ALLOCSET_DEFAULT_SIZES);

	ctx->orig_context = MemoryContextSwitchTo(ctx->tuple_context);
	return ctx;
}

/* Populates the passed in pointer with an array of output functions and returns the array size.
 * Note that we size the array to the number of columns in the hypertable for convenience, but only
 * populate the functions for columns used in the copy command.
 */
static int
get_copy_conversion_functions(Hypertable *ht, List *copy_attnums, FmgrInfo **functions)
{
	ListCell *lc;
	Relation rel = relation_open(ht->main_table_relid, AccessShareLock);
	TupleDesc tupDesc = RelationGetDescr(rel);

	*functions = palloc(tupDesc->natts * sizeof(FmgrInfo));
	foreach (lc, copy_attnums)
	{
		int attnum = lfirst_int(lc);
		Oid out_func_oid;
		bool isvarlena;
		Form_pg_attribute attr = TupleDescAttr(tupDesc, attnum - 1);

		getTypeBinaryOutputInfo(attr->atttypid, &out_func_oid, &isvarlena);
		fmgr_info(out_func_oid, &((*functions)[attnum - 1]));
	}
	relation_close(rel, AccessShareLock);

	return tupDesc->natts;
}

static BinaryCopyContext *
generate_binary_copy_context(const CopyStmt *stmt, Hypertable *ht, List *attnums)
{
	BinaryCopyContext *ctx = palloc(sizeof(BinaryCopyContext));
	int columns = get_copy_conversion_functions(ht, attnums, &ctx->out_functions);
	ctx->estate = CreateExecutorState();
	ctx->values = palloc(columns * sizeof(Datum));
	ctx->nulls = palloc(columns * sizeof(bool));

	ctx->orig_context = MemoryContextSwitchTo(GetPerTupleMemoryContext(ctx->estate));
	return ctx;
}

static CopyContext *
begin_remote_copy_operation(const CopyStmt *stmt, Hypertable *ht, List *attnums)
{
	CopyContext *context = palloc(sizeof(CopyContext));
	bool binary_copy = copy_should_send_binary();

	context->binary_operation = binary_copy;
	context->connection_state.connections_in_use = NIL;
	context->connection_state.cached_connections = NIL;
	context->connection_state.mctx = CurrentMemoryContext;
	context->connection_state.using_binary = binary_copy;
	context->connection_state.outgoing_copy_cmd = deparse_copy_cmd(stmt, ht, binary_copy);

	if (binary_copy)
		context->data_context = generate_binary_copy_context(stmt, ht, attnums);
	else
		context->data_context = generate_text_copy_context(stmt, ht, attnums);

	return context;
}

static StringInfo
parse_next_text_row(CopyState cstate, List *attnums, TextCopyContext *ctx)
{
	StringInfo row_data;
	int i;

	MemoryContextReset(ctx->tuple_context);

	if (!NextCopyFromRawFields(cstate, &ctx->fields, &ctx->nfields))
		return NULL;

	Assert(ctx->nfields == list_length(attnums));
	row_data = makeStringInfo();

	for (i = 0; i < ctx->nfields - 1; ++i)
		appendStringInfo(row_data,
						 "%s%c",
						 ctx->fields[i] ? ctx->fields[i] : ctx->null_string,
						 ctx->delimiter);

	appendStringInfo(row_data,
					 "%s\n",
					 ctx->fields[ctx->nfields - 1] ? ctx->fields[ctx->nfields - 1] :
													 ctx->null_string);

	return row_data;
}

static StringInfo
generate_binary_copy_data(Datum *values, bool *nulls, List *attnums, FmgrInfo *out_functions)
{
	StringInfo row_data = makeStringInfo();
	uint16_t buf16;
	uint32_t buf32;
	ListCell *lc;

	buf16 = htons((uint16_t) attnums->length);
	appendBinaryStringInfo(row_data, (char *) &buf16, sizeof(buf16));

	foreach (lc, attnums)
	{
		int attnum = lfirst_int(lc);

		if (nulls[attnum - 1])
		{
			buf32 = htonl((uint32_t) -1);
			appendBinaryStringInfo(row_data, (char *) &buf32, sizeof(buf32));
		}
		else
		{
			Datum value = values[attnum - 1];
			bytea *outputbytes;
			int output_length;

			outputbytes = SendFunctionCall(&out_functions[attnum - 1], value);
			output_length = VARSIZE(outputbytes) - VARHDRSZ;
			buf32 = htonl((uint32_t) output_length);
			appendBinaryStringInfo(row_data, (char *) &buf32, sizeof(buf32));
			appendBinaryStringInfo(row_data, VARDATA(outputbytes), output_length);
		}
	}

	return row_data;
}

static StringInfo
parse_next_binary_row(CopyState cstate, List *attnums, BinaryCopyContext *ctx)
{
	ResetPerTupleExprContext(ctx->estate);
	if (!NextCopyFrom(cstate, GetPerTupleExprContext(ctx->estate), ctx->values, ctx->nulls, NULL))
		return NULL;

	return generate_binary_copy_data(ctx->values, ctx->nulls, attnums, ctx->out_functions);
}

static bool
read_next_copy_row(CopyContext *context, CopyState cstate, List *attnums)
{
	if (context->binary_operation)
		context->row_data = parse_next_binary_row(cstate, attnums, context->data_context);
	else
		context->row_data = parse_next_text_row(cstate, attnums, context->data_context);

	return context->row_data != NULL;
}

static Point *
get_current_point_for_text_copy(Hypertable *ht, TextCopyContext *ctx)
{
	return calculate_hyperspace_point_from_fields(ctx->fields, ctx->dimensions, ctx->ndimensions);
}

static Point *
calculate_hyperspace_point_from_binary(Datum *values, bool *nulls, Hyperspace *space)
{
	Point *p;
	int i;

	p = palloc0(POINT_SIZE(space->num_dimensions));
	p->cardinality = space->num_dimensions;
	p->num_coords = space->num_dimensions;

	for (i = 0; i < space->num_dimensions; ++i)
	{
		Dimension *dim = &space->dimensions[i];
		Datum datum = values[dim->column_attno - 1];

		if (nulls[dim->column_attno - 1])
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("value required for partitioning column %s",
							NameStr(dim->fd.column_name))));
		p->coordinates[i] = convert_datum_to_dim_idx(datum, dim);
	}

	return p;
}

static Point *
get_current_point_for_binary_copy(Hypertable *ht, BinaryCopyContext *ctx)
{
	return calculate_hyperspace_point_from_binary(ctx->values, ctx->nulls, ht->space);
}

static void
reset_copy_connection_state(CopyConnectionState *state)
{
	finish_outstanding_copies(state);
	list_free(state->cached_connections);
	list_free(state->connections_in_use);
	state->cached_connections = NIL;
	state->connections_in_use = NIL;
}

static Chunk *
get_target_chunk(Hypertable *ht, Point *p, CopyConnectionState *state)
{
	Chunk *chunk = ts_hypertable_find_chunk_if_exists(ht, p);

	if (chunk == NULL)
	{
		/* Here we need to create a new chunk.  However, any in-progress copy operations
		 * will be tying up the connection we need to create the chunk on a backend.  Since
		 * the backends for the new chunk aren't yet known, just close all in progress COPYs
		 * before creating the chunk. */
		reset_copy_connection_state(state);
		chunk = ts_hypertable_get_or_create_chunk(ht, p);
	}

	return chunk;
}

static void
send_copy_data(StringInfo row_data, List *connections)
{
	ListCell *lc;

	foreach (lc, connections)
	{
		PGconn *pg_conn = remote_connection_get_pg_conn(lfirst(lc));
		int result = PQputCopyData(pg_conn, row_data->data, row_data->len);

		if (result != 1)
			ereport(ERROR,
					(errcode(ERRCODE_CONNECTION_EXCEPTION),
					 errmsg("%s",
							result == -1 ? PQerrorMessage(pg_conn) :
										   "unexpected response while sending copy data")));
	}
}

static void
process_and_send_copy_data(CopyContext *context, Hypertable *ht)
{
	Point *point;
	Chunk *chunk;
	List *connections;

	if (context->binary_operation)
		point = get_current_point_for_binary_copy(ht, context->data_context);
	else
		point = get_current_point_for_text_copy(ht, context->data_context);

	chunk = get_target_chunk(ht, point, &context->connection_state);
	connections = get_connections_for_chunk(&context->connection_state, chunk);
	send_copy_data(context->row_data, connections);
}

static void
cleanup_text_copy_context(TextCopyContext *ctx)
{
	MemoryContextSwitchTo(ctx->orig_context);
	MemoryContextDelete(ctx->tuple_context);
}

static void
cleanup_binary_copy_context(BinaryCopyContext *ctx)
{
	MemoryContextSwitchTo(ctx->orig_context);
	FreeExecutorState(ctx->estate);
}

static void
end_copy_operation(CopyContext *context)
{
	finish_outstanding_copies(&context->connection_state);
	if (context->binary_operation)
		cleanup_binary_copy_context(context->data_context);
	else
		cleanup_text_copy_context(context->data_context);
}

void
remote_distributed_copy(const CopyStmt *stmt, uint64 *processed, Hypertable *ht, CopyState cstate,
						List *attnums)
{
	CopyContext *context = begin_remote_copy_operation(stmt, ht, attnums);

	*processed = 0;

	PG_TRY();
	{
		while (true)
		{
			CHECK_FOR_INTERRUPTS();

			if (!read_next_copy_row(context, cstate, attnums))
				break;

			process_and_send_copy_data(context, ht);
			++*processed;
		}
	}
	PG_CATCH();
	{
		/* If we hit an error, make sure we end our in-progress COPYs */
		end_copy_operation(context);

		PG_RE_THROW();
	}
	PG_END_TRY();

	end_copy_operation(context);
}
