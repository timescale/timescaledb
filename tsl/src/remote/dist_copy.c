/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <access/tupdesc.h>
#include <catalog/namespace.h>
#include <executor/executor.h>
#include <libpq-fe.h>
#include <miscadmin.h>
#include <parser/parse_type.h>
#include <port/pg_bswap.h>
#include <utils/builtins.h>
#include <utils/lsyscache.h>

#include "compat/compat.h"
#include "dist_copy.h"
#include "copy.h"
#include "nodes/chunk_dispatch.h"
#include "nodes/chunk_insert_state.h"
#include "dimension.h"
#include "hypertable.h"
#include "partitioning.h"
#include "chunk.h"
#include "ts_catalog/chunk_data_node.h"
#include "guc.h"
#include "remote/connection_cache.h"
#include "remote/dist_txn.h"
#include "data_node.h"

#define DEFAULT_PG_DELIMITER '\t'
#define DEFAULT_PG_NULL_VALUE "\\N"

/* This will maintain a list of connections associated with a given chunk so we don't have to keep
 * looking them up every time.
 */
typedef struct ChunkConnectionList
{
	int32 chunk_id;
	List *connections;
} ChunkConnectionList;

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

/* This contains information about connections currently in use by the copy as well as how to create
 * and end the copy command.
 */
typedef struct CopyConnectionState
{
	List *cached_connections;
	List *connections_in_use;
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
	List *attnums;
	void *data_context; /* TextCopyContext or BinaryCopyContext */
	bool binary_operation;
	MemoryContext mctx; /* MemoryContext that holds the RemoteCopyContext */

	/* Data for the current read row */
	StringInfo row_data;
} RemoteCopyContext;

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
start_remote_copy_on_new_connection(CopyConnectionState *state, TSConnection *connection)
{
	TSConnectionError err;

	state->connections_in_use = list_append_unique_ptr(state->connections_in_use, connection);

	if (remote_connection_get_status(connection) == CONN_IDLE &&
		!remote_connection_begin_copy(connection,
									  state->outgoing_copy_cmd,
									  state->using_binary,
									  &err))
		remote_connection_error_elog(&err, ERROR);
}

static const ChunkConnectionList *
create_connection_list_for_chunk(CopyConnectionState *state, int32 chunk_id,
								 const List *chunk_data_nodes, Oid userid)
{
	ChunkConnectionList *chunk_connections;
	ListCell *lc;

	chunk_connections = palloc0(sizeof(ChunkConnectionList));
	chunk_connections->chunk_id = chunk_id;
	chunk_connections->connections = NIL;

	foreach (lc, chunk_data_nodes)
	{
		ChunkDataNode *cdn = lfirst(lc);
		TSConnectionId id = remote_connection_id(cdn->foreign_server_oid, userid);
		TSConnection *connection = remote_dist_txn_get_connection(id, REMOTE_TXN_NO_PREP_STMT);

		start_remote_copy_on_new_connection(state, connection);
		chunk_connections->connections = lappend(chunk_connections->connections, connection);
	}
	state->cached_connections = lappend(state->cached_connections, chunk_connections);

	return chunk_connections;
}

static void
finish_outstanding_copies(const CopyConnectionState *state)
{
	ListCell *lc;
	TSConnectionError err;
	bool failure = false;

	foreach (lc, state->connections_in_use)
	{
		TSConnection *conn = lfirst(lc);

		if (remote_connection_get_status(conn) == CONN_COPY_IN &&
			!remote_connection_end_copy(conn, &err))
			failure = true;
	}

	if (failure)
		remote_connection_error_elog(&err, ERROR);
}

static const List *
get_connections_for_chunk(RemoteCopyContext *context, int32 chunk_id, const List *chunk_data_nodes,
						  Oid userid)
{
	CopyConnectionState *state = &context->connection_state;
	MemoryContext oldmctx;
	ListCell *lc;
	List *conns;

	foreach (lc, state->cached_connections)
	{
		ChunkConnectionList *chunkconns = lfirst(lc);

		if (chunkconns->chunk_id == chunk_id)
		{
#ifdef USE_ASSERT_CHECKING
			ListCell *lc2;

			/* Check that connections are in COPY_IN mode */
			foreach (lc2, chunkconns->connections)
			{
				TSConnection *conn = lfirst(lc2);

				Assert(remote_connection_get_status(conn) == CONN_COPY_IN);
			}
#endif /* USE_ASSERT_CHECKING */
			return chunkconns->connections;
		}
	}

	oldmctx = MemoryContextSwitchTo(context->mctx);
	conns =
		create_connection_list_for_chunk(state, chunk_id, chunk_data_nodes, userid)->connections;
	MemoryContextSwitchTo(oldmctx);

	return conns;
}

static bool
copy_should_send_binary()
{
	return ts_guc_enable_connection_binary_data;
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
			appendStringInfo(command, "%sFORMAT binary", first ? "" : ", ");
		appendStringInfo(command, ")");
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
						 errmsg("remote copy does not support binary data")));
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

RemoteCopyContext *
remote_copy_begin(const CopyStmt *stmt, Hypertable *ht, ExprContext *per_tuple_ctx, List *attnums,
				  bool binary_copy)
{
	MemoryContext mctx =
		AllocSetContextCreate(CurrentMemoryContext, "Remote COPY", ALLOCSET_DEFAULT_SIZES);
	RemoteCopyContext *context;
	MemoryContext oldmctx;

	oldmctx = MemoryContextSwitchTo(mctx);
	context = palloc0(sizeof(RemoteCopyContext));
	context->ht = ht;
	context->attnums = attnums;
	context->mctx = mctx;
	context->binary_operation = binary_copy;
	context->connection_state.connections_in_use = NIL;
	context->connection_state.cached_connections = NIL;
	context->connection_state.using_binary = binary_copy;
	context->connection_state.outgoing_copy_cmd = deparse_copy_cmd(stmt, ht, binary_copy);

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

static StringInfo
parse_next_text_row(CopyFromState cstate, List *attnums, TextCopyContext *ctx)
{
	StringInfo row_data = makeStringInfo();
	int i;

	if (!NextCopyFromRawFields(cstate, &ctx->fields, &ctx->nfields))
		return NULL;

	Assert(ctx->nfields == list_length(attnums));

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

	return row_data;
}

static StringInfo
parse_next_binary_row(CopyFromState cstate, List *attnums, BinaryCopyContext *ctx)
{
	if (!NextCopyFrom(cstate, ctx->econtext, ctx->values, ctx->nulls))
		return NULL;

	return generate_binary_copy_data(ctx->values, ctx->nulls, attnums, ctx->out_functions);
}

static bool
read_next_copy_row(RemoteCopyContext *context, CopyFromState cstate)
{
	if (context->binary_operation)
		context->row_data = parse_next_binary_row(cstate, context->attnums, context->data_context);
	else
		context->row_data = parse_next_text_row(cstate, context->attnums, context->data_context);

	return context->row_data != NULL;
}

static Point *
get_current_point_for_text_copy(TextCopyContext *ctx)
{
	return calculate_hyperspace_point_from_fields(ctx->fields, ctx->dimensions, ctx->ndimensions);
}

static Point *
calculate_hyperspace_point_from_binary(Datum *values, bool *nulls, const Hyperspace *space)
{
	Point *p;
	int i;

	p = palloc0(POINT_SIZE(space->num_dimensions));
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

	return p;
}

static Point *
get_current_point_for_binary_copy(BinaryCopyContext *ctx, const Hyperspace *hs)
{
	return calculate_hyperspace_point_from_binary(ctx->values, ctx->nulls, hs);
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
		 * will be tying up the connection we need to create the chunk on a data node.  Since
		 * the data nodes for the new chunk aren't yet known, just close all in progress COPYs
		 * before creating the chunk. */
		reset_copy_connection_state(state);
		chunk = ts_hypertable_get_or_create_chunk(ht, p);
	}

	return chunk;
}

static bool
send_copy_data(StringInfo row_data, const List *connections)
{
	ListCell *lc;

	foreach (lc, connections)
	{
		TSConnection *conn = lfirst(lc);
		TSConnectionError err;

		if (!remote_connection_put_copy_data(conn, row_data->data, row_data->len, &err))
			remote_connection_error_elog(&err, ERROR);
	}

	return true;
}

static bool
remote_copy_process_and_send_data(RemoteCopyContext *context)
{
	Hypertable *ht = context->ht;
	Chunk *chunk;
	Point *point;
	const List *connections;

	if (context->binary_operation)
		point = get_current_point_for_binary_copy(context->data_context, ht->space);
	else
		point = get_current_point_for_text_copy(context->data_context);

	chunk = get_target_chunk(ht, point, &context->connection_state);
	connections = get_connections_for_chunk(context, chunk->fd.id, chunk->data_nodes, GetUserId());

	/* for remote copy, we don't use chunk insert states on the AN.
	 * so we need to explicitly set the chunk as unordered when copies
	 * are directed to previously compressed chunks
	 */
	if (ts_chunk_is_compressed(chunk) && (!ts_chunk_is_unordered(chunk)))
		ts_chunk_set_unordered(chunk);

	return send_copy_data(context->row_data, connections);
}

void
remote_copy_end(RemoteCopyContext *context)
{
	finish_outstanding_copies(&context->connection_state);
	MemoryContextDelete(context->mctx);
}

uint64
remote_distributed_copy(const CopyStmt *stmt, CopyChunkState *ccstate, List *attnums)
{
	MemoryContext oldmctx = CurrentMemoryContext;
	EState *estate = ccstate->estate;
	Hypertable *ht = ccstate->dispatch->hypertable;
	RemoteCopyContext *context = remote_copy_begin(stmt,
												   ht,
												   GetPerTupleExprContext(estate),
												   attnums,
												   copy_should_send_binary());
	uint64 processed = 0;

	PG_TRY();
	{
		while (true)
		{
			ResetPerTupleExprContext(ccstate->estate);
			MemoryContextSwitchTo(GetPerTupleMemoryContext(ccstate->estate));

			CHECK_FOR_INTERRUPTS();

			if (!read_next_copy_row(context, ccstate->cstate))
				break;

			remote_copy_process_and_send_data(context);
			++processed;
		}
	}
	PG_CATCH();
	{
		/* If we hit an error, make sure we end our in-progress COPYs */
		remote_copy_end(context);
		PG_RE_THROW();
	}
	PG_END_TRY();

	remote_copy_end(context);
	MemoryContextSwitchTo(oldmctx);

	return processed;
}

/*
 * Send a tuple/row to data nodes.
 *
 * The slot is serialized in text or binary format, depending on setting. The
 * data is already "routed" to the "right" chunk as indicated by the chunk
 * insert state.
 */
bool
remote_copy_send_slot(RemoteCopyContext *context, TupleTableSlot *slot, const ChunkInsertState *cis)
{
	ListCell *lc;
	bool result;

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

		context->row_data = generate_binary_copy_data(binctx->values,
													  binctx->nulls,
													  context->attnums,
													  binctx->out_functions);
	}
	else
	{
		TextCopyContext *textctx = context->data_context;
		char delim = textctx->delimiter;

		context->row_data = makeStringInfo();

		foreach (lc, context->attnums)
		{
			AttrNumber attnum = lfirst_int(lc);
			bool isnull;
			Datum value;

			if (lc == list_tail(context->attnums))
				delim = '\n';

			value = slot_getattr(slot, attnum, &isnull);

			if (isnull)
				appendStringInfo(context->row_data, "%s%c", textctx->null_string, delim);
			else
			{
				int off = AttrNumberGetAttrOffset(attnum);
				const char *output = OutputFunctionCall(&textctx->out_functions[off], value);
				appendStringInfo(context->row_data, "%s%c", output, delim);
			}
		}
	}

	PG_TRY();
	{
		const List *connections;

		connections =
			get_connections_for_chunk(context, cis->chunk_id, cis->chunk_data_nodes, cis->user_id);
		Assert(list_length(connections) == list_length(cis->chunk_data_nodes));
		Assert(list_length(connections) > 0);
		result = send_copy_data(context->row_data, connections);
	}
	PG_CATCH();
	{
		/* If we hit an error, make sure we end our in-progress COPYs */
		remote_copy_end(context);
		PG_RE_THROW();
	}
	PG_END_TRY();

	return result;
}
