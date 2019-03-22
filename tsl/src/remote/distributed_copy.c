/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include "distributed_copy.h"
#include "remote/connection_cache.h"
#include "dimension.h"
#include "hypertable.h"
#include <utils/lsyscache.h>
#include "partitioning.h"
#include "chunk.h"
#include "chunk_server.h"
#include <miscadmin.h>
#include "remote/dist_txn.h"
#include <utils/builtins.h>
#include <libpq-fe.h>
#include <catalog/namespace.h>
#include <parser/parse_type.h>

typedef struct ChunkConnectionList
{
	Chunk *chunk;
	List *connections;
} ChunkConnectionList;

typedef struct CopyDimensionInfo
{
	Dimension *dim;
	int corresponding_copy_field;
	Datum default_value;
	FmgrInfo io_func;
	Oid typioparams;
	int32 atttypmod;
} CopyDimensionInfo;

#define DEFAULT_PG_DELIMITER '\t'
#define DEFAULT_PG_NULL_VALUE "\\N"

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
					 errmsg("unable to use default value for partitioning column %s",
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
calculate_hyperspace_point(char **data, CopyDimensionInfo *dimensions, int num_dimensions)
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

static List *
get_server_connections(List *servers, List **existing_conns, const char *initial_copy_command)
{
	List *result = NIL;
	ListCell *lc;

	foreach (lc, servers)
	{
		ChunkServer *cs = lfirst(lc);
		ForeignServer *fs = GetForeignServerByName(NameStr(cs->fd.server_name), false);
		UserMapping *um = GetUserMapping(GetUserId(), fs->serverid);
		PGconn *connection = remote_dist_txn_get_connection(um, REMOTE_TXN_NO_PREP_STMT);

		if (PQisnonblocking(connection))
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("distributed copy doesn't support non-blocking connections")));

		if (!list_member_ptr(*existing_conns, connection))
		{
			PGresult *res = PQexec(connection, initial_copy_command);

			if (PQresultStatus(res) != PGRES_COPY_IN)
				ereport(ERROR,
						(errcode(ERRCODE_CONNECTION_FAILURE),
						 errmsg("unable to start remote COPY on backend data server (%d)",
								PQresultStatus(res))));

			*existing_conns = lappend(*existing_conns, connection);
		}

		result = lappend(result, connection);
	}

	return result;
}

static void
finish_copy_commands(List *conns)
{
	ListCell *lc;
	List *results = NIL;

	/* First send all the copyEnd commands, then check the results (potentially ereporting) */
	foreach (lc, conns)
	{
		PGconn *connection = lfirst(lc);
		PGresult *res;

		if (PQputCopyEnd(connection, NULL) == -1)
			ereport(ERROR,
					(errcode(ERRCODE_CONNECTION_EXCEPTION),
					 errmsg("%s", PQerrorMessage(connection))));

		results = lappend(results, PQgetResult(connection));
		/* Need to get result a second time to move the connection out of copy mode */
		res = PQgetResult(connection);
		Assert(res == NULL);
	}

	foreach (lc, results)
		if (PQresultStatus(lfirst(lc)) != PGRES_COMMAND_OK)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR), errmsg("error during copy completion")));
}

static List *
get_chunk_connections(Chunk *target, List **cached_connections, List **connections_in_use,
					  const char *copyStartSql, MemoryContext function_context)
{
	ListCell *lc;
	ChunkConnectionList *conns;
	MemoryContext oldcontext = CurrentMemoryContext;

	foreach (lc, *cached_connections)
	{
		conns = lfirst(lc);
		if (conns->chunk == target)
			return conns->connections;
	}

	MemoryContextSwitchTo(function_context);
	conns = palloc(sizeof(ChunkConnectionList));
	conns->chunk = target;
	conns->connections = get_server_connections(target->servers, connections_in_use, copyStartSql);
	*cached_connections = lappend(*cached_connections, conns);
	MemoryContextSwitchTo(oldcontext);
	return conns->connections;
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

static const char *
deparse_copy_cmd(const CopyStmt *stmt, Hypertable *ht)
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

	if (stmt->options != NULL)
	{
		bool first = true;
		appendStringInfo(command, " WITH (");
		foreach (lc, stmt->options)
		{
			DefElem *defel = lfirst_node(DefElem, lc);
			const char *option = defel->defname;

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
		appendStringInfo(command, ")");
	}

	return command->data;
}

/* This function checks the options specified for the copy command and makes sure they're supported.
It also determines what delimiter and null encoding are being specified and will use these values
when sending data to the backend as they presumably won't conflict with the values being passed.
Note that the CopyBegin call will have such validation as checking for duplicate options, this
function just checks added constraints for the distributed copy. */
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

static Chunk *
get_target_chunk(Hypertable *ht, Point *p, List **all_connections, List **per_chunk_connections,
				 MemoryContext function_context)
{
	Chunk *chunk = ts_hypertable_find_chunk_if_exists(ht, p);

	if (chunk == NULL)
	{
		MemoryContext oldcontext = MemoryContextSwitchTo(function_context);

		/* Here we need to create a new chunk.  However, any in-progress copy operations
		 * will be tying up the connection we need to create the chunk on a backend.  Since
		 * the backends for the new chunk aren't yet known, just close all in progress COPYs
		 * before creating the chunk. */
		if (*all_connections != NIL)
		{
			finish_copy_commands(*all_connections);
			list_free(*all_connections);
			*all_connections = NIL;
			list_free(*per_chunk_connections);
			*per_chunk_connections = NIL;
		}

		chunk = ts_hypertable_get_or_create_chunk(ht, p);
		MemoryContextSwitchTo(oldcontext);
	}

	return chunk;
}

static void
send_copy_data(StringInfo row_data, List *connections)
{
	ListCell *lc;

	foreach (lc, connections)
	{
		int result = PQputCopyData(lfirst(lc), row_data->data, row_data->len);

		if (result != 1)
			ereport(ERROR,
					(errcode(ERRCODE_CONNECTION_EXCEPTION),
					 errmsg("%s",
							result == -1 ? PQerrorMessage(lfirst(lc)) :
										   "unexpected response while sending copy data")));
	}
}

void
remote_distributed_copy(const CopyStmt *stmt, uint64 *processed, Hypertable *ht, CopyState cstate,
						List *attnums)
{
	char **fields;
	int nfields;
	const int ndimensions = ht->space->num_dimensions;
	CopyDimensionInfo *dimensions;
	const char *copy_start_sql = deparse_copy_cmd(stmt, ht);
	char delimiter;
	char *null_string;
	List *all_connections = NIL;	   /* connections currently being copied to */
	List *per_chunk_connections = NIL; /* maps chunk to connections, should never have more than a
										  few chunks, so list is efficient enough */
	MemoryContext tuple_context;
	MemoryContext function_context;

	validate_options(stmt->options, &delimiter, &null_string);
	*processed = 0;
	dimensions = generate_copy_dimensions(ht->space->dimensions, ndimensions, attnums, ht);
	tuple_context = AllocSetContextCreate(CurrentMemoryContext, "COPY", ALLOCSET_DEFAULT_SIZES);
	function_context = MemoryContextSwitchTo(tuple_context);

	PG_TRY();
	{
		/* This will assert if input is in binary format */
		while (NextCopyFromRawFields(cstate, &fields, &nfields))
		{
			Point *p;
			StringInfo row_data = makeStringInfo();
			Chunk *chunk;
			int i;
			List *connections = NIL;

			Assert(nfields == list_length(attnums));
			CHECK_FOR_INTERRUPTS();

			for (i = 0; i < nfields - 1; ++i)
				appendStringInfo(row_data, "%s%c", fields[i] ? fields[i] : null_string, delimiter);

			appendStringInfo(row_data,
							 "%s\n",
							 fields[nfields - 1] ? fields[nfields - 1] : null_string);

			p = calculate_hyperspace_point(fields, dimensions, ndimensions);
			chunk =
				get_target_chunk(ht, p, &all_connections, &per_chunk_connections, function_context);
			connections = get_chunk_connections(chunk,
												&per_chunk_connections,
												&all_connections,
												copy_start_sql,
												function_context);

			send_copy_data(row_data, connections);

			++*processed;
			MemoryContextReset(tuple_context);
		}
	}
	PG_CATCH();
	{
		MemoryContextSwitchTo(function_context);
		MemoryContextDelete(tuple_context);

		/* If we hit an error, make sure we end our in-progress COPYs */
		finish_copy_commands(all_connections);

		PG_RE_THROW();
	}
	PG_END_TRY();

	MemoryContextSwitchTo(function_context);
	MemoryContextDelete(tuple_context);

	finish_copy_commands(all_connections);
}
