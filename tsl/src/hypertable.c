/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <postgres.h>
#include <catalog/pg_type.h>
#include <catalog/pg_proc.h>
#include <utils/builtins.h>
#include <utils/lsyscache.h>
#include <utils/syscache.h>
#include <utils/array.h>
#include <miscadmin.h>
#include <access/xact.h>
#include <cache.h>
#include <nodes/pg_list.h>
#include <foreign/foreign.h>
#include <libpq-fe.h>

#include <errors.h>

#include "errors.h"
#include "fdw/fdw.h"
#include "hypertable.h"
#include "dimension.h"
#include "license.h"
#include "utils.h"
#include "hypertable_cache.h"
#include "data_node.h"
#include "deparse.h"
#include "remote/dist_commands.h"
#include "compat.h"
#include "hypertable_data_node.h"
#include <extension.h>

Datum
hypertable_valid_ts_interval(PG_FUNCTION_ARGS)
{
	/* this function does all the necessary validation and if successfull,
	returns the interval which is not necessary here */
	ts_interval_from_tuple(PG_GETARG_DATUM(0));
	PG_RETURN_BOOL(true);
}

#if PG_VERSION_SUPPORTS_MULTINODE

static List *
server_append(List *servers, int32 hypertable_id, const char *servername,
			  int32 server_hypertable_id, bool block_chunks)
{
	ForeignServer *server = GetForeignServerByName(servername, false);
	HypertableServer *hs = palloc0(sizeof(HypertableServer));
	ForeignDataWrapper *fdw = GetForeignDataWrapper(server->fdwid);

	if (strcmp(fdw->fdwname, TIMESCALEDB_FDW_NAME) != 0)
		elog(ERROR, "invalid foreign data wrapper \"%s\" for hypertable", fdw->fdwname);

	hs->fd.hypertable_id = hypertable_id;
	namestrcpy(&hs->fd.server_name, servername);
	hs->fd.server_hypertable_id = server_hypertable_id;
	hs->foreign_server_oid = server->serverid;
	hs->fd.block_chunks = block_chunks;

	return lappend(servers, hs);
}

/*  Returns the remote hypertable ids for the servers (in the same order)
 */
static List *
hypertable_create_backend_tables(int32 hypertable_id, List *servers)
{
	Hypertable *ht = ts_hypertable_get_by_id(hypertable_id);
	ListCell *cell;
	List *remote_ids = NIL;
	DistCmdResult *dist_res;
	DeparsedHypertableCommands *commands = deparse_get_distributed_hypertable_create_command(ht);

	foreach (cell, deparse_get_tabledef_commands(ht->main_table_relid))
		ts_dist_cmd_run_on_servers(lfirst(cell), servers);

	dist_res = ts_dist_cmd_invoke_on_servers(commands->table_create_command, servers);
	foreach (cell, servers)
	{
		PGresult *res = ts_dist_cmd_get_server_result(dist_res, lfirst(cell));

		Assert(PQntuples(res) == 1);
		Assert(PQnfields(res) == AttrNumberGetAttrOffset(_Anum_create_hypertable_max));
		remote_ids =
			lappend(remote_ids,
					(void *) Int32GetDatum(atoi(
						PQgetvalue(res, 0, AttrNumberGetAttrOffset(Anum_create_hypertable_id)))));
	}
	ts_dist_cmd_close_response(dist_res);

	foreach (cell, commands->dimension_add_commands)
		ts_dist_cmd_run_on_servers(lfirst(cell), servers);

	return remote_ids;
}

/*
 * Assign servers to a hypertable.
 *
 * Given a list of (foreign) server names, add mappings to ensure the
 * hypertable is distributed across those servers.
 *
 * Returns a list of HypertableServer objects that correspond to the given
 * server names.
 */
List *
hypertable_assign_servers(int32 hypertable_id, List *servers)
{
	ListCell *lc;
	List *assigned_servers = NIL;
	List *remote_ids = hypertable_create_backend_tables(hypertable_id, servers);
	ListCell *id_cell;

	Assert(servers->length == remote_ids->length);
	forboth (lc, servers, id_cell, remote_ids)
	{
		assigned_servers =
			server_append(assigned_servers, hypertable_id, lfirst(lc), lfirst_int(id_cell), false);
	}

	ts_hypertable_server_insert_multi(assigned_servers);

	return assigned_servers;
}

void
hypertable_make_distributed(Hypertable *ht, ArrayType *servers)
{
	List *serverlist;

	if (NULL == servers)
		serverlist = server_get_servername_list();
	else
		serverlist = hypertable_server_array_to_list(servers);

	if (list_length(serverlist) == 0)
		ereport(ERROR,
				(errcode(ERRCODE_TS_NO_SERVERS),
				 errmsg("no servers can be assigned to \"%s\"", get_rel_name(ht->main_table_relid)),
				 errhint("Add servers using the add_server() function.")));

	hypertable_assign_servers(ht->fd.id, serverlist);
}

List *
hypertable_server_array_to_list(ArrayType *serverarr)
{
	ArrayIterator it = array_create_iterator(serverarr, 0, NULL);
	Datum server_datum;
	bool isnull;
	List *servers = NIL;

	while (array_iterate(it, &server_datum, &isnull))
	{
		if (!isnull)
			servers = lappend(servers, NameStr(*DatumGetName(server_datum)));
	}

	array_free_iterator(it);

	return servers;
}
#endif /* PG_VERSION_SUPPORTS_MULTINODE */
