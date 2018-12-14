/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <postgres.h>
#include <catalog/pg_type.h>
#include <utils/builtins.h>
#include <utils/lsyscache.h>
#include <utils/syscache.h>
#include <miscadmin.h>
#include <access/xact.h>
#include <cache.h>
#include <nodes/pg_list.h>
#include <utils/array.h>
#include <foreign/foreign.h>

#include <hypertable_server.h>
#include <errors.h>

#include "errors.h"
#include "hypertable.h"
#include "dimension.h"
#include "license.h"
#include "utils.h"
#include "hypertable_cache.h"
#include "fdw/timescaledb_fdw.h"
#include "server.h"

Datum
hypertable_valid_ts_interval(PG_FUNCTION_ARGS)
{
	/* this function does all the necessary validation and if successfull,
	returns the interval which is not necessary here */
	ts_interval_from_tuple(PG_GETARG_DATUM(0));
	PG_RETURN_BOOL(true);
}

static List *
server_append(List *servers, int32 hypertable_id, const char *servername)
{
	ForeignServer *server = GetForeignServerByName(servername, false);
	HypertableServer *hs = palloc0(sizeof(HypertableServer));
	ForeignDataWrapper *fdw = GetForeignDataWrapper(server->fdwid);

	if (strcmp(fdw->fdwname, TIMESCALEDB_FDW_NAME) != 0)
		elog(ERROR, "invalid foreign data wrapper \"%s\" for hypertable", fdw->fdwname);

	hs->fd.hypertable_id = hypertable_id;
	namestrcpy(&hs->fd.server_name, servername);
	hs->foreign_server_oid = server->serverid;

	return lappend(servers, hs);
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
static List *
hypertable_assign_servers(int32 hypertable_id, List *servers)
{
	ListCell *lc;
	List *assigned_servers = NIL;

	foreach (lc, servers)
	{
		const char *servername = lfirst(lc);

		assigned_servers = server_append(assigned_servers, hypertable_id, servername);
	}

	ts_hypertable_server_insert_multi(assigned_servers);

	return assigned_servers;
}

static List *
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
