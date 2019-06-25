/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <catalog/pg_type.h>
#include <catalog/pg_proc.h>
#include <nodes/pg_list.h>
#include <utils/array.h>
#include <utils/builtins.h>
#include <utils/lsyscache.h>
#include <utils/syscache.h>
#include <foreign/foreign.h>
#include <libpq-fe.h>

#include <errors.h>

#include "fdw/fdw.h"
#include "hypertable.h"
#include "data_node.h"
#include "deparse.h"
#include "remote/dist_commands.h"
#include "compat.h"
#include "hypertable_data_node.h"
#include <utils.h>
#include <extension.h>

static List *
data_node_append(List *data_nodes, int32 hypertable_id, const char *node_name,
				 int32 node_hypertable_id, bool block_chunks)
{
	ForeignServer *server = GetForeignServerByName(node_name, false);
	HypertableDataNode *hdn = palloc0(sizeof(HypertableDataNode));
	ForeignDataWrapper *fdw = GetForeignDataWrapper(server->fdwid);

	if (strcmp(fdw->fdwname, TIMESCALEDB_FDW_NAME) != 0)
		elog(ERROR, "invalid foreign data wrapper \"%s\" for hypertable", fdw->fdwname);

	hdn->fd.hypertable_id = hypertable_id;
	namestrcpy(&hdn->fd.node_name, node_name);
	hdn->fd.node_hypertable_id = node_hypertable_id;
	hdn->foreign_server_oid = server->serverid;
	hdn->fd.block_chunks = block_chunks;

	return lappend(data_nodes, hdn);
}

/*  Returns the remote hypertable ids for the data_nodes (in the same order)
 */
static List *
hypertable_create_backend_tables(int32 hypertable_id, List *data_nodes)
{
	Hypertable *ht = ts_hypertable_get_by_id(hypertable_id);
	ListCell *cell;
	List *remote_ids = NIL;
	DistCmdResult *dist_res;
	DeparsedHypertableCommands *commands = deparse_get_distributed_hypertable_create_command(ht);

	foreach (cell, deparse_get_tabledef_commands(ht->main_table_relid))
		ts_dist_cmd_run_on_data_nodes(lfirst(cell), data_nodes);

	dist_res = ts_dist_cmd_invoke_on_data_nodes(commands->table_create_command, data_nodes);
	foreach (cell, data_nodes)
	{
		PGresult *res = ts_dist_cmd_get_data_node_result(dist_res, lfirst(cell));

		Assert(PQntuples(res) == 1);
		Assert(PQnfields(res) == AttrNumberGetAttrOffset(_Anum_create_hypertable_max));
		remote_ids =
			lappend(remote_ids,
					(void *) Int32GetDatum(atoi(
						PQgetvalue(res, 0, AttrNumberGetAttrOffset(Anum_create_hypertable_id)))));
	}
	ts_dist_cmd_close_response(dist_res);

	foreach (cell, commands->dimension_add_commands)
		ts_dist_cmd_run_on_data_nodes(lfirst(cell), data_nodes);

	return remote_ids;
}

/*
 * Assign data nodes to a hypertable.
 *
 * Given a list of data node names, add mappings to ensure the
 * hypertable is distributed across those nodes.
 *
 * Returns a list of HypertableDataNode objects that correspond to the given
 * data node names.
 */
List *
hypertable_assign_data_nodes(int32 hypertable_id, List *nodes)
{
	ListCell *lc;
	List *assigned_nodes = NIL;
	List *remote_ids = hypertable_create_backend_tables(hypertable_id, nodes);
	ListCell *id_cell;

	Assert(nodes->length == remote_ids->length);
	forboth (lc, nodes, id_cell, remote_ids)
	{
		assigned_nodes =
			data_node_append(assigned_nodes, hypertable_id, lfirst(lc), lfirst_int(id_cell), false);
	}

	ts_hypertable_data_node_insert_multi(assigned_nodes);

	return assigned_nodes;
}

void
hypertable_make_distributed(Hypertable *ht, ArrayType *data_nodes)
{
	List *nodelist;

	if (NULL == data_nodes)
		nodelist = data_node_get_node_name_list();
	else
		nodelist = hypertable_data_node_array_to_list(data_nodes);

	if (list_length(nodelist) == 0)
		ereport(ERROR,
				(errcode(ERRCODE_TS_NO_DATA_NODES),
				 errmsg("no data nodes can be assigned to \"%s\"",
						get_rel_name(ht->main_table_relid)),
				 errhint("Add data nodes using the add_data_node() function.")));

	hypertable_assign_data_nodes(ht->fd.id, nodelist);
}

List *
hypertable_data_node_array_to_list(ArrayType *nodearr)
{
	ArrayIterator it = array_create_iterator(nodearr, 0, NULL);
	Datum data_node_datum;
	bool isnull;
	List *data_nodes = NIL;

	while (array_iterate(it, &data_node_datum, &isnull))
	{
		if (!isnull)
			data_nodes = lappend(data_nodes, NameStr(*DatumGetName(data_node_datum)));
	}

	array_free_iterator(it);

	return data_nodes;
}
