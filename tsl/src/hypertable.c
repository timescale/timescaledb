/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <postgres.h>
#include <catalog/pg_type.h>
#include <catalog/pg_proc.h>
#include <catalog/pg_inherits.h>
#include <funcapi.h>
#include <utils/builtins.h>
#include <utils/lsyscache.h>
#include <utils/syscache.h>
#include <utils/array.h>
#include <miscadmin.h>
#include <access/xact.h>
#include <cache.h>
#include <nodes/pg_list.h>

#include "dimension.h"
#include "errors.h"
#include "hypertable.h"
#include "utils.h"
#include "hypertable_cache.h"
#include "chunk.h"
#include "ts_catalog/chunk_data_node.h"

#include <foreign/foreign.h>
#include <libpq-fe.h>

#include "fdw/fdw.h"
#include "data_node.h"
#include "deparse.h"
#include "remote/dist_commands.h"
#include "compat/compat.h"
#include "ts_catalog/hypertable_data_node.h"
#include "extension.h"

static List *
data_node_append(List *data_nodes, int32 hypertable_id, const char *node_name,
				 int32 node_hypertable_id, bool block_chunks)
{
	ForeignServer *server = data_node_get_foreign_server(node_name, ACL_NO_CHECK, true, false);
	HypertableDataNode *hdn = palloc0(sizeof(HypertableDataNode));

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
hypertable_create_data_node_tables(int32 hypertable_id, List *data_nodes)
{
	Hypertable *ht = ts_hypertable_get_by_id(hypertable_id);
	ListCell *cell;
	List *remote_ids = NIL;
	DistCmdResult *dist_res;
	DeparsedHypertableCommands *commands = deparse_get_distributed_hypertable_create_command(ht);

	foreach (cell, deparse_get_tabledef_commands(ht->main_table_relid))
		ts_dist_cmd_run_on_data_nodes(lfirst(cell), data_nodes, true);

	dist_res = ts_dist_cmd_invoke_on_data_nodes(commands->table_create_command, data_nodes, true);
	foreach (cell, data_nodes)
	{
		PGresult *res = ts_dist_cmd_get_result_by_node_name(dist_res, lfirst(cell));

		Assert(PQntuples(res) == 1);
		Assert(PQnfields(res) == AttrNumberGetAttrOffset(_Anum_create_hypertable_max));
		remote_ids =
			lappend(remote_ids,
					(void *) Int32GetDatum(atoi(
						PQgetvalue(res, 0, AttrNumberGetAttrOffset(Anum_create_hypertable_id)))));
	}
	ts_dist_cmd_close_response(dist_res);

	foreach (cell, commands->dimension_add_commands)
		ts_dist_cmd_run_on_data_nodes(lfirst(cell), data_nodes, true);

	foreach (cell, commands->grant_commands)
		ts_dist_cmd_run_on_data_nodes(lfirst(cell), data_nodes, true);

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
	List *remote_ids = hypertable_create_data_node_tables(hypertable_id, nodes);
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

/*
 * Validate data nodes when creating a new hypertable.
 *
 * The function is passed the explicit array of data nodes given by the user,
 * if any.
 *
 * If the data node array is NULL (no data nodes specified), we return all
 * data nodes that the user is allowed to use.
 *
 */
List *
hypertable_get_and_validate_data_nodes(ArrayType *nodearr)
{
	bool fail_on_aclcheck = nodearr != NULL;
	List *data_nodes = NIL;
	int num_data_nodes;
	List *all_data_nodes = NIL;

	/* If the user explicitly specified a set of data nodes (data_node_arr is
	 * non-NULL), we validate the given array and fail if the user doesn't
	 * have USAGE on all of them. Otherwise, we get a list of all
	 * database-configured data nodes that the user has USAGE on. */
	data_nodes = data_node_get_filtered_node_name_list(nodearr, ACL_USAGE, fail_on_aclcheck);
	num_data_nodes = list_length(data_nodes);

	if (NULL == nodearr)
	{
		/* No explicit set of data nodes given. Check if there are any data
		 * nodes that the user cannot use due to lack of permissions and
		 * raise a NOTICE if some of them cannot be used. */
		all_data_nodes = data_node_get_node_name_list();
		int num_nodes_not_used = list_length(all_data_nodes) - list_length(data_nodes);

		if (num_nodes_not_used > 0)
			ereport(NOTICE,
					(errmsg("%d of %d data nodes not used by this hypertable due to lack of "
							"permissions",
							num_nodes_not_used,
							list_length(all_data_nodes)),
					 errhint("Grant USAGE on data nodes to attach them to a hypertable.")));
	}

	/*
	 * In this case, if we couldn't find any valid data nodes to assign, it
	 * means that they do not have the right permissions on the data nodes or
	 * that there were no data nodes to assign. Depending on the case, we
	 * print different error details and hints to aid the user.
	 */
	if (num_data_nodes == 0)
		ereport(ERROR,
				(errcode(ERRCODE_TS_INSUFFICIENT_NUM_DATA_NODES),
				 errmsg("no data nodes can be assigned to the hypertable"),
				 errdetail(list_length(all_data_nodes) == 0 ?
							   "No data nodes where available to assign to the hypertable." :
							   "Data nodes exist, but none have USAGE privilege."),
				 errhint(list_length(all_data_nodes) == 0 ?
							 "Add data nodes to the database." :
							 "Grant USAGE on data nodes to attach them to the hypertable.")));

	if (num_data_nodes == 1)
		ereport(WARNING,
				(errmsg("only one data node was assigned to the hypertable"),
				 errdetail("A distributed hypertable should have at least two data nodes for best "
						   "performance."),
				 errhint(
					 list_length(all_data_nodes) == 1 ?
						 "Add more data nodes to the database and attach them to the hypertable." :
						 "Grant USAGE on data nodes and attach them to the hypertable.")));

	if (num_data_nodes > MAX_NUM_HYPERTABLE_DATA_NODES)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("max number of data nodes exceeded"),
				 errhint("The number of data nodes cannot exceed %d.",
						 MAX_NUM_HYPERTABLE_DATA_NODES)));

	return data_nodes;
}

void
hypertable_make_distributed(Hypertable *ht, List *data_node_names)
{
	hypertable_assign_data_nodes(ht->fd.id, data_node_names);
}

static bool
hypertable_is_underreplicated(Hypertable *const ht, const int16 replication_factor)
{
	ListCell *lc;
	List *chunks = find_inheritance_children(ht->main_table_relid, NoLock);

	Assert(hypertable_is_distributed(ht));

	foreach (lc, chunks)
	{
		Oid chunk_oid = lfirst_oid(lc);
		Chunk *chunk = ts_chunk_get_by_relid(chunk_oid, true);
		List *replicas = ts_chunk_data_node_scan_by_chunk_id(chunk->fd.id, CurrentMemoryContext);

		Assert(get_rel_relkind(chunk_oid) == RELKIND_FOREIGN_TABLE);

		if (list_length(replicas) < replication_factor)
			return true;
	}
	return false;
}

static void
update_replication_factor(Hypertable *const ht, const int32 replication_factor_in)
{
	const int16 replication_factor =
		ts_validate_replication_factor(replication_factor_in, false, true);

	ht->fd.replication_factor = replication_factor;
	ts_hypertable_update(ht);
	if (list_length(ht->data_nodes) < replication_factor)
		ereport(ERROR,
				(errcode(ERRCODE_TS_INSUFFICIENT_NUM_DATA_NODES),
				 errmsg("replication factor too large for hypertable \"%s\"",
						NameStr(ht->fd.table_name)),
				 errdetail("The hypertable has %d data nodes attached, while "
						   "the replication factor is %d.",
						   list_length(ht->data_nodes),
						   replication_factor),
				 errhint("Decrease the replication factor or attach more data "
						 "nodes to the hypertable.")));
	if (hypertable_is_underreplicated(ht, replication_factor))
		ereport(WARNING,
				(errcode(ERRCODE_WARNING),
				 errmsg("hypertable \"%s\" is under-replicated", NameStr(ht->fd.table_name)),
				 errdetail("Some chunks have less than %d replicas.", replication_factor)));
}

Datum
hypertable_set_replication_factor(PG_FUNCTION_ARGS)
{
	const Oid table_relid = PG_ARGISNULL(0) ? InvalidOid : PG_GETARG_OID(0);
	const int32 replication_factor_in = PG_ARGISNULL(1) ? 0 : PG_GETARG_INT32(1);
	Cache *hcache;
	Hypertable *ht;

	TS_PREVENT_FUNC_IF_READ_ONLY();

	if (!OidIsValid(table_relid))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid hypertable: cannot be NULL")));

	hcache = ts_hypertable_cache_pin();
	ht = ts_hypertable_cache_get_entry(hcache, table_relid, CACHE_FLAG_NONE);

	if (!hypertable_is_distributed(ht))
		ereport(ERROR,
				(errcode(ERRCODE_TS_HYPERTABLE_NOT_DISTRIBUTED),
				 errmsg("hypertable \"%s\" is not distributed", get_rel_name(table_relid))));

	update_replication_factor(ht, replication_factor_in);

	ts_cache_release(hcache);

	PG_RETURN_VOID();
}
