/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <funcapi.h>
#include <utils/builtins.h>
#include <utils/pg_lsn.h>
#include <utils/guc.h>
#include <access/xlog_internal.h>
#include <access/xlog.h>
#include <access/xact.h>
#include <catalog/pg_foreign_server.h>
#include <storage/lmgr.h>
#include <miscadmin.h>

#include "errors.h"
#include "guc.h"
#include "ts_catalog/catalog.h"
#include "debug_point.h"
#include "dist_util.h"
#include "remote/dist_commands.h"
#include "data_node.h"
#include "dist_backup.h"

#define TS_ACCESS_NODE_TYPE "access_node"
#define TS_DATA_NODE_TYPE "data_node"

enum
{
	Anum_restore_point_node_name = 1,
	Anum_restore_point_node_type,
	Anum_restore_point_lsn,
	_Anum_restore_point_max
};

static Datum
create_restore_point_datum(TupleDesc tupdesc, const char *node_name, XLogRecPtr lsn)
{
	Datum values[_Anum_restore_point_max] = { 0 };
	bool nulls[_Anum_restore_point_max] = { false };
	HeapTuple tuple;
	NameData node_name_nd;

	tupdesc = BlessTupleDesc(tupdesc);
	if (node_name == NULL)
	{
		nulls[AttrNumberGetAttrOffset(Anum_restore_point_node_name)] = true;
		values[AttrNumberGetAttrOffset(Anum_restore_point_node_type)] =
			CStringGetTextDatum(TS_ACCESS_NODE_TYPE);
	}
	else
	{
		namestrcpy(&node_name_nd, node_name);
		values[AttrNumberGetAttrOffset(Anum_restore_point_node_name)] = NameGetDatum(&node_name_nd);
		values[AttrNumberGetAttrOffset(Anum_restore_point_node_type)] =
			CStringGetTextDatum(TS_DATA_NODE_TYPE);
	}
	values[AttrNumberGetAttrOffset(Anum_restore_point_lsn)] = LSNGetDatum(lsn);
	tuple = heap_form_tuple(tupdesc, values, nulls);
	return HeapTupleGetDatum(tuple);
}

Datum
create_distributed_restore_point(PG_FUNCTION_ARGS)
{
	const char *name = TextDatumGetCString(PG_GETARG_DATUM(0));
	DistCmdResult *result_cmd;
	FuncCallContext *funcctx;
	XLogRecPtr lsn;

	if (SRF_IS_FIRSTCALL())
	{
		int name_len = strlen(name);
		MemoryContext oldctx;
		TupleDesc tupdesc;
		char *sql;

		if (name_len >= MAXFNAMELEN)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("restore point name is too long"),
					 errdetail("Maximum length is %d, while provided name has %d chars.",
							   MAXFNAMELEN - 1,
							   name_len)));

		if (RecoveryInProgress())
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 (errmsg("recovery is in progress"),
					  errdetail("WAL control functions cannot be executed during recovery."))));

		if (!XLogIsNeeded())
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("WAL level '%s' is not sufficient for creating a restore point",
							GetConfigOptionByName("wal_level", NULL, false)),
					 errhint("Set wal_level to \"replica\" or \"logical\" at server start.")));

		if (!superuser())
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
					 errmsg("must be superuser to create restore point")));

		if (!ts_guc_enable_2pc)
			ereport(ERROR,
					(errcode(ERRCODE_TS_OPERATION_NOT_SUPPORTED),
					 errmsg("two-phase commit transactions are not enabled"),
					 errhint("Set timescaledb.enable_2pc to TRUE.")));

		if (dist_util_membership() != DIST_MEMBER_ACCESS_NODE)
			ereport(ERROR,
					(errcode(ERRCODE_TS_OPERATION_NOT_SUPPORTED),
					 errmsg("distributed restore point must be created on the access node"),
					 errhint("Connect to the access node and create the distributed restore point "
							 "from there.")));

		/* Ensure all data nodes are available */
		data_node_fail_if_nodes_are_unavailable();

		/*
		 * In order to achieve synchronization across the multinode cluster,
		 * we must ensure that the restore point created on the access node is
		 * synchronized with each data node.
		 *
		 * We must ensure that no concurrent prepared transactions are
		 * committed (COMMIT PREPARED) while we create the restore point.
		 * Otherwise, the distributed restore point might include prepared transactions
		 * that have committed on some data nodes but not others, leading to an
		 * inconsistent state when the distributed database is restored from a backup
		 * using the restore point.
		 *
		 * To do that we take an access exclusive lock on the remote transaction
		 * table, which will force any concurrent transaction
		 * wait during their PREPARE phase.
		 */
		LockRelationOid(ts_catalog_get()->tables[REMOTE_TXN].id, AccessExclusiveLock);

		/* Prevent situation when new data node added during the execution */
		LockRelationOid(ForeignServerRelationId, ExclusiveLock);

		DEBUG_WAITPOINT("create_distributed_restore_point_lock");

		funcctx = SRF_FIRSTCALL_INIT();
		oldctx = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("function returning record called in context "
							"that cannot accept type record")));

		/* Create local restore point and on each data node */
		lsn = XLogRestorePoint(name);

		sql = psprintf("SELECT pg_create_restore_point AS lsn "
					   "FROM "
					   "pg_catalog.pg_create_restore_point(%s)",
					   quote_literal_cstr(name));

		result_cmd = ts_dist_cmd_invoke_on_all_data_nodes(sql);

		funcctx->attinmeta = TupleDescGetAttInMetadata(tupdesc);
		funcctx->user_fctx = result_cmd;

		MemoryContextSwitchTo(oldctx);

		/* Return access node restore point first */
		SRF_RETURN_NEXT(funcctx, create_restore_point_datum(tupdesc, NULL, lsn));
	}

	funcctx = SRF_PERCALL_SETUP();
	result_cmd = funcctx->user_fctx;

	/* Return data node restore point data */
	if (result_cmd)
	{
		int result_index = funcctx->call_cntr - 1;

		if (result_index < (int) ts_dist_cmd_response_count(result_cmd))
		{
			const char *node_name;
			PGresult *result =
				ts_dist_cmd_get_result_by_index(result_cmd, result_index, &node_name);
			AttInMetadata *attinmeta = funcctx->attinmeta;
			const int lsn_attr_pos = AttrNumberGetAttrOffset(Anum_restore_point_lsn);

			lsn = DatumGetLSN(InputFunctionCall(&attinmeta->attinfuncs[lsn_attr_pos],
												PQgetvalue(result, 0, 0),
												attinmeta->attioparams[lsn_attr_pos],
												attinmeta->atttypmods[lsn_attr_pos]));

			SRF_RETURN_NEXT(funcctx,
							create_restore_point_datum(attinmeta->tupdesc, node_name, lsn));
		}

		ts_dist_cmd_close_response(result_cmd);
	}

	SRF_RETURN_DONE(funcctx);
}
