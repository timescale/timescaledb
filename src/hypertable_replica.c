#include <postgres.h>
#include <catalog/namespace.h>
#include <utils/lsyscache.h>

#include "hypertable_replica.h"

#include "scanner.h"
#include "catalog.h"


typedef struct FormScanCtx
{
	size_t		len;
	void	   *record;
} FormScanCtx;


static bool
form_tuple_found(TupleInfo *ti, void *data)
{
	FormScanCtx *ctx = data;

	if (NULL == ctx->record)
	{
		ctx->record = palloc(ctx->len);
	}
	memcpy(ctx->record, GETSTRUCT(ti->tuple), ctx->len);
	return false;
}

/*
 *	Retrieve a hypertable replica by hypertable_id and replica_id.
 *
 *	The first argument can be null for a newly palloc'ed record;
 *
 */

static Form_hypertable_replica
hypertable_replica_scan(Form_hypertable_replica rec, int32 hypertable_id, int32 replica_id)
{
	Catalog    *catalog = catalog_get();
	FormScanCtx ctx = {
		.len = sizeof(FormData_hypertable_replica),
		.record = rec,
	};
	ScanKeyData scankey[2];
	ScannerCtx	scanCtx = {
		.table = catalog->tables[HYPERTABLE_REPLICA].id,
		.index = catalog->tables[HYPERTABLE_REPLICA].index_ids[HYPERTABLE_REPLICA_HYPERTABLE_REPLICA_INDEX],
		.scantype = ScannerTypeIndex,
		.nkeys = 2,
		.scankey = scankey,
		.data = &ctx,
		.tuple_found = form_tuple_found,
		.lockmode = AccessShareLock,
		.scandirection = ForwardScanDirection,
	};

	/* Perform an index scan on primary key. */
	ScanKeyInit(&scankey[0], Anum_hypertable_replica_hypertable_replica_idx_hypertable, BTEqualStrategyNumber,
				F_INT4EQ, Int32GetDatum(hypertable_id));
	ScanKeyInit(&scankey[1], Anum_hypertable_replica_hypertable_replica_idx_replica, BTEqualStrategyNumber,
				F_INT4EQ, Int32GetDatum(replica_id));

	if (scanner_scan(&scanCtx) == 0)
	{
		elog(ERROR, "Could not find hypertable_replica(%d, %d)", hypertable_id, replica_id);
	}

	return (Form_hypertable_replica) ctx.record;
}

/*
 *	Retrieve the default replica for the current database by hypertable_id.
 *
 *	The first argument can be null for a newly palloc'ed record;
 */

static Form_default_replica_node
default_replica_node_scan(Form_default_replica_node rec, int32 hypertable_id)
{
	Catalog    *catalog = catalog_get();
	FormScanCtx ctx = {
		.len = sizeof(FormData_default_replica_node),
		.record = rec,
	};
	ScanKeyData scankey[2];
	ScannerCtx	scanCtx = {
		.table = catalog->tables[DEFAULT_REPLICA_NODE].id,
		.index = catalog->tables[DEFAULT_REPLICA_NODE].index_ids[DEFAULT_REPLICA_NODE_DATABASE_HYPERTABLE_INDEX],
		.scantype = ScannerTypeIndex,
		.nkeys = 2,
		.scankey = scankey,
		.data = &ctx,
		.tuple_found = form_tuple_found,
		.lockmode = AccessShareLock,
		.scandirection = ForwardScanDirection,
	};

	/* Perform an index scan on primary key. */
	ScanKeyInit(&scankey[0], Anum_default_replica_node_database_hypertable_idx_database, BTEqualStrategyNumber,
				F_NAMEEQ, NameGetDatum(catalog->database_name));
	ScanKeyInit(&scankey[1], Anum_default_replica_node_database_hypertable_idx_hypertable, BTEqualStrategyNumber,
				F_INT4EQ, Int32GetDatum(hypertable_id));

	if (scanner_scan(&scanCtx) == 0)
	{
		elog(ERROR, "Could not find default_replica_node(%d, %s)", hypertable_id, catalog->database_name);
	}

	return (Form_default_replica_node) ctx.record;
}

Oid
hypertable_replica_get_table_relid(int32 hypertable_id)
{
	FormData_default_replica_node drn;
	FormData_hypertable_replica hr;
	Oid			replica_table;

	default_replica_node_scan(&drn, hypertable_id);
	hypertable_replica_scan(&hr, hypertable_id, drn.replica_id);
	replica_table = get_relname_relid(hr.table_name.data,
							  get_namespace_oid(hr.schema_name.data, false));

	return replica_table;
}
