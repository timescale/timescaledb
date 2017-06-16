#include <postgres.h>
#include <utils/builtins.h>
#include <utils/lsyscache.h>
#include <catalog/namespace.h>
#include <catalog/pg_type.h>
#include <access/hash.h>
#include <parser/parse_coerce.h>

#include "partitioning.h"
#include "metadata_queries.h"
#include "scanner.h"
#include "catalog.h"
#include "utils.h"

static void
partitioning_func_set_func_fmgr(PartitioningFunc *pf)
{
	FuncCandidateList funclist =
	FuncnameGetCandidates(list_make2(makeString(pf->schema), makeString(pf->name)),
						  2, NULL, false, false, false);

	if (funclist == NULL || funclist->next)
	{
		elog(ERROR, "Could not resolve the partitioning function");
	}

	fmgr_info_cxt(funclist->oid, &pf->func_fmgr, CurrentMemoryContext);
}

static void
partitioning_info_set_textfunc_fmgr(PartitioningInfo *pi, Oid relid)
{
	Oid			type_id,
				func_id;
	bool		isVarlena;
	CoercionPathType cpt;

	pi->column_attnum = get_attnum(relid, pi->column);
	type_id = get_atttype(relid, pi->column_attnum);

	/*
	 * First look for an explicit cast type. Needed since the output of for
	 * example character(20) not the same as character(20)::text
	 */
	cpt = find_coercion_pathway(TEXTOID, type_id, COERCION_EXPLICIT, &func_id);
	if (cpt != COERCION_PATH_FUNC)
	{
		getTypeOutputInfo(type_id, &func_id, &isVarlena);
	}
	fmgr_info_cxt(func_id, &pi->partfunc.textfunc_fmgr, CurrentMemoryContext);
}

PartitioningInfo *
partitioning_info_create(int num_partitions,
						 const char *schema,
						 const char *partfunc,
						 const char *partcol,
						 int32 partmod,
						 Oid relid)
{
	PartitioningInfo *pi;

	pi = palloc0(sizeof(PartitioningInfo));
	pi->partfunc.modulos = partmod;
	strncpy(pi->partfunc.name, partfunc, NAMEDATALEN);
	strncpy(pi->column, partcol, NAMEDATALEN);

	if (schema != NULL)
	{
		strncpy(pi->partfunc.schema, schema, NAMEDATALEN);
	}

	partitioning_func_set_func_fmgr(&pi->partfunc);
	partitioning_info_set_textfunc_fmgr(pi, relid);

	return pi;
}

int16
partitioning_func_apply(PartitioningInfo *pinfo, Datum value)
{
	Datum text = FunctionCall1(&pinfo->partfunc.textfunc_fmgr, value);
	char	   *partition_val = DatumGetCString(text);
	Datum		keyspace_datum = FunctionCall2(&pinfo->partfunc.func_fmgr,
										  CStringGetTextDatum(partition_val),
									 Int32GetDatum(pinfo->partfunc.modulos));

	return DatumGetInt16(keyspace_datum);
}

int16
partitioning_func_apply_tuple(PartitioningInfo *pinfo, HeapTuple tuple, TupleDesc desc)
{
	Datum		value;
	bool		isnull;

	value = heap_getattr(tuple, pinfo->column_attnum, desc, &isnull);

	if (isnull)
	{
		return 0;
	}

	return partitioning_func_apply(pinfo, value);
}


#if 0
/* PartitionEpochCtx is used to pass on information during a partition epoch and
 * partition scans. */
typedef struct
{
	PartitionEpoch *pe;
	int16		num_partitions;
	int32		hypertable_id;
	int64		starttime,
				endtime,
				timepoint;
	Oid			relid;
} PartitionEpochCtx;

static int
			partition_scan(PartitionEpochCtx *pctx);

/* Filter partition epoch tuples based on hypertable ID and start/end time. */
static bool
partition_epoch_filter(TupleInfo *ti, void *arg)
{
	bool		is_null;
	PartitionEpochCtx *pctx = arg;
	Datum		hypertable_id = heap_getattr(ti->tuple, Anum_partition_epoch_hypertable_id, ti->desc, &is_null);

	if (DatumGetInt32(hypertable_id) == pctx->hypertable_id)
	{
		bool		starttime_is_null,
					endtime_is_null;
		Datum		starttime = heap_getattr(ti->tuple, Anum_partition_epoch_start_time, ti->desc, &starttime_is_null);
		Datum		endtime = heap_getattr(ti->tuple, Anum_partition_epoch_end_time, ti->desc, &endtime_is_null);

		pctx->starttime = starttime_is_null ? OPEN_START_TIME : DatumGetInt64(starttime);
		pctx->endtime = endtime_is_null ? OPEN_END_TIME : DatumGetInt64(endtime);

		return (starttime_is_null || pctx->timepoint >= pctx->starttime) &&
			(endtime_is_null || pctx->timepoint <= pctx->endtime);
	}
	return false;
}

#define PARTITION_EPOCH_SIZE(num_partitions) \
	sizeof(PartitionEpoch) + (sizeof(Partition) * num_partitions)

static PartitionEpoch *
partition_epoch_create(int32 epoch_id, PartitionEpochCtx *ctx)
{
	PartitionEpoch *pe;

	pe = palloc(PARTITION_EPOCH_SIZE(ctx->num_partitions));
	pe->id = epoch_id;
	pe->num_partitions = ctx->num_partitions;
	pe->hypertable_id = ctx->hypertable_id;
	pe->start_time = ctx->starttime;
	pe->end_time = ctx->endtime;
	return pe;
}

void
partition_epoch_free(PartitionEpoch *epoch)
{
	if (epoch->partitioning != NULL)
		pfree(epoch->partitioning);
	pfree(epoch);
}

/* Callback for partition epoch scan. For every epoch tuple found, create a
 * partition epoch entry and scan for associated partitions. */
static bool
partition_epoch_tuple_found(TupleInfo *ti, void *arg)
{
	PartitionEpochCtx *pctx = arg;
	PartitionEpoch *pe;
	int32		epoch_id;
	Datum		datum;
	bool		is_null;

	datum = heap_getattr(ti->tuple, Anum_partition_epoch_num_partitions, ti->desc, &is_null);
	pctx->num_partitions = DatumGetInt16(datum);
	datum = heap_getattr(ti->tuple, Anum_partition_epoch_id, ti->desc, &is_null);
	epoch_id = DatumGetInt32(datum);

	pe = partition_epoch_create(epoch_id, pctx);
	pctx->pe = pe;

	if (pctx->num_partitions > 1)
	{
		Datum		partfunc,
					partmod,
					partcol;
		bool		partfunc_is_null,
					partmod_is_null,
					partcol_is_null;

		partfunc = heap_getattr(ti->tuple, Anum_partition_epoch_partitioning_func, ti->desc, &partfunc_is_null);
		partmod = heap_getattr(ti->tuple, Anum_partition_epoch_partitioning_mod, ti->desc, &partmod_is_null);
		partcol = heap_getattr(ti->tuple, Anum_partition_epoch_partitioning_column, ti->desc, &partcol_is_null);

		if (partfunc_is_null || partmod_is_null || partcol_is_null)
		{
			elog(ERROR, "Invalid partitioning configuration for partition epoch %d", epoch_id);
		}

		datum = heap_getattr(ti->tuple, Anum_partition_epoch_partitioning_func_schema, ti->desc, &is_null);

		pe->partitioning = partitioning_info_create(pctx->num_partitions,
									 is_null ? NULL : DatumGetCString(datum),
													DatumGetCString(partfunc),
													DatumGetCString(partcol),
													DatumGetInt32(partmod),
													pctx->relid);
	}
	else
	{
		pe->partitioning = NULL;
	}

	/* Scan for the epoch's partitions */
	partition_scan(pctx);

	return true;
}

static bool
partition_tuple_found(TupleInfo *ti, void *arg)
{
	PartitionEpochCtx *pctx = arg;
	PartitionEpoch *pe = pctx->pe;
	Partition  *p = &pe->partitions[--pctx->num_partitions];
	Datum		values[Natts_partition];
	bool		isnull[Natts_partition];

	heap_deform_tuple(ti->tuple, ti->desc, values, isnull);

	p->id = DatumGetInt32(DATUM_GET(values, Anum_partition_id));
	p->keyspace_start = DatumGetInt16(DATUM_GET(values, Anum_partition_keyspace_start));
	p->keyspace_end = DatumGetInt16(DATUM_GET(values, Anum_partition_keyspace_end));
	p->index = pctx->num_partitions;

	/* Abort the scan if we have found all partitions */
	if (pctx->num_partitions == 0)
		return false;

	return true;
}

static int
partition_scan(PartitionEpochCtx *pctx)
{
	ScanKeyData scankey[1];
	Catalog    *catalog = catalog_get();
	int			num_partitions = pctx->num_partitions;
	ScannerCtx	scanCtx = {
		.table = catalog->tables[PARTITION].id,
		.index = catalog->tables[PARTITION].index_ids[PARTITION_PARTITION_EPOCH_ID_INDEX],
		.scantype = ScannerTypeIndex,
		.nkeys = 1,
		.scankey = scankey,
		.data = pctx,
		.tuple_found = partition_tuple_found,
		.lockmode = AccessShareLock,
		.scandirection = ForwardScanDirection,
	};

	/*
	 * Perform an index scan on epoch ID to find the partitions for the epoch.
	 */
	ScanKeyInit(&scankey[0],
				Anum_partition_epoch_id_idx_epoch_id,
				BTEqualStrategyNumber,
				F_INT4EQ,
				Int32GetDatum(pctx->pe->id));

	scanner_scan(&scanCtx);

	/*
	 * The scan decremented the number of partitions in the context, so check
	 * that it is zero for correct number of partitions scanned.
	 */
	if (pctx->num_partitions != 0)
	{
		elog(ERROR, "%d partitions found for epoch %d, expected %d",
		num_partitions - pctx->num_partitions, pctx->pe->id, num_partitions);
	}

	return num_partitions;
}

PartitionEpoch *
partition_epoch_scan(int32 hypertable_id, int64 timepoint, Oid relid)
{
	ScanKeyData scankey[1];
	Catalog    *catalog = catalog_get();
	PartitionEpochCtx pctx = {
		.hypertable_id = hypertable_id,
		.timepoint = timepoint,
		.relid = relid,
	};
	ScannerCtx	scanctx = {
		.table = catalog->tables[PARTITION_EPOCH].id,
		.index = catalog->tables[PARTITION_EPOCH].index_ids[PARTITION_EPOCH_TIME_INDEX],
		.scantype = ScannerTypeIndex,
		.nkeys = 1,
		.scankey = scankey,
		.data = &pctx,
		.filter = partition_epoch_filter,
		.tuple_found = partition_epoch_tuple_found,
		.lockmode = AccessShareLock,
		.scandirection = ForwardScanDirection,
	};

	/*
	 * Perform an index scan on hypertable ID. We filter on start and end
	 * time.
	 */
	ScanKeyInit(&scankey[0],
	   Anum_partition_epoch_hypertable_start_time_end_time_idx_hypertable_id,
				BTEqualStrategyNumber,
				F_INT4EQ,
				Int32GetDatum(hypertable_id));

	scanner_scan(&scanctx);

	return pctx.pe;
}


/* function to compare partitions */
static int
cmp_partitions(const void *keyspace_pt_arg, const void *value)
{
	/* note in keyspace asc; assume oldest stuff last */
	int16		keyspace_pt = *((int16 *) keyspace_pt_arg);
	const Partition *part = value;

	if (partition_keyspace_pt_is_member(part, keyspace_pt))
	{
		return 0;
	}

	if (keyspace_pt > part->keyspace_end)
	{
		return 1;
	}
	return -1;
}

Partition *
partition_epoch_get_partition(PartitionEpoch *epoch, int16 keyspace_pt)
{
	Partition  *part;

	if (epoch == NULL)
	{
		elog(ERROR, "No partitioning information for epoch");
		return NULL;
	}

	if (keyspace_pt == KEYSPACE_PT_NO_PARTITIONING)
	{
		if (epoch->num_partitions > 1)
		{
			elog(ERROR, "Found many partitions(%d) for an unpartitioned epoch",
				 epoch->num_partitions);
		}
		return &epoch->partitions[0];
	}

	part = bsearch(&keyspace_pt, epoch->partitions, epoch->num_partitions,
				   sizeof(Partition), cmp_partitions);

	if (part == NULL)
	{
		elog(ERROR, "could not find partition for epoch with %d partitions", epoch->num_partitions);
	}

	return part;
}

bool
partition_keyspace_pt_is_member(const Partition *part, const int16 keyspace_pt)
{
	return keyspace_pt == KEYSPACE_PT_NO_PARTITIONING || (part->keyspace_start <= keyspace_pt && part->keyspace_end >= keyspace_pt);
}

#endif

/* _timescaledb_catalog.get_partition_for_key(key TEXT, mod_factor INT) RETURNS SMALLINT */
Datum		get_partition_for_key(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(get_partition_for_key);
Datum
get_partition_for_key(PG_FUNCTION_ARGS)
{
	struct varlena *data;
	int32		mod;
	uint32		hash_u;
	int16		res;

	data = PG_GETARG_VARLENA_PP(0);
	mod = PG_GETARG_INT32(1);

	hash_u = hash_any((unsigned char *) VARDATA_ANY(data),
					  VARSIZE_ANY_EXHDR(data));

	res = (int16) ((hash_u & 0x7fffffff) % mod);

	PG_FREE_IF_COPY(data, 0);
	PG_RETURN_INT16(res);
}
