#include <postgres.h>
#include <utils/builtins.h>

#include "partitioning.h"
#include "metadata_queries.h"
#include "scanner.h"
#include "catalog.h"

static void partitioning_func_set_func_fmgr(PartitioningFunc *pf)
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

static void partitioning_info_set_textfunc_fmgr(PartitioningInfo *pi, Oid relid)
{
	Oid	type_id, func_id;
	bool isVarlena;
	pi->column_attnum = get_attnum(relid, pi->column);
	type_id = get_atttype(relid, pi->column_attnum);
	getTypeOutputInfo(type_id, &func_id, &isVarlena);
	fmgr_info_cxt(func_id, &pi->partfunc.textfunc_fmgr, CurrentMemoryContext);
}

static PartitioningInfo *
partitioning_info_create(int num_partitions,
						 const char *schema,
						 const char *partfunc,
						 const char *partcol,
						 int16 partmod,
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

int16 partitioning_func_apply(PartitioningFunc *pf, Datum value)
{
	Datum text = FunctionCall1(&pf->textfunc_fmgr, value);
	char *partition_val = DatumGetCString(text);
	Datum keyspace_datum = FunctionCall2(&pf->func_fmgr,
										 CStringGetTextDatum(partition_val),
										 Int32GetDatum(pf->modulos));
	return DatumGetInt16(keyspace_datum);
}

/* Partition epoch index column numbers from sql/common/table.sql */
#define PE_IDX_COL_HTID 1
#define PE_IDX_COL_STARTTIME 2
#define PE_IDX_COL_ENDTIME 3

/* Partition epoch table column numbers from sql/common/table.sql */
#define PE_TBL_COL_ID 1
#define PE_TBL_COL_HT_ID 2
#define PE_TBL_COL_STARTTIME 3
#define PE_TBL_COL_ENDTIME 4
#define PE_TBL_COL_NUMPARTITIONS 5
#define PE_TBL_COL_PARTFUNC_SCHEMA 6
#define PE_TBL_COL_PARTFUNC 7
#define PE_TBL_COL_PARTMOD 8
#define PE_TBL_COL_PARTCOL 9


/* PartitionEpochCtx is used to pass on information during a partition epoch and
 * partition scans. */
typedef struct {
	epoch_and_partitions_set *pe;
	int16 num_partitions;
	int32 hypertable_id;   
	int64 starttime, endtime, timepoint;
	Oid relid;
} PartitionEpochCtx;

static int
partition_scan(PartitionEpochCtx *pctx);

/* Filter partition epoch tuples based on hypertable ID and start/end time. */ 
static bool
partition_epoch_filter(TupleInfo *ti, void *arg)
{
	bool is_null;
	PartitionEpochCtx *pctx = arg;
	Datum id = heap_getattr(ti->tuple, PE_TBL_COL_HT_ID, ti->desc, &is_null);

	if (DatumGetInt32(id) == pctx->hypertable_id)
	{
		bool starttime_is_null, endtime_is_null;
		Datum starttime = heap_getattr(ti->tuple, PE_TBL_COL_STARTTIME, ti->desc, &starttime_is_null);
		Datum endtime = heap_getattr(ti->tuple, PE_TBL_COL_ENDTIME, ti->desc, &endtime_is_null);

		pctx->starttime = starttime_is_null ? OPEN_START_TIME : DatumGetInt64(starttime);
		pctx->endtime = endtime_is_null ? OPEN_END_TIME : DatumGetInt64(endtime);

		return (starttime_is_null || pctx->timepoint >= pctx->starttime) &&
			(endtime_is_null || pctx->timepoint <= pctx->endtime);
	}
	return false;
}

#define PARTITION_EPOCH_SIZE(num_partitions) \
	sizeof(epoch_and_partitions_set) + (sizeof(Partition) * num_partitions)

static epoch_and_partitions_set *
partition_epoch_create(int32 epoch_id, PartitionEpochCtx *ctx)
{
	epoch_and_partitions_set *pe;
	pe = palloc(PARTITION_EPOCH_SIZE(ctx->num_partitions));
	pe->id = epoch_id;
	pe->num_partitions = ctx->num_partitions;
	pe->hypertable_id = ctx->hypertable_id;
	pe->start_time = ctx->starttime;
	pe->end_time = ctx->endtime;
	return pe;
}

/* Callback for partition epoch scan. For every epoch tuple found, create a
 * partition epoch entry and scan for associated partitions. */
static bool
partition_epoch_tuple_found(TupleInfo *ti, void *arg)
{
	PartitionEpochCtx *pctx = arg;
	epoch_and_partitions_set *pe;
	int32 epoch_id;
	Datum datum;
	bool is_null;

	datum = heap_getattr(ti->tuple, PE_TBL_COL_NUMPARTITIONS, ti->desc, &is_null);
	pctx->num_partitions = DatumGetInt16(datum);
	datum = heap_getattr(ti->tuple, PE_TBL_COL_ID, ti->desc, &is_null);
	epoch_id = DatumGetInt32(datum);
	
	pe = partition_epoch_create(epoch_id, pctx);
	pctx->pe = pe;
	
	if (pctx->num_partitions > 1)
	{
		Datum partfunc, partmod, partcol;
		bool partfunc_is_null, partmod_is_null, partcol_is_null;
		partfunc = heap_getattr(ti->tuple, PE_TBL_COL_PARTFUNC, ti->desc, &partfunc_is_null);	
		partmod = heap_getattr(ti->tuple, PE_TBL_COL_PARTMOD, ti->desc, &partmod_is_null);
		partcol = heap_getattr(ti->tuple, PE_TBL_COL_PARTCOL, ti->desc, &partcol_is_null);
		
		if (partfunc_is_null || partmod_is_null || partcol_is_null)
		{
			elog(ERROR, "Invalid partitioning configuration for partition epoch %d", epoch_id);
		}

		datum = heap_getattr(ti->tuple, PE_TBL_COL_PARTFUNC_SCHEMA, ti->desc, &is_null);
		
		pe->partitioning = partitioning_info_create(pctx->num_partitions,
													is_null ? NULL : DatumGetCString(datum),
													DatumGetCString(partfunc),
													DatumGetCString(partcol),
													DatumGetInt16(partmod),
													pctx->relid);
	}

	/* Scan for the epoch's partitions */
	partition_scan(pctx);

	return true;
}

/* Partition table columns */
#define PARTITION_TBL_COL_ID 1
#define PARTITION_TBL_COL_EPOCH_ID 2
#define PARTITION_TBL_COL_KEYSPACE_START 3
#define PARTITION_TBL_COL_KEYSPACE_END 4
#define PARTITION_TBL_COL_TABLESPACE 5

/* Partition index columns */
#define PARTITION_IDX_COL_ID 1

static bool
partition_tuple_found(TupleInfo *ti, void *arg)
{
	PartitionEpochCtx *pctx = arg;
	epoch_and_partitions_set *pe = pctx->pe;
	Datum datum;
	bool is_null;

   	pctx->num_partitions--;
	datum = heap_getattr(ti->tuple, PARTITION_TBL_COL_ID, ti->desc, &is_null);
	pe->partitions[pctx->num_partitions].id = DatumGetInt32(datum);
	datum = heap_getattr(ti->tuple, PARTITION_TBL_COL_KEYSPACE_START, ti->desc, &is_null);
	pe->partitions[pctx->num_partitions].keyspace_start = DatumGetInt16(datum);
	datum = heap_getattr(ti->tuple, PARTITION_TBL_COL_KEYSPACE_END, ti->desc, &is_null);
	pe->partitions[pctx->num_partitions].keyspace_end = DatumGetInt16(datum);

	/* Abort the scan if we have found all partitions */
	if (pctx->num_partitions == 0)
		return false;

	return true;
}

static int
partition_scan(PartitionEpochCtx *pctx)
{
	ScanKeyData scankey[1];
	Catalog *catalog = catalog_get();
	int num_partitions = pctx->num_partitions;
	ScannerCtx scanCtx = {
		.table = catalog->tables[PARTITION].id,
		.index = get_relname_relid(PARTITION_EPOCH_ID_INDEX_NAME, catalog->schema_id),
		.scantype = ScannerTypeIndex,
		.nkeys = 1,
		.scankey = scankey,
		.data = pctx,
		.tuple_found = partition_tuple_found,
		.lockmode = AccessShareLock,
		.scandirection = ForwardScanDirection,
	};

	/* Perform an index scan on epoch ID to find the partitions for the
	 * epoch. */
	ScanKeyInit(&scankey[0], PARTITION_IDX_COL_ID, BTEqualStrategyNumber,
				F_INT4EQ, Int32GetDatum(pctx->pe->id));

	scanner_scan(&scanCtx);

	/* The scan decremented the number of partitions in the context, so check
	   that it is zero for correct number of partitions scanned. */
	if (pctx->num_partitions != 0)
	{
		elog(ERROR, "%d partitions found for epoch %d, expected %d",
			 num_partitions - pctx->num_partitions, pctx->pe->id, num_partitions);
	}

	return num_partitions;
}

epoch_and_partitions_set *
partition_epoch_scan(int32 hypertable_id, int64 timepoint, Oid relid)
{
	ScanKeyData scankey[1];
	Catalog *catalog = catalog_get();
	PartitionEpochCtx pctx = {
		.hypertable_id = hypertable_id,
		.timepoint = timepoint,
		.relid = relid,
	};
	ScannerCtx scanctx = {
		.table = catalog->tables[PARTITION_EPOCH].id,
		.index = get_relname_relid(PARTITION_EPOCH_TIME_INDEX_NAME, catalog->schema_id),
		.scantype = ScannerTypeIndex,
		.nkeys = 1,
		.scankey = scankey,
		.data = &pctx,
		.filter = partition_epoch_filter,
		.tuple_found = partition_epoch_tuple_found,		
		.lockmode = AccessShareLock,
		.scandirection = ForwardScanDirection,
	};

	/* Perform an index scan on hypertable ID. We filter on start and end
	 * time. */
	ScanKeyInit(&scankey[0], PE_IDX_COL_HTID, BTEqualStrategyNumber,
				F_INT4EQ, Int32GetDatum(hypertable_id));
	
	scanner_scan(&scanctx);

	return pctx.pe;
}


/* function to compare partitions */
static int
cmp_partitions(const void *keyspace_pt_arg, const void *value)
{
	/* note in keyspace asc; assume oldest stuff last */
	int16      keyspace_pt = *((int16 *) keyspace_pt_arg);
	const Partition   *part = value;
	
	if (part->keyspace_start <= keyspace_pt && part->keyspace_end >= keyspace_pt)
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
partition_epoch_get_partition(epoch_and_partitions_set *epoch, int16 keyspace_pt)
{
	Partition *part;
	
	if (epoch == NULL)
	{
		elog(ERROR, "No partitioning information for epoch");
		return NULL;
	}
	
	if (keyspace_pt < 0)
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

