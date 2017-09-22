#include <postgres.h>
#include <utils/builtins.h>
#include <utils/lsyscache.h>
#include <catalog/namespace.h>
#include <catalog/pg_type.h>
#include <access/hash.h>
#include <access/htup_details.h>
#include <parser/parse_coerce.h>

#include "partitioning.h"
#include "catalog.h"
#include "utils.h"

static void
partitioning_func_set_func_fmgr(PartitioningFunc *pf)
{
	FuncCandidateList funclist =
	FuncnameGetCandidates(list_make2(makeString(pf->schema), makeString(pf->name)),
						  1, NULL, false, false, false);

	if (funclist == NULL || funclist->next)
		elog(ERROR, "Could not resolve the partitioning function");

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
						 Oid relid)
{
	PartitioningInfo *pi;

	pi = palloc0(sizeof(PartitioningInfo));
	strncpy(pi->partfunc.name, partfunc, NAMEDATALEN);
	strncpy(pi->column, partcol, NAMEDATALEN);

	if (schema != NULL)
		strncpy(pi->partfunc.schema, schema, NAMEDATALEN);

	partitioning_func_set_func_fmgr(&pi->partfunc);
	partitioning_info_set_textfunc_fmgr(pi, relid);

	return pi;
}

int32
partitioning_func_apply(PartitioningInfo *pinfo, Datum value)
{
	Datum text = FunctionCall1(&pinfo->partfunc.textfunc_fmgr, value);
	char	   *partition_val = DatumGetCString(text);
	Datum		keyspace_datum = FunctionCall1(&pinfo->partfunc.func_fmgr,
										 CStringGetTextDatum(partition_val));

	return DatumGetInt32(keyspace_datum);
}

int32
partitioning_func_apply_tuple(PartitioningInfo *pinfo, HeapTuple tuple, TupleDesc desc)
{
	Datum		value;
	bool		isnull;

	value = heap_getattr(tuple, pinfo->column_attnum, desc, &isnull);

	if (isnull)
		return 0;

	return partitioning_func_apply(pinfo, value);
}

/* _timescaledb_catalog.get_partition_for_key(key TEXT) RETURNS INT */
PGDLLEXPORT Datum get_partition_for_key(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(get_partition_for_key);

Datum
get_partition_for_key(PG_FUNCTION_ARGS)
{
	struct varlena *data;
	uint32		hash_u;
	int32		res;

	data = PG_GETARG_VARLENA_PP(0);

	hash_u = hash_any((unsigned char *) VARDATA_ANY(data),
					  VARSIZE_ANY_EXHDR(data));

	res = (int32) (hash_u & 0x7fffffff);		/* Only positive numbers */

	PG_FREE_IF_COPY(data, 0);
	PG_RETURN_INT32(res);
}
