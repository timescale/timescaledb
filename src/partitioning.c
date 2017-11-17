#include <postgres.h>
#include <utils/builtins.h>
#include <utils/lsyscache.h>
#include <utils/numeric.h>
#include <utils/timestamp.h>
#include <utils/inet.h>
#include <utils/cash.h>
#include <utils/date.h>
#include <utils/nabstime.h>
#include <utils/jsonb.h>
#include <utils/acl.h>
#include <utils/rangetypes.h>
#include <catalog/namespace.h>
#include <catalog/pg_type.h>
#include <access/hash.h>
#include <access/htup_details.h>
#include <parser/parse_coerce.h>
#include <nodes/makefuncs.h>
#include <nodes/pg_list.h>

#include "partitioning.h"
#include "catalog.h"
#include "utils.h"

/*
 * Resolve the partitioning function set for a hypertable.
 */
static void
partitioning_func_set_func_fmgr(PartitioningFunc *pf)
{
	FuncCandidateList funclist =
	FuncnameGetCandidates(partitioning_func_qualified_name(pf),
						  1, NULL, false, false, false);

	while (funclist != NULL &&
		   (funclist->nargs != 1 || funclist->args[0] != ANYELEMENTOID))
		funclist = funclist->next;

	if (NULL == funclist)
		elog(ERROR, "Partitioning function not found");

	pf->paramtype = funclist->args[0];

	fmgr_info_cxt(funclist->oid, &pf->func_fmgr, CurrentMemoryContext);
}

List *
partitioning_func_qualified_name(PartitioningFunc *pf)
{
	return list_make2(makeString(pf->schema), makeString(pf->name));
}

static Oid
find_text_coercion_func(Oid type)
{
	Oid			funcid;
	bool		is_varlena;
	CoercionPathType cpt;

	/*
	 * First look for an explicit cast type. Needed since the output of for
	 * example character(20) not the same as character(20)::text
	 */
	cpt = find_coercion_pathway(TEXTOID, type, COERCION_EXPLICIT, &funcid);

	if (cpt != COERCION_PATH_FUNC)
		getTypeOutputInfo(type, &funcid, &is_varlena);

	return funcid;
}

#define TYPECACHE_HASH_FLAGS (TYPECACHE_HASH_PROC | TYPECACHE_HASH_PROC_FINFO)

PartitioningInfo *
partitioning_info_create(const char *schema,
						 const char *partfunc,
						 const char *partcol,
						 Oid relid)
{
	PartitioningInfo *pinfo;
	Oid			columntype,
				varcollid,
				funccollid = InvalidOid;
	Var		   *var;
	FuncExpr   *expr;

	pinfo = palloc0(sizeof(PartitioningInfo));
	StrNCpy(pinfo->partfunc.name, partfunc, NAMEDATALEN);
	StrNCpy(pinfo->column, partcol, NAMEDATALEN);
	pinfo->column_attnum = get_attnum(relid, pinfo->column);

	if (schema != NULL)
		StrNCpy(pinfo->partfunc.schema, schema, NAMEDATALEN);

	/* Lookup the type cache entry to access the hash function for the type */
	columntype = get_atttype(relid, pinfo->column_attnum);
	pinfo->typcache_entry = lookup_type_cache(columntype, TYPECACHE_HASH_FLAGS);

	if (pinfo->typcache_entry->hash_proc == InvalidOid)
		elog(ERROR, "No hash function for type %u", columntype);

	partitioning_func_set_func_fmgr(&pinfo->partfunc);

	/*
	 * Prepare a function expression for this function. The partition hash
	 * function needs this to be able to resolve the type of the value to be
	 * hashed.
	 */
	varcollid = get_typcollation(columntype);

	var = makeVar(1,
				  pinfo->column_attnum,
				  columntype,
				  -1,
				  varcollid,
				  0);

	expr = makeFuncExpr(pinfo->partfunc.func_fmgr.fn_oid, INT4OID, list_make1(var),
						funccollid, varcollid, COERCE_EXPLICIT_CALL);

	fmgr_info_set_expr((Node *) expr, &pinfo->partfunc.func_fmgr);

	/*
	 * Set the type cache entry in fn_extra to avoid an extry lookup in the
	 * partition hash function
	 */
	pinfo->partfunc.func_fmgr.fn_extra = pinfo->typcache_entry;

	return pinfo;
}

/*
 * Apply the partitioning function of a hypertable to a value.
 *
 * We support both partitioning functions with the signature int
 * func(anyelement).
 */
int32
partitioning_func_apply(PartitioningInfo *pinfo, Datum value)
{
	return DatumGetInt32(FunctionCall1(&pinfo->partfunc.func_fmgr, value));
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

/*
 * Resolve the type of the argument passed to a function.
 *
 * The type is resolved from the function expression in the function call info.
 */
static Oid
resolve_function_argtype(FunctionCallInfo fcinfo)
{
	FuncExpr   *fe;
	Node	   *node;
	Oid			argtype;

	/* Get the function expression from the call info */
	fe = (FuncExpr *) fcinfo->flinfo->fn_expr;

	if (NULL == fe || !IsA(fe, FuncExpr))
		elog(ERROR, "No function expression set when invoking partitioning function");

	if (list_length(fe->args) != 1)
		elog(ERROR, "Unexpected number of arguments in function expression");

	node = linitial(fe->args);

	switch (nodeTag(node))
	{
		case T_Var:
			argtype = ((Var *) node)->vartype;
			break;
		case T_Const:
			argtype = ((Const *) node)->consttype;
			break;
		case T_CoerceViaIO:
			argtype = ((CoerceViaIO *) node)->resulttype;
			break;
		default:
			elog(ERROR, "Unsupported expression argument node type %u", nodeTag(node));
	}

	return argtype;
}

/* _timescaledb_catalog.get_partition_for_key(key anyelement) RETURNS INT */
PGDLLEXPORT Datum get_partition_for_key(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(get_partition_for_key);

/*
 * Partition hash function that first converts all inputs to text before
 * hashing.
 */
Datum
get_partition_for_key(PG_FUNCTION_ARGS)
{
	Datum		arg = PG_GETARG_DATUM(0);
	struct varlena *data;
	Oid			argtype;
	uint32		hash_u;
	int32		res;

	if (PG_NARGS() != 1)
		elog(ERROR, "Unexpected number of arguments to partitioning function");

	argtype = resolve_function_argtype(fcinfo);

	if (argtype != TEXTOID)
	{
		/* Not TEXT input -> need to convert to text */
		Oid			funcid = find_text_coercion_func(argtype);

		if (!OidIsValid(funcid))
			elog(ERROR, "Could not coerce type %u to text",
				 argtype);

		arg = OidFunctionCall1(funcid, arg);
		arg = CStringGetTextDatum(DatumGetCString(arg));
	}

	data = DatumGetTextPP(arg);
	hash_u = DatumGetUInt32(hash_any((unsigned char *) VARDATA_ANY(data),
									 VARSIZE_ANY_EXHDR(data)));

	res = (int32) (hash_u & 0x7fffffff);		/* Only positive numbers */

	PG_FREE_IF_COPY(data, 0);
	PG_RETURN_INT32(res);
}

PGDLLEXPORT Datum get_partition_hash(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(get_partition_hash);

/*
 * Compute a partition hash value for any input type.
 *
 * get_partition_hash() takes a single argument of anyelement type. We compute
 * the hash based on the argument type information that we expect to find in the
 * function expression in the function call context. If no such expression
 * exists, or the type cannot be resolved from the expression, the function
 * throws an error.
 */
Datum
get_partition_hash(PG_FUNCTION_ARGS)
{
	Datum		arg = PG_GETARG_DATUM(0);
	TypeCacheEntry *tce = fcinfo->flinfo->fn_extra;
	Oid			argtype;
	Datum		hash;
	int32		res;

	if (PG_NARGS() != 1)
		elog(ERROR, "Unexpected number of arguments to partitioning function");

	argtype = resolve_function_argtype(fcinfo);

	if (tce == NULL)
		tce = lookup_type_cache(argtype, TYPECACHE_HASH_FLAGS);

	if (tce->hash_proc == InvalidOid)
		elog(ERROR, "No hash function for type %u", argtype);

	hash = FunctionCall1(&tce->hash_proc_finfo, arg);

	/* Only positive numbers */
	res = (int32) (DatumGetUInt32(hash) & 0x7fffffff);

	PG_RETURN_INT32(res);
}
