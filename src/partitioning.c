#include <postgres.h>
#include <utils/builtins.h>
#include <utils/lsyscache.h>
#include <utils/syscache.h>
#include <utils/catcache.h>
#include <utils/numeric.h>
#include <utils/timestamp.h>
#include <utils/inet.h>
#include <utils/cash.h>
#include <utils/date.h>
#include <utils/nabstime.h>
#include <utils/jsonb.h>
#include <utils/acl.h>
#include <utils/rangetypes.h>
#include <utils/memutils.h>
#include <catalog/namespace.h>
#include <catalog/pg_type.h>
#include <access/hash.h>
#include <access/htup_details.h>
#include <parser/parse_coerce.h>
#include <nodes/makefuncs.h>
#include <nodes/pg_list.h>

#include "partitioning.h"
#include "compat.h"
#include "catalog.h"
#include "utils.h"

#define IS_VALID_PARTITIONING_FUNC(proform)							\
	((proform)->prorettype == INT4OID &&							\
	 ((proform)->provolatile == PROVOLATILE_IMMUTABLE) &&			\
	 (proform)->pronargs == 1 &&									\
	 (proform)->proargtypes.values[0] == ANYELEMENTOID)
/*
 * Get the OID of the partitioning function given a qualified name.
 *
 * Only functions that match the supported function signature will be returned.
 */
static regproc
partitioning_func_get(const char *schema, const char *funcname)
{
	Oid			namespace_oid = LookupExplicitNamespace(schema, false);
	regproc		func = InvalidOid;
	CatCList   *catlist;
	NameData	proname;
	int			i;

	namestrcpy(&proname, funcname);

	catlist = SearchSysCacheList1(PROCNAMEARGSNSP, NameGetDatum(&proname));

	for (i = 0; i < catlist->n_members; i++)
	{
		HeapTuple	proctup = &catlist->members[i]->tuple;
		Form_pg_proc procform = (Form_pg_proc) GETSTRUCT(proctup);

		if (procform->pronamespace == namespace_oid &&
			IS_VALID_PARTITIONING_FUNC(procform))
		{
			func = HeapTupleGetOid(proctup);
			break;
		}
	}

	ReleaseSysCacheList(catlist);

	return func;
}

bool
partitioning_func_is_valid(regproc funcoid)
{
	HeapTuple	tuple;
	bool		isvalid;

	tuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcoid));

	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for function %u", funcoid);

	isvalid = IS_VALID_PARTITIONING_FUNC((Form_pg_proc) GETSTRUCT(tuple));

	ReleaseSysCache(tuple);

	return isvalid;
}

Oid
partitioning_func_get_default(void)
{
	return partitioning_func_get(DEFAULT_PARTITIONING_FUNC_SCHEMA,
								 DEFAULT_PARTITIONING_FUNC_NAME);
}

bool
partitioning_func_is_default(const char *schema, const char *funcname)
{
	Assert(schema != NULL && funcname != NULL);

	return strcmp(DEFAULT_PARTITIONING_FUNC_SCHEMA, schema) == 0 &&
		strcmp(DEFAULT_PARTITIONING_FUNC_NAME, funcname) == 0;
}

/*
 * Resolve the partitioning function set for a hypertable.
 */
static void
partitioning_func_set_func_fmgr(PartitioningFunc *pf)
{
	Oid			funcoid = partitioning_func_get(pf->schema, pf->name);

	if (!OidIsValid(funcoid))
		ereport(ERROR,
				(errmsg("invalid partitioning function"),
				 errhint("A partitioning function for a closed (space) dimension "
						 "must be IMMUTABLE and have the signature (anyelement) -> integer")));

	fmgr_info_cxt(funcoid, &pf->func_fmgr, CurrentMemoryContext);
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
	TypeCacheEntry *tce;
	Oid			columntype,
				varcollid,
				funccollid = InvalidOid;
	Var		   *var;
	FuncExpr   *expr;

	if (schema == NULL || partfunc == NULL || partcol == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("partitioning function information cannot be null")));

	pinfo = palloc0(sizeof(PartitioningInfo));
	StrNCpy(pinfo->partfunc.name, partfunc, NAMEDATALEN);
	StrNCpy(pinfo->column, partcol, NAMEDATALEN);
	pinfo->column_attnum = get_attnum(relid, pinfo->column);

	/* handle the case that the attribute has been dropped */
	if (pinfo->column_attnum == InvalidAttrNumber)
		return NULL;


	StrNCpy(pinfo->partfunc.schema, schema, NAMEDATALEN);

	/* Lookup the type cache entry to access the hash function for the type */
	columntype = get_atttype(relid, pinfo->column_attnum);
	tce = lookup_type_cache(columntype, TYPECACHE_HASH_FLAGS);

	if (tce->hash_proc == InvalidOid && partitioning_func_is_default(schema, partfunc))
		elog(ERROR, "could not find hash function for type %s", format_type_be(columntype));

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

	return pinfo;
}

/*
 * Apply the partitioning function of a hypertable to a value.
 *
 * We support partitioning functions with the signature (anyelement) -> int.
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
		elog(ERROR, "no function expression set when invoking partitioning function");

	if (list_length(fe->args) != 1)
		elog(ERROR, "unexpected number of arguments in function expression");

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
		case T_FuncExpr:
			/* Argument is function, so our input is its result type */
			argtype = ((FuncExpr *) node)->funcresulttype;
			break;
		default:
			elog(ERROR, "unsupported expression argument node type %u", nodeTag(node));
	}

	return argtype;
}

/*
 * Partitioning function cache.
 *
 * Holds type information to avoid repeated lookups. The cache is allocated on a
 * child memory context of the context that created the associated FmgrInfo
 * struct. For partitioning functions invoked on the insert path, this is
 * typically the Hypertable cache's memory context. Hence, the type cache lives
 * for the duration of the hypertable cache and can be reused across multiple
 * invokations of the partitioning function, even across transactions.
 *
 * If the partitioning function is invoked outside the insert path, the FmgrInfo
 * and its memory context has a lifetime corresponding to that invokation.
 */
typedef struct PartFuncCache
{
	Oid			argtype;
	Oid			coerce_funcid;
	TypeCacheEntry *tce;
} PartFuncCache;

static PartFuncCache *
part_func_cache_create(Oid argtype, TypeCacheEntry *tce, Oid coerce_funcid, MemoryContext mcxt)
{
	PartFuncCache *pfc;

	pfc = MemoryContextAlloc(mcxt, sizeof(PartFuncCache));
	pfc->argtype = argtype;
	pfc->tce = tce;
	pfc->coerce_funcid = coerce_funcid;

	return pfc;
}

/* _timescaledb_catalog.get_partition_for_key(key anyelement) RETURNS INT */
PGDLLEXPORT Datum get_partition_for_key(PG_FUNCTION_ARGS);

TS_FUNCTION_INFO_V1(get_partition_for_key);

/*
 * Partition hash function that first converts all inputs to text before
 * hashing.
 */
Datum
get_partition_for_key(PG_FUNCTION_ARGS)
{
	Datum		arg = PG_GETARG_DATUM(0);
	PartFuncCache *pfc = fcinfo->flinfo->fn_extra;
	struct varlena *data;
	uint32		hash_u;
	int32		res;

	if (PG_NARGS() != 1)
		elog(ERROR, "unexpected number of arguments to partitioning function");

	if (NULL == pfc)
	{
		Oid			funcid = InvalidOid;
		Oid			argtype = resolve_function_argtype(fcinfo);

		if (argtype != TEXTOID)
		{
			/* Not TEXT input -> need to convert to text */
			funcid = find_text_coercion_func(argtype);

			if (!OidIsValid(funcid))
				elog(ERROR, "could not coerce type %u to text", argtype);
		}

		pfc = part_func_cache_create(argtype, NULL, funcid, fcinfo->flinfo->fn_mcxt);
		fcinfo->flinfo->fn_extra = pfc;
	}

	if (pfc->argtype != TEXTOID)
	{
		arg = OidFunctionCall1(pfc->coerce_funcid, arg);
		arg = CStringGetTextDatum(DatumGetCString(arg));
	}

	data = DatumGetTextPP(arg);
	hash_u = DatumGetUInt32(hash_any((unsigned char *) VARDATA_ANY(data),
									 VARSIZE_ANY_EXHDR(data)));

	res = (int32) (hash_u & 0x7fffffff);	/* Only positive numbers */

	PG_FREE_IF_COPY(data, 0);
	PG_RETURN_INT32(res);
}

PGDLLEXPORT Datum get_partition_hash(PG_FUNCTION_ARGS);

TS_FUNCTION_INFO_V1(get_partition_hash);

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
	PartFuncCache *pfc = fcinfo->flinfo->fn_extra;
	Datum		hash;
	int32		res;

	if (PG_NARGS() != 1)
		elog(ERROR, "unexpected number of arguments to partitioning function");

	if (NULL == pfc)
	{
		Oid			argtype = resolve_function_argtype(fcinfo);
		TypeCacheEntry *tce = lookup_type_cache(argtype, TYPECACHE_HASH_FLAGS);

		pfc = part_func_cache_create(argtype, tce, InvalidOid, fcinfo->flinfo->fn_mcxt);
		fcinfo->flinfo->fn_extra = pfc;
	}

	if (pfc->tce->hash_proc == InvalidOid)
		elog(ERROR, "could not find hash function for type %u", pfc->argtype);

	hash = FunctionCall1(&pfc->tce->hash_proc_finfo, arg);

	/* Only positive numbers */
	res = (int32) (DatumGetUInt32(hash) & 0x7fffffff);

	PG_RETURN_INT32(res);
}
