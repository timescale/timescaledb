#include "chunk.h"

#include <catalog/namespace.h>
#include <postgres.h>
#include <fmgr.h>
#include <utils/builtins.h>
#include <utils/lsyscache.h>

PG_FUNCTION_INFO_V1(local_chunk_size);
Datum
local_chunk_size(PG_FUNCTION_ARGS)
{
	Name		schema = PG_GETARG_NAME(0);
	Name		table = PG_GETARG_NAME(1);

	Oid			relOid = get_relname_relid(table->data, get_namespace_oid(schema->data, false));
	Datum		res = DirectFunctionCall1(pg_table_size, ObjectIdGetDatum(relOid));

	return res;
}
