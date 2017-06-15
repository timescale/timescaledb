#include <postgres.h>
#include <access/htup_details.h>
#include <utils/lsyscache.h>
#include <catalog/namespace.h>

#include "hypertable.h"
#include "dimension.h"

Hypertable *
hypertable_from_tuple(HeapTuple tuple)
{
	Hypertable *h;
	Oid namespace_oid;
	
	h = palloc0(sizeof(Hypertable));
	memcpy(&h->fd, GETSTRUCT(tuple), sizeof(FormData_hypertable));
	namespace_oid = get_namespace_oid(NameStr(h->fd.schema_name), false);
	h->main_table = get_relname_relid(NameStr(h->fd.table_name), namespace_oid);

	return h;
}

Dimension *
hypertable_time_dimension(Hypertable *h)
{
	return h->space->time_dimensions[0];
}

Dimension *
hypertable_space_dimension(Hypertable *h)
{
	return h->space->space_dimensions[0];
}
