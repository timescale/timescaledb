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
	h->main_table_relid = get_relname_relid(NameStr(h->fd.table_name), namespace_oid);

	return h;
}

Dimension *
hypertable_get_open_dimension(Hypertable *h)
{
	if (h->space->num_open_dimensions == 0)
		return NULL;	

	return h->space->open_dimensions[0];
}

Dimension *
hypertable_get_closed_dimension(Hypertable *h)
{
	if (h->space->num_closed_dimensions == 0)
		return NULL;
	
	return h->space->closed_dimensions[0];
}
