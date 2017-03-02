#include <postgres.h>
#include <catalog/namespace.h>
#include <utils/lsyscache.h>
#include <miscadmin.h>
#include <commands/dbcommands.h>

#include "catalog.h"

static char *catalog_table_names[_MAX_CATALOG_TABLES] = {
	[HYPERTABLE] = HYPERTABLE_TABLE_NAME,
	[PARTITION] = PARTITION_TABLE_NAME,
	[PARTITION_EPOCH] = PARTITION_EPOCH_TABLE_NAME,
	[CHUNK] = CHUNK_TABLE_NAME,
};

static char *catalog_table_index_names[_MAX_CATALOG_TABLES] = {
	[HYPERTABLE] = HYPERTABLE_INDEX_NAME,
	[PARTITION] = PARTITION_INDEX_NAME,
	[PARTITION_EPOCH] = PARTITION_EPOCH_INDEX_NAME,
	[CHUNK] = CHUNK_INDEX_NAME,
};

/* Catalog information for the current database. Should probably be invalidated
 * if the extension is unloaded for the database. */
static Catalog catalog = {
	.database_id = InvalidOid,
};

Catalog *catalog_get(void)
{
	AclResult aclresult;		
	int i;

	if (MyDatabaseId == InvalidOid)
		elog(ERROR, "Invalid database ID");

	/* Check that the user has CREATE permissions on the database, since the
	   operation may involve creating chunks and inserting into them. */
	aclresult = pg_database_aclcheck(MyDatabaseId, GetUserId(), ACL_CREATE);
	
	if (aclresult != ACLCHECK_OK)
		aclcheck_error(aclresult, ACL_KIND_DATABASE,
					   get_database_name(MyDatabaseId));

	if (MyDatabaseId == catalog.database_id)
		return &catalog;

	memset(&catalog, 0, sizeof(Catalog));
	catalog.database_id = MyDatabaseId;
	strncpy(catalog.database_name, get_database_name(MyDatabaseId), NAMEDATALEN);
	catalog.schema_id = get_namespace_oid(CATALOG_SCHEMA_NAME, false);

	if (catalog.schema_id == InvalidOid)
	{
		elog(ERROR, "Oid lookup failed for schema %s", CATALOG_SCHEMA_NAME);
	}

	for (i = 0; i < _MAX_CATALOG_TABLES; i++)
	{
		Oid id;
		
		id = get_relname_relid(catalog_table_names[i], catalog.schema_id);
		
		if (id == InvalidOid)
		{
			elog(ERROR, "Oid lookup failed for table %s", catalog_table_names[i]);
		}

		catalog.tables[i].id = id;

		id = get_relname_relid(catalog_table_index_names[i], catalog.schema_id);
		
		if (id == InvalidOid)
		{
			elog(ERROR, "Oid lookup failed for table index %s", catalog_table_index_names[i]);
		}

		catalog.tables[i].index_id = id;
		catalog.tables[i].name = catalog_table_names[i];
	}	

	return &catalog;
}
