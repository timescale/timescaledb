#include <stdlib.h>
#include <unistd.h>
#include <postgres.h>
#include <fmgr.h>
#include <access/htup_details.h>
#include <utils/builtins.h>
#include <utils/fmgroids.h>
#include <utils/snapmgr.h>

#include "catalog.h"
#include "installation_metadata.h"
#include "scanner.h"

static bool
installation_metadata_tuple_get_value(TupleInfo *ti, void *data) {
	bool isnull;
	char **ret = data;
	Datum value = heap_getattr(ti->tuple, Anum_installation_metadata_value, ti->desc, &isnull);
	if (isnull)
		*ret = NULL;
	else
		*ret = TextDatumGetCString(value);

	return false;
}

const char *
installation_metadata_get_value(const char *metadata_key)
{
	NameData	key_name;
	ScanKeyData scankey[1];
	char *data = NULL;
	Catalog    *catalog = catalog_get();
	ScannerCtx scanctx = {
		.table = catalog->tables[INSTALLATION_METADATA].id,
		.index = CATALOG_INDEX(catalog, INSTALLATION_METADATA, INSTALLATION_METADATA_PKEY_IDX),
		.nkeys = 1,
		.scankey = scankey,
		.tuple_found = installation_metadata_tuple_get_value,
		.data = &data,
		.lockmode = AccessShareLock,
		.scandirection = ForwardScanDirection,
	};

	namestrcpy(&key_name, metadata_key);
	ScanKeyInit(&scankey[0], Anum_installation_metadata_key, BTEqualStrategyNumber, F_NAMEEQ,
				NameGetDatum(&key_name));

	scanner_scan(&scanctx);

	return data;
}

/*  
 *  Insert a row into the installation_metadata table. Acquires a lock in SHARE mode, before
 *  verifying that the desired metadata KV pair still does not exist. Otherwise, exits without
 *  inserting to avoid underlying database error on PK conflict.
 *  Returns the value of the key; this is either the requested insert value or the 
 *  existing value if nothing was inserted.
 */ 
const char *
installation_metadata_insert(const char *metadata_key, const char *metadata_value)
{
	NameData	key_name;
	const char *existing_value;
	Relation	rel;
	Datum		values[Natts_installation_metadata];
	bool		nulls[Natts_installation_metadata] = {false};
	Catalog    *catalog = catalog_get();

	namestrcpy(&key_name, metadata_key);
	rel = heap_open(catalog->tables[INSTALLATION_METADATA].id, ShareLock);

	/* Check for row existence while we have the lock */
	existing_value = installation_metadata_get_value(metadata_key);
	if (existing_value != NULL)
	{
		heap_close(rel, ShareLock);
		return existing_value;
	}

	/* Insert into the catalog table for persistence */
	values[AttrNumberGetAttrOffset(Anum_installation_metadata_key)] =
		NameGetDatum(&key_name);
	values[AttrNumberGetAttrOffset(Anum_installation_metadata_value)] =
		PointerGetDatum(cstring_to_text(metadata_value));
	catalog_insert_values(rel, RelationGetDescr(rel), values, nulls);

	heap_close(rel, ShareLock);
	return metadata_value;
}
