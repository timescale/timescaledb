/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include "osm_callbacks.h"

#include <commands/extension.h>
#include <fmgr.h>

#include "export.h"
#include "extension_constants.h"
#include "loader/loader.h"

#define OSM_CALLBACKS "osm_callbacks"
#define OSM_CALLBACKS_VAR_NAME "osm_callbacks_versioned"

static OsmCallbacks_Versioned *
ts_get_osm_callbacks(void)
{
	OsmCallbacks_Versioned **ptr =
		(OsmCallbacks_Versioned **) find_rendezvous_variable(OSM_CALLBACKS_VAR_NAME);

	return *ptr;
}

/* This interface and version of the struct will be removed once we have a new version of OSM on all
 * instances
 */
static OsmCallbacks *
ts_get_osm_callbacks_old(void)
{
	OsmCallbacks **ptr = (OsmCallbacks **) find_rendezvous_variable(OSM_CALLBACKS);

	return *ptr;
}

chunk_insert_check_hook_type
ts_get_osm_chunk_insert_hook()
{
	OsmCallbacks_Versioned *ptr = ts_get_osm_callbacks();
	if (ptr)
	{
		if (ptr->version_num == 1)
			return ptr->chunk_insert_check_hook;
	}
	else
	{
		OsmCallbacks *ptr_old = ts_get_osm_callbacks_old();
		if (ptr_old)
			return ptr_old->chunk_insert_check_hook;
	}
	return NULL;
}

hypertable_drop_hook_type
ts_get_osm_hypertable_drop_hook()
{
	OsmCallbacks_Versioned *ptr = ts_get_osm_callbacks();
	if (ptr)
	{
		if (ptr->version_num == 1)
			return ptr->hypertable_drop_hook;
	}
	else
	{
		OsmCallbacks *ptr_old = ts_get_osm_callbacks_old();
		if (ptr_old)
			return ptr_old->hypertable_drop_hook;
	}
	return NULL;
}

hypertable_drop_chunks_hook_type
ts_get_osm_hypertable_drop_chunks_hook()
{
	OsmCallbacks_Versioned *ptr = ts_get_osm_callbacks();
	if (ptr && ptr->version_num == 1)
		return ptr->hypertable_drop_chunks_hook;
	return NULL;
}

TSDLLEXPORT bool
ts_try_load_osm(void) {
	bool res = false;
	Oid osm_extension_oid = get_extension_oid(OSM_EXTENSION_NAME, true);

	if (OidIsValid(osm_extension_oid))
	{
		MemoryContext old_mcxt = CurrentMemoryContext;

		PG_TRY();
		{
			char version[MAX_VERSION_LEN];
			char soname[MAX_SO_NAME_LEN];

			// Form library path
			strlcpy(version, ts_osm_extension_version(), MAX_VERSION_LEN);
			snprintf(soname, MAX_SO_NAME_LEN, "%s%s-%s",
				TS_LIBDIR,
				OSM_EXTENSION_NAME,
				version);

			// Load library
			load_external_function(soname, "pfdw_handler_wrapper", false, NULL);
			res = true;
		}
		PG_CATCH();
		{
			ErrorData *error;

			/* Switch to the original context & copy edata */
			MemoryContextSwitchTo(old_mcxt);
			error = CopyErrorData();
			FlushErrorState();

			elog(LOG, "failed to load OSM extension: %s", error->message);

			/* Finally, free error data */
			FreeErrorData(error);
		}
		PG_END_TRY();
	}

	return res;
}
