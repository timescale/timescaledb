/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include "osm_callbacks.h"

#include <fmgr.h>

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

chunk_startup_exclusion_hook_type
ts_get_osm_chunk_startup_exclusion_hook()
{
	OsmCallbacks_Versioned *ptr = ts_get_osm_callbacks();
	if (ptr && ptr->version_num == 1)
		return ptr->chunk_startup_exclusion_hook;
	return NULL;
}
