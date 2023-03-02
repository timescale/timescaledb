/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include "osm_callbacks.h"

#include <fmgr.h>

#define OSM_CALLBACKS_VAR_NAME "osm_callbacks"

OsmCallbacks *
ts_get_osm_callbacks(void)
{
	OsmCallbacks **ptr = (OsmCallbacks **) find_rendezvous_variable(OSM_CALLBACKS_VAR_NAME);

	return *ptr;
}
