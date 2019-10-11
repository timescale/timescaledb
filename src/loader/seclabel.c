/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include "seclabel.h"
#include <miscadmin.h>

bool
ts_seclabel_get_dist_uuid(Oid dbid, char **uuid)
{
	ObjectAddress dbobj;
	char *uuid_ptr;
	char *label;

	*uuid = NULL;
	ObjectAddressSet(dbobj, DatabaseRelationId, dbid);
	label = GetSecurityLabel(&dbobj, SECLABEL_DIST_PROVIDER);
	if (label == NULL)
		return false;

	uuid_ptr = strchr(label, SECLABEL_DIST_TAG_SEPARATOR);
	if (uuid_ptr == NULL)
		return false;
	++uuid_ptr;
	*uuid = uuid_ptr;
	return true;
}

static void
seclabel_provider(const ObjectAddress *object, const char *seclabel)
{
}

void
ts_seclabel_init(void)
{
	register_label_provider(SECLABEL_DIST_PROVIDER, seclabel_provider);
}
