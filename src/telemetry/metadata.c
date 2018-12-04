/*
 * Copyright (c) 2016-2018  Timescale, Inc. All Rights Reserved.
 *
 * This file is licensed under the Apache License,
 * see LICENSE-APACHE at the top level directory.
 */
#include <postgres.h>
#include <catalog/pg_type.h>
#include <utils/timestamp.h>

#include "catalog.h"
#include "installation_metadata.h"
#include "telemetry/uuid.h"
#include "telemetry/metadata.h"

#define INSTALLATION_METADATA_UUID_KEY_NAME			"uuid"
#define INSTALLATION_METADATA_EXPORTED_UUID_KEY_NAME	"exported_uuid"
#define INSTALLATION_METADATA_TIMESTAMP_KEY_NAME		"install_timestamp"

static Datum
get_uuid_by_key(const char *key)
{
	bool		isnull;
	Datum		uuid;

	uuid = ts_installation_metadata_get_value(CStringGetDatum(key), CSTRINGOID, UUIDOID, &isnull);

	if (isnull)
		uuid = ts_installation_metadata_insert(CStringGetDatum(key),
											   CSTRINGOID,
											   UUIDPGetDatum(ts_uuid_create()),
											   UUIDOID);
	return uuid;
}

Datum
ts_metadata_get_uuid(void)
{
	return get_uuid_by_key(INSTALLATION_METADATA_UUID_KEY_NAME);
}

Datum
ts_metadata_get_exported_uuid(void)
{
	return get_uuid_by_key(INSTALLATION_METADATA_EXPORTED_UUID_KEY_NAME);
}

Datum
ts_metadata_get_install_timestamp(void)
{
	bool		isnull;
	Datum		timestamp;

	timestamp = ts_installation_metadata_get_value(CStringGetDatum(INSTALLATION_METADATA_TIMESTAMP_KEY_NAME),
												   CSTRINGOID,
												   TIMESTAMPTZOID,
												   &isnull);

	if (isnull)
		timestamp = ts_installation_metadata_insert(CStringGetDatum(INSTALLATION_METADATA_TIMESTAMP_KEY_NAME),
													CSTRINGOID,
													TimestampTzGetDatum(GetCurrentTimestamp()),
													TIMESTAMPTZOID);

	return timestamp;
}
