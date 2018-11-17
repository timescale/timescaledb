/*
 * Copyright (c) 2016-2018  Timescale, Inc. All Rights Reserved.
 *
 * This file is licensed under the Apache License,
 * see LICENSE-APACHE at the top level directory.
 */
#ifndef TIMESCALEDB_TELEMETRY_TELEMETRY_H
#define TIMESCALEDB_TELEMETRY_TELEMETRY_H
#include <postgres.h>
#include <fmgr.h>
#include <pg_config.h> // To get USE_OPENSSL from postgres build
#include <utils/builtins.h>

#include "compat.h"
#include "version.h"
#include "net/conn.h"
#include "net/http.h"
#include "utils.h"

#define TELEMETRY_SCHEME "https"
#define TELEMETRY_HOST "telemetry.timescale.com"
#define TELEMETRY_PATH "/v1/metrics"

#define	MAX_VERSION_STR_LEN	128

typedef struct VersionResult
{
	const char *versionstr;
	const char *errhint;
} VersionResult;

HttpRequest *build_version_request(const char *host, const char *path);
Connection *telemetry_connect(const char *host, const char *service);
bool		validate_server_version(const char *json, VersionResult *result);

/*
 *	This function is intended as the main function for a BGW.
 *  Its job is to send metrics and fetch the most up-to-date version of
 *  Timescale via HTTPS.
 */
bool		telemetry_main(const char *host, const char *path, const char *service);
bool		telemetry_main_wrapper(void);

#endif							/* TIMESCALEDB_TELEMETRY_TELEMETRY_H */
