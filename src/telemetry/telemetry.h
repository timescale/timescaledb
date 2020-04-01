/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
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

#define MAX_VERSION_STR_LEN 128

typedef struct VersionResult
{
	const char *versionstr;
	const char *errhint;
} VersionResult;

extern HttpRequest *ts_build_version_request(const char *host, const char *path);
extern Connection *ts_telemetry_connect(const char *host, const char *service);
extern bool ts_validate_server_version(const char *json, VersionResult *result);
extern void ts_check_version_response(const char *json);

/*
 *	This function is intended as the main function for a BGW.
 *  Its job is to send metrics and fetch the most up-to-date version of
 *  Timescale via HTTPS.
 */
extern bool ts_telemetry_main(const char *host, const char *path, const char *service);
extern bool ts_telemetry_main_wrapper(void);

#endif /* TIMESCALEDB_TELEMETRY_TELEMETRY_H */
