#ifndef TIMESCALEDB_TELEMETRY_TELEMETRY_H
#define TIMESCALEDB_TELEMETRY_TELEMETRY_H
#include <postgres.h>
#include <fmgr.h>
#include <pg_config.h> // To get USE_OPENSSL from postgres build
#include <utils/builtins.h>

#include "compat.h"
#include "guc.h"
#include "version.h"
#include "net/conn.h"
#include "net/http.h"
#include "utils.h"

#define HTTPS_PORT	443

char *get_guc_endpoint_hostname(void);
int get_guc_endpoint_port(void);
HttpRequest *build_version_request(void);

/*
 *	This function is intended as the main function for a BGW.
 *  Its job is to send metrics and fetch the most up-to-date version of
 *  Timescale via HTTPS.
 */
void		telemetry_main(void);

#endif							/* TIMESCALEDB_TELEMETRY_TELEMETRY_H */
