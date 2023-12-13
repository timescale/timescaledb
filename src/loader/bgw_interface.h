/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#pragma once

#include <postgres.h>

extern void ts_bgw_interface_register_api_version(void);
extern const int32 ts_bgw_loader_api_version;
