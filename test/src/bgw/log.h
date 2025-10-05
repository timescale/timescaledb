/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#pragma once

extern void ts_bgw_log_set_application_name(char *name);
extern void ts_register_emit_log_hook(void);
extern void ts_unregister_emit_log_hook(void);
