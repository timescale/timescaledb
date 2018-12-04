/*
 * Copyright (c) 2016-2018  Timescale, Inc. All Rights Reserved.
 *
 * This file is licensed under the Apache License,
 * see LICENSE-APACHE at the top level directory.
 */
#ifndef TEST_BGW_LOG_H
#define TEST_BGW_LOG_H

extern void ts_bgw_log_set_application_name(char *name);
extern void ts_register_emit_log_hook(void);

#endif							/* TEST_BGW_LOG_H */
