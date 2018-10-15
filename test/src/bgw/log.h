/*
 * Copyright (c) 2016-2018  Timescale, Inc. All Rights Reserved.
 *
 * This file is licensed under the Apache License,
 * see LICENSE-APACHE at the top level directory.
 */
#ifndef TEST_BGW_LOG_H
#define TEST_BGW_LOG_H

void		bgw_log_set_application_name(char *name);
void		register_emit_log_hook(void);

#endif							/* TEST_BGW_LOG_H */
