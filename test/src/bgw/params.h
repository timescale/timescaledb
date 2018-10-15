/*
 * Copyright (c) 2016-2018  Timescale, Inc. All Rights Reserved.
 *
 * This file is licensed under the Apache License,
 * see LICENSE-APACHE at the top level directory.
 */
#ifndef TEST_BGW_PARAMS_H
#define TEST_BGW_PARAMS_H
#include <postgres.h>
typedef struct TestParams
{
	int64		current_time;
	bool		mock_wait_returns_immediately;
} TestParams;

extern TestParams *params_get(void);
extern void params_set_time(int64 new_val);

#endif							/* TEST_BGW_PARAMS_H */
