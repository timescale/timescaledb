/*
 * Copyright (c) 2016-2018  Timescale, Inc. All Rights Reserved.
 *
 * This file is licensed under the Apache License,
 * see LICENSE-APACHE at the top level directory.
 */
#ifndef TEST_BGW_PARAMS_H
#define TEST_BGW_PARAMS_H
#include <postgres.h>
#include <storage/latch.h>

typedef enum MockWaitType
{
	WAIT_ON_JOB = 0,
	IMMEDIATELY_SET_UNTIL,
	WAIT_FOR_OTHER_TO_ADVANCE,
	_MAX_MOCK_WAIT_TYPE
} MockWaitType;

typedef struct TestParams
{
	Latch		timer_latch;
	int64		current_time;
	MockWaitType mock_wait_type;
} TestParams;

extern TestParams *ts_params_get(void);
extern void ts_params_set_time(int64 new_val, bool set_latch);
extern void ts_initialize_timer_latch(void);
extern void ts_reset_and_wait_timer_latch(void);

#endif							/* TEST_BGW_PARAMS_H */
