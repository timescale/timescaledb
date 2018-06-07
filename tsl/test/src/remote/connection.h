/*
 * Copyright (c) 2018  Timescale, Inc. All Rights Reserved.
 *
 * This file is licensed under the Timescale License,
 * see LICENSE-TIMESCALE at the top of the tsl directory.
 */
#ifndef TSL_TEST_REMOTE_CONNECTION_H
#define TSL_TEST_REMOTE_CONNECTION_H

#include <postgres.h>
#include <libpq-fe.h>

#include <remote/connection.h>

extern PGconn *get_connection(void);

#endif /* TSL_TEST_REMOTE_CONNECTION_H */
