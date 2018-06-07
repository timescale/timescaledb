/*
 * Copyright (c) 2018  Timescale, Inc. All Rights Reserved.
 *
 * This file is licensed under the Timescale License,
 * see LICENSE-TIMESCALE at the top of the tsl directory.
 */
#ifndef REMOTE_TEST_UTILS_H
#define REMOTE_TEST_UTILS_H

#include <postgres.h>

#include "remote/connection.h"
#include <commands/dbcommands.h>
#include <nodes/makefuncs.h>
#include <utils/guc.h>
#include <miscadmin.h>

static PGconn *
get_connection()
{
	return remote_connection_open("testdb",
								  list_make2(makeDefElem("dbname",
														 (Node *) makeString(
															 get_database_name(MyDatabaseId)),
														 -1),
											 makeDefElem("port",
														 (Node *) makeString(
															 pstrdup(GetConfigOption("port",
																					 false,
																					 false))),
														 -1)),
								  NIL);
}

#define EXPECT_ERROR                                                                               \
	PG_TRY();                                                                                      \
	{
#define EXPECT_ERROR_END                                                                           \
	Assert(false);                                                                                 \
	}                                                                                              \
	PG_CATCH();                                                                                    \
	{                                                                                              \
		FlushErrorState();                                                                         \
	}                                                                                              \
	PG_END_TRY();

#define EXPECT_ERROR_STMT(stmt)                                                                    \
	EXPECT_ERROR                                                                                   \
	stmt;                                                                                          \
	EXPECT_ERROR_END

#endif /* REMOTE_TEST_UTILS_H */
