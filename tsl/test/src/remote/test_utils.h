/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_TSL_REMOTE_TEST_UTILS_H
#define TIMESCALEDB_TSL_REMOTE_TEST_UTILS_H

#include <postgres.h>
#include <common/username.h>

#include "remote/connection.h"
#include <commands/dbcommands.h>
#include <nodes/makefuncs.h>
#include <utils/guc.h>
#include <miscadmin.h>

static PGconn *
get_connection()
{
	return remote_connection_open("testdb",
								  list_make3(makeDefElem("user",
														 (Node *) makeString(
															 GetUserNameFromId(GetUserId(), false)),
														 -1),
											 makeDefElem("dbname",
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

#endif /* TIMESCALEDB_TSL_REMOTE_TEST_UTILS_H */
