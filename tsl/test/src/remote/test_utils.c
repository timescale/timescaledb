/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <common/username.h>

#include "test_utils.h"
#include "remote/connection.h"
#include <commands/dbcommands.h>
#include <nodes/makefuncs.h>
#include <utils/guc.h>
#include <miscadmin.h>

PGconn *
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
								  NIL,
								  false);
}
