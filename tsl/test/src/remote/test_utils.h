/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_TSL_REMOTE_TEST_UTILS_H
#define TIMESCALEDB_TSL_REMOTE_TEST_UTILS_H

#include <postgres.h>
#include <common/username.h>

#include <commands/dbcommands.h>
#include <nodes/makefuncs.h>
#include <utils/guc.h>
#include <access/xact.h>
#include <miscadmin.h>

#include "test_utils.h"
#include "remote/connection.h"

extern TSConnection *get_connection(void);

extern pid_t remote_connecton_get_remote_pid(TSConnection *conn);
extern char *remote_connecton_get_application_name(TSConnection *conn);

/*
 * Error recover macro for tests.  The only valid way to deal with exceptions
 * is to rethrow them or rollback a subtransaction. Everything else leads to
 * continuing in an undefined state.
 */
#define EXPECT_ERROR                                                                               \
	{                                                                                              \
		MemoryContext oldctx = CurrentMemoryContext;                                               \
		BeginInternalSubTransaction("error expected");                                             \
		PG_TRY();                                                                                  \
		{
#define EXPECT_ERROR_END                                                                           \
	Assert(false);                                                                                 \
	}                                                                                              \
	PG_CATCH();                                                                                    \
	{                                                                                              \
		RollbackAndReleaseCurrentSubTransaction();                                                 \
		FlushErrorState();                                                                         \
	}                                                                                              \
	PG_END_TRY();                                                                                  \
	MemoryContextSwitchTo(oldctx);                                                                 \
	}

#define EXPECT_ERROR_STMT(stmt)                                                                    \
	EXPECT_ERROR                                                                                   \
	stmt;                                                                                          \
	EXPECT_ERROR_END

#endif /* TIMESCALEDB_TSL_REMOTE_TEST_UTILS_H */
