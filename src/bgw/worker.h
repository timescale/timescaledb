/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */

#ifndef BGW_WORKER_H
#define BGW_WORKER_H

#include <postgres.h>

#include <postmaster/bgworker.h>

/**
 * Parameters to background workers.
 *
 * Do not add data here that cannot be simply copied to the background worker
 * using memcpy(3). If it is necessary to add fields that cannot simply be
 * copied, we need to start using the send and recv functions for the types.
 *
 * Only one of `job_id` and `ttl` is passed currently, with `job_id` being used
 * for normal jobs and `ttl` being used for tests.
 *
 * The `bgw_main` is the function to execute when starting the job and is
 * different depending on whether this is a test runner or the real runner.
 *
 * @see ts_bgw_db_scheduler_test_main
 * @see ts_bgw_job_entrypoint
 */
typedef struct BgwParams
{
	/** User oid to run the job as. Used when initializing the database
	 * connection. */
	Oid user_oid;

	/** Job id to use for the worker when executing the job */
	int32 job_id;

	/** Time to live. Only used in tests. */
	int32 ttl;

	/** Name of function to call when starting the background worker. */
	char bgw_main[BGW_MAXLEN];
} BgwParams;

/**
 * Compile-time check to ensure that the size of BgwParams fit into the bgw_extra field
 * of BackgroundWorker.
 */
StaticAssertDecl(sizeof(BgwParams) <= sizeof(((BackgroundWorker *) 0)->bgw_extra),
				 "sizeof(BgwParams) exceeds sizeof(bgw_extra) field of BackgroundWorker");

#endif /* BGW_WORKER_H */
