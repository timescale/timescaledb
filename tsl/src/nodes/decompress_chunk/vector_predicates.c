/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

/*
 * Functions for working with vectorized predicates.
 */

#include <postgres.h>

#include <utils/date.h>
#include <utils/fmgroids.h>

#include "compat/compat.h"
#include "compression/arrow_c_data_interface.h"

#include "vector_predicates.h"

#include "pred_vector_const_arithmetic_all.c"

/*
 * Look up the vectorized implementation for a Postgres predicate, specified by
 * its Oid in pg_proc. Note that this Oid is different from the opcode.
 */
void (*get_vector_const_predicate(Oid pg_predicate))(const ArrowArray *, const Datum,
													 uint64 *restrict)
{
	switch (pg_predicate)
	{
#define GENERATE_DISPATCH_TABLE
#include "pred_vector_const_arithmetic_all.c"
#undef GENERATE_DISPATCH_TABLE
	}
	return NULL;
}
