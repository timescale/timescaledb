/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

/*
 * Functions for working with vectorized predicates.
 */

#pragma once

void (*get_vector_const_predicate(Oid pg_predicate))(const ArrowArray *, const Datum,
													 uint64 *restrict);
