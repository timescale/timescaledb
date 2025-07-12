/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

#include <postgres.h>

#include "compression/arrow_c_data_interface.h"

extern void vector_const_texteq(const ArrowArray *arrow, const Datum constdatum,
								uint64 *restrict result);

extern void vector_const_textne(const ArrowArray *arrow, const Datum constdatum,
								uint64 *restrict result);

extern void vector_const_textlike_utf8(const ArrowArray *arrow, const Datum constdatum,
									   uint64 *restrict result);

extern void vector_const_textnlike_utf8(const ArrowArray *arrow, const Datum constdatum,
										uint64 *restrict result);
