/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

#include <postgres.h>
#include "compression/compression.h"

extern DecompressionIterator *aic_decompression_iterator_from_datum_forward(Datum compressed,
																			Oid element_type);

extern DecompressionIterator *aic_decompression_iterator_from_datum_reverse(Datum compressed,
																			Oid element_type);
