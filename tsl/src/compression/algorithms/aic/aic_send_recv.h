/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

#include <postgres.h>
#include "compression/compression.h"

extern void aic_compressed_send(CompressedDataHeader *header, StringInfo buffer);
extern Datum aic_compressed_recv(StringInfo buffer);
