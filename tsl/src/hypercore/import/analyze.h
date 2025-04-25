/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#pragma once

#include <postgres.h>

#include <commands/vacuum.h>
#include <storage/read_stream.h>

extern int hypercore_analyze_compute_vacattrstats(Relation onerel, VacAttrStats ***vacattrstats_out,
												  MemoryContext mcxt);
extern BlockNumber hypercore_block_sampling_read_stream_next(ReadStream *stream,
															 void *callback_private_data,
															 void *per_buffer_data);
