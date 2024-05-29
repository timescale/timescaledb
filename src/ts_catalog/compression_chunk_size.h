/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#pragma once

#include <compat/compat.h>
#include <postgres.h>

extern TSDLLEXPORT int ts_compression_chunk_size_delete(int32 uncompressed_chunk_id);
