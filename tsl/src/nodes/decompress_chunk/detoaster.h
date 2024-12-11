/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#pragma once

#include <postgres.h>

#include <access/genam.h>
#include <access/relscan.h>
#include <access/skey.h>
#include <utils/snapshot.h>

typedef struct RelationData *Relation;

typedef struct Detoaster
{
	MemoryContext mctx;
	Relation toastrel;
	Relation index;
	SnapshotData SnapshotToast;
	ScanKeyData toastkey;
	SysScanDesc toastscan;
} Detoaster;

void detoaster_init(Detoaster *detoaster, MemoryContext mctx);
void detoaster_close(Detoaster *detoaster);
struct varlena *detoaster_detoast_attr_copy(struct varlena *attr, Detoaster *detoaster,
											MemoryContext dest_mctx);
