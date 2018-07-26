/*
 * Copyright (c) 2016-2018  Timescale, Inc. All Rights Reserved.
 *
 * This file is licensed under the Apache License,
 * see LICENSE-APACHE at the top level directory.
 */
#include "compat.h"

#if PG96
/* this was statically defined in pg_enum.c but was moved to
 buitlins.c in March 2017 and exposed in buitlins.h in PG10.
 This makes sure that we can use oid_cmp in PG96
 The change in postgres happened here:
 https://git.postgresql.org/gitweb/?p=postgresql.git;a=commitdiff;h=20f6d74242b3c9c84924e890248d027d30283e21
 */
/* qsort comparison function for Oids */
int
oid_cmp(const void *p1, const void *p2)
{
	Oid			v1 = *((const Oid *) p1);
	Oid			v2 = *((const Oid *) p2);

	if (v1 < v2)
		return -1;
	if (v1 > v2)
		return 1;
	return 0;
}

#endif
