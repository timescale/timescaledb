-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

--------------------------
-- cheap rand generator --
--------------------------
-- The state is kept in a sequence, seeded with 321. Updating a sequence
-- does not leave dead row versions behind like updating a table does, so
-- each call costs the same no matter how often it was called before.
create sequence rand_minstd_state minvalue 1 maxvalue 2147483646 start with 321;

create function rand_minstd_advance(bigint) returns bigint
language sql immutable as
$$
	select (16807 * $1) % 2147483647
$$;

create function gen_rand_minstd() returns bigint
language sql security definer as
$$
	select setval('rand_minstd_state', rand_minstd_advance(last_value)) from rand_minstd_state
$$;
