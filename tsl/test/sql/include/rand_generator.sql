-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license

--------------------------
-- cheap rand generator --
--------------------------
create table rand_minstd_state(i bigint);

create function rand_minstd_advance(bigint) returns bigint
language sql immutable as
$$
	select (16807 * $1) % 2147483647
$$;

create function gen_rand_minstd() returns bigint
language sql as
$$
	update rand_minstd_state set i = rand_minstd_advance(i) returning i
$$;

-- seed the random num generator
insert into rand_minstd_state values (321);
