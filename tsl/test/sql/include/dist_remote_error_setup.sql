-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- bs is for "broken send", the type is int4
create type bs;

create or replace function bssend(bs) returns bytea
    as :MODULE_PATHNAME, 'ts_debug_broken_int4send'
    language c immutable strict parallel safe;

create or replace function bsrecv(internal) returns bs as 'int4recv' language internal;

create or replace function bsin(cstring) returns bs as 'int4in' language internal;

create or replace function bsout(bs) returns cstring as 'int4out' language internal;

create type bs(input = bsin, output = bsout, send = bssend, receive = bsrecv,
    internallength = 4, passedbyvalue = true);

create cast (int4 as bs) without function as implicit;

create cast (bs as int4) without function as implicit;

-- same but for broken recv
create type br;

create or replace function brsend(br) returns bytea as 'int4send' language internal;

create or replace function brrecv(internal) returns br
    as :MODULE_PATHNAME, 'ts_debug_broken_int4recv'
    language c immutable strict parallel safe;

create or replace function brin(cstring) returns br as 'int4in' language internal;

create or replace function brout(br) returns cstring as 'int4out' language internal;

create type br(input = brin, output = brout, send = brsend, receive = brrecv,
    internallength = 4, passedbyvalue = true);

create cast (int4 as br) without function as implicit;

create cast (br as int4) without function as implicit;

-- recv that sleeps, optionally (want that only on one data node)
create type sr;

create or replace function srsend(sr) returns bytea as 'int4send' language internal;

\if :{?sleepy_sendrecv}
create or replace function srrecv(internal) returns sr
    as :MODULE_PATHNAME, 'ts_debug_sleepy_int4recv'
    language c immutable strict parallel safe;
\else
create or replace function srrecv(internal) returns sr as 'int4recv' language internal;
\endif

create or replace function srin(cstring) returns sr as 'int4in' language internal;

create or replace function srout(sr) returns cstring as 'int4out' language internal;

create type sr(input = srin, output = srout, send = srsend, receive = srrecv,
    internallength = 4, passedbyvalue = true);

create cast (int4 as sr) without function as implicit;

create cast (sr as int4) without function as implicit;

-- send that sleeps, optionally (want that only on one data node)
create type ss;

create or replace function ssrecv(internal) returns ss as 'int4recv' language internal;

\if :{?sleepy_sendrecv}
create or replace function sssend(ss) returns bytea
    as :MODULE_PATHNAME, 'ts_debug_sleepy_int4send'
    language c immutable strict parallel safe;
\else
create or replace function sssend(ss) returns bytea as 'int4send' language internal;
\endif

create or replace function ssin(cstring) returns ss as 'int4in' language internal;

create or replace function ssout(ss) returns cstring as 'int4out' language internal;

create type ss(input = ssin, output = ssout, send = sssend, receive = ssrecv,
    internallength = 4, passedbyvalue = true);

create cast (int4 as ss) without function as implicit;

create cast (ss as int4) without function as implicit;

-- int4out that sometimes outputs not an int (name is abbreviation of Incorrect Out)
create type io;

create or replace function iorecv(internal) returns io as 'int4recv' language internal;

create or replace function iosend(io) returns bytea as 'int4send' language internal;

create or replace function ioin(cstring) returns io as 'int4in' language internal;

create or replace function ioout(io) returns cstring
    as :MODULE_PATHNAME, 'ts_debug_incorrect_int4out'
    language c immutable strict parallel safe;

create type io(input = ioin, output = ioout, send = iosend, receive = iorecv,
    internallength = 4, passedbyvalue = true);

create cast (int4 as io) without function as implicit;

create cast (io as int4) without function as implicit;


-- Create a function that raises an error every nth row.
-- It's stable, takes a second argument and returns current number of rows,
-- so that it is shipped to data nodes and not optimized out.
create or replace function ts_debug_shippable_error_after_n_rows(integer, anyelement)
    returns integer as :MODULE_PATHNAME language C stable strict;

-- Same as above, but fatal.
create or replace function ts_debug_shippable_fatal_after_n_rows(integer, anyelement)
    returns integer as :MODULE_PATHNAME language C stable strict;
