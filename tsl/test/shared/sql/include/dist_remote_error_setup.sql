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
create type bl;

create or replace function blsend(bl) returns bytea as 'int4send' language internal;

\if :{?sleepy_recv}
create or replace function blrecv(internal) returns bl
    as :MODULE_PATHNAME, 'ts_debug_sleepy_int4recv'
    language c immutable strict parallel safe;
\else
create or replace function blrecv(internal) returns bl as 'int4recv' language internal;
\endif

create or replace function blin(cstring) returns bl as 'int4in' language internal;

create or replace function blout(bl) returns cstring as 'int4out' language internal;

create type bl(input = blin, output = blout, send = blsend, receive = blrecv,
    internallength = 4, passedbyvalue = true);

create cast (int4 as bl) without function as implicit;

create cast (bl as int4) without function as implicit;

-- Create a function that raises an error every nth row.
-- It's stable, takes a second argument and returns current number of rows,
-- so that it is shipped to data nodes and not optimized out.
-- It's written in one line because I don't know how to make \set accept
-- several lines.
CREATE OR REPLACE FUNCTION ts_debug_shippable_error_after_n_rows(integer, anyelement)
    RETURNS integer AS :MODULE_PATHNAME LANGUAGE C STABLE STRICT;

-- Same as above, but fatal.
CREATE OR REPLACE FUNCTION ts_debug_shippable_fatal_after_n_rows(integer, anyelement)
    RETURNS integer AS :MODULE_PATHNAME LANGUAGE C STABLE STRICT;
