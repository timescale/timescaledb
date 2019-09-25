-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER
SET timescaledb.debug_optimizer_flags = '';
SHOW timescaledb.debug_optimizer_flags;
SET timescaledb.debug_optimizer_flags = 'show_upper=final';
SHOW timescaledb.debug_optimizer_flags;
SET timescaledb.debug_optimizer_flags = 'show_upper=fin';
SHOW timescaledb.debug_optimizer_flags;
SET timescaledb.debug_optimizer_flags = 'show_upper=fin,win';
SHOW timescaledb.debug_optimizer_flags;
SET timescaledb.debug_optimizer_flags = 'show_upper=*,fin,win';
SHOW timescaledb.debug_optimizer_flags;
SET timescaledb.debug_optimizer_flags = 'show_upper=*';
SHOW timescaledb.debug_optimizer_flags;
SET timescaledb.debug_optimizer_flags = 'show_upper=win:show_rel';
SHOW timescaledb.debug_optimizer_flags;
SET timescaledb.debug_optimizer_flags = '"show_upper=win":show_rel';
SHOW timescaledb.debug_optimizer_flags;
SET timescaledb.debug_optimizer_flags = 'show_upper=win : show_rel';
SHOW timescaledb.debug_optimizer_flags;
SET timescaledb.debug_optimizer_flags = 'show_rel:show_upper=win';
SHOW timescaledb.debug_optimizer_flags;

-- These should all fail
\set ON_ERROR_STOP 0
SET timescaledb.debug_optimizer_flags = NULL;
SET timescaledb.debug_optimizer_flags = 'invalid';
SET timescaledb.debug_optimizer_flags = '"unmatched quote:';
SET timescaledb.debug_optimizer_flags = 'space between';
SET timescaledb.debug_optimizer_flags = 'space between:';
SET timescaledb.debug_optimizer_flags = 'show_rel:invalid';
SET timescaledb.debug_optimizer_flags = 'invalid:show_rel';
SET timescaledb.debug_optimizer_flags = 'show_upper:*';
SET timescaledb.debug_optimizer_flags = 'show_upper=xxx';
SET timescaledb.debug_optimizer_flags = 'show_upper=fin,xxx';
SET timescaledb.debug_optimizer_flags = 'show_upper=xxx,fin';
SET timescaledb.debug_optimizer_flags = 'show_upper=win,xxx,fin';
SET timescaledb.debug_optimizer_flags = 'show_upper=xxx';
SET timescaledb.debug_optimizer_flags = 'show_upper=*,xxx';
SET timescaledb.debug_optimizer_flags = 'show_upper=xxx,*';
SET timescaledb.debug_optimizer_flags = 'show_upper=xxx,*,yyy';
SET timescaledb.debug_optimizer_flags = 'show_upper=supercalifragilisticexpialidochious';
SET timescaledb.debug_optimizer_flags = 'show_upper=super,califragilisticexpialidochious';
\set ON_ERROR_STOP 1
