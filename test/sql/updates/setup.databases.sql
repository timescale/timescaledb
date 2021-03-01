-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

CREATE DATABASE single;
-- Always pre-create the data node database 'dn1' so that we can dump
-- and restore it even on TimescaleDB versions that don't support
-- multinode. Otherwise, we'd have to create version-dependent scripts
-- to specifically handle multinode tests. We use template0, or
-- otherwise dn1 will have the same UUID as 'single' since template1
-- has the extension pre-installed.
CREATE DATABASE dn1 TEMPLATE template0;
\c dn1
-- Make sure the extension is installed so that extension versions
-- that don't support multinode will still be able to update the
-- extension with ALTER EXTENSION ... UPDATE.
CREATE EXTENSION IF NOT EXISTS timescaledb;

\c single
CREATE EXTENSION IF NOT EXISTS timescaledb;

