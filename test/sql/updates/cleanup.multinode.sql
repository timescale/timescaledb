-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

DROP TABLE disthyper;
SELECT delete_data_node('dn1');
drop database if exists dn1;
