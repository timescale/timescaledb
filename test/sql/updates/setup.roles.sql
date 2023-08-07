-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

CREATE ROLE cagg_user;
CREATE USER tsdbadmin;

-- These are used to test job creation and updating job owners.
CREATE USER "dotted.name";	--non-identifier character in name
CREATE USER "Kim Possible";	--case-sensitive names
