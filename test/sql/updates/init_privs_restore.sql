-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- Recreate the installation defaults used by init_privs.sql so its initial
-- ACL baseline is identical when checking the restored current privileges.
ALTER DEFAULT PRIVILEGES GRANT SELECT ON TABLES TO initacl_keep, initacl_restore;
ALTER DEFAULT PRIVILEGES GRANT SELECT ON SEQUENCES TO initacl_keep, initacl_restore;
CREATE EXTENSION timescaledb VERSION :'TO_VERSION';
ALTER DEFAULT PRIVILEGES REVOKE SELECT ON TABLES FROM initacl_keep, initacl_restore;
ALTER DEFAULT PRIVILEGES REVOKE SELECT ON SEQUENCES FROM initacl_keep, initacl_restore;
