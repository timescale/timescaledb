--Fix any potential catalog issues that may have been introduced if a
--trigger was dropped on a hypertable before the current bugfix
--Only deletes orphaned rows from pg_depend.
DELETE FROM pg_depend d 
WHERE d.classid = 'pg_trigger'::regclass 
AND NOT EXISTS (SELECT 1 FROM pg_trigger WHERE oid = d.objid);
