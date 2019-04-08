-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

--expects QUERY to be set
\o /dev/null

drop view if exists mat_test cascade;

create view mat_test
WITH ( timescaledb.continuous)
as :QUERY
;

select h.schema_name AS "MAT_SCHEMA_NAME",
       h.table_name AS "MAT_TABLE_NAME",
       partial_view_name as "PART_VIEW_NAME",
       partial_view_schema as "PART_VIEW_SCHEMA"
from _timescaledb_catalog.continuous_agg ca
INNER JOIN _timescaledb_catalog.hypertable h ON(h.id = ca.mat_hypertable_id)
where user_view_name = 'mat_test'
\gset

\c :TEST_DBNAME :ROLE_SUPERUSER
INSERT INTO :"MAT_SCHEMA_NAME".:"MAT_TABLE_NAME" SELECT * FROM :"PART_VIEW_SCHEMA".:"PART_VIEW_NAME";
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER
\o

with original AS (
  SELECT row_number() OVER(ORDER BY q.*) row_number, * FROM (:QUERY) as q
),
view AS (
  SELECT row_number() OVER (ORDER BY q.*) row_number, * FROM mat_test as q
)
SELECT 'Number of rows different between view and original (expect 0)', count(*)
FROM original
FULL OUTER JOIN view ON (original.row_number = view.row_number)
WHERE (original.*) IS DISTINCT FROM (view.*);
