-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

--expects QUERY and VIEW_NAME to be set
with original AS (
  SELECT row_number() OVER(ORDER BY q.*) row_number, * FROM (:QUERY) as q
),
view AS (
  SELECT row_number() OVER (ORDER BY q.*) row_number, * FROM :VIEW_NAME as q
)
SELECT 'Number of rows different between view and original (expect 0)' as description, :'VIEW_NAME' as view_name, count(*)
FROM original
FULL OUTER JOIN view ON (original.row_number = view.row_number)
WHERE (original.*) IS DISTINCT FROM (view.*);

