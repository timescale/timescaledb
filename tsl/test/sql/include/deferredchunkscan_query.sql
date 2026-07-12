-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

SELECT value FROM metrics ORDER BY time LIMIT 5;
SELECT time, device, value FROM metrics ORDER BY time DESC LIMIT 3;
SELECT value * 2 AS v2, device FROM metrics ORDER BY time LIMIT 4;  -- expression
SELECT device, value FROM metrics ORDER BY time OFFSET 3 LIMIT 4;  -- offset
SELECT metrics FROM metrics ORDER BY time LIMIT 2;  -- whole row
SELECT value FROM metrics ORDER BY time LIMIT (SELECT 5);  -- non-const limit
SELECT value FROM metrics_compressed ORDER BY time LIMIT 5;  -- compressed
SELECT metrics_compressed FROM metrics_compressed ORDER BY time LIMIT 2;  -- compressed whole row

-- WHERE clauses
SELECT time, device, value FROM metrics WHERE device = 1 ORDER BY time LIMIT 5;  -- Var, Const, OpExpr
SELECT time, device, value FROM metrics WHERE device = 1 AND value > 5 ORDER BY time DESC LIMIT 5;  -- BoolExpr
SELECT time, value FROM metrics WHERE value IS NOT NULL ORDER BY time LIMIT 5;  -- NullTest, non-dimension
SELECT time, value FROM metrics WHERE time IS NOT NULL AND value > 5 ORDER BY time LIMIT 5;  -- NullTest on dimension (always true)
SELECT time, value FROM metrics WHERE time IS NULL ORDER BY time LIMIT 5;  -- NullTest on dimension (always empty)
SELECT time, device FROM metrics WHERE device IN (0, 2) ORDER BY time LIMIT 5;  -- ScalarArrayOpExpr
SELECT time, value FROM metrics WHERE abs(value) > 5 ORDER BY time LIMIT 5;  -- FuncExpr
SELECT time, value FROM metrics WHERE coalesce(value, 0) > 5 ORDER BY time LIMIT 5;  -- CoalesceExpr
SELECT time, value FROM metrics WHERE (CASE device WHEN 0 THEN value ELSE -1 END) >= 0 ORDER BY time LIMIT 5;  -- simple CaseExpr (CaseTestExpr)
SELECT time, value FROM metrics WHERE (CASE WHEN device = 0 THEN value ELSE -1 END) >= 0 ORDER BY time LIMIT 5;  -- searched CaseExpr
SELECT time, device FROM metrics WHERE device IS DISTINCT FROM 1 ORDER BY time LIMIT 5;  -- DistinctExpr
SELECT time, value FROM metrics WHERE nullif(value, 0) > 5 ORDER BY time LIMIT 5;  -- NullIfExpr
SELECT time, value FROM metrics WHERE greatest(value, 0) > 15 ORDER BY time LIMIT 5;  -- MinMaxExpr
SELECT time, device FROM metrics WHERE (device = 1) IS TRUE ORDER BY time LIMIT 5;  -- BooleanTest
SELECT time, device FROM metrics WHERE device::text = '1' ORDER BY time LIMIT 5;  -- CoerceViaIO
SELECT time, value FROM metrics WHERE value = ANY(ARRAY[value, 0]) ORDER BY time LIMIT 5;  -- ArrayExpr
SELECT time, device, value FROM metrics WHERE (device, value) < (2, 50) ORDER BY time LIMIT 5;  -- RowCompareExpr
SELECT value FROM metrics WHERE value >= 0 AND current_user IS NOT NULL ORDER BY time LIMIT 5;  -- SQLValueFunction
SELECT value FROM metrics_compressed WHERE device = 2 ORDER BY time LIMIT 5;  -- compressed, non-dimension
SELECT time, tag FROM metrics_typed WHERE tag::text = 'tag1' ORDER BY time LIMIT 5;  -- RelabelType
SELECT time, value FROM metrics_typed WHERE value = ANY(arr::float8[]) ORDER BY time LIMIT 5;  -- ArrayCoerceExpr
SELECT time, device FROM metrics_typed WHERE device::dcs_posint >= 0 ORDER BY time LIMIT 5;  -- CoerceToDomain
SELECT time, device FROM metrics_typed WHERE pair = ROW(device, 0)::dcs_pair ORDER BY time LIMIT 5;  -- RowExpr
SELECT time, device FROM metrics_typed WHERE (pair).a >= 0 ORDER BY time LIMIT 5;  -- FieldSelect
SELECT time, arr FROM metrics_typed WHERE arr[1] >= 0 ORDER BY time LIMIT 5;  -- SubscriptingRef

-- unordered full reads, compared through an order-independent aggregate
SELECT count(*), sum(value) FROM (SELECT value FROM metrics LIMIT 1000) x;
SELECT count(*), sum(value) FROM (SELECT value FROM metrics_space LIMIT 1000) x;
SELECT count(*), sum(value) FROM (SELECT value FROM metrics WHERE device >= 1 LIMIT 1000) x;  -- WHERE, unordered

-- multi-key ORDER BY on a leading dimension: rows sharing a time force the
-- trailing key to break ties, so the diff catches a wrong per-chunk order
SELECT time, device FROM metrics_ord ORDER BY time, device LIMIT 8;  -- trailing key ascending
SELECT time, device FROM metrics_ord ORDER BY time DESC, device DESC LIMIT 8;  -- both descending
SELECT time, device FROM metrics_ord ORDER BY time, device DESC LIMIT 8;  -- mixed directions
SELECT time, value FROM metrics_ord ORDER BY time, value NULLS FIRST LIMIT 8;  -- trailing NULLS FIRST
SELECT time, value FROM metrics_ord ORDER BY time, value DESC NULLS LAST LIMIT 8;  -- trailing DESC NULLS LAST
SELECT time, grp FROM metrics_ord ORDER BY time, grp USING ~<~ LIMIT 8;  -- trailing USING operator

-- subqueries in the target list: the node scans the hypertable while the SubPlan
-- and InitPlan sit in the projection above it, so the diff catches a wrong result
SELECT time, (SELECT count(*) FROM metrics_regular) AS c FROM metrics ORDER BY time LIMIT 5;  -- uncorrelated scalar subquery (InitPlan)
SELECT time, device, (SELECT count(*) FROM metrics_regular r WHERE r.x = metrics.device) AS m FROM metrics ORDER BY time LIMIT 5;  -- correlated scalar subquery (SubPlan)
SELECT time, EXISTS(SELECT 1 FROM metrics_regular r WHERE r.x = metrics.device) AS e FROM metrics ORDER BY time LIMIT 5;  -- correlated EXISTS
SELECT time, ARRAY(SELECT x FROM metrics_regular ORDER BY x) AS a FROM metrics ORDER BY time LIMIT 3;  -- ARRAY subquery
