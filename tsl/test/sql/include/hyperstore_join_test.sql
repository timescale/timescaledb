-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\set inner :jointype _join
\set outer :jointype _outer_join

-- Test inner join to make sure that it works.
select explain_anonymize(format($$
    select * from %s join the_hyperstore using (device_id)
$$, :'chunk1'));

-- Check that it generates the right result
select * into :inner from :chunk1 join the_hyperstore using (device_id);

\x on
select * from :inner r full join expected_inner e on row(r) = row(e)
where r.device_id is null or e.device_id is null;
\x off

-- Test outer join (left in this case) to make sure that it works.
select explain_anonymize(format($$
select created_at, updated_at, o.device_id, i.humidity, o.height
  from :chunk1 i left join the_hyperstore o
    on i.created_at = o.updated_at and i.device_id = o.device_id;

select created_at, updated_at, o.device_id, i.humidity, o.height
  into :outer
  from :chunk1 i left join the_hyperstore o
    on i.created_at = o.updated_at and i.device_id = o.device_id;
$$, ':chunk1'));

\x on
select * from :outer r full join expected_left e on row(r) = row(e)
where r.device_id is distinct from e.device_id;
\x off

drop table :inner;
drop table :outer;

