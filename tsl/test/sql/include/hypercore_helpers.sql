-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Function to run an explain analyze with and do replacements on the
-- emitted plan. This is intended to be used when the structure of the
-- plan is important, but not the specific chunks scanned nor the
-- number of heap fetches, rows, loops, etc.
create function anonymize(ln text) returns text language plpgsql as
$$
begin
    ln := regexp_replace(ln, '_hyper_\d+_\d+_chunk', '_hyper_I_N_chunk', 1, 0);
    ln := regexp_replace(ln, 'Heap Fetches: \d+', 'Heap Fetches: N');
    ln := regexp_replace(ln, 'Workers Launched: \d+', 'Workers Launched: N');
    ln := regexp_replace(ln, 'actual rows=\d+ loops=\d+', 'actual rows=N loops=N');

    if trim(both from ln) like 'Array: %' then
       ln := regexp_replace(ln, 'hits=\d+', 'hits=N');
       ln := regexp_replace(ln, 'misses=\d+', 'misses=N');
       ln := regexp_replace(ln, 'count=\d+', 'count=N');
       ln := regexp_replace(ln, 'calls=\d+', 'calls=N');
    end if;
    return ln;
end
$$;

create function explain_analyze_anonymize(text) returns setof text
language plpgsql as
$$
declare
    ln text;
begin
    for ln in
        execute format('explain (analyze, costs off, summary off, timing off, decompress_cache_stats) %s', $1)
    loop
	if trim(both from ln) like 'Group Key:%' then
       	   continue;
        end if;

        return next anonymize(ln);
    end loop;
end;
$$;

create function explain_anonymize(text) returns setof text
language plpgsql as
$$
declare
    ln text;
begin
    for ln in
        execute format('explain (costs off, summary off, timing off) %s', $1)
    loop
	if trim(both from ln) like 'Group Key:%' then
       	   continue;
        end if;
        return next anonymize(ln);
    end loop;
end;
$$;
