# Variable-Sized Buckets and Timezones Support for Continuous Aggregates

## Goals

Our [survey][survey] showed that:

* Users need the support of buckets for continuous aggregates (caggs) that are
  N-month and/or N-years in size;
* There is also a demand for timezones (including daylight saving time) support;

So we have two goals. At the moment of writing, it is the most upvoted
functionality in our Community Slack.


## Background

See [TimescaleDocs / Continuous Aggregates][docs] for the general information
about caggs.

Currently, we support only buckets that are fixed in size, like 3 hours or
5 days. Months and years are variable in size (29..31 days, 365..366 days),
thus we call them “variable-sized buckets”. Timezones are not supported at the
moment. If there is a [daylight saving time][dst] in a given timezone it means
there are days that have 23..25 hours, which is a case of variable-sized buckets
as well.

There are workarounds, e.g. you can [build a VIEW on top of continuous
aggregate][workaround] This is not very convenient though. Also we don’t expect
the users to be advanced enough to feel comfortable with such workarounds.

## Proposal

We can’t use [date_trunc()][date_trunc] in caggs in the general case. There are
several reasons (see below) but the most important one is that the first
argument of this function can only be as simple as ‘month’, ‘year’, etc.
However, we need to support buckets like ‘5 months’ or ‘1 year 2 months’.
[date_bin()][date_bin] can’t be used because it doesn't support intervals of
month and larger. We could support date_trunc()/date_bin() in caggs as well as
a separate feature, but this is not a priority.

Thus the proposal is to introduce a new function, similar to time_bucket(),
but which also supports variable-sized buckets and timezones. The intent is to
replace time_bucket() at some point, but we would like to start with a separate
function in the experimental schema.

To clarify, these are two separate goals. The first goal is to support
variable-sized buckets, like months and years:

```
=# SELECT time_bucket_ng('1 month', date '2021-06-23');
2021-06-01

=# SELECT time_bucket_ng('3 month', date '2021-06-23');
2021-04-01

=# SELECT time_bucket_ng('1 year', date '2021-06-23');
 2021-01-01
```

The second goal is to support timezones:

```
-- returns the beginning of the bucket in UTC
=# SELECT time_bucket_ng('7 days', ‘2021-06-23 12:34:56', ‘Europe/Moscow’);
 2021-06-19 00:00:00+03
```

Further explanation of the semantic can be found below. And the third goal is
to make (1) and (2) work together.

We also have to support an `origin` argument, which determines the timestamp
from which we start to divide the time into buckets. To understand why, let's
consider ‘5 months’ buckets. Each bucket will start:

```
2021-01-01 + 5 months =
2021-06-01 + 5 months =
2021-11-01 + 5 months =
2022-04-01
```

Notice that the 4th bucket doesn't start in January anymore. Thus we need some
sort of `origin` to understand from what point in time we start to split time
into buckets. Supporting `origin` argument in caggs is not a top priority
though. We believe that most users will be OK with the default origin.

Given the above, here is the function signature:

```
-- basic version; `origin` is some default value
CREATE FUNCTION time_bucket_ng(
      bucket_width INTERVAL,
      ts DATE
  ) RETURNS DATE;

-- version with `origin`
CREATE FUNCTION time_bucket_ng(
      bucket_width INTERVAL,
      ts DATE,
      origin DATE
  ) RETURNS DATE

-- version with timezone
CREATE FUNCTION time_bucket_ng(
      bucket_width INTERVAL,
      ts TIMESTAMPTZ,
      timezone TEXT -- e.g. ‘UTC+3’ or ‘Europe/Berlin’
  ) RETURNS TIMESTAMPTZ

-- and also multiple wrappers for convenience…
-- for more examples see https://github.com/timescale/timescaledb/pull/3211/files
```

Although time_bucket_ng() works with time zones and time zones change, it’s
[volatility][volatility] should be IMMUTABLE. This will
[make it consistent][consistent] with the volatility of other PostgreSQL
functions, like timezone().

Here is how the function can be used for continuous aggregates:

```
CREATE TABLE conditions(
  tstamp timestamptz NOT NULL,
  device VARCHAR(32) NOT NULL,
  temperature FLOAT NOT NULL);

SELECT create_hypertable(
  'conditions', 'tstamp',
  chunk_time_interval => INTERVAL '1 day'
);

INSERT INTO conditions
  SELECT tstamp, 'device-' || (random()*30)::INT, random()*80 - 40
  FROM generate_series(NOW() - INTERVAL '90 days', NOW(), '1 min') AS tstamp;

CREATE MATERIALIZED VIEW conditions_summary_monthly
WITH (timescaledb.continuous) AS
SELECT device,
       time_bucket_ng('5 month', tstamp, origin => ‘2000-01-01’) AS bucket,
       AVG(temperature),
       MAX(temperature),
       MIN(temperature)
FROM conditions
GROUP BY device, bucket;
```

When the `timezone` is provided all arguments are converted from whatever
timezone they are in, to the given timezone. E.g. if `Europe/Berlin` timezone
is specified, then daily buckets will start 00:00:00 in `Europe/Berlin`
timezone, not 00:00:00 in UTC. Same for months and years with respect to
provided (or default) `origin`.

This means that a single continuous aggregate will store data in a single
timezone. Users can create multiple continuous aggregates for each required
timezone.

## Iterative changes in the catalog schema

While reviewing [Monthly buckets support in CAGGs][monthly_pr] pull request
the team agreed on the following iterative changes in the catalog schema.

When user creates a CAGG with a variable bucket, the corresponding
`bucket_width` value in the `_timescaledb_catalog.continuous_agg` table
is -1 (declared as BUCKET_WIDTH_VARIABLE in continuous_agg.h). This indicates
that the bucket width is variable and the information about the bucketing
function is stored in the `_timescaledb_catalog.continuous_aggs_bucket_function`
table:

```
CREATE TABLE IF NOT EXISTS _timescaledb_catalog.continuous_aggs_bucket_function(
  mat_hypertable_id integer PRIMARY KEY
    REFERENCES _timescaledb_catalog.hypertable (id) ON DELETE CASCADE,

  -- The schema of the function.
  -- Equals TRUE for "timescaledb_experimental", FALSE otherwise.
  experimental bool NOT NULL,

  -- Name of the bucketing function
  name text NOT NULL,

  -- `bucket_width` argument of the function, e.g. "1 month"
  bucket_width text NOT NULL,

  -- `origin` argument of the function provided by the user
  origin text NOT NULL,

  -- `timezone` argument of the function provided by the user
  timezone text NOT NULL);
```

This is a **temporary schema** that will be used while the feature evolves.
It is flexible because of using TEXT fields, and also because basically, it
stores all the data provided by the user when creating the CAGG. On the flip
side, there are no constraints and/or type checks.

Sooner or later we are going to graduate time_bucket_ng() from the experimental
schema. We discovered at least three different ways to do it, all with their
pros and cons. The least risky one is to let time_bucket() and time_bucket_ng()
co-exist for several releases and let the users do the migration manually.
This is why there are `experimental` and `name` columns in
the `continuous_aggs_bucket_function` table:

* experimental = true, name = "time_bucket_ng" says that
  timescaledb_experimental.time_bucket_ng() is used as a bucketing function
  for the given CAGG;
* experimental = false, name = "time_bucket_ng" indicates
  public.time_bucket_ng();
* experimental = false, name = "time_bucket" indicates public.time_bucket();

These columns can be deleted after the graduation of time_bucket_ng(). There
will be only one possibility - public.time_bucket().

`bucket_with` will be converted to INTERVAL type, `origin` - to TIMESTAMP
and `timezone` - to TEXT with CHECK. We almost certainly will be able to use
the same algorithms for variable-sized and fixed-sized buckets,
thus `bucket_width` column in the `_timescaledb_catalog.continuous_agg` table
will be deleted. The table `continuous_aggs_bucket_function` will be deleted
and its `bucket_width`, `origin` and `timezone` columns will be transferred
to `continuous_agg`.

The signatures of:

* \_timescaledb_internal.invalidation_process_hypertable_log()
* \_timescaledb_internal.invalidation_process_cagg_log()


... will be changed accordingly. Instead of `bucket_functions TEXT[]` there
will be:

* bucket_width INTERVAL[]
* origin TIMESTAMP[]
* timezone TEXT[]

... arguments. Also, the deprecated `max_bucket_width` argument will be removed.

At least, that’s the plan. Since there are way too many unknowns at this point,
we prefer to use a more flexible and less constraining schema until the feature
stabilizes.

## Open Issues

* **Mixed-type buckets.** Should we support buckets like
  `1 month 8 days 13 hours`, i.e. fixed-sized + variable-sized? We will not
  support this in the first implementations, but we wonder if there is any
  reasonable way to implement it.
* **Going backward in the past from `origin`** doesn't seem to make much sense
  in the general case. We could partially support this at some point, but this
  is not a priority.

[survey]: https://github.com/timescale/timescaledb/discussions/3258#discussioncomment-767842
[docs]: https://docs.timescale.com/timescaledb/latest/overview/core-concepts/continuous-aggregates/
[dst]: https://en.wikipedia.org/wiki/Daylight_saving_time
[workaround]: https://github.com/timescale/timescaledb/issues/414#issuecomment-828451914
[date_trunc]: https://www.postgresql.org/docs/current/functions-datetime.html#FUNCTIONS-DATETIME-TRUNC
[date_bin]: https://www.postgresql.org/docs/14/functions-datetime.html#FUNCTIONS-DATETIME-BIN
[volatility]: https://www.postgresql.org/docs/current/xfunc-volatility.html
[consistent]: https://www.postgresql.org/message-id/flat/CAJ7c6TOMG8zSNEZtCn5SPe+cCk3Lfxb71ZaQwT2F4T7PJ_t=KA@mail.gmail.com
[monthly_pr]: https://github.com/timescale/timescaledb/pull/3753
