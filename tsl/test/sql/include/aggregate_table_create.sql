-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- This file creates a table with a lot of different types to allow a range of aggregate functions.
-- This does not include the creation of a corresponding hypertable, as we may want to vary how that is done.

CREATE TYPE custom_type AS (high int, low int);

CREATE TABLE :TEST_TABLE (
      timec       TIMESTAMPTZ       NOT NULL,
      location    TEXT              NOT NULL,
      region      TEXT              NOT NULL,
      temperature DOUBLE PRECISION  NULL,
      humidity    DOUBLE PRECISION  NULL,
      lowp        double precision NULL,
      highp       double precision null,
      allnull     double precision null,
      highlow     custom_type null,
      bit_int     smallint,
      good_life   boolean
    );
