-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- This file is meant to contain operator classes that need to be created only
-- once and not recreated during updates.
-- There is no CREATE OR REPLACE OPERATOR which means that the only way to replace
-- an operator is to DROP then CREATE which is problematic as it will fail
-- if the previous version of the operator has dependencies.
-- NOTE that WHEN CREATING NEW FUNCTIONS HERE you should also make sure they are
-- created in an update script so that both new users and people updating from a
-- previous version get the new function

-- T-Digest Operator Class
-- Implement complete operator class to support use of DISTINCT and ORDER BY on T-Digest columns
-- Metric for T-Digest comparison is total number of data points added to the T-Digest

CREATE OPERATOR < (
    LEFTARG = @extschema@.tdigest,
    RIGHTARG = @extschema@.tdigest,
    PROCEDURE = _timescaledb_internal.tdigest_lt,
    commutator = >,
    NEGATOR = >=
);

CREATE OPERATOR <= (
    leftarg = @extschema@.tdigest,
    rightarg = @extschema@.tdigest,
    procedure = _timescaledb_internal.tdigest_le,
    commutator = >=,
    NEGATOR = >
);

CREATE OPERATOR = (
    leftarg = @extschema@.tdigest,
    rightarg = @extschema@.tdigest,
    procedure = _timescaledb_internal.tdigest_equal,
    commutator = =,
    negator = !=
);

CREATE OPERATOR >= (
    LEFTARG = @extschema@.tdigest,
    RIGHTARG = @extschema@.tdigest,
    PROCEDURE = _timescaledb_internal.tdigest_ge,
    commutator = <=,
    NEGATOR = < 
);

CREATE OPERATOR > (
    LEFTARG = @extschema@.tdigest,
    RIGHTARG = @extschema@.tdigest,
    PROCEDURE = _timescaledb_internal.tdigest_gt,
    commutator = <,
    NEGATOR = <= 
);

CREATE OPERATOR CLASS @extschema@.tdigest_ops
    DEFAULT FOR TYPE @extschema@.tdigest USING btree AS
        OPERATOR 1 <,
        OPERATOR 2 <=,
        OPERATOR 3 =,
        OPERATOR 4 >=,
        OPERATOR 5 >,
        FUNCTION 1 _timescaledb_internal.tdigest_cmp(tdigest, tdigest);
