-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

--make sure diff only has explain output not result output
\! diff ${TEST_OUTPUT_DIR}/results/append_unoptimized.out ${TEST_OUTPUT_DIR}/results/append.out
