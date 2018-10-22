-- Copyright (c) 2016-2018  Timescale, Inc. All Rights Reserved.
--
-- This file is licensed under the Apache License,
-- see LICENSE-APACHE at the top level directory.

--make sure diff only has explain output not result output
\! diff ${TEST_OUTPUT_DIR}/results/append_unoptimized.out ${TEST_OUTPUT_DIR}/results/append.out
