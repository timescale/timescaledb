-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

ALTER TABLE "two_Partitions"
  DROP CONSTRAINT IF EXISTS two_Partitions_device_id_2_fkey;

DROP TABLE IF EXISTS devices;
