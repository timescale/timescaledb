# PostgreSQL Timestamp Precision for Orbital Applications

## PostgreSQL TIMESTAMPTZ Internals

PostgreSQL stores `TIMESTAMPTZ` as a 64-bit signed integer representing
**microseconds since 2000-01-01 00:00:00 UTC**.

| Property | Value |
|----------|-------|
| Storage | 8 bytes (INT64) |
| Resolution | 1 microsecond |
| Range | 4713 BC to 294276 AD |
| Epoch | 2000-01-01 00:00:00 UTC |
| Leap seconds | **Not encoded** — UTC without leap second awareness |

## Position Error from Timestamp Resolution

The question: if the timestamp is wrong by 1 microsecond, how far off is
the computed position?

### Error = velocity * time_error

| Regime | Typical Velocity | Position Error per 1 us |
|--------|-----------------|------------------------|
| LEO (400 km) | 7,660 m/s | **7.7 mm** |
| MEO/GPS (20,200 km) | 3,870 m/s | **3.9 mm** |
| GEO (35,786 km) | 3,075 m/s | **3.1 mm** |
| Lunar distance | 1,020 m/s | **1.0 mm** |
| Earth rotation (equator) | 465 m/s | **0.5 mm** |

**Conclusion**: 1-microsecond resolution introduces sub-centimeter errors
even for LEO. This is well below the accuracy of most tracking systems.

## Leap Second Impact

PostgreSQL does not encode leap seconds. When a leap second occurs (e.g.,
`2016-12-31 23:59:60 UTC`), PostgreSQL either:
- Skips it (no `23:59:60` representable)
- Or the OS smears it (common with NTP/chrony)

### Impact on ECEF<->ECI Conversion

The ECEF<->ECI rotation angle depends on UT1 (true Earth rotation), not UTC.
The difference UT1-UTC (called dUT1) is tracked in EOP data and is always < 0.9s.

| Concern | Impact | Mitigation |
|---------|--------|------------|
| Leap second during observation | ~465 m position error at equator if not accounted for | Use dUT1 from EOP data |
| Accumulated leap seconds since epoch | Handled by UTC definition | No action needed |
| UT1-UTC interpolation error | < 0.1 ms typical | Daily EOP updates sufficient |

### Practical Guidance

For **sub-meter orbital accuracy** (most SSA/STM applications):
- PostgreSQL microsecond timestamps are more than adequate
- Use EOP dUT1 corrections for ECEF<->ECI conversion
- No leap second workaround needed

For **millimeter-level precision** (geodesy, VLBI, GNSS carrier phase):
- Store an auxiliary `time_correction_s FLOAT8` column for sub-microsecond offsets
- Or use a separate `tai_epoch FLOAT8` column storing TAI seconds since J2000
- The PostGIS fork's frame conversion should accept either representation

## Time System Comparison

| Time System | Continuity | Relationship to UTC | Use Case |
|-------------|-----------|---------------------|----------|
| **UTC** | Discontinuous (leap seconds) | Definition | Civil time, PostgreSQL native |
| **UT1** | Continuous (Earth rotation) | UT1 = UTC + dUT1 | Frame conversion |
| **TAI** | Continuous (atomic) | TAI = UTC + leap_seconds | Precision timing |
| **GPS** | Continuous (atomic) | GPS = TAI - 19s | GPS receivers |
| **TDB** | Continuous (relativistic) | TDB ≈ TT + periodic terms | Deep-space ephemerides |
| **TT** | Continuous (atomic) | TT = TAI + 32.184s | Astronomy standard |

For TimescaleDB hypertable partitioning, the time column must be UTC
(or an integer proxy). The partitioning only needs to find the right
chunk — microsecond precision is irrelevant for chunk boundaries
(typically 1-hour intervals).

## Recommendations

1. **Use TIMESTAMPTZ for the hypertable time column** — it's the partitioning
   key, chunk boundaries are 1-hour intervals, microseconds are overkill.

2. **For ECEF<->ECI conversion**, pass the same TIMESTAMPTZ to the conversion
   function. The PostGIS fork should internally apply dUT1 from EOP data
   to get UT1 for the Earth rotation angle.

3. **For sub-microsecond needs** (rare), add a `FLOAT8` column for fractional
   seconds offset. This does not affect TimescaleDB partitioning.

4. **Store the time system** in metadata (the `frame` column or a separate
   `time_system SMALLINT` column) so consumers know whether timestamps
   are UTC, GPS, or TAI.

5. **Leap second table**: The PostGIS fork or the EOP module should maintain
   a leap second table for UTC<->TAI conversion:
   ```sql
   CREATE TABLE ecef_eci.leap_seconds (
       effective_date DATE PRIMARY KEY,
       tai_utc_offset FLOAT8 NOT NULL  -- TAI - UTC in seconds
   );
   ```
   Current value (as of 2025): TAI - UTC = 37 seconds.
