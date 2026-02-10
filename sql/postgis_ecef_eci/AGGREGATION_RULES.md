# Frame Conversion & Aggregation Rules

## The Core Problem

ECEF <-> ECI conversion is a **time-dependent rotation**. The rotation matrix `R(t)`
changes continuously as the Earth rotates (~15 degrees/hour).

This means frame conversion **does not commute with aggregation**:

```
avg(R(t_i) * pos_i)  !=  R(t_avg) * avg(pos_i)
```

The left side rotates each position by its own epoch's rotation, then averages.
The right side averages positions first, then rotates by a single epoch.
They differ because `R(t)` varies across the averaging window.

## Rules

### SAFE: Aggregate in Storage Frame

If data is stored in ECEF, aggregate ECEF coordinates directly:

```sql
-- SAFE: averaging ECEF coordinates
SELECT time_bucket('1 hour', time) AS bucket, object_id,
       avg(x) AS avg_x, avg(y) AS avg_y, avg(z) AS avg_z
FROM trajectories
GROUP BY bucket, object_id;
```

The result is a valid average position in ECEF. It can be used in continuous aggregates.

### SAFE: Convert Aggregated Result at Midpoint Epoch

After aggregating in ECEF, convert the result to ECI using the bucket midpoint:

```sql
-- SAFE: convert the aggregated ECEF position to ECI at midpoint
SELECT bucket, object_id,
       ecef_eci.stub_ecef_to_eci(avg_x, avg_y, avg_z, bucket + INTERVAL '30 min') AS eci
FROM traj_hourly;
```

**Error bound**: For a 1-hour bucket and LEO (7.7 km/s orbital velocity):
- Earth rotation during bucket: ~15 degrees
- Position error from midpoint approximation: < R * sin(omega * bucket_width/2)
- For R=6771 km, omega=7.29e-5 rad/s, T=3600s: ~1750 km maximum
- Typically much less for objects that don't move far in the bucket window

This error is acceptable for visualization and screening queries. For precision
work, query raw data and convert per-row.

### UNSAFE: Convert Per-Row Then Aggregate

```sql
-- UNSAFE: DO NOT use in continuous aggregates
SELECT time_bucket('1 hour', time) AS bucket, object_id,
       avg(ecef_eci.stub_ecef_to_eci(x, y, z, time).eci_x) AS avg_eci_x
FROM trajectories
GROUP BY bucket, object_id;
```

This is mathematically incorrect because:
1. Each row is rotated by a different angle
2. Averaging the rotated coordinates mixes positions from different inertial orientations
3. The result is not a physically meaningful ECI position

Additionally, the STABLE volatility of frame conversion functions means they
**cannot** be used inside continuous aggregate definitions (which require IMMUTABLE).

### SAFE: Store in Target Frame

If queries are predominantly in ECI, store positions in ECI at ingest time:

```sql
-- Ingest pipeline converts to ECI before INSERT
INSERT INTO trajectories_eci (time, object_id, x, y, z, ...)
SELECT time, object_id,
       (ecef_eci.stub_ecef_to_eci(x, y, z, time)).eci_x,
       (ecef_eci.stub_ecef_to_eci(x, y, z, time)).eci_y,
       (ecef_eci.stub_ecef_to_eci(x, y, z, time)).eci_z,
       ...
FROM staging_table;
```

Then aggregate directly in ECI. This avoids the conversion-aggregation problem entirely.

## Summary Table

| Pattern | Safe? | Continuous Aggregate? | Notes |
|---------|-------|-----------------------|-------|
| `avg(ecef_x), avg(ecef_y), avg(ecef_z)` | Yes | Yes | Aggregate in storage frame |
| Convert aggregate to ECI at midpoint | Yes | No (do in query) | Approximate, bounded error |
| `avg(to_eci(x,y,z,t))` | **No** | No (STABLE function) | Physically meaningless result |
| Store ECI, `avg(eci_x), avg(eci_y), avg(eci_z)` | Yes | Yes | Requires ingest-time conversion |
| Per-row conversion in SELECT | Yes | No (do in query) | Exact, but slower |

## Recommendations

1. **Default**: Store ECEF, aggregate ECEF, convert at query time when needed
2. **High-frequency ECI queries**: Store dual (ECEF + ECI), aggregate each independently
3. **Precision applications**: Never aggregate across frame conversions — query raw data
4. **Continuous aggregates**: Always aggregate in the storage frame; do NOT include
   frame conversion functions in the cagg definition
