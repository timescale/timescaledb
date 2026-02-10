-- Unit Tests: Partitioning functions — comprehensive boundary coverage
-- Tests every CASE branch in altitude_band_bucket() and octree_bucket()

\set ON_ERROR_STOP 1

\ir include/setup.sql

-- =============================================================================
-- altitude_band_bucket: All 16 bucket boundaries
-- =============================================================================
-- Each test checks a point firmly INSIDE the band and one AT the boundary.
-- Boundary points fall into the NEXT higher band (ranges are [lower, upper)).

-- Earth radius = 6371 km = 6371000 m

-- Bucket 15: sub-orbital (altitude < 0 km)
SELECT ecef_eci.altitude_band_bucket(6000000.0, 0.0, 0.0) AS b15_inside;  -- ~371 km below surface
SELECT ecef_eci.altitude_band_bucket(6370999.0, 0.0, 0.0) AS b15_near_surface;  -- just below

-- Bucket 0: LEO 0-500 km (radius 6371-6871 km)
SELECT ecef_eci.altitude_band_bucket(6371000.0, 0.0, 0.0) AS b0_at_surface;  -- exact 0 km alt
SELECT ecef_eci.altitude_band_bucket(6600000.0, 0.0, 0.0) AS b0_mid;  -- ~229 km
SELECT ecef_eci.altitude_band_bucket(6870999.0, 0.0, 0.0) AS b0_top;  -- just under 500 km

-- Bucket 1: LEO 500-1000 km (radius 6871-7371 km)
SELECT ecef_eci.altitude_band_bucket(6871000.0, 0.0, 0.0) AS b1_boundary;  -- exact 500 km
SELECT ecef_eci.altitude_band_bucket(7100000.0, 0.0, 0.0) AS b1_mid;  -- ~729 km

-- Bucket 2: LEO 1000-1500 km
SELECT ecef_eci.altitude_band_bucket(7371000.0, 0.0, 0.0) AS b2_boundary;  -- exact 1000 km
SELECT ecef_eci.altitude_band_bucket(7571000.0, 0.0, 0.0) AS b2_mid;  -- ~1200 km

-- Bucket 3: LEO 1500-2000 km
SELECT ecef_eci.altitude_band_bucket(7871000.0, 0.0, 0.0) AS b3_boundary;  -- exact 1500 km
SELECT ecef_eci.altitude_band_bucket(8100000.0, 0.0, 0.0) AS b3_mid;  -- ~1729 km

-- Bucket 4: MEO 2000-10000 km
SELECT ecef_eci.altitude_band_bucket(8371000.0, 0.0, 0.0) AS b4_boundary;  -- exact 2000 km
SELECT ecef_eci.altitude_band_bucket(10000000.0, 0.0, 0.0) AS b4_mid;  -- ~3629 km

-- Bucket 5: MEO 10000-20000 km
SELECT ecef_eci.altitude_band_bucket(16371000.0, 0.0, 0.0) AS b5_boundary;  -- exact 10000 km
SELECT ecef_eci.altitude_band_bucket(20000000.0, 0.0, 0.0) AS b5_mid;

-- Bucket 6: MEO 20000-30000 km
SELECT ecef_eci.altitude_band_bucket(26371000.0, 0.0, 0.0) AS b6_boundary;  -- exact 20000 km
SELECT ecef_eci.altitude_band_bucket(30000000.0, 0.0, 0.0) AS b6_mid;

-- Bucket 7: MEO 30000-35786 km
SELECT ecef_eci.altitude_band_bucket(36371000.0, 0.0, 0.0) AS b7_boundary;  -- exact 30000 km
SELECT ecef_eci.altitude_band_bucket(40000000.0, 0.0, 0.0) AS b7_mid;

-- Buckets 8-11: GEO belt 35786-35986 km, quadrants by atan2(y,x)
-- GEO altitude ~35786 km → radius ~42157 km
-- Q1: atan2 in [0, pi/2) — positive x, positive y
SELECT ecef_eci.altitude_band_bucket(42157000.0, 1.0, 0.0) AS b8_geo_q1;  -- +X, +Y
-- Q2: atan2 in [pi/2, pi] — negative x, positive y
SELECT ecef_eci.altitude_band_bucket(-1.0, 42157000.0, 0.0) AS b9_geo_q2;  -- -X, +Y
-- Q3: atan2 in [-pi, -pi/2) — negative x, negative y
SELECT ecef_eci.altitude_band_bucket(-42157000.0, -1.0, 0.0) AS b10_geo_q3;  -- -X, -Y
-- Q4: atan2 in [-pi/2, 0) — positive x, negative y
SELECT ecef_eci.altitude_band_bucket(1.0, -42157000.0, 0.0) AS b11_geo_q4;  -- +X, -Y

-- Bucket 12: HEO 35986-50000 km
SELECT ecef_eci.altitude_band_bucket(42357000.0, 0.0, 0.0) AS b12_boundary;  -- ~35986 km
SELECT ecef_eci.altitude_band_bucket(48000000.0, 0.0, 0.0) AS b12_mid;

-- Bucket 13: HEO 50000-100000 km
SELECT ecef_eci.altitude_band_bucket(56371000.0, 0.0, 0.0) AS b13_boundary;  -- exact 50000 km
SELECT ecef_eci.altitude_band_bucket(80000000.0, 0.0, 0.0) AS b13_mid;

-- Bucket 14: HEO >100000 km
SELECT ecef_eci.altitude_band_bucket(106371000.0, 0.0, 0.0) AS b14_boundary;  -- exact 100000 km
SELECT ecef_eci.altitude_band_bucket(200000000.0, 0.0, 0.0) AS b14_far;

-- =============================================================================
-- altitude_band_bucket: Off-axis points (verify sqrt works correctly)
-- =============================================================================

-- ISS at 400km but off-axis: radius = sqrt(x^2+y^2+z^2) should equal ~6771 km
-- Using equal components: x=y=z=6771000/sqrt(3) ≈ 3909241 m
SELECT ecef_eci.altitude_band_bucket(3909241.0, 3909241.0, 3909241.0) AS offaxis_iss;

-- =============================================================================
-- altitude_band_bucket: Edge cases
-- =============================================================================

-- Zero vector (inside Earth)
SELECT ecef_eci.altitude_band_bucket(0.0, 0.0, 0.0) AS zero_vector;

-- Very small positive (still inside Earth)
SELECT ecef_eci.altitude_band_bucket(1.0, 0.0, 0.0) AS tiny_vector;

-- Negative coordinates (valid ECEF — just different hemisphere)
SELECT ecef_eci.altitude_band_bucket(-6771000.0, 0.0, 0.0) AS negative_x;
SELECT ecef_eci.altitude_band_bucket(0.0, -6771000.0, 0.0) AS negative_y;
SELECT ecef_eci.altitude_band_bucket(0.0, 0.0, -6771000.0) AS negative_z;
SELECT ecef_eci.altitude_band_bucket(-4000000.0, -4000000.0, -4000000.0) AS all_negative;

-- =============================================================================
-- octree_bucket: All 8 L1 octants
-- =============================================================================

-- Octant determination: bit0=X>=0, bit1=Y>=0, bit2=Z>=0
-- Points at radius 6771 km, one per octant
SELECT ecef_eci.octree_bucket( 6771000.0,  6771000.0,  6771000.0) AS oct_ppp;  -- octant 7 (1+2+4)
SELECT ecef_eci.octree_bucket( 6771000.0,  6771000.0, -6771000.0) AS oct_ppn;  -- octant 3 (1+2+0)
SELECT ecef_eci.octree_bucket( 6771000.0, -6771000.0,  6771000.0) AS oct_pnp;  -- octant 5 (1+0+4)
SELECT ecef_eci.octree_bucket( 6771000.0, -6771000.0, -6771000.0) AS oct_pnn;  -- octant 1 (1+0+0)
SELECT ecef_eci.octree_bucket(-6771000.0,  6771000.0,  6771000.0) AS oct_npp;  -- octant 6 (0+2+4)
SELECT ecef_eci.octree_bucket(-6771000.0,  6771000.0, -6771000.0) AS oct_npn;  -- octant 2 (0+2+0)
SELECT ecef_eci.octree_bucket(-6771000.0, -6771000.0,  6771000.0) AS oct_nnp;  -- octant 4 (0+0+4)
SELECT ecef_eci.octree_bucket(-6771000.0, -6771000.0, -6771000.0) AS oct_nnn;  -- octant 0 (0+0+0)

-- =============================================================================
-- octree_bucket: Boundary points (on axes)
-- =============================================================================

-- Origin — should be octant 0 at L1 (all >= 0 is true for 0.0)
SELECT ecef_eci.octree_bucket(0.0, 0.0, 0.0) AS oct_origin;

-- On X axis only
SELECT ecef_eci.octree_bucket(6771000.0, 0.0, 0.0) AS oct_x_pos;
SELECT ecef_eci.octree_bucket(-6771000.0, 0.0, 0.0) AS oct_x_neg;

-- =============================================================================
-- octree_bucket: Different bucket counts
-- =============================================================================

SELECT ecef_eci.octree_bucket(6771000.0, 0.0, 0.0, 8) AS oct_8buckets;
SELECT ecef_eci.octree_bucket(6771000.0, 0.0, 0.0, 16) AS oct_16buckets;
SELECT ecef_eci.octree_bucket(6771000.0, 0.0, 0.0, 32) AS oct_32buckets;

-- =============================================================================
-- ecef_altitude_km: Precision checks
-- =============================================================================

-- Exact surface
SELECT round(ecef_eci.ecef_altitude_km(6371000.0, 0.0, 0.0)::numeric, 6) AS alt_surface;

-- Zero vector
SELECT round(ecef_eci.ecef_altitude_km(0.0, 0.0, 0.0)::numeric, 1) AS alt_zero;

-- Known altitude: ISS ~400 km
SELECT round(ecef_eci.ecef_altitude_km(6771000.0, 0.0, 0.0)::numeric, 1) AS alt_iss;

-- Cleanup
DROP SCHEMA ecef_eci CASCADE;
