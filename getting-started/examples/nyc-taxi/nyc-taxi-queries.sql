-- Sample analytical queries for NYC Taxi dataset
-- These showcase TimescaleDB's columnstore performance

\echo '=== Sample Queries for NYC Taxi Data ==='
\echo ''

-- Query 1: Total trips and revenue
\echo 'Query 1: Overall statistics'
SELECT
    COUNT(*) as total_trips,
    SUM(fare_amount) as total_revenue,
    AVG(fare_amount) as avg_fare,
    AVG(trip_distance) as avg_distance
FROM trips;

-- Query 2: Trips by cab type
\echo ''
\echo 'Query 2: Breakdown by cab type'
SELECT
    cab_type,
    COUNT(*) as trips,
    AVG(fare_amount) as avg_fare,
    AVG(tip_amount) as avg_tip,
    AVG(passenger_count) as avg_passengers
FROM trips
GROUP BY cab_type
ORDER BY trips DESC;

-- Query 3: Hourly patterns using time_bucket
\echo ''
\echo 'Query 3: Hourly trip patterns (using time_bucket)'
SELECT
    time_bucket('1 hour', pickup_datetime) AS hour,
    COUNT(*) as trips,
    AVG(fare_amount) as avg_fare,
    SUM(tip_amount) as total_tips
FROM trips
GROUP BY hour
ORDER BY hour DESC
LIMIT 24;

-- Query 4: Top pickup locations by revenue
\echo ''
\echo 'Query 4: Top pickup locations by revenue'
SELECT
    pickup_location_id,
    COUNT(*) as trip_count,
    SUM(fare_amount) as total_revenue,
    AVG(trip_distance) as avg_distance,
    AVG(tip_amount) as avg_tip
FROM trips
GROUP BY pickup_location_id
ORDER BY total_revenue DESC
LIMIT 10;

-- Query 5: Daily aggregation with time_bucket
\echo ''
\echo 'Query 5: Daily statistics by cab type'
SELECT
    time_bucket('1 day', pickup_datetime) AS day,
    cab_type,
    COUNT(*) as trips,
    AVG(fare_amount) as avg_fare,
    MAX(fare_amount) as max_fare
FROM trips
GROUP BY day, cab_type
ORDER BY day DESC, cab_type
LIMIT 20;

-- Query 6: Distance-based analysis
\echo ''
\echo 'Query 6: Trips by distance category'
SELECT
    CASE
        WHEN trip_distance < 1 THEN 'Short (< 1 mile)'
        WHEN trip_distance < 5 THEN 'Medium (1-5 miles)'
        WHEN trip_distance < 10 THEN 'Long (5-10 miles)'
        ELSE 'Very Long (> 10 miles)'
    END as distance_category,
    COUNT(*) as trips,
    AVG(fare_amount) as avg_fare,
    AVG(tip_amount) as avg_tip
FROM trips
GROUP BY distance_category
ORDER BY trips DESC;

\echo ''
\echo '=== Query examples complete! ==='
\echo 'Notice how fast these analytical queries run on compressed columnar data.'
