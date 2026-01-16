-- ClickHouse NYC Taxi Example Queries
-- These queries mirror the TimescaleDB queries for comparison

-- Query 1: Overall statistics
SELECT
    COUNT(*) as total_trips,
    SUM(fare_amount) as total_revenue,
    AVG(fare_amount) as avg_fare,
    AVG(trip_distance) as avg_distance
FROM trips;

-- Query 2: Trips by vendor
SELECT
    vendor_id,
    COUNT(*) as trips,
    AVG(fare_amount) as avg_fare,
    AVG(tip_amount) as avg_tip,
    AVG(passenger_count) as avg_passengers
FROM trips
GROUP BY vendor_id
ORDER BY trips DESC;

-- Query 3: Hourly patterns using toStartOfHour
SELECT
    toStartOfHour(pickup_datetime) AS hour,
    COUNT(*) as trips,
    AVG(fare_amount) as avg_fare,
    SUM(tip_amount) as total_tips
FROM trips
GROUP BY hour
ORDER BY hour DESC
LIMIT 24;

-- Query 4: Payment type analysis
SELECT
    payment_type,
    COUNT(*) as trip_count,
    SUM(fare_amount) as total_revenue,
    AVG(trip_distance) as avg_distance,
    AVG(tip_amount) as avg_tip
FROM trips
GROUP BY payment_type
ORDER BY total_revenue DESC;

-- Query 5: Daily aggregation by borough
SELECT
    toDate(pickup_datetime) AS day,
    pickup_boroname,
    COUNT(*) as trips,
    AVG(fare_amount) as avg_fare,
    MAX(fare_amount) as max_fare
FROM trips
GROUP BY day, pickup_boroname
ORDER BY day DESC, pickup_boroname
LIMIT 20;

-- Query 6: Distance-based analysis
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
