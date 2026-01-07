USE mobility_analytics;

SELECT pickup_hour, COUNT(*) AS total_trips
FROM taxi_trips
GROUP BY pickup_hour
ORDER BY total_trips DESC;

SELECT pickup_hour, SUM(total_amount) AS total_revenue
FROM taxi_trips
GROUP BY pickup_hour
ORDER BY total_revenue DESC;

SELECT DATE(tpep_pickup_datetime) AS trip_date,
       SUM(total_amount) AS daily_revenue
FROM taxi_trips
GROUP BY trip_date
ORDER BY daily_revenue DESC
LIMIT 10;

SELECT pickup_day,
       AVG(fare_amount) AS avg_fare
FROM taxi_trips
GROUP BY pickup_day
ORDER BY avg_fare DESC;

SELECT pickup_month,
       SUM(total_amount) AS monthly_revenue,
       LAG(SUM(total_amount)) OVER (ORDER BY pickup_month) AS prev_month,
       ROUND(
           (SUM(total_amount) - LAG(SUM(total_amount)) OVER (ORDER BY pickup_month))
           / LAG(SUM(total_amount)) OVER (ORDER BY pickup_month) * 100,
           2
       ) AS growth_pct
FROM taxi_trips
GROUP BY pickup_month;

SELECT 
    ROUND(pickup_latitude, 3) AS zone_lat, 
    ROUND(pickup_longitude, 3) AS zone_long, 
    SUM(total_amount) AS zone_revenue,
    COUNT(*) AS trip_count
FROM taxi_trips
GROUP BY zone_lat, zone_long
ORDER BY zone_revenue DESC
LIMIT 10;
