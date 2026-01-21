-- peak_hours
SELECT
    pickup_hour,
    COUNT(*) AS total_trips
FROM trips
GROUP BY pickup_hour
ORDER BY pickup_hour;

-- revenue_by_weekday
SELECT
    pickup_day,
    ROUND(SUM(total_amount), 2) AS total_revenue
FROM trips
GROUP BY pickup_day
ORDER BY total_revenue DESC;

-- top_revenue_days
SELECT
    DATE(tpep_pickup_datetime) AS trip_date,
    ROUND(SUM(total_amount), 2) AS daily_revenue
FROM trips
GROUP BY trip_date
ORDER BY daily_revenue DESC
LIMIT 10;

-- avg_fare_by_weekday
SELECT
    pickup_day,
    ROUND(AVG(fare_amount), 2) AS avg_fare
FROM trips
GROUP BY pickup_day
ORDER BY avg_fare DESC;

-- monthly_revenue_growth
SELECT
    pickup_month,
    ROUND(SUM(total_amount), 2) AS monthly_revenue,
    ROUND(
        SUM(total_amount)
        - LAG(SUM(total_amount)) OVER (ORDER BY pickup_month),
        2
    ) AS revenue_change
FROM trips
GROUP BY pickup_month
ORDER BY pickup_month;
