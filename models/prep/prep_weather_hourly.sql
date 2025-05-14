WITH hourly_data AS (
    SELECT * 
    FROM {{ref('staging_weather_hourly')}}
),
add_features AS (
    SELECT *
        , timestamp::DATE AS date
        , timestamp::TIME AS time
        , TO_CHAR(timestamp,'HH24:MI') as hour
        , TO_CHAR(timestamp, 'FMmonth') AS month_name
        , TO_CHAR(timestamp, 'FMDay') AS weekday        
        , DATE_PART('day', timestamp) AS date_day
        , DATE_PART('month', timestamp) AS date_month
        , DATE_PART('year', timestamp) AS date_year
        , DATE_PART('week', timestamp) AS cw
    FROM hourly_data
),
add_more_features AS (
    SELECT *
        , CASE 
            WHEN time >= '22:01:00' OR time <= '06:00:00' THEN 'night'
            WHEN time > '06:00:00' AND time <= '18:00:00' THEN 'day'
            WHEN time > '18:00:00' AND time <= '22:00:00' THEN 'evening'
        END AS day_part
    FROM add_features
)
SELECT amf.*, wc.weather_condition
FROM add_more_features as amf
JOIN {{ref('weather_codes')}} as wc
ON wc.code = amf.condition_code