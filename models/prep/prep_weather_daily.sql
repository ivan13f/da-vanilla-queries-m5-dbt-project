WITH daily_data AS (
    SELECT * 
    FROM {{ref('staging_weather_daily')}}
),
add_features AS (
    SELECT *
		, DATE_PART('day', date) AS date_day 		-- number of the day of month
		, DATE_PART('month', date) AS date_month 	-- number of the month of year
		, DATE_PART('year', date) AS date_year 		-- number of year
		, DATE_PART('week', date) AS cw 			-- number of the week of year
		, TO_CHAR (date, 'Month') AS month_name 	-- name of the month
		, TO_CHAR (date, 'Day') AS weekday 		-- name of the weekday
    FROM daily_data 
),
add_more_features AS (
    SELECT *
		, (CASE 
			WHEN date_month in (12, 1, 2) THEN 'Winter'
			WHEN date_month in (3, 4, 5) THEN 'Spring'
            WHEN date_month in (6, 7, 8) THEN 'Summer'
            WHEN date_month in (9, 10, 11) THEN 'Autumn'
		END) AS season
    FROM add_features
)
SELECT *
FROM add_more_features
ORDER BY date