WITH ag AS(
		SELECT origin, dest,
		COUNT(*) AS total_flights,
		COUNT(DISTINCT tail_number) AS unique_airplanes,
		COUNT(DISTINCT airline) AS unique_airline,
		DATE_TRUNC('second', AVG(actual_elapsed_time_interval)) AS avg_elapsed_time,
		DATE_TRUNC('second', AVG(arr_delay_interval)) AS avg_arr_delay,
		MAX(arr_delay_interval) AS max_delay,
		MIN(arr_delay_interval) AS min_delay,
		SUM(cancelled) AS flights_cancelled,
		SUM(diverted) AS flights_diverted
		FROM {{ref('prep_flights')}} pf
		GROUP BY origin, dest
		)
SELECT ag.origin, 
		pa1.name AS dep_airport_name, 
		pa1.city AS dep_airport_city, 
		pa1.country AS dep_airport_country, 
		ag. dest,
		pa2.name AS dest_airport_name, 
		pa2.city AS dest_airport_city, 
		pa2.country AS dest_airport_country,
		ag.unique_airplanes,
		ag.unique_airline,
		ag.avg_elapsed_time,
		ag.avg_arr_delay,
		ag.max_delay,
		ag.min_delay,
		ag.flights_cancelled,
		ag.flights_diverted
FROM ag
JOIN {{ref('prep_airports')}} pa1 ON ag.origin = pa1.faa
JOIN {{ref('prep_airports')}} pa2 ON ag.dest = pa2.faa