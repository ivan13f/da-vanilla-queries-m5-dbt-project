WITH departures AS (
				SELECT origin AS airport_code,
							COUNT(DISTINCT dest) AS unique_to,
							COUNT(SCHED_DEP_TIME) AS dep_planned,
							SUM (cancelled) AS dep_cancelled,
							SUM (diverted) AS dep_diverted,
							COUNT(ARR_TIME) AS dep_n_diverted
				FROM {{ref('prep_flights')}}
				GROUP BY origin
	),
	arrivals AS (
				SELECT dest AS airport_code,
						COUNT(DISTINCT origin) AS unique_from,
						COUNT(SCHED_ARR_TIME ) AS arr_planned,
						SUM (cancelled) AS arr_cancelled,
						SUM (diverted) AS arr_diverted,
						COUNT(ARR_TIME) AS arr_n_diverted
				FROM {{ref('prep_flights')}}
				GROUP BY dest
	),
	total_stats AS (
				SELECT d.airport_code, unique_to, unique_from, 
						(dep_planned + arr_planned) AS total_planned,
						(dep_cancelled + arr_cancelled) AS total_cancelled,
						(dep_diverted + arr_n_diverted) AS total_diverted
				FROM departures d
				JOIN arrivals a
				USING (airport_code)
	)
SELECT pa.name, pa.city, pa.country, ts.*
FROM total_stats ts
JOIN {{ref('prep_airports')}} pa
ON ts.airport_code = pa.faa