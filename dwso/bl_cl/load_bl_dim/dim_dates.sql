-- DROP PROCEDURE IF EXISTS bl_cl.dim_dates_insertion();
CREATE OR REPLACE PROCEDURE bl_cl.dim_dates_insertion()

LANGUAGE plpgsql 
AS $$

DECLARE
  schema_name   TEXT := 'bl_dim';
  table_name    TEXT := 'dim_dates';
  command_name  TEXT := 'insert';
  rows_affected INTEGER := 0;
  error_type    TEXT := '';
  output_message TEXT := '';

BEGIN 
	
INSERT INTO bl_dim.dim_dates 
	(date_id,
	 full_date,
	 day_name,
	 day_of_week,
	 day_of_month,
	 day_of_quarter,
	 day_of_year,
	 week_of_month,
	 week_of_year,
	 month_actual,
	 month_name,
	 month_name_abbreviated,
	 quarter_actual,
	 quarter_name,
	 year_actual,
	 first_day_of_week,
	 last_day_of_week,
	 first_day_of_month,
	 last_day_of_month,
	 first_day_of_quarter,
	 last_day_of_quarter,
	 first_day_of_year,
	 last_day_of_year,
	 mmyyyy,
	 mmddyyyy,
	 weekend_indr
	)
SELECT
	TO_CHAR(datum, 'yyyymmdd')::INT AS date_id,
	datum AS full_date,
	TO_CHAR(datum, 'Day') AS day_name,
	EXTRACT(isodow FROM datum) AS day_of_week,
	EXTRACT(day FROM datum) AS day_of_month,
	datum - DATE_TRUNC('quarter', datum)::DATE + 1 AS day_of_quarter,
	EXTRACT(doy FROM datum) AS day_of_year,
	TO_CHAR(datum, 'w')::int AS week_of_month,
	EXTRACT(week FROM datum) AS week_of_year,
	EXTRACT(MONTH FROM datum) AS month_actual,
	TO_CHAR(datum, 'tmmonth') AS month_name,
	TO_CHAR(datum, 'mon') AS month_name_abbreviated,
	EXTRACT(quarter FROM datum) AS quarter_actual,
	CASE
		WHEN EXTRACT(quarter FROM datum) = 1 THEN 'First'
		WHEN EXTRACT(quarter FROM datum) = 2 THEN 'Second'
		WHEN EXTRACT(quarter FROM datum) = 3 THEN 'Third'
		WHEN EXTRACT(quarter FROM datum) = 4 THEN 'Fourth'
	END AS quarter_name,
	EXTRACT(YEAR FROM datum) AS year_actual,
	datum + (1 - EXTRACT(isodow FROM datum))::INTEGER AS first_day_of_week,
	datum + (7 - EXTRACT(isodow FROM datum))::INTEGER AS last_day_of_week,
	datum + (1 - EXTRACT(DAY FROM datum))::INTEGER AS first_day_of_month,
	(DATE_TRUNC('month', datum) + INTERVAL '1 month - 1 day')::DATE AS last_day_of_month,
	DATE_TRUNC('quarter', datum)::DATE AS first_day_of_quarter,
	(DATE_TRUNC('quarter', datum) + INTERVAL '3 month - 1 day')::DATE AS last_day_of_quarter,
	TO_DATE(EXTRACT(YEAR FROM datum) || '-01-01', 'yyyy-mm-dd') AS first_day_of_year,
	TO_DATE(EXTRACT(YEAR FROM datum) || '-12-31', 'yyyy-mm-dd') AS last_day_of_year,
	TO_CHAR(datum, 'mmyyyy') AS mmyyyy,
	TO_CHAR(datum, 'mmddyyyy') AS mmddyyyy,
	CASE
		WHEN EXTRACT(isodow FROM datum) IN (6, 7) THEN TRUE
		ELSE FALSE
	END AS weekend_indr
FROM
	(SELECT '2003-01-01'::date + SEQUENCE.DAY AS datum
	FROM GENERATE_SERIES(0, 29219) AS SEQUENCE (DAY)
	GROUP BY SEQUENCE.DAY) dq
ORDER BY full_date
ON CONFLICT (date_id) DO NOTHING;

GET DIAGNOSTICS rows_affected = row_count;

RAISE NOTICE '% row(s) inserted', rows_affected;
output_message = 'Success';

CALL bl_cl.logging_insertion(schema_name, 
							 table_name,
							 command_name,
							 rows_affected,
							 error_type,
							 output_message);

EXCEPTION
WHEN OTHERS THEN
     GET stacked DIAGNOSTICS error_type = pg_exception_context,
                             output_message = message_text;

RAISE NOTICE 'Error: % %', error_type, output_message;

CALL bl_cl.logging_insertion(schema_name, 
							 table_name,
							 command_name,
							 rows_affected,
							 error_type,
							 output_message);
END;
$$; 