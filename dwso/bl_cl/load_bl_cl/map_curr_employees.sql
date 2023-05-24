-- DROP PROCEDURE IF EXISTS bl_cl.map_curr_employees_insertion();
CREATE OR REPLACE PROCEDURE bl_cl.map_curr_employees_insertion()

LANGUAGE plpgsql 
AS $$

DECLARE
  schema_name   TEXT := 'bl_cl';
  table_name    TEXT := 'map_curr_employees';
  command_name  TEXT := 'insert';
  rows_affected INTEGER := 0;
  error_type    TEXT := '';
  output_message TEXT := '';

BEGIN 
	
TRUNCATE bl_cl.map_curr_employees;

INSERT INTO bl_cl.map_curr_employees
SELECT 
	mdnw.store_number, 
	employee_id,
	'bl_cl' AS source_system,
	'map_curr_employees' AS source_table,
	emp_first_name, 
	emp_last_name, 
	emp_phone, 
	emp_email,
	CURRENT_TIMESTAMP,
	CURRENT_TIMESTAMP
FROM 
	bl_3nf.incr_view_ce_curr_emp_profiles_no AS nw
	INNER JOIN (
		SELECT 
			MAX(date) AS max_sell_date, 
			store_number
		FROM 
			bl_3nf.incr_view_ce_curr_emp_profiles_no
		GROUP BY 
			store_number
	) AS mdnw 
	ON nw.date = mdnw.max_sell_date 
	AND nw.store_number = mdnw.store_number
UNION ALL
SELECT 
	mdil.store_number, 
	employee_id,
	'bl_cl' AS source_system,
	'map_curr_employees' AS source_table,
	emp_first_name, 
	emp_last_name, 
	emp_phone, 
	emp_email,
	CURRENT_TIMESTAMP,
	CURRENT_TIMESTAMP
FROM 
	bl_3nf.incr_view_ce_curr_emp_profiles_io AS il
	INNER JOIN (
		SELECT 
			MAX(date) AS max_sell_date, 
			store_number
		FROM 
			bl_3nf.incr_view_ce_curr_emp_profiles_io
		GROUP BY 
			store_number
	) AS mdil 
	ON il.date = mdil.max_sell_date 
	AND il.store_number = mdil.store_number;

GET DIAGNOSTICS rows_affected = row_count;

UPDATE bl_cl.prm_mta_incremental_load
SET previous_loaded_dt = CURRENT_TIMESTAMP,
	procedure_name = 'bl_cl.map_curr_employees_insertion()'	
WHERE source_table IN ('src_northwest_sales', 'src_iowalakes_sales')
	  AND target_table = 'ce_curr_emp_profiles'; 

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