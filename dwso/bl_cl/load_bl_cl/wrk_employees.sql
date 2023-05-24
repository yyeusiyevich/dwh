-- DROP PROCEDURE IF EXISTS bl_cl.wrk_employees_insertion();
CREATE OR REPLACE PROCEDURE bl_cl.wrk_employees_insertion()

LANGUAGE plpgsql 
AS $$

DECLARE
  schema_name   TEXT := 'bl_cl';
  table_name    TEXT := 'wrk_employees';
  command_name  TEXT := 'insert';
  rows_affected INTEGER := 0;
  error_type    TEXT := '';
  output_message TEXT := '';
 
  last_load_date TIMESTAMP;

BEGIN 
	
TRUNCATE bl_cl.wrk_employees;

SELECT COALESCE(MAX(update_dt), '1900-01-01'::TIMESTAMP) INTO last_load_date FROM bl_dim.dim_employees_scd;

INSERT INTO bl_cl.wrk_employees (
					employee_id,
					employee_src_id,
					source_system,
					source_entity,
					first_name,
					last_name,
					full_name,
					gender,
					dob,
					hire_date,
					start_date,
					end_date,
					is_active,
					phone,
					email,
					postal_code,
					city,
					address,
					position_name,
					vacation_hours,
					sick_leave_hours,
					insert_dt,
					update_dt)
SELECT employee_id,
	   employee_src_id,
	   'bl_3nf' AS source_system,
	   'ce_employees' AS source_entity,
	    first_name,
		last_name,
		full_name,
		gender,
		dob,
		hire_date,
		start_date,
		end_date,
		is_active,
		phone,
		email,
		loc.postal_code,
		ct.city_name,
		loc.address,
		pos.position_name,
		vacation_hours,
		sick_leave_hours,
	    CURRENT_TIMESTAMP,
	    CURRENT_TIMESTAMP
FROM bl_3nf.ce_employees AS emp
LEFT OUTER JOIN bl_3nf.ce_locations AS loc ON emp.location_id = loc.location_id 
LEFT OUTER JOIN bl_3nf.ce_positions AS pos ON emp.position_id = pos.position_id 
LEFT OUTER JOIN bl_3nf.ce_cities    AS ct  ON loc.city_id = ct.city_id
WHERE GREATEST(emp.update_dt, loc.update_dt, pos.update_dt, ct.update_dt) > last_load_date;
		 
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