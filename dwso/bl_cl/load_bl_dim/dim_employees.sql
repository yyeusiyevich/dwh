-- DROP PROCEDURE IF EXISTS bl_cl.dim_employees_scd_insertion();
CREATE OR REPLACE PROCEDURE bl_cl.dim_employees_scd_insertion()

LANGUAGE plpgsql 
AS $$

DECLARE
  schema_name   TEXT := 'bl_dim';
  table_name    TEXT := 'dim_employees_scd';
  command_name  TEXT := 'insert';
  rows_affected INTEGER := 0;
  error_type    TEXT := '';
  output_message TEXT := '';

BEGIN 
	
WITH inserted AS (
    SELECT
        employee_id,
        'bl_cl' AS source_system,
        'wrk_employees' AS source_entity,
        COALESCE(first_name, 'N/A') AS first_name,
	    COALESCE(last_name, 'N/A') AS last_name,
	    COALESCE(gender, 'N/A') AS gender,
	    COALESCE(dob, '9999-12-31'::DATE) AS dob,
	    COALESCE(hire_date, '9999-12-31'::DATE) AS hire_date,
		start_date,
		end_date,
		is_active,
		COALESCE(phone, 'N/A') AS phone,
	    COALESCE(email, 'N/A') AS email,
	    COALESCE(postal_code, 'N/A') AS postal_code,
	    COALESCE(city, 'N/A') AS city,
	    COALESCE(address, 'N/A') AS address,
	    COALESCE(position_name, 'N/A') AS position_name,
	    COALESCE(vacation_hours, -1) AS vacation_hours,
	    COALESCE(sick_leave_hours, -1) AS sick_leave_hours
    FROM bl_cl.wrk_employees
    WHERE employee_id != -1
	), 
	upd AS (
      UPDATE bl_dim.dim_employees_scd AS target
             SET is_active = 'N',
             end_date = CURRENT_TIMESTAMP,
             update_dt = CURRENT_TIMESTAMP
      FROM inserted
      WHERE inserted.employee_id = target.employee_id AND
            UPPER(target.is_active) = 'Y'
     		 )
INSERT INTO bl_dim.dim_employees_scd
	(
        employee_surr_id,
		employee_id,
		source_system,
		source_entity,
		first_name,
		last_name,
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
		update_dt
    )
    SELECT
        NEXTVAL('bl_dim.dim_employees_seq'),
        employee_id,
        source_system,
        source_entity,
        first_name,
		last_name,
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
        CURRENT_TIMESTAMP,
        CURRENT_TIMESTAMP
   FROM inserted
   WHERE UPPER(is_active) = 'Y';

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