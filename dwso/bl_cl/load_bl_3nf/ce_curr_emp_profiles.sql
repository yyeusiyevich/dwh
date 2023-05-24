CREATE OR REPLACE PROCEDURE bl_cl.ce_curr_emp_profiles_insertion()

LANGUAGE plpgsql 
AS $$

DECLARE
  schema_name   TEXT := 'bl_3nf';
  table_name    TEXT := 'ce_curr_emp_profiles';
  command_name  TEXT := 'insert';
  rows_affected INTEGER := 0;
  error_type    TEXT := '';
  output_message TEXT := '';

BEGIN 

WITH inserted_data (curr_employee_id,
					curr_employee_src_id,
					source_system,
					source_entity,
					first_name,
					last_name,
					phone,
					email,
					insert_dt) AS (
     SELECT DISTINCT COALESCE(nf.employee_id, -1), 
					 COALESCE(mp.employee_src_id, 'N/A'),
				     mp.source_system,
				     mp.source_table,
				     COALESCE(mp.emp_first_name, 'N/A'),
				     COALESCE(mp.emp_last_name, 'N/A'),
				     COALESCE(mp.emp_phone, 'N/A'),
				     COALESCE(mp.emp_email, 'N/A'),
				     CURRENT_TIMESTAMP
	 FROM bl_cl.map_curr_employees AS mp
	 LEFT OUTER JOIN bl_3nf.ce_employees AS nf
	 ON nf.employee_src_id = mp.employee_src_id 
	 WHERE UPPER(nf.is_active) = 'Y'
	 )
INSERT INTO bl_3nf.ce_curr_emp_profiles
			(curr_emp_profile_id, 
			 curr_employee_id,
			 curr_employee_src_id,
			 source_system,
			 source_entity,
			 first_name,
			 last_name,
			 phone,
			 email,
			 insert_dt)	
SELECT NEXTVAL('bl_3nf.ce_curr_emp_profiles_seq'),
	   *
FROM   inserted_data AS ins
WHERE NOT EXISTS (SELECT 1
				  FROM bl_3nf.ce_curr_emp_profiles AS t
				  WHERE t.curr_employee_id = ins.curr_employee_id AND
				        t.curr_employee_src_id = ins.curr_employee_src_id AND
				        t.source_system = ins.source_system AND
				        t.source_entity =  ins.source_entity AND
				        t.first_name = ins.first_name AND 
				        t.last_name =  ins.last_name AND
				        t.phone = ins.phone AND
				        t.email = ins.email);

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