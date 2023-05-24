CREATE OR REPLACE PROCEDURE bl_cl.ce_employees_insertion()

LANGUAGE plpgsql 
AS $$

DECLARE
  schema_name   TEXT := 'bl_3nf';
  table_name    TEXT := 'ce_employees';
  command_name  TEXT := 'insert';
  rows_affected INTEGER := 0;
  error_type    TEXT := '';
  output_message TEXT := '';

BEGIN 

WITH inserted_data (employee_id,
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
      		        location_id,
      	            position_id,
      		        vacation_hours,
      		        sick_leave_hours,
      		        insert_dt,
      		        update_dt) AS(
     SELECT DISTINCT COALESCE(employee_id, 'N/A'),
			        'sa_northwest_sales',
			        'src_northwest_sales',
			        COALESCE(emp_first_name, 'N/A'),
			        COALESCE(emp_last_name, 'N/A'),
			        COALESCE(emp_gender, 'N/A'),
			        COALESCE(emp_dob::DATE, '9999-12-31'),
			        COALESCE(hire_date::DATE, '9999-12-31'),
				    '1900-01-01'::DATE,
			        '9999-12-31'::DATE,
			        'Y',
			        COALESCE(emp_phone, 'N/A'),
			        COALESCE(emp_email, 'N/A'),
			        COALESCE(loc.location_id, -1),
			        COALESCE(pos.position_id, -1),       
			        COALESCE(vacation_hours::INTEGER, -1),
			        COALESCE(sick_leave_hours::INTEGER, -1),       
			        CURRENT_TIMESTAMP,
				    CURRENT_TIMESTAMP 
     FROM bl_3nf.incr_view_ce_employees_no AS src
     LEFT OUTER JOIN bl_3nf.ce_locations AS loc ON src.emp_location_id = loc.location_src_id
     LEFT OUTER JOIN bl_3nf.ce_positions AS pos ON src.position_id = pos.position_src_id 
     WHERE loc.source_system = 'sa_northwest_sales'
     	   AND loc.source_entity = 'src_northwest_sales'
	 UNION ALL
     SELECT DISTINCT COALESCE(employee_id, 'N/A'),
			        'sa_iowalakes_sales',
			        'src_iowalakes_sales',
			        COALESCE(emp_first_name, 'N/A'),
			        COALESCE(emp_last_name, 'N/A'),
			        COALESCE(emp_gender, 'N/A'),
			        COALESCE(emp_dob::DATE, '9999-12-31'),
			        COALESCE(hire_date::DATE, '9999-12-31'),
				    '1900-01-01'::DATE,
			        '9999-12-31'::DATE,
			        'Y',
			        COALESCE(emp_phone, 'N/A'),
			        COALESCE(emp_email, 'N/A'),
			        COALESCE(loc.location_id, -1),
			        COALESCE(pos.position_id, -1),       
			        COALESCE(vacation_hours::INTEGER, -1),
			        COALESCE(sick_leave_hours::INTEGER, -1),      
			        CURRENT_TIMESTAMP,
				    CURRENT_TIMESTAMP 
     FROM bl_3nf.incr_view_ce_employees_io AS src
     LEFT OUTER JOIN bl_3nf.ce_locations AS loc ON src.emp_location_id = loc.location_src_id
     LEFT OUTER JOIN bl_3nf.ce_positions AS pos ON src.position_id = pos.position_src_id
     WHERE loc.source_system = 'sa_iowalakes_sales'
     	   AND loc.source_entity = 'src_iowalakes_sales'
	                               ),
	 new_rows AS (
	 SELECT *
     FROM inserted_data AS ins
     WHERE NOT EXISTS (SELECT 1
                       FROM  bl_3nf.ce_employees AS emp
                       WHERE UPPER(ins.employee_id) = UPPER(emp.employee_src_id) AND
						     UPPER(ins.source_system) = UPPER(emp.source_system) AND
						     UPPER(ins.source_entity) = UPPER(emp.source_entity) AND
						     UPPER(ins.first_name) = UPPER(emp.first_name) AND
						     UPPER(ins.last_name) = UPPER(emp.last_name) AND
						     UPPER(ins.gender) = UPPER(emp.gender) AND
						     ins.dob = emp.dob AND
						     ins.hire_date = emp.hire_date AND
						     UPPER(ins.phone) = UPPER(emp.phone) AND
						     UPPER(ins.email) = UPPER(emp.email) AND
                             ins.location_id = emp.location_id AND
                             ins.position_id = emp.position_id AND
                             ins.vacation_hours = emp.vacation_hours AND
                             ins.sick_leave_hours = emp.sick_leave_hours AND
                             UPPER(emp.is_active) = 'Y'
                        )
	 								),								
	  upd AS (
      UPDATE bl_3nf.ce_employees AS target
             SET is_active = 'N',
             end_date = CURRENT_TIMESTAMP,
             update_dt = CURRENT_TIMESTAMP
      FROM new_rows
      WHERE UPPER(new_rows.employee_id) = UPPER(target.employee_src_id) AND
            UPPER(target.is_active) = 'Y'
     		 )
INSERT INTO bl_3nf.ce_employees
            (employee_id,
             employee_src_id,
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
      		 location_id,
      		 position_id,
      		 vacation_hours,
      		 sick_leave_hours,
      		 insert_dt,
      		 update_dt)
SELECT CASE WHEN EXISTS (SELECT 1 
                         FROM bl_3nf.ce_employees AS emp 
                         WHERE emp.employee_src_id = new_rows.employee_id) 
            THEN (SELECT emp.employee_id 
                  FROM bl_3nf.ce_employees AS emp 
                  WHERE emp.employee_src_id = new_rows.employee_id) 
       		ELSE NEXTVAL('bl_3nf.ce_employees_seq')
       END,
	   employee_id,
	   source_system,
	   source_entity,
	   first_name,
	   last_name,
	   gender,
	   dob,
	   hire_date,
	   CASE WHEN EXISTS (SELECT 1
                         FROM bl_3nf.ce_employees AS emp, new_rows
                         WHERE  UPPER(new_rows.employee_id) = UPPER(emp.employee_src_id) AND
                                UPPER(new_rows.source_system) = UPPER(emp.source_system) AND
                                UPPER(new_rows.source_entity) = UPPER(emp.source_entity)) 
            THEN CURRENT_DATE
            ELSE '1900-01-01'::DATE 
       END,
       end_date,
       is_active,
       phone,
       email,
       location_id,
       position_id,
       vacation_hours,
       sick_leave_hours,
       insert_dt,
       update_dt
FROM   new_rows
ON CONFLICT ON CONSTRAINT emp_pk DO NOTHING;     

GET DIAGNOSTICS rows_affected = row_count;

UPDATE bl_cl.prm_mta_incremental_load
SET previous_loaded_dt = CURRENT_TIMESTAMP,
	procedure_name = 'bl_cl.ce_employees_insertion()'	
WHERE source_table IN ('src_northwest_sales', 'src_iowalakes_sales')
	  AND target_table = 'ce_employees';    

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