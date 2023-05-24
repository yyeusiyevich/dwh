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
VALUES(-1, -1, 'MANUAL', 'MANUAL', 'N/A', 'N/A', 'N/A', '9999-12-31', '9999-12-31', '9999-12-31', '9999-12-31', 'N', 'N/A', 'N/A', -1, -1, -1, -1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
ON CONFLICT ON CONSTRAINT ce_employees_pkey DO NOTHING;         
	 