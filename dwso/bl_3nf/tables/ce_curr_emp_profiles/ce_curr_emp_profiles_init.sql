INSERT INTO bl_3nf.ce_curr_emp_profiles (
			curr_emp_profile_id, 
			curr_employee_id, 
			curr_employee_src_id, 
			source_system, 
			source_entity, 
			first_name, 
			last_name,
			phone, 
			email, 
			insert_dt)
VALUES (-1, -1, 'N/A', 'MANUAL', 'MANUAL', 'N/A', 'N/A', 'N/A', 'N/A', CURRENT_TIMESTAMP)
ON CONFLICT ON CONSTRAINT ce_curr_emp_profiles_pkey DO NOTHING; 
