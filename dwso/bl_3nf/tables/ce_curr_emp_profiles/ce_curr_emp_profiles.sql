CREATE TABLE IF NOT EXISTS bl_3nf.ce_curr_emp_profiles
  (
     curr_emp_profile_id     	INTEGER PRIMARY KEY,
     curr_employee_id			INTEGER NOT NULL,
     curr_employee_src_id 	    TEXT NOT NULL,
     source_system          	TEXT NOT NULL,
     source_entity          	TEXT NOT NULL,
     first_name  		    	TEXT NOT NULL,
     last_name					TEXT NOT NULL,
     full_name					TEXT GENERATED ALWAYS AS (first_name || ' ' || last_name) STORED,
     phone						TEXT NOT NULL,
     email						TEXT NOT NULL,    
     insert_dt              	TIMESTAMP WITH TIME ZONE NOT NULL
  ); 