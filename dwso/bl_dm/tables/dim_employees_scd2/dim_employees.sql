CREATE TABLE IF NOT EXISTS bl_dim.dim_employees
  (
     employee_surr_id     		INTEGER PRIMARY KEY,
     employee_id 				INTEGER NOT NULL,
     first_name  		    	TEXT NOT NULL,
     last_name					TEXT NOT NULL,
     full_name					TEXT GENERATED ALWAYS AS (first_name || ' ' || last_name) STORED,
     gender						TEXT NOT NULL,
     dob						DATE NOT NULL,
     hire_date					DATE NOT NULL,
     start_date					DATE NOT NULL,
     end_date     				DATE NOT NULL,
     is_active					TEXT NOT NULL,
     phone						TEXT NOT NULL,
     email						TEXT NOT NULL,
     postal_code				TEXT NOT NULL,				
     city						TEXT NOT NULL,
     address					TEXT NOT NULL,
     position_name 				TEXT NOT NULL,
     vacation_hours				INTEGER NOT NULL,
     sick_leave_hours			INTEGER NOT NULL,     
     insert_dt              	TIMESTAMP WITH TIME ZONE NOT NULL,
     update_dt              	TIMESTAMP WITH TIME ZONE NOT NULL
  ); 