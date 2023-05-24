CREATE TABLE IF NOT EXISTS bl_3nf.ce_employees
  (
     employee_id     			INTEGER PRIMARY KEY,
     employee_src_id 			TEXT NOT NULL,
     source_system          	TEXT NOT NULL,
     source_entity          	TEXT NOT NULL,
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
     location_id 				INTEGER  NOT NULL REFERENCES bl_3nf.ce_locations(location_id) ON UPDATE CASCADE ON DELETE RESTRICT,
     position_id				INTEGER  NOT NULL REFERENCES bl_3nf.ce_positions(position_id) ON UPDATE CASCADE ON DELETE RESTRICT,
     vacation_hours				INTEGER NOT NULL,
     sick_leave_hours			INTEGER NOT NULL,     
     insert_dt              	TIMESTAMP WITH TIME ZONE NOT NULL,
     update_dt              	TIMESTAMP WITH TIME ZONE NOT NULL
  ); 