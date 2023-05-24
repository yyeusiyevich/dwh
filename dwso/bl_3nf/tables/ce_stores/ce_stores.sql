CREATE TABLE IF NOT EXISTS bl_3nf.ce_stores
  (
     store_id     				INTEGER PRIMARY KEY,
     store_src_id 				TEXT NOT NULL,
     source_system          	TEXT NOT NULL,
     source_entity          	TEXT NOT NULL,
     name  		    			TEXT NOT NULL,
     start_date					DATE NOT NULL,
     end_date     				DATE NOT NULL,
     is_active					TEXT NOT NULL,
     contact_phone				TEXT NOT NULL,
     contact_email				TEXT NOT NULL,
     location_id 				INTEGER  NOT NULL REFERENCES bl_3nf.ce_locations(location_id) ON UPDATE CASCADE ON DELETE RESTRICT,
     payment_type_pref_id		INTEGER  NOT NULL REFERENCES bl_3nf.ce_payment_pref_types(payment_type_pref_id) ON UPDATE CASCADE ON DELETE RESTRICT,
     curr_emp_profile_id		INTEGER  NOT NULL REFERENCES bl_3nf.ce_curr_emp_profiles(curr_emp_profile_id) ON UPDATE CASCADE ON DELETE RESTRICT,
     opt_in_flag				TEXT NOT NULL,
     membership_flag			TEXT NOT NULL,     
     insert_dt              	TIMESTAMP WITH TIME ZONE NOT NULL,
     update_dt              	TIMESTAMP WITH TIME ZONE NOT NULL
  ); 