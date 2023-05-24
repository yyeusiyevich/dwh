CREATE TABLE IF NOT EXISTS bl_3nf.ce_payment_pref_types
  (
     payment_type_pref_id     	 INTEGER PRIMARY KEY,
     payment_type_pref_src_id 	 TEXT NOT NULL,
     source_system           	 TEXT NOT NULL,
     source_entity           	 TEXT NOT NULL,
     payment_type_pref  		 TEXT NOT NULL,
     insert_dt               	 TIMESTAMP WITH TIME ZONE NOT NULL,
     update_dt               	 TIMESTAMP WITH TIME ZONE NOT NULL
  ); 