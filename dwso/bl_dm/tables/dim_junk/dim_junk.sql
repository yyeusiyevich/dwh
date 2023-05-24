CREATE TABLE IF NOT EXISTS bl_dim.dim_junk
  (
     junk_surr_id     			INTEGER PRIMARY KEY,
	 transaction_status			TEXT NOT NULL,
	 transaction_type		    TEXT NOT NULL,
	 tax_type					TEXT NOT NULL,
     insert_dt           	    TIMESTAMP WITH TIME ZONE NOT NULL,
     update_dt           	    TIMESTAMP WITH TIME ZONE NOT NULL
  ); 