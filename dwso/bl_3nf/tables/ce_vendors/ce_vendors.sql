CREATE TABLE IF NOT EXISTS bl_3nf.ce_vendors
  (
     vendor_id     			INTEGER PRIMARY KEY,
     vendor_src_id 			TEXT NOT NULL,
     source_system          TEXT NOT NULL,
     source_entity          TEXT NOT NULL,
     name  		    		TEXT NOT NULL,
     start_date				DATE NOT NULL,
     end_date				DATE NOT NULL,
     is_active				TEXT NOT NULL,
     rating				    DECIMAL(2, 1) NOT NULL,
     size					TEXT NOT NULL,
     contact_phone			TEXT NOT NULL,
     contact_name			TEXT NOT NULL,
     homepage				TEXT NOT NULL,
     insert_dt              TIMESTAMP WITH TIME ZONE NOT NULL,
     update_dt              TIMESTAMP WITH TIME ZONE NOT NULL
  ); 