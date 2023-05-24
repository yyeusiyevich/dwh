CREATE TABLE IF NOT EXISTS bl_3nf.ce_shippers
  (
     shipper_id     			INTEGER PRIMARY KEY,
     shipper_src_id 			TEXT NOT NULL,
     source_system          	TEXT NOT NULL,
     source_entity          	TEXT NOT NULL,
     name  		    			TEXT NOT NULL,
     start_date					DATE NOT NULL,
     end_date     				DATE NOT NULL,
     is_active					TEXT NOT NULL,
     rating				    	DECIMAL(2, 1) NOT NULL,
     ship_base					DECIMAL(10, 2) NOT NULL,
     ship_rate					DECIMAL(10, 2) NOT NULL,
     contact_phone				TEXT NOT NULL,
     contact_name				TEXT NOT NULL,
     curr_county_id			    INTEGER NOT NULL REFERENCES bl_3nf.ce_counties(county_id) ON UPDATE CASCADE ON DELETE RESTRICT,
     historical_county_id		INTEGER NOT NULL REFERENCES bl_3nf.ce_counties(county_id) ON UPDATE CASCADE ON DELETE RESTRICT,
     insert_dt              	TIMESTAMP WITH TIME ZONE NOT NULL,
     update_dt              	TIMESTAMP WITH TIME ZONE NOT NULL
  ); 