CREATE TABLE IF NOT EXISTS bl_3nf.ce_locations
  (
     location_id     				INTEGER PRIMARY KEY,
     location_src_id 				TEXT NOT NULL,
     source_system          		TEXT NOT NULL,
     source_entity          		TEXT NOT NULL,
     address  		    			TEXT NOT NULL,
	 postal_code					TEXT NOT NULL,
     city_id 						INTEGER  NOT NULL REFERENCES bl_3nf.ce_cities(city_id) ON UPDATE CASCADE ON DELETE RESTRICT,   
     insert_dt              		TIMESTAMP WITH TIME ZONE NOT NULL,
     update_dt              		TIMESTAMP WITH TIME ZONE NOT NULL
  ); 