CREATE TABLE IF NOT EXISTS bl_3nf.ce_cities
  (
     city_id     				INTEGER PRIMARY KEY,
     city_src_id 				TEXT NOT NULL,
     source_system              TEXT NOT NULL,
     source_entity              TEXT NOT NULL,
     county_id					INTEGER NOT NULL REFERENCES bl_3nf.ce_counties(county_id) ON UPDATE CASCADE ON DELETE RESTRICT,
	 city_name					TEXT NOT NULL,
     insert_dt                  TIMESTAMP WITH TIME ZONE NOT NULL,
     update_dt                  TIMESTAMP WITH TIME ZONE NOT NULL
  ); 