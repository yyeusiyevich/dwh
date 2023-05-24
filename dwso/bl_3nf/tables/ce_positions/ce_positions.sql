CREATE TABLE IF NOT EXISTS bl_3nf.ce_positions
  (
     position_id     		 INTEGER PRIMARY KEY,
     position_src_id 		 TEXT NOT NULL,
     source_system           TEXT NOT NULL,
     source_entity           TEXT NOT NULL,
     position_name  		 TEXT NOT NULL,
     insert_dt               TIMESTAMP WITH TIME ZONE NOT NULL,
     update_dt               TIMESTAMP WITH TIME ZONE NOT NULL
  ); 