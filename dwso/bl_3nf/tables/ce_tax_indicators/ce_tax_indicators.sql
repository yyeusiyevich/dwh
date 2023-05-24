CREATE TABLE IF NOT EXISTS bl_3nf.ce_tax_indicators
  (
     tax_indicator_id     		INTEGER PRIMARY KEY,
     tax_indicator_src_id 		TEXT NOT NULL,
     source_system              TEXT NOT NULL,
     source_entity              TEXT NOT NULL,
     tax_type  		    	    TEXT NOT NULL,
     insert_dt                  TIMESTAMP WITH TIME ZONE NOT NULL,
     update_dt                  TIMESTAMP WITH TIME ZONE NOT NULL
  ); 