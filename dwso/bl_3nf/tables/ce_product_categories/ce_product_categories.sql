CREATE TABLE IF NOT EXISTS bl_3nf.ce_product_categories
  (
     product_category_id     INTEGER PRIMARY KEY,
     product_category_src_id TEXT NOT NULL,
     source_system           TEXT NOT NULL,
     source_entity           TEXT NOT NULL,
     category_name  		 TEXT NOT NULL,
     insert_dt               TIMESTAMP WITH TIME ZONE NOT NULL,
     update_dt               TIMESTAMP WITH TIME ZONE NOT NULL
  ); 