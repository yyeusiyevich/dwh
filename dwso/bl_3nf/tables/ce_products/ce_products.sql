CREATE TABLE IF NOT EXISTS bl_3nf.ce_products
  (
     product_id     			INTEGER PRIMARY KEY,
     product_src_id 			TEXT NOT NULL,
     product_desc				TEXT NOT NULL,
     source_system              TEXT NOT NULL,
     source_entity              TEXT NOT NULL,
     category_id  		    	INTEGER NOT NULL REFERENCES bl_3nf.ce_product_categories(product_category_id) ON UPDATE CASCADE ON DELETE RESTRICT,
     pack						INTEGER NOT NULL,
     bottle_volume				DECIMAL(10, 2) NOT NULL,
     safety_stock_lvl			INTEGER NOT NULL,
     reorder_point				INTEGER NOT NULL,
     start_date					DATE NOT NULL,
     end_date					DATE NOT NULL,
     is_active					TEXT NOT NULL,
     on_sale					TEXT NOT NULL,
     insert_dt                  TIMESTAMP WITH TIME ZONE NOT NULL,
     update_dt                  TIMESTAMP WITH TIME ZONE NOT NULL
  ); 