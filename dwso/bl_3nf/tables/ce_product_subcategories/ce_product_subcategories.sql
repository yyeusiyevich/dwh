CREATE TABLE IF NOT EXISTS bl_3nf.ce_product_subcategories
  (
     product_subcategory_id     INTEGER PRIMARY KEY,
     product_subcategory_src_id TEXT NOT NULL,
     source_system              TEXT NOT NULL,
     source_entity              TEXT NOT NULL,
     category_id  		    	INTEGER  NOT NULL REFERENCES bl_3nf.ce_product_categories(product_category_id) ON UPDATE CASCADE ON DELETE RESTRICT,
     subcategory_name			TEXT NOT NULL,
     insert_dt                  TIMESTAMP WITH TIME ZONE NOT NULL,
     update_dt                  TIMESTAMP WITH TIME ZONE NOT NULL
  ); 