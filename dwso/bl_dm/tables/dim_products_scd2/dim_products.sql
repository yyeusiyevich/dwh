CREATE TABLE IF NOT EXISTS bl_dim.dim_products (
    product_surr_id 			INTEGER PRIMARY KEY,
    product_id 					INTEGER NOT NULL,
    produc_desc 				TEXT NOT NULL,
    category_name 				TEXT NOT NULL,    
    subcategory_name		 	TEXT NOT NULL,
    pack						INTEGER NOT NULL,
    bottle_volume_ml  			DECIMAL(10, 2) NOT NULL,
	safety_stock_lvl 			INTEGER NOT NULL,
	reorder_point 				INTEGER NOT NULL,
	start_date					DATE NOT NULL,
	end_date					DATE NOT NULL,
	is_active					TEXT NOT NULL,
	product_on_sale 			TEXT NOT NULL,
    insert_dt                   TIMESTAMP WITH TIME ZONE NOT NULL,
    update_dt                   TIMESTAMP WITH TIME ZONE NOT NULL
  ); 