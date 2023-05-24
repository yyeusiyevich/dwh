CREATE TABLE IF NOT EXISTS bl_dim.fct_sales
  (
     junk_surr_id     				INTEGER  NOT NULL REFERENCES bl_dim.dim_junk(junk_surr_id) ON UPDATE CASCADE ON DELETE RESTRICT,
     shipper_surr_id				INTEGER  NOT NULL REFERENCES bl_dim.dim_shippers(shipper_surr_id) ON UPDATE CASCADE ON DELETE RESTRICT,
     date_id						INTEGER  NOT NULL REFERENCES bl_dim.dim_dates(date_id) ON UPDATE CASCADE ON DELETE RESTRICT,
     store_surr_id					INTEGER  NOT NULL REFERENCES bl_dim.dim_stores(store_surr_id) ON UPDATE CASCADE ON DELETE RESTRICT,
     employee_surr_id				INTEGER  NOT NULL REFERENCES bl_dim.dim_employees(employee_surr_id) ON UPDATE CASCADE ON DELETE RESTRICT,
     vendor_surr_id					INTEGER  NOT NULL REFERENCES bl_dim.dim_vendors(vendor_surr_id) ON UPDATE CASCADE ON DELETE RESTRICT,
     product_surr_id				INTEGER  NOT NULL REFERENCES bl_dim.dim_products(product_surr_id) ON UPDATE CASCADE ON DELETE RESTRICT,
     transaction_id     			INTEGER NOT NULL,
     quantity_sold					INTEGER NOT NULL,
     total_amount					DECIMAL(10, 2) NOT NULL,
     volume_sold_liters				DECIMAL(10, 2) NOT NULL,
     volume_sold_gallons			DECIMAL(10, 2) NOT NULL,
     state_bottle_cost				DECIMAL(10, 2) NOT NULL,
     state_bottle_retail			DECIMAL(10, 2) NOT NULL,
     insert_dt           	    	TIMESTAMP WITH TIME ZONE NOT NULL,
     CONSTRAINT pk_fct_sales PRIMARY KEY (junk_surr_id, shipper_surr_id, date_id, store_surr_id, employee_surr_id, vendor_surr_id, product_surr_id, transaction_id)
  ); 