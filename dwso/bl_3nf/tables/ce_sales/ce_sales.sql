CREATE TABLE IF NOT EXISTS bl_3nf.ce_sales
  (
     transaction_id     			INTEGER PRIMARY KEY,
     transaction_src_id 			TEXT NOT NULL,
     source_system          		TEXT NOT NULL,
     source_entity          		TEXT NOT NULL,
     status_id 				        INTEGER  NOT NULL REFERENCES bl_3nf.ce_transaction_statuses(status_id) ON UPDATE CASCADE ON DELETE RESTRICT,
     type_id       				    INTEGER  NOT NULL REFERENCES bl_3nf.ce_transaction_types(type_id) ON UPDATE CASCADE ON DELETE RESTRICT,
     tax_indicator_id			    INTEGER  NOT NULL REFERENCES bl_3nf.ce_tax_indicators(tax_indicator_id) ON UPDATE CASCADE ON DELETE RESTRICT,
     shipper_id						INTEGER  NOT NULL REFERENCES bl_3nf.ce_shippers(shipper_id) ON UPDATE CASCADE ON DELETE RESTRICT,
     event_date						DATE NOT NULL,
     store_id						INTEGER  NOT NULL REFERENCES bl_3nf.ce_stores(store_id) ON UPDATE CASCADE ON DELETE RESTRICT,
     employee_id					INTEGER  NOT NULL REFERENCES bl_3nf.ce_employees(employee_id) ON UPDATE CASCADE ON DELETE RESTRICT,
     vendor_id						INTEGER  NOT NULL REFERENCES bl_3nf.ce_vendors(vendor_id) ON UPDATE CASCADE ON DELETE RESTRICT,
     product_id						INTEGER  NOT NULL REFERENCES bl_3nf.ce_products(product_id) ON UPDATE CASCADE ON DELETE RESTRICT,
     quantity_sold					INTEGER,
     total_amount					DECIMAL(10, 2),
     volume_sold_liters				DECIMAL(10, 2),
     volume_sold_gallons			DECIMAL(10, 2),
     state_bottle_cost				DECIMAL(10, 2),
     state_bottle_retail			DECIMAL(10, 2),
     insert_dt              		TIMESTAMP WITH TIME ZONE NOT NULL
  ); 