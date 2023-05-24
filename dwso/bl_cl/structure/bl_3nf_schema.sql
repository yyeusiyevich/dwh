CREATE OR REPLACE PROCEDURE bl_cl.bl_3nf_schema()

LANGUAGE plpgsql 
AS $$

DECLARE
  schema_name   TEXT := 'bl_3nf';
  table_name    TEXT := 'All tables';
  command_name  TEXT := 'create';
  rows_affected INTEGER := 0;
  error_type    TEXT := '';
  output_message TEXT := '';

BEGIN 
	
-- DROP SCHEMA IF EXISTS bl_3nf CASCADE;
CREATE SCHEMA IF NOT EXISTS bl_3nf; 	

-- tables and sequences
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
 
CREATE SEQUENCE IF NOT EXISTS bl_3nf.ce_tax_indicators_seq
START WITH 1
INCREMENT BY 1
MINVALUE 1
NO MAXVALUE;

CREATE TABLE IF NOT EXISTS bl_3nf.ce_transaction_types
  (
     type_id     		INTEGER PRIMARY KEY,
     type_src_id 		TEXT NOT NULL,
     source_system      TEXT NOT NULL,
     source_entity      TEXT NOT NULL,
     type_name	    	TEXT NOT NULL,
     insert_dt          TIMESTAMP WITH TIME ZONE NOT NULL,
     update_dt          TIMESTAMP WITH TIME ZONE NOT NULL
  ); 
 
CREATE SEQUENCE IF NOT EXISTS bl_3nf.ce_transaction_types_seq
START WITH 1
INCREMENT BY 1
MINVALUE 1
NO MAXVALUE;

CREATE TABLE IF NOT EXISTS bl_3nf.ce_transaction_statuses
  (
     status_id     		INTEGER PRIMARY KEY,
     status_src_id 		TEXT NOT NULL,
     source_system      TEXT NOT NULL,
     source_entity      TEXT NOT NULL,
     status_name  		TEXT NOT NULL,
     insert_dt          TIMESTAMP WITH TIME ZONE NOT NULL,
     update_dt          TIMESTAMP WITH TIME ZONE NOT NULL
  ); 
 
CREATE SEQUENCE IF NOT EXISTS bl_3nf.ce_transaction_statuses_seq
START WITH 1
INCREMENT BY 1
MINVALUE 1
NO MAXVALUE;
	
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
 
CREATE SEQUENCE IF NOT EXISTS bl_3nf.ce_positions_seq
START WITH 1
INCREMENT BY 1
MINVALUE 1
NO MAXVALUE;

CREATE TABLE IF NOT EXISTS bl_3nf.ce_payment_pref_types
  (
     payment_type_pref_id     	 INTEGER PRIMARY KEY,
     payment_type_pref_src_id 	 TEXT NOT NULL,
     source_system           	 TEXT NOT NULL,
     source_entity           	 TEXT NOT NULL,
     payment_type_pref  		 TEXT NOT NULL,
     insert_dt               	 TIMESTAMP WITH TIME ZONE NOT NULL,
     update_dt               	 TIMESTAMP WITH TIME ZONE NOT NULL
  ); 
 
CREATE SEQUENCE IF NOT EXISTS bl_3nf.ce_payment_pref_types_seq
START WITH 1
INCREMENT BY 1
MINVALUE 1
NO MAXVALUE;

CREATE TABLE IF NOT EXISTS bl_3nf.ce_regions
  (
     region_id     				INTEGER PRIMARY KEY,
     region_src_id 				TEXT NOT NULL,
     source_system              TEXT NOT NULL,
     source_entity              TEXT NOT NULL,
	 region_name				TEXT NOT NULL,
     insert_dt                  TIMESTAMP WITH TIME ZONE NOT NULL,
     update_dt                  TIMESTAMP WITH TIME ZONE NOT NULL
  ); 
 
CREATE SEQUENCE IF NOT EXISTS bl_3nf.ce_regions_seq
START WITH 1
INCREMENT BY 1
MINVALUE 1
NO MAXVALUE; 

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
 
CREATE SEQUENCE IF NOT EXISTS bl_3nf.ce_product_categories_seq
START WITH 1
INCREMENT BY 1
MINVALUE 1
NO MAXVALUE;

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
 
CREATE SEQUENCE IF NOT EXISTS bl_3nf.ce_product_subcategories_seq
START WITH 1
INCREMENT BY 1
MINVALUE 1
NO MAXVALUE;

CREATE TABLE IF NOT EXISTS bl_3nf.ce_counties
  (
     county_id     				INTEGER PRIMARY KEY,
     county_src_id 				TEXT NOT NULL,
     source_system              TEXT NOT NULL,
     source_entity              TEXT NOT NULL,
     region_id					INTEGER NOT NULL REFERENCES bl_3nf.ce_regions(region_id) ON UPDATE CASCADE ON DELETE RESTRICT,
     county_code				TEXT NOT NULL,
	 county_name				TEXT NOT NULL,
     insert_dt                  TIMESTAMP WITH TIME ZONE NOT NULL,
     update_dt                  TIMESTAMP WITH TIME ZONE NOT NULL
  ); 
 
CREATE SEQUENCE IF NOT EXISTS bl_3nf.ce_counties_seq
START WITH 1
INCREMENT BY 1
MINVALUE 1
NO MAXVALUE;

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
 
CREATE SEQUENCE IF NOT EXISTS bl_3nf.ce_cities_seq
START WITH 1
INCREMENT BY 1
MINVALUE 1
NO MAXVALUE;

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
 
CREATE SEQUENCE IF NOT EXISTS bl_3nf.ce_locations_seq
START WITH 1
INCREMENT BY 1
MINVALUE 1
NO MAXVALUE; 

CREATE TABLE IF NOT EXISTS bl_3nf.ce_curr_emp_profiles
  (
     curr_emp_profile_id     	INTEGER PRIMARY KEY,
     curr_employee_id			INTEGER NOT NULL,
     curr_employee_src_id 	    TEXT NOT NULL,
     source_system          	TEXT NOT NULL,
     source_entity          	TEXT NOT NULL,
     first_name  		    	TEXT NOT NULL,
     last_name					TEXT NOT NULL,
     full_name					TEXT GENERATED ALWAYS AS (first_name || ' ' || last_name) STORED,
     phone						TEXT NOT NULL,
     email						TEXT NOT NULL,    
     insert_dt              	TIMESTAMP WITH TIME ZONE NOT NULL
  ); 
 
CREATE SEQUENCE IF NOT EXISTS bl_3nf.ce_curr_emp_profiles_seq
START WITH 1
INCREMENT BY 1
MINVALUE 1
NO MAXVALUE;

CREATE TABLE IF NOT EXISTS bl_3nf.ce_products
  (
     product_id     			INTEGER NOT NULL,
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
     update_dt                  TIMESTAMP WITH TIME ZONE NOT NULL,
     CONSTRAINT prod_pk PRIMARY KEY (product_id, start_date)
  ); 
 
CREATE SEQUENCE IF NOT EXISTS bl_3nf.ce_products_seq
START WITH 1
INCREMENT BY 1
MINVALUE 1
NO MAXVALUE;

CREATE TABLE IF NOT EXISTS bl_3nf.ce_stores
  (
     store_id     				INTEGER NOT NULL,
     store_src_id 				TEXT NOT NULL,
     source_system          	TEXT NOT NULL,
     source_entity          	TEXT NOT NULL,
     name  		    			TEXT NOT NULL,
     start_date					DATE NOT NULL,
     end_date     				DATE NOT NULL,
     is_active					TEXT NOT NULL,
     contact_phone				TEXT NOT NULL,
     contact_email				TEXT NOT NULL,
     location_id 				INTEGER  NOT NULL REFERENCES bl_3nf.ce_locations(location_id) ON UPDATE CASCADE ON DELETE RESTRICT,
     payment_type_pref_id		INTEGER  NOT NULL REFERENCES bl_3nf.ce_payment_pref_types(payment_type_pref_id) ON UPDATE CASCADE ON DELETE RESTRICT,
     curr_emp_profile_id		INTEGER  NOT NULL REFERENCES bl_3nf.ce_curr_emp_profiles(curr_emp_profile_id) ON UPDATE CASCADE ON DELETE RESTRICT,
     opt_in_flag				TEXT NOT NULL,
     membership_flag			TEXT NOT NULL,     
     insert_dt              	TIMESTAMP WITH TIME ZONE NOT NULL,
     update_dt              	TIMESTAMP WITH TIME ZONE NOT NULL,
     CONSTRAINT stores_pk PRIMARY KEY (store_id, start_date)
  ); 
 
CREATE SEQUENCE IF NOT EXISTS bl_3nf.ce_stores_seq
START WITH 1
INCREMENT BY 1
MINVALUE 1
NO MAXVALUE;

CREATE TABLE IF NOT EXISTS bl_3nf.ce_vendors
  (
     vendor_id     			INTEGER NOT NULL,
     vendor_src_id 			TEXT NOT NULL,
     source_system          TEXT NOT NULL,
     source_entity          TEXT NOT NULL,
     name  		    		TEXT NOT NULL,
     start_date				DATE NOT NULL,
     end_date				DATE NOT NULL,
     is_active				TEXT NOT NULL,
     rating				    DECIMAL(2, 1) NOT NULL,
     size					TEXT NOT NULL,
     contact_phone			TEXT NOT NULL,
     contact_name			TEXT NOT NULL,
     homepage				TEXT NOT NULL,
     insert_dt              TIMESTAMP WITH TIME ZONE NOT NULL,
     update_dt              TIMESTAMP WITH TIME ZONE NOT NULL,
     CONSTRAINT vendors_pk PRIMARY KEY (vendor_id, start_date)     
  ); 
 
CREATE SEQUENCE IF NOT EXISTS bl_3nf.ce_vendors_seq
START WITH 1
INCREMENT BY 1
MINVALUE 1
NO MAXVALUE;

CREATE TABLE IF NOT EXISTS bl_3nf.ce_employees
  (
     employee_id     			INTEGER NOT NULL,
     employee_src_id 			TEXT NOT NULL,
     source_system          	TEXT NOT NULL,
     source_entity          	TEXT NOT NULL,
     first_name  		    	TEXT NOT NULL,
     last_name					TEXT NOT NULL,
     full_name					TEXT GENERATED ALWAYS AS (first_name || ' ' || last_name) STORED,
     gender						TEXT NOT NULL,
     dob						DATE NOT NULL,
     hire_date					DATE NOT NULL,
     start_date					DATE NOT NULL,
     end_date     				DATE NOT NULL,
     is_active					TEXT NOT NULL,
     phone						TEXT NOT NULL,
     email						TEXT NOT NULL,
     location_id 				INTEGER  NOT NULL REFERENCES bl_3nf.ce_locations(location_id) ON UPDATE CASCADE ON DELETE RESTRICT,
     position_id				INTEGER  NOT NULL REFERENCES bl_3nf.ce_positions(position_id) ON UPDATE CASCADE ON DELETE RESTRICT,
     vacation_hours				INTEGER NOT NULL,
     sick_leave_hours			INTEGER NOT NULL,     
     insert_dt              	TIMESTAMP WITH TIME ZONE NOT NULL,
     update_dt              	TIMESTAMP WITH TIME ZONE NOT NULL,
     CONSTRAINT emp_pk PRIMARY KEY (employee_id, start_date)     
  ); 
 
CREATE SEQUENCE IF NOT EXISTS bl_3nf.ce_employees_seq
START WITH 1
INCREMENT BY 1
MINVALUE 1
NO MAXVALUE;

CREATE TABLE IF NOT EXISTS bl_3nf.ce_shippers
  (
     shipper_id     			INTEGER NOT NULL,
     shipper_src_id 			TEXT NOT NULL,
     source_system          	TEXT NOT NULL,
     source_entity          	TEXT NOT NULL,
     name  		    			TEXT NOT NULL,
     start_date					DATE NOT NULL,
     end_date     				DATE NOT NULL,
     is_active					TEXT NOT NULL,
     rating				    	DECIMAL(2, 1) NOT NULL,
     ship_base					DECIMAL(10, 2) NOT NULL,
     ship_rate					DECIMAL(10, 2) NOT NULL,
     contact_phone				TEXT NOT NULL,
     contact_name				TEXT NOT NULL,
     curr_region_id			    INTEGER NOT NULL REFERENCES bl_3nf.ce_regions(region_id) ON UPDATE CASCADE ON DELETE RESTRICT,
     historical_region_id		INTEGER NOT NULL REFERENCES bl_3nf.ce_regions(region_id) ON UPDATE CASCADE ON DELETE RESTRICT,
     insert_dt              	TIMESTAMP WITH TIME ZONE NOT NULL,
     update_dt              	TIMESTAMP WITH TIME ZONE NOT NULL,
     CONSTRAINT ship_pk PRIMARY KEY (shipper_id, start_date)     
  ); 
 
CREATE SEQUENCE IF NOT EXISTS bl_3nf.ce_shippers_seq
START WITH 1
INCREMENT BY 1
MINVALUE 1
NO MAXVALUE;

CREATE TABLE IF NOT EXISTS bl_3nf.ce_sales
  (
     transaction_id     			INTEGER PRIMARY KEY,
     transaction_src_id 			TEXT NOT NULL,
     source_system          		TEXT NOT NULL,
     source_entity          		TEXT NOT NULL,
     status_id 				        INTEGER  NOT NULL,
     type_id       				    INTEGER  NOT NULL,
     tax_indicator_id			    INTEGER  NOT NULL,
     shipper_id						INTEGER  NOT NULL,
     event_date						DATE NOT NULL,
     store_id						INTEGER  NOT NULL,
     employee_id					INTEGER  NOT NULL,
     vendor_id						INTEGER  NOT NULL,
     product_id						INTEGER  NOT NULL,
     quantity_sold					INTEGER  NOT NULL,
     total_amount					DECIMAL(10, 2) NOT NULL,
     volume_sold_liters				DECIMAL(10, 2) NOT NULL,
     volume_sold_gallons			DECIMAL(10, 2) NOT NULL,
     state_bottle_cost				DECIMAL(10, 2) NOT NULL,
     state_bottle_retail			DECIMAL(10, 2) NOT NULL,
     insert_dt              		TIMESTAMP WITH TIME ZONE NOT NULL
  ); 
 
CREATE SEQUENCE IF NOT EXISTS bl_3nf.ce_sales_seq
START WITH 1
INCREMENT BY 1
MINVALUE 1
NO MAXVALUE;
	
GET DIAGNOSTICS rows_affected = row_count;

RAISE NOTICE '% row(s) inserted', rows_affected;
output_message = 'Success';

CALL bl_cl.logging_insertion(schema_name, 
							 table_name,
							 command_name,
							 rows_affected,
							 error_type,
							 output_message);

EXCEPTION
WHEN OTHERS THEN
     GET stacked DIAGNOSTICS error_type = pg_exception_context,
                             output_message = message_text;

RAISE NOTICE 'Error: % %', error_type, output_message;

CALL bl_cl.logging_insertion(schema_name, 
							 table_name,
							 command_name,
							 rows_affected,
							 error_type,
							 output_message);
END;
$$;