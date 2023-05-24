CREATE OR REPLACE PROCEDURE bl_cl.bl_dim_schema()

LANGUAGE plpgsql 
AS $$

DECLARE
  schema_name   TEXT := 'bl_dim';
  table_name    TEXT := 'All tables';
  command_name  TEXT := 'create';
  rows_affected INTEGER := 0;
  error_type    TEXT := '';
  output_message TEXT := '';

BEGIN 
	
-- DROP SCHEMA IF EXISTS bl_3nf CASCADE;
CREATE SCHEMA IF NOT EXISTS bl_dim; 	

-- tables and sequences
CREATE TABLE IF NOT EXISTS bl_dim.dim_dates (
  date_id                   INTEGER PRIMARY KEY,
  full_date                 DATE,
  day_name                  TEXT NOT NULL,
  day_of_week               INTEGER NOT NULL,
  day_of_month              INTEGER NOT NULL,
  day_of_quarter            INTEGER NOT NULL,
  day_of_year               INTEGER NOT NULL,
  week_of_month             INTEGER NOT NULL,
  week_of_year              INTEGER NOT NULL,
  month_actual              INTEGER NOT NULL,
  month_name                TEXT NOT NULL,
  month_name_abbreviated    TEXT NOT NULL,
  quarter_actual            INTEGER NOT NULL,
  quarter_name              TEXT NOT NULL,
  year_actual               INTEGER NOT NULL,
  first_day_of_week         DATE NOT NULL,
  last_day_of_week          DATE NOT NULL,
  first_day_of_month        DATE NOT NULL,
  last_day_of_month         DATE NOT NULL,
  first_day_of_quarter      DATE NOT NULL,
  last_day_of_quarter       DATE NOT NULL,
  first_day_of_year         DATE NOT NULL,
  last_day_of_year          DATE NOT NULL,
  mmyyyy                    TEXT NOT NULL,
  mmddyyyy                  TEXT NOT NULL,
  weekend_indr              TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS bl_dim.dim_employees_scd
  (
     employee_surr_id     		INTEGER PRIMARY KEY,
     employee_id 				INTEGER NOT NULL,
     source_system				TEXT NOT NULL,
     source_entity				TEXT NOT NULL,
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
     postal_code				TEXT NOT NULL,				
     city						TEXT NOT NULL,
     address					TEXT NOT NULL,
     position_name 				TEXT NOT NULL,
     vacation_hours				INTEGER NOT NULL,
     sick_leave_hours			INTEGER NOT NULL,     
     insert_dt              	TIMESTAMP WITH TIME ZONE NOT NULL,
     update_dt              	TIMESTAMP WITH TIME ZONE NOT NULL
  ); 
 
CREATE SEQUENCE IF NOT EXISTS bl_dim.dim_employees_seq
START WITH 1
INCREMENT BY 1
MINVALUE 1
NO MAXVALUE;

CREATE TABLE IF NOT EXISTS bl_dim.dim_junk
  (
     junk_surr_id     			INTEGER PRIMARY KEY,
     source_system				TEXT NOT NULL,
     source_entity				TEXT NOT NULL,
	 transaction_status			TEXT NOT NULL,
	 transaction_type		    TEXT NOT NULL,
	 tax_type					TEXT NOT NULL,
     insert_dt           	    TIMESTAMP WITH TIME ZONE NOT NULL,
     update_dt           	    TIMESTAMP WITH TIME ZONE NOT NULL
  ); 
 
CREATE SEQUENCE IF NOT EXISTS bl_dim.dim_junk_seq
START WITH 1
INCREMENT BY 1
MINVALUE 1
NO MAXVALUE;

CREATE TABLE IF NOT EXISTS bl_dim.dim_products_scd (
    product_surr_id 			INTEGER PRIMARY KEY,
    product_id 					INTEGER NOT NULL,
    source_system				TEXT NOT NULL,
    source_entity				TEXT NOT NULL,
    product_desc 				TEXT NOT NULL,
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
 
CREATE SEQUENCE IF NOT EXISTS bl_dim.dim_products_seq
START WITH 1
INCREMENT BY 1
MINVALUE 1
NO MAXVALUE;

CREATE TABLE IF NOT EXISTS bl_dim.dim_shippers_scd
  (
     shipper_surr_id     		INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
     shipper_id 			    INTEGER NOT NULL,
     source_system				TEXT NOT NULL,
     source_entity				TEXT NOT NULL,
     name  		    			TEXT NOT NULL,
     start_date					DATE NOT NULL,
     end_date     				DATE NOT NULL,
     is_active					TEXT NOT NULL,
     rating				    	DECIMAL(2, 1) NOT NULL,
     ship_base					DECIMAL(10, 2) NOT NULL,
     ship_rate					DECIMAL(10, 2) NOT NULL,     
     contact_phone				TEXT NOT NULL,
     contact_name				TEXT NOT NULL,     
     current_region				TEXT NOT NULL,
     historic_region            TEXT NOT NULL,
     insert_dt              	TIMESTAMP WITH TIME ZONE NOT NULL,
     update_dt              	TIMESTAMP WITH TIME ZONE NOT NULL
  );
 
CREATE SEQUENCE IF NOT EXISTS bl_dim.dim_shippers_seq
START WITH 1
INCREMENT BY 1
MINVALUE 1
NO MAXVALUE;

CREATE TABLE IF NOT EXISTS bl_dim.dim_stores_scd
  (
     store_surr_id     			INTEGER PRIMARY KEY,
     store_id 					INTEGER NOT NULL,
     source_system				TEXT NOT NULL,
     source_entity				TEXT NOT NULL,
     name  		    			TEXT NOT NULL,
     start_date					DATE NOT NULL,
     end_date     				DATE NOT NULL,
     is_active					TEXT NOT NULL,
     contact_phone				TEXT NOT NULL,
     contact_email				TEXT NOT NULL,
	 region						TEXT NOT NULL,
	 county_code				TEXT NOT NULL,
	 county_name				TEXT NOT NULL,
	 postal_code				TEXT NOT NULL,
	 city						TEXT NOT NULL,
	 address					TEXT NOT NULL,
     payment_type_pref			TEXT NOT NULL,
     membership_flag			TEXT NOT NULL,    
     opt_in_flag				TEXT NOT NULL, 
     curr_emp_first_name		TEXT NOT NULL,
     curr_emp_last_name			TEXT NOT NULL,
     curr_emp_full_name			TEXT NOT NULL,
     curr_emp_phone				TEXT NOT NULL,
     curr_emp_email				TEXT NOT NULL,
     insert_dt              	TIMESTAMP WITH TIME ZONE NOT NULL,
     update_dt              	TIMESTAMP WITH TIME ZONE NOT NULL
  ); 
 
CREATE SEQUENCE IF NOT EXISTS bl_dim.dim_stores_seq
START WITH 1
INCREMENT BY 1
MINVALUE 1
NO MAXVALUE;

CREATE TABLE IF NOT EXISTS bl_dim.dim_vendors_scd
  (
     vendor_surr_id     		INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
     vendor_id 					INTEGER NOT NULL,
     source_system				TEXT NOT NULL,
     source_entity				TEXT NOT NULL,
     name  		    			TEXT NOT NULL,
     start_date					DATE NOT NULL,
     end_date					DATE NOT NULL,
     is_active					TEXT NOT NULL,
     contact_phone				TEXT NOT NULL,
     contact_name				TEXT NOT NULL,
     rating					    DECIMAL(2, 1) NOT NULL,
     size						TEXT NOT NULL,
     homepage					TEXT NOT NULL,
     insert_dt           	    TIMESTAMP WITH TIME ZONE NOT NULL,
     update_dt           	    TIMESTAMP WITH TIME ZONE NOT NULL
  ); 

CREATE SEQUENCE IF NOT EXISTS bl_dim.dim_vendors_seq
START WITH 1
INCREMENT BY 1
MINVALUE 1
NO MAXVALUE;

CREATE TABLE IF NOT EXISTS bl_dim.fct_sales
  (
     transaction_id     			INTEGER NOT NULL,
     source_system					TEXT NOT NULL,
     source_entity					TEXT NOT NULL,
  	 junk_surr_id     				INTEGER  NOT NULL,
     shipper_surr_id				INTEGER  NOT NULL,
     date_id						INTEGER  NOT NULL,
     store_surr_id					INTEGER  NOT NULL,
     employee_surr_id				INTEGER  NOT NULL,
     vendor_surr_id					INTEGER  NOT NULL,
     product_surr_id				INTEGER  NOT NULL,
     event_date						DATE NOT NULL,
     quantity_sold					INTEGER NOT NULL,
     total_amount					DECIMAL(10, 2) NOT NULL,
     volume_sold_liters				DECIMAL(10, 2) NOT NULL,
     volume_sold_gallons			DECIMAL(10, 2) NOT NULL,
     state_bottle_cost				DECIMAL(10, 2) NOT NULL,
     state_bottle_retail			DECIMAL(10, 2) NOT NULL,
     insert_dt           	    	TIMESTAMP WITH TIME ZONE NOT NULL
  )
 	 PARTITION BY RANGE (event_date); 

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