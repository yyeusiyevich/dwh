-- DROP PROCEDURE bl_cl.bl_cl_schema_creation();
CREATE OR REPLACE PROCEDURE bl_cl.bl_cl_schema_creation()

LANGUAGE plpgsql 
AS $$

DECLARE
  schema_name   TEXT := 'bl_cl';
  table_name    TEXT := 'All structures';
  command_name  TEXT := 'create';
  rows_affected INTEGER := 0;
  error_type    TEXT := '';
  output_message TEXT := '';
 
BEGIN

CREATE TABLE IF NOT EXISTS bl_cl.logging (
				log_id serial4  NOT NULL,
				user_name 		TEXT NOT NULL,
				event_time 		TIMESTAMPTZ NOT NULL,
				schema_name 	TEXT NULL,
				table_name	    TEXT NULL,
				command_name 	TEXT NULL,
				rows_affected 	INT4 NULL,
				error_type	    TEXT NULL,
				output_message   TEXT NULL,
				CONSTRAINT logging_pkey PRIMARY KEY (log_id)
);

CREATE TABLE IF NOT EXISTS bl_cl.map_categories
				(
				category_src_id  TEXT,
				source_system    TEXT,
				source_table     TEXT,
				category_name    TEXT,
				insert_dt        TIMESTAMP NOT NULL,
				update_dt        TIMESTAMP NOT NULL
				);

CREATE TABLE IF NOT EXISTS bl_cl.map_subcategories
				(
				subcategory_src_id      TEXT,
				source_system           TEXT,
				source_table            TEXT,
				subcategory_name        TEXT,
				product_category_src_id TEXT,
				insert_dt 				TIMESTAMP NOT NULL,
				update_dt 				TIMESTAMP NOT NULL
				);			
			
CREATE TABLE IF NOT EXISTS bl_cl.map_positions
				(
				position_src_id      TEXT,
				source_system        TEXT,
				source_table         TEXT,
				position_name        TEXT,
				insert_dt 			 TIMESTAMP NOT NULL,
				update_dt 			 TIMESTAMP NOT NULL
				);		
			
CREATE TABLE IF NOT EXISTS bl_cl.map_payment_pref_types
				(
				payment_pref_type_src_id      TEXT,
				source_system        		  TEXT,
				source_table         		  TEXT,
				payment_pref_type        	  TEXT,
				insert_dt 					  TIMESTAMP NOT NULL,
				update_dt 					  TIMESTAMP NOT NULL
				);		
			
CREATE TABLE IF NOT EXISTS bl_cl.map_tax_indicators
				(
				tax_indicator_src_id 	      TEXT,
				source_system        		  TEXT,
				source_table         		  TEXT,
				tax_type        			  TEXT,
				insert_dt 					  TIMESTAMP NOT NULL,
				update_dt 					  TIMESTAMP NOT NULL
				);		
			
CREATE TABLE IF NOT EXISTS bl_cl.map_transaction_statuses
				(
				status_src_id      TEXT,
				source_system      TEXT,
				source_table       TEXT,
				status_name        TEXT,
				insert_dt 		   TIMESTAMP NOT NULL,
				update_dt 		   TIMESTAMP NOT NULL
				);		
			
CREATE TABLE IF NOT EXISTS bl_cl.map_transaction_types
				(
				type_src_id      	TEXT,
				source_system       TEXT,
				source_table        TEXT,
				type_name        	TEXT,
				insert_dt 			TIMESTAMP NOT NULL,
				update_dt 			TIMESTAMP NOT NULL
				);			
			
CREATE TABLE IF NOT EXISTS bl_cl.map_products
				(
				product_src_id     		TEXT,
				source_system      		TEXT,
				source_table        	TEXT,
				product_desc       		TEXT,
				product_category_src_id TEXT,
				pack					TEXT,
				bottle_volume			TEXT,
     			safety_stock_lvl		TEXT,
     			reorder_point			TEXT,
			    on_sale					TEXT,
				insert_dt 				TIMESTAMP NOT NULL,
				update_dt 				TIMESTAMP NOT NULL
				);	
			
CREATE TABLE IF NOT EXISTS bl_cl.map_curr_employees
				(
				store_src_id     		TEXT,
				employee_src_id    		TEXT,
				source_system			TEXT,
				source_table        	TEXT,
				emp_first_name       	TEXT,
				emp_last_name			TEXT,
				emp_phone				TEXT,
				emp_email				TEXT,
				insert_dt 				TIMESTAMP NOT NULL,
				update_dt 				TIMESTAMP NOT NULL
				);	
			
CREATE TABLE IF NOT EXISTS bl_cl.wrk_shippers
				(
				 shipper_id INTEGER,
				 shipper_src_id TEXT,
				 source_system TEXT,
				 source_entity TEXT,
				 name TEXT,
				 start_date DATE,
				 end_date DATE,
				 is_active TEXT,
				 rating NUMERIC(2, 1),
				 ship_base NUMERIC(10, 2),
				 ship_rate NUMERIC(10, 2),
				 contact_phone TEXT,
				 contact_name TEXT,
				 current_region TEXT,
				 historic_region TEXT,
				 insert_dt TIMESTAMP NOT NULL,
				 update_dt TIMESTAMP NOT NULL
				);	
			
CREATE TABLE IF NOT EXISTS bl_cl.wrk_employees
				(
				employee_id INTEGER,
				employee_src_id TEXT,
				source_system TEXT,
				source_entity TEXT,
				first_name TEXT,
				last_name TEXT,
				full_name TEXT,
				gender TEXT,
				dob DATE,
				hire_date DATE,
				start_date DATE,
				end_date DATE,
				is_active TEXT,
				phone TEXT,
				email TEXT,
				postal_code TEXT,
				city TEXT,
				address TEXT,
				position_name TEXT,
				vacation_hours INTEGER,
				sick_leave_hours INTEGER,
				insert_dt TIMESTAMP NOT NULL,
				update_dt TIMESTAMP NOT NULL
				);	
			
CREATE TABLE IF NOT EXISTS bl_cl.wrk_products
				(
				product_id INTEGER,
				product_src_id TEXT,
				product_desc TEXT,
				source_system TEXT,
				source_entity TEXT,
				category_name TEXT,
				subcategory_name TEXT,
				pack INTEGER,
				bottle_volume INTEGER,
				safety_stock_lvl INTEGER,
				reorder_point INTEGER,
				start_date DATE,
				end_date DATE,
				is_active TEXT,
				on_sale TEXT,
				insert_dt TIMESTAMP NOT NULL,
				update_dt TIMESTAMP NOT NULL
				);		
			
CREATE TABLE IF NOT EXISTS bl_cl.wrk_stores
				(
				store_id INTEGER,
				store_src_id TEXT,
				source_system TEXT,
				source_entity TEXT,
				name TEXT,
				start_date DATE,
				end_date DATE,
				is_active TEXT,
				contact_phone TEXT,
				contact_email TEXT,
				region_name TEXT,
				county_code TEXT,
				county_name TEXT,
				postal_code TEXT,
				city_name TEXT,
				address TEXT,
				payment_type_pref TEXT,
				membership_flag TEXT,
				opt_in_flag TEXT,
				curr_emp_first_name TEXT,
				curr_emp_last_name TEXT,
				curr_emp_full_name TEXT,
				curr_emp_phone TEXT,
				curr_emp_email TEXT,
				insert_dt TIMESTAMP NOT NULL,
				update_dt TIMESTAMP NOT NULL
				);	
			
CREATE TABLE IF NOT EXISTS bl_cl.wrk_junk
				(
				source_system TEXT NOT NULL,
				source_entity TEXT NOT NULL,
				tax_indicator_id INT NOT NULL,
				tax_type TEXT NOT NULL,
				status_id INT NOT NULL,
			    status_name TEXT NOT NULL,
				type_id INT NOT NULL,
				type_name TEXT NOT NULL,
				insert_dt TIMESTAMP NOT NULL,
				update_dt TIMESTAMP NOT NULL
				);		
			
CREATE TABLE IF NOT EXISTS  bl_cl.prm_mta_incremental_load(
							source_system TEXT,
							source_table TEXT,
							target_table TEXT,
							procedure_name TEXT,
							previous_loaded_dt TIMESTAMP WITHOUT TIME ZONE);	
						
CREATE TABLE IF NOT EXISTS  bl_cl.map_shippers
				(
				shipper_src_id     		TEXT,
				source_system      		TEXT,
				source_table        	TEXT,
				name       				TEXT,
				rating				    TEXT,
				ship_base				TEXT,
				ship_rate				TEXT,
     			contact_phone			TEXT,
     			contact_name			TEXT,
			    region_id				TEXT,
			    region_name				TEXT,
				insert_dt 				TIMESTAMP NOT NULL,
				update_dt 				TIMESTAMP NOT NULL
				);	

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