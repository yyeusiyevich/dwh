-- DROP PROCEDURE IF EXISTS bl_cl.bl3nf_init_load();
CREATE OR REPLACE PROCEDURE bl_cl.bl3nf_init_load()

LANGUAGE plpgsql 
AS $$

DECLARE
  schema_name   TEXT := 'bl_3nf';
  table_name    TEXT := 'All tables';
  command_name  TEXT := 'insert';
  rows_affected INTEGER := 0;
  error_type    TEXT := '';
  output_message TEXT := '';

BEGIN 
	
INSERT INTO bl_3nf.ce_vendors 
            (vendor_id,
             vendor_src_id,
             source_system,
             source_entity,
             name,
             start_date,
             end_date,
             is_active,
             rating,
             size,
      		 contact_phone,
      		 contact_name,
      		 homepage,
             insert_dt, 
             update_dt)      		 
VALUES(-1, -1, 'MANUAL', 'MANUAL', 'N/A', '9999-12-31', '9999-12-31', 'N', -1, 'N/A', 'N/A', 'N/A', 'N/A', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
ON CONFLICT ON CONSTRAINT vendors_pk DO NOTHING;  

INSERT INTO bl_3nf.ce_transaction_types
            (type_id,
             type_src_id,
             source_system,
             source_entity,
             type_name,
             insert_dt, 
             update_dt)           
VALUES (-1, -1, 'MANUAL', 'MANUAL', 'N/A', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
ON CONFLICT ON CONSTRAINT ce_transaction_types_pkey DO NOTHING;  

INSERT INTO bl_3nf.ce_transaction_statuses
            (status_id,
             status_src_id,
             source_system,
             source_entity,
             status_name,
             insert_dt, 
             update_dt)             
VALUES (-1, -1, 'MANUAL', 'MANUAL', 'N/A', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
ON CONFLICT ON CONSTRAINT ce_transaction_statuses_pkey DO NOTHING;  

INSERT INTO bl_3nf.ce_tax_indicators
            (tax_indicator_id,
             tax_indicator_src_id,
             source_system,
             source_entity,
             tax_type,
             insert_dt, 
             update_dt)             
VALUES (-1, -1, 'MANUAL', 'MANUAL', 'N/A', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
ON CONFLICT ON CONSTRAINT ce_tax_indicators_pkey DO NOTHING;   

INSERT INTO bl_3nf.ce_regions
            (region_id,
             region_src_id,
             source_system,
             source_entity,
             region_name,
             insert_dt, 
             update_dt)             
VALUES(-1, -1, 'MANUAL', 'MANUAL', 'N/A', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
ON CONFLICT ON CONSTRAINT ce_regions_pkey DO NOTHING;   

INSERT INTO bl_3nf.ce_counties
            (county_id,
             county_src_id,
             source_system,
             source_entity,
             region_id,
             county_code,
             county_name,
             insert_dt, 
             update_dt)           
VALUES(-1, -1, 'MANUAL', 'MANUAL', -1, 'N/A', 'N/A', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
ON CONFLICT ON CONSTRAINT ce_counties_pkey DO NOTHING;     

INSERT INTO bl_3nf.ce_cities
            (city_id,
             city_src_id,
             source_system,
             source_entity,
             county_id,
             city_name,
             insert_dt, 
             update_dt)        
VALUES(-1, -1, 'MANUAL', 'MANUAL', -1, 'N/A', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
ON CONFLICT ON CONSTRAINT ce_cities_pkey DO NOTHING;   

INSERT INTO bl_3nf.ce_locations 
            (location_id,
             location_src_id,
             source_system,
             source_entity,
             address,
             postal_code,
             city_id,             
      		 insert_dt,
      		 update_dt)
VALUES(-1, -1, 'MANUAL', 'MANUAL', 'N/A', 'N/A', -1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
ON CONFLICT ON CONSTRAINT ce_locations_pkey DO NOTHING;   

INSERT INTO bl_3nf.ce_payment_pref_types
            (payment_type_pref_id,
             payment_type_pref_src_id,
             source_system,
             source_entity,
             payment_type_pref, 
             insert_dt, 
             update_dt)
VALUES (-1, -1, 'MANUAL', 'MANUAL', 'N/A', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
ON CONFLICT ON CONSTRAINT ce_payment_pref_types_pkey DO NOTHING;  

INSERT INTO bl_3nf.ce_curr_emp_profiles (
			curr_emp_profile_id, 
			curr_employee_id, 
			curr_employee_src_id, 
			source_system, 
			source_entity, 
			first_name, 
			last_name,
			phone, 
			email, 
			insert_dt)
VALUES (-1, -1, 'N/A', 'MANUAL', 'MANUAL', 'N/A', 'N/A', 'N/A', 'N/A', CURRENT_TIMESTAMP)
ON CONFLICT ON CONSTRAINT ce_curr_emp_profiles_pkey DO NOTHING; 

INSERT INTO bl_3nf.ce_stores 
            (store_id,
             store_src_id,
             source_system,
             source_entity,
             name,
             start_date,
             end_date,             
             is_active,
      		 contact_phone,
      		 contact_email,
      		 location_id,
      		 payment_type_pref_id,
      		 curr_emp_profile_id,
      		 opt_in_flag,
      		 membership_flag,
      		 insert_dt,
      		 update_dt)
VALUES(-1, -1, 'MANUAL', 'MANUAL', 'N/A', '9999-12-31', '9999-12-31', 'N', 'N/A', 'N/A', -1, -1, -1, 'N', 'N', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
ON CONFLICT ON CONSTRAINT stores_pk DO NOTHING;  

INSERT INTO bl_3nf.ce_shippers 
            (shipper_id,
             shipper_src_id,
             source_system,
             source_entity,
             name,
             start_date,
             end_date,             
             is_active,
             rating,
             ship_base,
             ship_rate,
      		 contact_phone,
      		 contact_name,
      		 curr_region_id,
      		 historical_region_id,
      		 insert_dt,
      		 update_dt)
VALUES(-1, -1, 'MANUAL', 'MANUAL', 'N/A', '9999-12-31', '9999-12-31', 'N', -1, -1, -1, 'N/A', 'N/A', -1, -1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
ON CONFLICT ON CONSTRAINT ship_pk DO NOTHING;  

INSERT INTO bl_3nf.ce_product_categories
            (product_category_id,
             product_category_src_id,
             source_system,
             source_entity,
             category_name,
             insert_dt, 
             update_dt)
VALUES (-1, 'N/A', 'MANUAL', 'MANUAL', 'N/A', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
ON CONFLICT ON CONSTRAINT ce_product_categories_pkey DO NOTHING;  

INSERT INTO bl_3nf.ce_products
            (product_id,
             product_src_id,
             product_desc,
             source_system,
             source_entity,
             category_id,
             pack,
             bottle_volume,
             safety_stock_lvl,
             reorder_point,
             start_date,
             end_date,
             is_active,
             on_sale,
             insert_dt, 
             update_dt)             
VALUES(-1, -1, 'N/A', 'MANUAL', 'MANUAL', -1, -1, -1, -1, -1, '9999-12-31', '9999-12-31', 'N', 'N', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
ON CONFLICT ON CONSTRAINT prod_pk DO NOTHING;  

INSERT INTO bl_3nf.ce_product_subcategories
            (product_subcategory_id,
             product_subcategory_src_id,
             source_system,
             source_entity,
             category_id,
             subcategory_name,
             insert_dt, 
             update_dt)
VALUES (-1, -1, 'MANUAL', 'MANUAL', -1, 'N/A', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
ON CONFLICT ON CONSTRAINT ce_product_subcategories_pkey DO NOTHING;  

INSERT INTO bl_3nf.ce_positions
            (position_id,
             position_src_id,
             source_system,
             source_entity,
             position_name, 
             insert_dt, 
             update_dt)
VALUES (-1, -1, 'MANUAL', 'MANUAL', 'N/A', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
ON CONFLICT ON CONSTRAINT ce_positions_pkey DO NOTHING;  

INSERT INTO bl_3nf.ce_employees 
            (employee_id,
             employee_src_id,
             source_system,
             source_entity,
             first_name,
             last_name,
             gender,
             dob,
             hire_date,
             start_date,
             end_date,             
             is_active,
      		 phone,
      		 email,
      		 location_id,
      		 position_id,
      		 vacation_hours,
      		 sick_leave_hours,
      		 insert_dt,
      		 update_dt)
VALUES(-1, -1, 'MANUAL', 'MANUAL', 'N/A', 'N/A', 'N/A', '9999-12-31', '9999-12-31', '9999-12-31', '9999-12-31', 'N', 'N/A', 'N/A', -1, -1, -1, -1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
ON CONFLICT ON CONSTRAINT emp_pk DO NOTHING;         
	
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