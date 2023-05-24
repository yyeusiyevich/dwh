-- DROP PROCEDURE IF EXISTS bl_cl.sa_northwest_load();
CREATE OR REPLACE PROCEDURE bl_cl.sa_northwest_load()

LANGUAGE plpgsql 
AS $$

DECLARE
  schema_name   TEXT := 'sa_northwest_sales';
  table_name    TEXT := 'src_northwest_sales';
  command_name  TEXT := 'insert';
  rows_affected INTEGER := 0;
  error_type    TEXT := '';
  output_message TEXT := '';

BEGIN 
	
INSERT INTO sa_northwest_sales.src_northwest_sales (
	   invoice_and_item_number,
	   date,
	   store_number,
	   store_name,
	   address,
	   city,
	   zip_code,
	   county_number,
	   county,
	   state_bottle_cost,
	   state_bottle_retail,
	   bottles_sold,
	   sale_dollars,
	   volume_sold_liters,
	   volume_sold_gallons,
       transaction_type_id,
       transaction_type ,
       transaction_status_id,
	   status_name,
	   store_location_id,
	   region_id,
	   region_name,
	   payment_type_pref_id,
	   payment_type_pref,
	   opt_in_flag,
	   membership_flag,
	   store_contact_email,
	   store_contact_phone,
	   vendor_number,
	   vendor_name,
	   vendor_rating,
	   vendor_size,
	   vendor_contact,
	   vendor_contact_name,
	   vendor_web,
	   shipper_id,
	   shipper_name,
	   shipper_rating,
	   ship_base,
	   ship_rate,
	   shipper_phone,
	   shipper_contact_name,
	   employee_id,
	   position_id,
	   position_name,
	   emp_first_name,
	   emp_last_name,
	   emp_gender,
	   emp_dob,
	   hire_date,
	   vacation_hours,
	   sick_leave_hours,
	   emp_address,
	   emp_city,
	   emp_postal,
	   emp_location_id,
	   emp_email,
	   emp_phone,
	   city_id,
	   county_id,
	   emp_city_id,
	   item_id,
	   subcategory_id,
	   subcategory_name,
	   item_description,
	   category_id,
	   category_name,
	   pack,
	   bottle_volume_ml,
	   safety_stock_lvl,
	   reorder_point,
	   product_on_sale,
	   tax_info,
	   tax_indicator_id,	
	   insert_dt)
SELECT invoice_and_item_number,
	   date,
	   store_number,
	   store_name,
	   address,
	   city,
	   zip_code,
	   county_number,
	   county,
	   state_bottle_cost,
	   state_bottle_retail,
	   bottles_sold,
	   sale_dollars,
	   volume_sold_liters,
	   volume_sold_gallons,
       transaction_type_id,
       transaction_type ,
       transaction_status_id,
	   status_name,
	   store_location_id,
	   region_id,
	   region_name,
	   payment_type_pref_id,
	   payment_type_pref,
	   opt_in_flag,
	   membership_flag,
	   store_contact_email,
	   store_contact_phone,
	   vendor_number,
	   vendor_name,
	   vendor_rating,
	   vendor_size,
	   vendor_contact,
	   vendor_contact_name,
	   vendor_web,
	   shipper_id,
	   shipper_name,
	   shipper_rating,
	   ship_base,
	   ship_rate,
	   shipper_phone,
	   shipper_contact_name,
	   employee_id,
	   position_id,
	   position_name,
	   emp_first_name,
	   emp_last_name,
	   emp_gender,
	   emp_dob,
	   hire_date,
	   vacation_hours,
	   sick_leave_hours,
	   emp_address,
	   emp_city,
	   emp_postal,
	   emp_location_id,
	   emp_email,
	   emp_phone,
	   city_id,
	   county_id,
	   emp_city_id,
	   item_id,
	   subcategory_id,
	   subcategory_name,
	   item_description,
	   category_id,
	   category_name,
	   pack,
	   bottle_volume_ml,
	   safety_stock_lvl,
	   reorder_point,
	   product_on_sale,
	   tax_info,
	   tax_indicator_id,
	   CURRENT_TIMESTAMP
FROM sa_northwest_sales.ext_northwest_sales AS ext
WHERE NOT EXISTS (SELECT 1
                  FROM  sa_northwest_sales.src_northwest_sales AS src
                  WHERE UPPER(src.invoice_and_item_number) = UPPER(ext.invoice_and_item_number));

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