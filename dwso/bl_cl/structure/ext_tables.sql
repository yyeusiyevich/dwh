CREATE OR REPLACE PROCEDURE bl_cl.ext_creation()

LANGUAGE plpgsql 
AS $$

DECLARE
  schema_name   TEXT := 'sa_source_sales';
  table_name    TEXT := 'ext_tables';
  command_name  TEXT := 'create';
  rows_affected INTEGER := 0;
  error_type    TEXT := '';
  output_message TEXT := '';

BEGIN 
	
-- create ext/src schemas
CREATE SCHEMA IF NOT EXISTS sa_northwest_sales;
CREATE SCHEMA IF NOT EXISTS sa_iowalakes_sales;
	
-- foreign data wrapper (FDW) allows for accessing CSV files from a foreign server
CREATE EXTENSION IF NOT EXISTS file_fdw;

-- create a foreign server that uses the FDW to access files
-- DROP SERVER IF EXISTS fdw_server CASCADE;
CREATE SERVER IF NOT EXISTS fdw_server
FOREIGN DATA WRAPPER file_fdw;

-- create a user mapping for the current user to the foreign server
CREATE USER MAPPING IF NOT EXISTS FOR CURRENT_USER
SERVER fdw_server;	

-- DROP FOREIGN TABLE IF EXISTS sa_northwest_sales.ext_northwest_sales CASCADE;
CREATE FOREIGN TABLE IF NOT EXISTS sa_northwest_sales.ext_northwest_sales (
  invoice_and_item_number TEXT,
  date TEXT,
  store_number TEXT,
  store_name TEXT,
  address TEXT,
  city TEXT,
  zip_code TEXT,
  county_number TEXT,
  county TEXT,
  state_bottle_cost TEXT,
  state_bottle_retail TEXT,
  bottles_sold TEXT,
  sale_dollars TEXT,
  volume_sold_liters TEXT,
  volume_sold_gallons TEXT,
  transaction_type_id TEXT,
  transaction_type TEXT,
  transaction_status_id TEXT,
  status_name TEXT,
  store_location_id TEXT,
  region_id TEXT,
  region_name TEXT,
  payment_type_pref_id TEXT,
  payment_type_pref	TEXT,
  opt_in_flag TEXT,
  membership_flag TEXT,
  store_contact_email TEXT,
  store_contact_phone TEXT,
  vendor_number TEXT,
  vendor_name TEXT,
  vendor_rating TEXT,
  vendor_size TEXT,
  vendor_contact TEXT,
  vendor_contact_name TEXT,
  vendor_web TEXT,
  shipper_id TEXT,
  shipper_name TEXT,
  shipper_rating TEXT,
  ship_base TEXT,
  ship_rate TEXT,
  shipper_phone TEXT,
  shipper_contact_name TEXT,
  employee_id TEXT,
  position_id TEXT,
  position_name TEXT,
  emp_first_name TEXT,
  emp_last_name TEXT,
  emp_gender TEXT,
  emp_dob TEXT,
  hire_date TEXT,
  vacation_hours TEXT,
  sick_leave_hours TEXT,
  emp_address TEXT,
  emp_city TEXT,
  emp_postal TEXT,
  emp_location_id TEXT,
  emp_email TEXT,
  emp_phone TEXT,
  city_id TEXT,
  county_id TEXT,
  emp_city_id TEXT,
  item_id TEXT,
  subcategory_id TEXT,
  subcategory_name TEXT,
  item_description TEXT,
  category_id TEXT,
  category_name TEXT,
  pack TEXT,
  bottle_volume_ml TEXT,
  safety_stock_lvl TEXT,
  reorder_point TEXT,
  product_on_sale TEXT,
  tax_info TEXT,
  tax_indicator_id TEXT
) 
SERVER csv_server
OPTIONS (
    filename 'C:\Users\Public\iowa_northwest_1705.csv', -- northwest_sample_1805 iowa_northwest_1705
    format 'csv',
    HEADER 'true',
    ENCODING 'UTF8'
);

-- DROP FOREIGN TABLE IF EXISTS sa_iowalakes_sales.ext_iowalakes_sales CASCADE;
CREATE FOREIGN TABLE IF NOT EXISTS sa_iowalakes_sales.ext_iowalakes_sales (
  invoice_and_item_number TEXT,
  date TEXT,
  store_number TEXT,
  store_name TEXT,
  address TEXT,
  city TEXT,
  zip_code TEXT,
  county_number TEXT,
  county TEXT,
  state_bottle_cost TEXT,
  state_bottle_retail TEXT,
  bottles_sold TEXT,
  sale_dollars TEXT,
  volume_sold_liters TEXT,
  volume_sold_gallons TEXT,
  transaction_type_id TEXT,
  transaction_type TEXT,
  transaction_status_id TEXT,
  status_name TEXT,
  store_location_id TEXT,
  region_id TEXT,
  region_name TEXT,
  payment_type_pref_id TEXT,
  payment_type_pref	TEXT,
  opt_in_flag TEXT,
  membership_flag TEXT,
  store_contact_email TEXT,
  store_contact_phone TEXT,
  vendor_number TEXT,
  vendor_name TEXT,
  vendor_rating TEXT,
  vendor_size TEXT,
  vendor_contact TEXT,
  vendor_contact_name TEXT,
  vendor_web TEXT,
  shipper_id TEXT,
  shipper_name TEXT,
  shipper_rating TEXT,
  ship_base TEXT,
  ship_rate TEXT,
  shipper_phone TEXT,
  shipper_contact_name TEXT,
  employee_id TEXT,
  position_id TEXT,
  position_name TEXT,
  emp_first_name TEXT,
  emp_last_name TEXT,
  emp_gender TEXT,
  emp_dob TEXT,
  hire_date TEXT,
  vacation_hours TEXT,
  sick_leave_hours TEXT,
  emp_address TEXT,
  emp_city TEXT,
  emp_postal TEXT,
  emp_location_id TEXT,
  emp_email TEXT,
  emp_phone TEXT,
  city_id TEXT,
  county_id TEXT,
  emp_city_id TEXT,
  item_id TEXT,
  subcategory_id TEXT,
  subcategory_name TEXT,
  item_description TEXT,
  category_id TEXT,
  category_name TEXT,
  pack TEXT,
  bottle_volume_ml TEXT,
  safety_stock_lvl TEXT,
  reorder_point TEXT,
  product_on_sale TEXT,
  tax_info TEXT,
  tax_indicator_id TEXT
) 
SERVER csv_server
OPTIONS (
    filename 'C:\Users\Public\iowa_iowalakes_1705.csv', --iowalakes_sample_1805 iowa_iowalakes_1705
    format 'csv',
    HEADER 'true',
    ENCODING 'UTF8'
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