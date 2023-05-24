CREATE OR REPLACE PROCEDURE bl_cl.prm_mta_insertion()

LANGUAGE plpgsql 
AS $$

DECLARE
  schema_name   TEXT := 'bl_cl';
  table_name    TEXT := 'prm_mta_incremental_load';
  command_name  TEXT := 'create';
  rows_affected INTEGER := 0;
  error_type    TEXT := '';
  output_message TEXT := '';

BEGIN 
	
TRUNCATE bl_cl.prm_mta_incremental_load;

INSERT INTO bl_cl.prm_mta_incremental_load
VALUES 
	('sa_northwest_sales', 'src_northwest_sales', 'ce_cities', 'MANUAL', DATE '1900-01-01'),
	('sa_iowalakes_sales', 'src_iowalakes_sales', 'ce_cities', 'MANUAL', DATE '1900-01-01'),
	('sa_northwest_sales', 'src_northwest_sales', 'ce_counties', 'MANUAL', DATE '1900-01-01'),
	('sa_iowalakes_sales', 'src_iowalakes_sales', 'ce_counties', 'MANUAL', DATE '1900-01-01'),
	('sa_northwest_sales', 'src_northwest_sales', 'ce_curr_emp_profiles', 'MANUAL', DATE '1900-01-01'),
	('sa_iowalakes_sales', 'src_iowalakes_sales', 'ce_curr_emp_profiles', 'MANUAL', DATE '1900-01-01'),
	('sa_northwest_sales', 'src_northwest_sales', 'ce_employees', 'MANUAL', DATE '1900-01-01'),
	('sa_iowalakes_sales', 'src_iowalakes_sales', 'ce_employees', 'MANUAL', DATE '1900-01-01'),
	('sa_northwest_sales', 'src_northwest_sales', 'ce_locations', 'MANUAL', DATE '1900-01-01'),
	('sa_iowalakes_sales', 'src_iowalakes_sales', 'ce_locations', 'MANUAL', DATE '1900-01-01'),
	('sa_northwest_sales', 'src_northwest_sales', 'ce_payment_pref_types', 'MANUAL', DATE '1900-01-01'),
	('sa_iowalakes_sales', 'src_iowalakes_sales', 'ce_payment_pref_types', 'MANUAL', DATE '1900-01-01'),
	('sa_northwest_sales', 'src_northwest_sales', 'ce_positions', 'MANUAL', DATE '1900-01-01'),
	('sa_iowalakes_sales', 'src_iowalakes_sales', 'ce_positions', 'MANUAL', DATE '1900-01-01'),
	('sa_northwest_sales', 'src_northwest_sales', 'ce_product_categories', 'MANUAL', DATE '1900-01-01'),
	('sa_iowalakes_sales', 'src_iowalakes_sales', 'ce_product_categories', 'MANUAL', DATE '1900-01-01'),
	('sa_northwest_sales', 'src_northwest_sales', 'ce_product_subcategories', 'MANUAL', DATE '1900-01-01'),
	('sa_iowalakes_sales', 'src_iowalakes_sales', 'ce_product_subcategories', 'MANUAL', DATE '1900-01-01'),
	('sa_northwest_sales', 'src_northwest_sales', 'ce_products', 'MANUAL', DATE '1900-01-01'),
	('sa_iowalakes_sales', 'src_iowalakes_sales', 'ce_products', 'MANUAL', DATE '1900-01-01'),
	('sa_northwest_sales', 'src_northwest_sales', 'ce_regions', 'MANUAL', DATE '1900-01-01'),
	('sa_iowalakes_sales', 'src_iowalakes_sales', 'ce_regions', 'MANUAL', DATE '1900-01-01'),
	('sa_northwest_sales', 'src_northwest_sales', 'ce_sales', 'MANUAL', DATE '1900-01-01'),
	('sa_iowalakes_sales', 'src_iowalakes_sales', 'ce_sales', 'MANUAL', DATE '1900-01-01'),
	('sa_northwest_sales', 'src_northwest_sales', 'ce_shippers', 'MANUAL', DATE '1900-01-01'),
	('sa_iowalakes_sales', 'src_iowalakes_sales', 'ce_shippers', 'MANUAL', DATE '1900-01-01'),
	('sa_northwest_sales', 'src_northwest_sales', 'ce_stores', 'MANUAL', DATE '1900-01-01'),
	('sa_iowalakes_sales', 'src_iowalakes_sales', 'ce_stores', 'MANUAL', DATE '1900-01-01'),
	('sa_northwest_sales', 'src_northwest_sales', 'ce_tax_indicators', 'MANUAL', DATE '1900-01-01'),
    ('sa_iowalakes_sales', 'src_iowalakes_sales', 'ce_tax_indicators', 'MANUAL', DATE '1900-01-01'),
    ('sa_northwest_sales', 'src_northwest_sales', 'ce_transaction_statuses', 'MANUAL', DATE '1900-01-01'),
    ('sa_iowalakes_sales', 'src_iowalakes_sales', 'ce_transaction_statuses', 'MANUAL', DATE '1900-01-01'),
    ('sa_northwest_sales', 'src_northwest_sales', 'ce_transaction_types', 'MANUAL', DATE '1900-01-01'),
    ('sa_iowalakes_sales', 'src_iowalakes_sales', 'ce_transaction_types', 'MANUAL', DATE '1900-01-01'),
    ('sa_northwest_sales', 'src_northwest_sales', 'ce_vendors', 'MANUAL', DATE '1900-01-01'),
    ('sa_iowalakes_sales', 'src_iowalakes_sales', 'ce_vendors', 'MANUAL', DATE '1900-01-01');
    
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