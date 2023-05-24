-- DROP PROCEDURE IF EXISTS bl_cl.bldim_init_load();
CREATE OR REPLACE PROCEDURE bl_cl.bldim_init_load()

LANGUAGE plpgsql 
AS $$

DECLARE
  schema_name   TEXT := 'bl_dim';
  table_name    TEXT := 'All tables';
  command_name  TEXT := 'insert';
  rows_affected INTEGER := 0;
  error_type    TEXT := '';
  output_message TEXT := '';

BEGIN 
	
INSERT INTO bl_dim.dim_dates (
  date_id,
  full_date,
  day_name,
  day_of_week,
  day_of_month,
  day_of_quarter,
  day_of_year,
  week_of_month,
  week_of_year,
  month_actual,
  month_name,
  month_name_abbreviated,
  quarter_actual,
  quarter_name,
  year_actual,
  first_day_of_week,
  last_day_of_week,
  first_day_of_month,
  last_day_of_month,
  first_day_of_quarter,
  last_day_of_quarter,
  first_day_of_year,
  last_day_of_year,
  mmyyyy,
  mmddyyyy,
  weekend_indr
)
VALUES (-1, '01/01/1900'::DATE, 'N/A', -1, -1, -1, -1, -1, -1, -1, 'N/A', 'N/A', -1, 'N/A', -1,
	    '01/01/1900'::DATE, '01/01/1900'::DATE, '01/01/1900'::DATE, '01/01/1900'::DATE, '01/01/1900'::DATE,
	    '01/01/1900'::DATE, '01/01/1900'::DATE, '01/01/1900'::DATE, 'N/A', 'N/A', 'N/A')
ON CONFLICT ON CONSTRAINT dim_dates_pkey DO NOTHING;

INSERT INTO bl_dim.dim_employees_scd (
  employee_surr_id,
  employee_id,
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
  postal_code,
  city,
  address,
  position_name,
  vacation_hours,
  sick_leave_hours,
  insert_dt,
  update_dt
)
VALUES 
	   (-1, -1, 'MANUAL', 'MANUAL', 'N/A', 'N/A', 'N/A', '01/01/1900'::DATE, '01/01/1900'::DATE, '01/01/1900'::DATE, '01/01/1900'::DATE,
		'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', -1, -1, '01/01/1900'::DATE, '01/01/1900'::DATE)
ON CONFLICT ON CONSTRAINT dim_employees_scd_pkey DO NOTHING;

INSERT INTO bl_dim.dim_junk (
  junk_surr_id,
  source_system,
  source_entity,
  transaction_status,
  transaction_type,
  tax_type,
  insert_dt,
  update_dt
)
VALUES 
	   (-1, 'MANUAL', 'MANUAL', 'N/A', 'N/A', 'N/A', '01/01/1900'::DATE, '01/01/1900'::DATE)
ON CONFLICT ON CONSTRAINT dim_junk_pkey DO NOTHING;

INSERT INTO bl_dim.dim_products_scd (
  product_surr_id,
  product_id,
  source_system,
  source_entity,
  product_desc,
  category_name,
  subcategory_name,
  pack,
  bottle_volume_ml,
  safety_stock_lvl,
  reorder_point,
  start_date,
  end_date,
  is_active,
  product_on_sale,
  insert_dt,
  update_dt
)
VALUES 
	   (-1, -1, 'MANUAL', 'MANUAL', 'N/A', 'N/A', 'N/A', -1, -1, -1, -1, '01/01/1900'::DATE, '01/01/1900'::DATE, 'N/A', 'N/A', '01/01/1900'::DATE,
	    '01/01/1900'::DATE)
ON CONFLICT ON CONSTRAINT dim_products_scd_pkey DO NOTHING;

INSERT INTO bl_dim.dim_shippers_scd (
  shipper_surr_id,
  shipper_id,
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
  current_region,
  historic_region,
  insert_dt,
  update_dt
)
VALUES 
	   (-1, -1, 'MANUAL', 'MANUAL', 'N/A', '01/01/1900'::DATE, '01/01/1900'::DATE, 'N/A', -9.8, -9.8, -9.8, 'N/A',
		'N/A', 'N/A', 'N/A', '01/01/1900'::DATE, '01/01/1900'::DATE)
ON CONFLICT ON CONSTRAINT dim_shippers_scd_pkey DO NOTHING;

/*INSERT INTO bl_dim.dim_stores_scd (
  store_surr_id,
  store_id,
  source_system,
  source_entity,
  name,
  start_date,
  end_date,
  is_active,
  contact_phone,
  contact_email,
  region,
  county_code,
  county_name,
  postal_code,
  city,
  address,
  payment_type_pref,
  membership_flag,
  opt_in_flag,
  curr_emp_first_name,
  curr_emp_last_name,
  curr_emp_full_name,
  curr_emp_phone,
  curr_emp_email,
  insert_dt,
  update_dt
)
VALUES 
	   (-1, -1, 'MANUAL', 'MANUAL', 'N/A', '01/01/1900'::DATE, '01/01/1900'::DATE, 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A',
		 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', '01/01/1900'::DATE, '01/01/1900'::DATE)
ON CONFLICT ON CONSTRAINT dim_stores_scd_pkey DO NOTHING;*/

INSERT INTO bl_dim.dim_vendors_scd (
  vendor_surr_id,
  vendor_id,
  source_system,
  source_entity,
  name,
  start_date,
  end_date,
  is_active,
  contact_phone,
  contact_name,
  rating,
  size,
  homepage,
  insert_dt,
  update_dt
)
VALUES 
	   (-1, -1, 'MANUAL', 'MANUAL', 'N/A', '01/01/1900'::DATE, '01/01/1900'::DATE, 'N/A', 'N/A', 'N/A', -9.8, 'N/A', 
		'N/A', '01/01/1900'::DATE, '01/01/1900'::DATE)
ON CONFLICT ON CONSTRAINT dim_vendors_scd_pkey DO NOTHING;

CREATE TABLE IF NOT EXISTS bl_dim.fct_sales_def PARTITION OF bl_dim.fct_sales
        FOR VALUES FROM ('1990-01-01') TO ('1990-01-02');
INSERT INTO bl_dim.fct_sales (
		transaction_id,
		source_system,
		source_entity,
		junk_surr_id,
		shipper_surr_id,
		date_id,
		store_surr_id,
		employee_surr_id,
		vendor_surr_id,
		product_surr_id,
		event_date,
		quantity_sold,
		total_amount,
		volume_sold_liters,
		volume_sold_gallons,
		state_bottle_cost,
		state_bottle_retail,
		insert_dt
)
SELECT *
FROM (VALUES (-1, 'MANUAL', 'MANUAL', -1, -1, 19000101, -1, -1, -1, -1, '01/01/1990'::DATE, -1, -1, -1, -1, -1, -1, '01/01/1990'::DATE)) t
     WHERE NOT EXISTS (SELECT 1 FROM bl_dim.fct_sales fct WHERE fct.date_id = 19900101);

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