-- DROP PROCEDURE IF EXISTS bl_cl.wrk_stores_insertion();
CREATE OR REPLACE PROCEDURE bl_cl.wrk_stores_insertion()

LANGUAGE plpgsql 
AS $$

DECLARE
  schema_name   TEXT := 'bl_cl';
  table_name    TEXT := 'wrk_stores';
  command_name  TEXT := 'insert';
  rows_affected INTEGER := 0;
  error_type    TEXT := '';
  output_message TEXT := '';
 
  last_load_date TIMESTAMP; 

BEGIN 
	
TRUNCATE bl_cl.wrk_stores;

SELECT COALESCE(MAX(update_dt), '1900-01-01'::TIMESTAMP) INTO last_load_date FROM bl_dim.dim_stores_scd;

INSERT INTO bl_cl.wrk_stores (
					store_id,
					store_src_id,
					source_system,
					source_entity,
					name,
					start_date,
					end_date,
					is_active,
					contact_phone,
					contact_email,
					region_name,
					county_code,
					county_name,
					postal_code,
					city_name,
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
					update_dt)
SELECT store_id,
	   store_src_id,
	   'bl_3nf' AS source_system,
	   'ce_stores' AS source_entity,
	    name,
		start_date,
		end_date,
		is_active,
		contact_phone,
		contact_email,
		reg.region_name,
		cnt.county_code,
		cnt.county_name,
		loc.postal_code,
		ct.city_name,
		loc.address,
		pm.payment_type_pref,
		membership_flag,
		opt_in_flag,
		emp.first_name,
		emp.last_name,
		emp.full_name,
		emp.phone,
		emp.email,
	    CURRENT_TIMESTAMP,
	    CURRENT_TIMESTAMP
FROM bl_3nf.ce_stores AS st
LEFT OUTER JOIN bl_3nf.ce_curr_emp_profiles AS emp ON st.curr_emp_profile_id = emp.curr_emp_profile_id 
LEFT OUTER JOIN bl_3nf.ce_locations AS loc ON st.location_id = loc.location_id 
LEFT OUTER JOIN bl_3nf.ce_payment_pref_types AS pm ON st.payment_type_pref_id = pm.payment_type_pref_id 
LEFT OUTER JOIN bl_3nf.ce_cities    AS ct  ON loc.city_id = ct.city_id
LEFT OUTER JOIN bl_3nf.ce_counties AS cnt ON ct.county_id = cnt.county_id 
LEFT OUTER JOIN bl_3nf.ce_regions AS reg ON reg.region_id = cnt.region_id
WHERE GREATEST(loc.update_dt, pm.update_dt, ct.update_dt, cnt.update_dt, reg.update_dt, st.update_dt) > last_load_date;
		 
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