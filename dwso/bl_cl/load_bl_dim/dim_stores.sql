-- DROP PROCEDURE IF EXISTS bl_cl.dim_stores_scd_insertion();
CREATE OR REPLACE PROCEDURE bl_cl.dim_stores_scd_insertion()

LANGUAGE plpgsql 
AS $$

DECLARE
  schema_name   TEXT := 'bl_dim';
  table_name    TEXT := 'dim_stores_scd';
  command_name  TEXT := 'insert';
  rows_affected INTEGER := 0;
  error_type    TEXT := '';
  output_message TEXT := '';

BEGIN 
	
WITH inserted AS (
    SELECT
        store_id,
        'bl_cl' AS source_system,
        'wrk_stores' AS source_entity,
        COALESCE(name, 'N/A') AS name,
		start_date,
		end_date,
		is_active,
		COALESCE(contact_phone, 'N/A') AS contact_phone,
        COALESCE(contact_email, 'N/A') AS contact_email,
        COALESCE(region_name, 'N/A') AS region_name,
		COALESCE(county_code, 'N/A') AS county_code,
        COALESCE(county_name, 'N/A') AS county_name,
        COALESCE(postal_code, 'N/A') AS postal_code,
        COALESCE(city_name, 'N/A') AS city_name,
        COALESCE(address, 'N/A') AS address,
        COALESCE(payment_type_pref, 'N/A') AS payment_type_pref,
		COALESCE(membership_flag, 'N/A') AS membership_flag,
        COALESCE(opt_in_flag, 'N/A') AS opt_in_flag,
        COALESCE(curr_emp_first_name, 'N/A') AS curr_emp_first_name,
        COALESCE(curr_emp_last_name, 'N/A') AS curr_emp_last_name,
        COALESCE(curr_emp_full_name, 'N/A') AS curr_emp_full_name,
        COALESCE(curr_emp_phone, 'N/A') AS curr_emp_phone,
        COALESCE(curr_emp_email, 'N/A') AS curr_emp_email
    FROM bl_cl.wrk_stores
    WHERE store_id != -1
			),
	updated_curr_emp_profile AS (
	  	UPDATE bl_dim.dim_stores_scd AS target
	  	SET curr_emp_first_name = inserted.curr_emp_first_name,
	  		curr_emp_last_name = inserted.curr_emp_last_name,
	  		curr_emp_full_name = inserted.curr_emp_full_name,
	  		curr_emp_phone = inserted.curr_emp_phone,
	  		curr_emp_email = inserted.curr_emp_email,
	      	update_dt = CURRENT_TIMESTAMP
	  	FROM inserted
	  	WHERE   inserted.store_id = target.store_id AND
	        	UPPER(target.is_active) = 'Y' AND
	  			(target.curr_emp_full_name <> inserted.curr_emp_full_name OR
	  			target.curr_emp_phone <> inserted.curr_emp_phone OR
	  			target.curr_emp_email <> inserted.curr_emp_email)
	  ),       
	 upd AS (
      UPDATE bl_dim.dim_stores_scd AS target
             SET is_active = 'N',
             end_date = CURRENT_TIMESTAMP,
             update_dt = CURRENT_TIMESTAMP
      FROM inserted
      WHERE inserted.store_id = target.store_id AND
            UPPER(target.is_active) = 'Y' AND
            (inserted.source_system != target.source_system OR
             inserted.source_entity != target.source_entity OR
             inserted.name != target.name OR
			 inserted.contact_phone != target.contact_phone OR
			 inserted.contact_email != target.contact_email OR
			 inserted.region_name != target.region OR
			 inserted.county_code != target.county_code OR
			 inserted.county_name != target.county_name OR
			 inserted.postal_code != target.postal_code OR
			 inserted.city_name != target.city OR
			 inserted.address != target.address OR
			 inserted.payment_type_pref != target.payment_type_pref OR
			 inserted.membership_flag != target.membership_flag OR
			 inserted.opt_in_flag != target.opt_in_flag)
     		 )
INSERT INTO bl_dim.dim_stores_scd
(
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
SELECT
        NEXTVAL('bl_dim.dim_stores_seq'),
 		store_id,
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
        CURRENT_TIMESTAMP,
        CURRENT_TIMESTAMP
FROM inserted
WHERE UPPER(is_active) = 'Y' AND
	  NOT EXISTS (SELECT 1
	 			  FROM bl_dim.dim_stores_scd AS st
	 			  WHERE inserted.store_id = st.store_id AND
	 			 		inserted.source_system = st.source_system AND
	 			 		inserted.source_entity = st.source_entity AND
	 			 		inserted.name = st.name AND
	 			 		inserted.contact_phone = st.contact_phone AND
	 			 		inserted.contact_email = st.contact_email AND
	 			 		inserted.region_name = st.region AND
	 			 		inserted.county_name = st.county_name AND
	 			 		inserted.postal_code = st.postal_code AND
	 			 		inserted.city_name = st.city AND
	 			 		inserted.address = st.address AND
	 			 		inserted.payment_type_pref = st.payment_type_pref AND
	 			 		inserted.membership_flag = st.membership_flag AND
	 			 		inserted.opt_in_flag = st.opt_in_flag AND
	 			 		inserted.county_code = st.county_code);

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