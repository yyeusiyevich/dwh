CREATE OR REPLACE PROCEDURE bl_cl.ce_stores_insertion()

LANGUAGE plpgsql 
AS $$

DECLARE
  schema_name   TEXT := 'bl_3nf';
  table_name    TEXT := 'ce_stores';
  command_name  TEXT := 'insert';
  rows_affected INTEGER := 0;
  error_type    TEXT := '';
  output_message TEXT := '';

BEGIN 

WITH inserted_data (store_id,
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
      		        update_dt) AS (
     SELECT DISTINCT COALESCE(store_number, 'N/A'),
			        'sa_northwest_sales',
			        'src_northwest_sales',
			        COALESCE(store_name, 'N/A'),
				    '1900-01-01'::DATE,
			        '9999-12-31'::DATE,
			        'Y',
			        COALESCE(store_contact_phone, 'N/A'),
			        COALESCE(store_contact_email, 'N/A'),
			        COALESCE(loc.location_id, -1),
			        COALESCE(pm.payment_type_pref_id, -1),  
			        COALESCE(ccep.curr_emp_profile_id, -1),  			        
			        COALESCE(opt_in_flag, 'N/A'),
			        COALESCE(membership_flag, 'N/A'),       
			        CURRENT_TIMESTAMP,
				    CURRENT_TIMESTAMP 
     FROM bl_3nf.incr_view_ce_stores_no  AS src
     LEFT OUTER JOIN bl_3nf.ce_locations 		  AS loc   ON src.store_location_id = loc.location_src_id
     LEFT OUTER JOIN bl_3nf.ce_payment_pref_types AS pm    ON src.payment_type_pref_id = pm.payment_type_pref_src_id
     LEFT OUTER JOIN bl_cl.map_curr_employees     AS mce   ON src.store_number = mce.store_src_id 
     LEFT OUTER JOIN bl_3nf.ce_curr_emp_profiles  AS ccep  ON mce.employee_src_id = ccep.curr_employee_src_id
     WHERE loc.source_system = 'sa_northwest_sales' AND
     	   loc.source_entity = 'src_northwest_sales'
	 UNION ALL
     SELECT DISTINCT COALESCE(store_number, 'N/A'),
			        'sa_iowalakes_sales',
			        'src_iowalakes_sales',
			        COALESCE(store_name, 'N/A'),
				    '1900-01-01'::DATE,
			        '9999-12-31'::DATE,
			        'Y',
			        COALESCE(store_contact_phone, 'N/A'),
			        COALESCE(store_contact_email, 'N/A'),
			        COALESCE(loc.location_id, -1),
			        COALESCE(pm.payment_type_pref_id, -1),  
			        COALESCE(ccep.curr_emp_profile_id, -1),  			        
			        COALESCE(opt_in_flag, 'N/A'),
			        COALESCE(membership_flag, 'N/A'),       
			        CURRENT_TIMESTAMP,
				    CURRENT_TIMESTAMP 
     FROM bl_3nf.incr_view_ce_stores_io AS src
     LEFT OUTER JOIN bl_3nf.ce_locations 		  AS loc   ON src.store_location_id = loc.location_src_id
     LEFT OUTER JOIN bl_3nf.ce_payment_pref_types AS pm    ON src.payment_type_pref_id = pm.payment_type_pref_src_id
     LEFT OUTER JOIN bl_cl.map_curr_employees     AS mce   ON src.store_number = mce.store_src_id 
     LEFT OUTER JOIN bl_3nf.ce_curr_emp_profiles  AS ccep  ON mce.employee_src_id = ccep.curr_employee_src_id
     WHERE loc.source_system = 'sa_iowalakes_sales' AND
     	   loc.source_entity = 'src_iowalakes_sales'
	                               ),
	  updated_curr_emp_profile AS (
	  	UPDATE bl_3nf.ce_stores AS target
	  	SET curr_emp_profile_id = inserted_data.curr_emp_profile_id,
	      	update_dt = CURRENT_TIMESTAMP
	  	FROM inserted_data
	  	WHERE   UPPER(inserted_data.store_id) = UPPER(target.store_src_id) AND
	        	UPPER(target.is_active) = 'Y' AND
	        	inserted_data.curr_emp_profile_id <> target.curr_emp_profile_id
	  ),                                                       
	 new_rows AS (
	 SELECT *
     FROM inserted_data AS ins
     WHERE NOT EXISTS (SELECT 1
                       FROM  bl_3nf.ce_stores AS st
                       WHERE UPPER(ins.store_id) = UPPER(st.store_src_id) AND
						     UPPER(ins.source_system) = UPPER(st.source_system) AND
						     UPPER(ins.source_entity) = UPPER(st.source_entity) AND
						     UPPER(ins.name) = UPPER(st.name) AND
						     UPPER(ins.contact_phone) = UPPER(st.contact_phone) AND
						     UPPER(ins.contact_email) = UPPER(st.contact_email) AND
                             ins.location_id = st.location_id AND
                             ins.payment_type_pref_id = st.payment_type_pref_id AND
                             ins.opt_in_flag = st.opt_in_flag AND
                             ins.membership_flag = st.membership_flag AND 
                             UPPER(st.is_active) = 'Y'
                        )
	 								),														
	  upd AS (
      UPDATE bl_3nf.ce_stores AS target
             SET is_active = 'N',
             end_date = CURRENT_TIMESTAMP,
             update_dt = CURRENT_TIMESTAMP
      FROM new_rows
      WHERE UPPER(new_rows.store_id) = UPPER(target.store_src_id) AND
            UPPER(target.is_active) = 'Y'
     		 )
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
SELECT CASE WHEN EXISTS (SELECT 1 
                         FROM bl_3nf.ce_stores AS st 
                         WHERE st.store_src_id = new_rows.store_id) 
            THEN (SELECT st.store_id 
                  FROM bl_3nf.ce_stores AS st 
                  WHERE st.store_src_id = new_rows.store_id) 
       		ELSE NEXTVAL('bl_3nf.ce_stores_seq')
       END,
       store_id,
       source_system,
       source_entity,
       name,
	   CASE WHEN EXISTS (SELECT 1
                         FROM bl_3nf.ce_stores AS st, new_rows
                         WHERE  UPPER(new_rows.store_id) = UPPER(st.store_src_id) AND
                                UPPER(new_rows.source_system) = UPPER(st.source_system) AND
                                UPPER(new_rows.source_entity) = UPPER(st.source_entity)) 
            THEN CURRENT_DATE
            ELSE '1900-01-01'::DATE 
       END,
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
       update_dt
FROM   new_rows
ON CONFLICT ON CONSTRAINT stores_pk DO NOTHING;     		 

GET DIAGNOSTICS rows_affected = row_count;

UPDATE bl_cl.prm_mta_incremental_load
SET previous_loaded_dt = CURRENT_TIMESTAMP,
	procedure_name = 'bl_cl.ce_stores_insertion()'	
WHERE source_table IN ('src_northwest_sales', 'src_iowalakes_sales')
	  AND target_table = 'ce_stores';    

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