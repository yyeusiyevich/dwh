CREATE OR REPLACE PROCEDURE bl_cl.ce_shippers_insertion()

LANGUAGE plpgsql 
AS $$

DECLARE
  schema_name   TEXT := 'bl_3nf';
  table_name    TEXT := 'ce_shippers';
  command_name  TEXT := 'insert';
  rows_affected INTEGER := 0;
  error_type    TEXT := '';
  output_message TEXT := '';

BEGIN 
	
WITH new_rows AS  (
     SELECT COALESCE(mp.shipper_src_id, 'N/A') 		AS shipper_src_id,
			mp.source_system 				   		AS source_system,
			mp.source_table 				   		AS source_entity,
			COALESCE(name, 'N/A')				 	AS name, 	
			'1900-01-01'::DATE						AS start_date,
			'9999-12-31'::DATE						AS end_date,
			'Y' 									AS is_active,			
			COALESCE(mp.rating::NUMERIC, -1) 		AS rating,
			COALESCE(mp.ship_base::NUMERIC, -1)		AS ship_base,
			COALESCE(mp.ship_rate::NUMERIC, -1)		AS ship_rate,
			COALESCE(mp.contact_phone, 'N/A')		AS contact_phone,
			COALESCE(mp.contact_name, 'N/A')		AS contact_name,
			COALESCE(reg.region_id, -1)				AS curr_region_id,
			COALESCE(reg.region_id, -1)				AS historical_region_id,
 			CURRENT_TIMESTAMP						AS insert_dt,
			CURRENT_TIMESTAMP						AS update_dt 
		FROM bl_cl.map_shippers AS mp
		LEFT OUTER JOIN bl_3nf.ce_regions AS reg
		ON mp.region_id = reg.region_src_id 
		WHERE NOT EXISTS (SELECT 1
                       FROM  bl_3nf.ce_shippers AS sh
                       WHERE UPPER(mp.shipper_src_id) = UPPER(sh.shipper_src_id) AND
						     mp.source_system = sh.source_system AND
						     mp.source_table = sh.source_entity AND
						     UPPER(mp.name) = UPPER(sh.name) AND
						     mp.rating::NUMERIC(2, 1) = sh.rating AND
						     mp.ship_base::NUMERIC(10, 2) = sh.ship_base AND
						     mp.ship_rate::NUMERIC(10, 2) = sh.ship_rate AND						     
						     UPPER(mp.contact_phone) = UPPER(sh.contact_phone) AND
						     UPPER(mp.contact_name) = UPPER(sh.contact_name) AND
                             reg.region_id = sh.curr_region_id AND
                             UPPER(sh.is_active) = 'Y'
                        )
	 								),			
upd_active AS (
     UPDATE bl_3nf.ce_shippers AS sh
             SET is_active = 'N',
             end_date = CURRENT_TIMESTAMP,
             curr_region_id = new_rows.curr_region_id,
             update_dt = CURRENT_TIMESTAMP
     FROM  new_rows
     WHERE new_rows.shipper_src_id = sh.shipper_src_id AND
           sh.is_active = 'Y'
     		 ),
     ups_inactive AS (
     UPDATE bl_3nf.ce_shippers AS sh
             SET curr_region_id = new_rows.curr_region_id,
             	 update_dt = CURRENT_TIMESTAMP
     FROM  new_rows
     WHERE new_rows.shipper_src_id = sh.shipper_src_id AND
            sh.is_active = 'N')
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
SELECT CASE WHEN EXISTS (SELECT 1 
                         FROM bl_3nf.ce_shippers AS sh 
                         WHERE sh.shipper_src_id = new_rows.shipper_src_id) 
            THEN (SELECT sh.shipper_id 
                  FROM bl_3nf.ce_shippers AS sh 
                  WHERE sh.shipper_src_id = new_rows.shipper_src_id) 
       		ELSE NEXTVAL('bl_3nf.ce_shippers_seq')
       END,
	   shipper_src_id,
       source_system,
       source_entity,
       name,
	   CASE WHEN EXISTS (SELECT 1
                         FROM bl_3nf.ce_shippers AS sh, new_rows
                         WHERE  UPPER(new_rows.shipper_src_id) = UPPER(sh.shipper_src_id))
            THEN CURRENT_DATE
            ELSE '1900-01-01'::DATE 
       END,
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
       update_dt
FROM   new_rows
ON CONFLICT ON CONSTRAINT ship_pk DO NOTHING;   
	 								
GET DIAGNOSTICS rows_affected = row_count;

UPDATE bl_cl.prm_mta_incremental_load
SET previous_loaded_dt = CURRENT_TIMESTAMP,
	procedure_name = 'bl_cl.ce_shippers_insertion()'	
WHERE source_table IN ('src_northwest_sales', 'src_iowalakes_sales')
	  AND target_table = 'ce_shippers';  

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