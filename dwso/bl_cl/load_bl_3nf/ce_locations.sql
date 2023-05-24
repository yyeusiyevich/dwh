CREATE OR REPLACE PROCEDURE bl_cl.ce_locations_insertion()

LANGUAGE plpgsql 
AS $$

DECLARE
  schema_name   TEXT := 'bl_3nf';
  table_name    TEXT := 'ce_locations';
  command_name  TEXT := 'insert';
  rows_affected INTEGER := 0;
  error_type    TEXT := '';
  output_message TEXT := '';

BEGIN 

WITH inserted_data (location_id,
                    source_system,
                    source_entity,
                    address,
                    postal_code,
                    city_id,             
      		        insert_dt,
      		        update_dt) AS (
     SELECT DISTINCT COALESCE(store_location_id, 'N/A'),
                     'sa_northwest_sales',
                     'src_northwest_sales',
                     COALESCE(address, 'N/A'),
                     COALESCE(zip_code, 'N/A'),
                     COALESCE(ct.city_id, -1),    
                     CURRENT_TIMESTAMP,
	                 CURRENT_TIMESTAMP
     FROM bl_3nf.incr_view_ce_locations_no AS src
     LEFT JOIN bl_3nf.ce_cities AS ct ON src.city_id = ct.city_src_id
     WHERE ct.source_system = 'sa_northwest_sales' AND 
     	   ct.source_entity = 'src_northwest_sales'      
     UNION ALL
     SELECT DISTINCT COALESCE(store_location_id, 'N/A'),
                     'sa_iowalakes_sales',
                     'src_iowalakes_sales',
                     COALESCE(address, 'N/A'),
                     COALESCE(zip_code, 'N/A'),
                     COALESCE(ct.city_id, -1),    
                     CURRENT_TIMESTAMP,
	                 CURRENT_TIMESTAMP
     FROM bl_3nf.incr_view_ce_locations_io AS src
     LEFT JOIN bl_3nf.ce_cities AS ct ON src.city_id = ct.city_src_id
     WHERE ct.source_system = 'sa_iowalakes_sales' AND 
     	   ct.source_entity = 'src_iowalakes_sales' 
     UNION ALL
     SELECT DISTINCT COALESCE(emp_location_id, 'N/A'),
                     'sa_northwest_sales',
                     'src_northwest_sales',
                     COALESCE(emp_address, 'N/A'),
                     COALESCE(emp_postal, 'N/A'),
                     COALESCE(ct.city_id, -1),    
                     CURRENT_TIMESTAMP,
	                 CURRENT_TIMESTAMP       
      FROM bl_3nf.incr_view_ce_locations_no AS src
      LEFT JOIN bl_3nf.ce_cities AS ct ON src.emp_city_id = ct.city_src_id
      WHERE ct.source_system = 'sa_northwest_sales' AND 
     	    ct.source_entity = 'src_northwest_sales'        
      UNION ALL
            SELECT DISTINCT COALESCE(emp_location_id, 'N/A'),
                     'sa_iowalakes_sales',
                     'src_iowalakes_sales',
                     COALESCE(emp_address, 'N/A'),
                     COALESCE(emp_postal, 'N/A'),
                     COALESCE(ct.city_id, -1),    
                     CURRENT_TIMESTAMP,
	                 CURRENT_TIMESTAMP       
      FROM bl_3nf.incr_view_ce_locations_io AS src
      LEFT JOIN bl_3nf.ce_cities AS ct ON src.emp_city_id = ct.city_src_id
      WHERE ct.source_system = 'sa_iowalakes_sales' AND 
     	    ct.source_entity = 'src_iowalakes_sales' 
                   )
MERGE INTO bl_3nf.ce_locations AS nf
USING inserted_data ON UPPER(nf.location_src_id) = UPPER(COALESCE(inserted_data.location_id, 'N/A')) AND
                       nf.source_system = inserted_data.source_system AND
                       nf.source_entity = inserted_data.source_entity	
WHEN MATCHED AND UPPER(inserted_data.address) != UPPER(nf.address) OR
				 UPPER(inserted_data.postal_code) != UPPER(nf.postal_code) OR
				 inserted_data.city_id != nf.city_id
THEN
        UPDATE
        SET address = inserted_data.address,
        	postal_code = inserted_data.postal_code,
        	city_id = inserted_data.city_id,
            update_dt = CURRENT_TIMESTAMP
WHEN NOT MATCHED 
THEN
        INSERT
            (location_id,
             location_src_id,
             source_system,
             source_entity,
             address,
             postal_code,
             city_id,             
      		 insert_dt,
      		 update_dt)
         VALUES 
         	(NEXTVAL('bl_3nf.ce_locations_seq'),
         	inserted_data.location_id, 
            inserted_data.source_system, 
            inserted_data.source_entity, 
            inserted_data.address, 
            inserted_data.postal_code, 
            inserted_data.city_id, 
            inserted_data.insert_dt, 
            inserted_data.update_dt);                   

GET DIAGNOSTICS rows_affected = row_count;

UPDATE bl_cl.prm_mta_incremental_load
SET previous_loaded_dt = CURRENT_TIMESTAMP,
	procedure_name = 'bl_cl.ce_locations_insertion()'	
WHERE source_table IN ('src_northwest_sales', 'src_iowalakes_sales')
	  AND target_table = 'ce_locations';   

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