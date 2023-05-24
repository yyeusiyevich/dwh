CREATE OR REPLACE PROCEDURE bl_cl.ce_cities_insertion()

LANGUAGE plpgsql 
AS $$

DECLARE
  schema_name   TEXT := 'bl_3nf';
  table_name    TEXT := 'ce_cities';
  command_name  TEXT := 'insert';
  rows_affected INTEGER := 0;
  error_type    TEXT := '';
  output_message TEXT := '';

BEGIN 

WITH inserted_data (city_id,
                    source_system,
                    source_entity,
                    county_id,
                    city_name,
                    insert_dt, 
                    update_dt) AS (
     SELECT DISTINCT COALESCE(city_id, 'N/A'),
                     'sa_northwest_sales',
                     'src_northwest_sales',
                     COALESCE(cnt.county_id, -1),
                     COALESCE(city, 'N/A'),
                     CURRENT_TIMESTAMP,
                     CURRENT_TIMESTAMP   
     FROM bl_3nf.incr_view_ce_cities_no AS src
     LEFT JOIN bl_3nf.ce_counties AS cnt ON src.county_id = cnt.county_src_id  
     WHERE cnt.source_system = 'sa_northwest_sales' AND 
     	   cnt.source_entity = 'src_northwest_sales'     
     UNION ALL
     SELECT DISTINCT COALESCE(city_id, 'N/A'),
                     'sa_iowalakes_sales',
                     'src_iowalakes_sales',
                     COALESCE(cnt.county_id, -1),
                     COALESCE(city, 'N/A'),
                     CURRENT_TIMESTAMP,
                     CURRENT_TIMESTAMP   
     FROM bl_3nf.incr_view_ce_cities_io AS src
     LEFT JOIN bl_3nf.ce_counties AS cnt ON src.county_id = cnt.county_src_id    
     WHERE cnt.source_system = 'sa_iowalakes_sales' AND 
     	   cnt.source_entity = 'src_iowalakes_sales' 
                   )
MERGE INTO bl_3nf.ce_cities AS nf
USING inserted_data ON UPPER(nf.city_src_id) = UPPER(COALESCE(inserted_data.city_id, 'N/A')) AND
                       nf.source_system = inserted_data.source_system AND
                       nf.source_entity = inserted_data.source_entity	
WHEN MATCHED AND inserted_data.county_id != nf.county_id OR
				 UPPER(inserted_data.city_name) != UPPER(nf.city_name)
THEN
        UPDATE
        SET county_id = inserted_data.county_id,
        	city_name = inserted_data.city_name,
            update_dt = CURRENT_TIMESTAMP
WHEN NOT MATCHED 
THEN
        INSERT
            (city_id,
             city_src_id,
             source_system,
             source_entity,
             county_id,
             city_name,
             insert_dt, 
             update_dt)
         VALUES 
         	(NEXTVAL('bl_3nf.ce_cities_seq'),
         	inserted_data.city_id, 
            inserted_data.source_system, 
            inserted_data.source_entity, 
            inserted_data.county_id, 
            inserted_data.city_name, 
            inserted_data.insert_dt, 
            inserted_data.update_dt);       
                  

GET DIAGNOSTICS rows_affected = row_count;

UPDATE bl_cl.prm_mta_incremental_load
SET previous_loaded_dt = CURRENT_TIMESTAMP,
	procedure_name = 'bl_cl.ce_cities_insertion()'	
WHERE source_table IN ('src_northwest_sales', 'src_iowalakes_sales')
	  AND target_table = 'ce_cities';    

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