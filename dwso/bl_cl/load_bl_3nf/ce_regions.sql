CREATE OR REPLACE PROCEDURE bl_cl.ce_regions_insertion()

LANGUAGE plpgsql 
AS $$

DECLARE
  schema_name   TEXT := 'bl_3nf';
  table_name    TEXT := 'ce_regions';
  command_name  TEXT := 'insert';
  rows_affected INTEGER := 0;
  error_type    TEXT := '';
  output_message TEXT := '';

BEGIN 

WITH inserted_data (region_id,
                    source_system,
                    source_entity,
                    region_name,
                    insert_dt, 
                    update_dt) AS (
     SELECT DISTINCT COALESCE(region_id, 'N/A'),
                     'sa_northwest_sales',
                     'src_northwest_sales',
                     COALESCE(region_name, 'N/A'),
                     CURRENT_TIMESTAMP,
                     CURRENT_TIMESTAMP
     FROM bl_3nf.incr_view_ce_regions_no 
     UNION ALL
     SELECT DISTINCT COALESCE(region_id, 'N/A'),
                     'sa_iowalakes_sales',
                     'src_iowalakes_sales',
                     COALESCE(region_name, 'N/A'),
                     CURRENT_TIMESTAMP,
                     CURRENT_TIMESTAMP
     FROM bl_3nf.incr_view_ce_regions_io
                   )
MERGE INTO bl_3nf.ce_regions AS nf
USING inserted_data ON UPPER(nf.region_src_id) = UPPER(COALESCE(inserted_data.region_id, 'N/A')) AND
                       nf.source_system = inserted_data.source_system AND
                       nf.source_entity = inserted_data.source_entity	
WHEN MATCHED AND UPPER(inserted_data.region_name) != UPPER(nf.region_name)
THEN
        UPDATE
        SET region_name = inserted_data.region_name,
            update_dt = CURRENT_TIMESTAMP
WHEN NOT MATCHED 
THEN
        INSERT
            (region_id,
             region_src_id,
             source_system,
             source_entity,
             region_name,
             insert_dt, 
             update_dt)
         VALUES 
         	(NEXTVAL('bl_3nf.ce_regions_seq'),
         	inserted_data.region_id, 
            inserted_data.source_system, 
            inserted_data.source_entity, 
            inserted_data.region_name, 
            inserted_data.insert_dt, 
            inserted_data.update_dt);                   

GET DIAGNOSTICS rows_affected = row_count;

UPDATE bl_cl.prm_mta_incremental_load
SET previous_loaded_dt = CURRENT_TIMESTAMP,
	procedure_name = 'bl_cl.ce_regions_insertion()'	
WHERE source_table IN ('src_northwest_sales', 'src_iowalakes_sales')
	  AND target_table = 'ce_regions';    

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