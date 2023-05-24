CREATE OR REPLACE PROCEDURE bl_cl.ce_counties_insertion()

LANGUAGE plpgsql 
AS $$

DECLARE
  schema_name   TEXT := 'bl_3nf';
  table_name    TEXT := 'ce_counties';
  command_name  TEXT := 'insert';
  rows_affected INTEGER := 0;
  error_type    TEXT := '';
  output_message TEXT := '';

BEGIN 

WITH inserted_data (county_id,
                    source_system,
                    source_entity,
                    region_id,
                    county_code,
                    county_name,
                    insert_dt, 
                    update_dt) AS (
     SELECT DISTINCT COALESCE(county_id, 'N/A'),
                     'sa_northwest_sales',
                     'src_northwest_sales',
                     COALESCE(reg.region_id, -1),
                     COALESCE(county_number, 'N/A'),
                     COALESCE(county, 'N/A'),
                     CURRENT_TIMESTAMP,
                     CURRENT_TIMESTAMP   
     FROM bl_3nf.incr_view_ce_counties_no AS src
     LEFT OUTER JOIN bl_3nf.ce_regions AS reg ON src.region_id = reg.region_src_id
     WHERE reg.source_system = 'sa_northwest_sales' AND 
     	   reg.source_entity = 'src_northwest_sales'   
     UNION ALL
     SELECT DISTINCT COALESCE(county_id, 'N/A'),
                     'sa_iowalakes_sales',
                     'src_iowalakes_sales',
                     COALESCE(reg.region_id, -1),
                     COALESCE(county_number, 'N/A'),
                     COALESCE(county, 'N/A'),
                     CURRENT_TIMESTAMP,
                     CURRENT_TIMESTAMP   
      FROM bl_3nf.incr_view_ce_counties_io AS src
      LEFT OUTER JOIN bl_3nf.ce_regions AS reg ON src.region_id = reg.region_src_id
     WHERE reg.source_system = 'sa_iowalakes_sales' AND 
     	   reg.source_entity = 'src_iowalakes_sales' 
                   )
MERGE INTO bl_3nf.ce_counties AS nf
USING inserted_data ON UPPER(nf.county_src_id) = UPPER(COALESCE(inserted_data.county_id, 'N/A')) AND
                       nf.source_system = inserted_data.source_system AND
                       nf.source_entity = inserted_data.source_entity	
WHEN MATCHED AND UPPER(inserted_data.county_name) != UPPER(nf.county_name) OR
				 UPPER(inserted_data.county_code) != UPPER(nf.county_code)
THEN
        UPDATE
        SET county_name = inserted_data.county_name,
        	county_code = inserted_data.county_code,
            update_dt = CURRENT_TIMESTAMP
WHEN NOT MATCHED 
THEN
        INSERT
            (county_id,
             county_src_id,
             source_system,
             source_entity,
             region_id,
             county_code,
             county_name,
             insert_dt, 
             update_dt)
         VALUES 
         	(NEXTVAL('bl_3nf.ce_counties_seq'),
         	inserted_data.county_id, 
            inserted_data.source_system, 
            inserted_data.source_entity, 
            inserted_data.region_id, 
            inserted_data.county_code, 
            inserted_data.county_name, 
            inserted_data.insert_dt, 
            inserted_data.update_dt);    
                        

GET DIAGNOSTICS rows_affected = row_count;

RAISE NOTICE '% row(s) inserted', rows_affected;
output_message = 'Success';

CALL bl_cl.logging_insertion(schema_name, 
							 table_name,
							 command_name,
							 rows_affected,
							 error_type,
							 output_message);
							
UPDATE bl_cl.prm_mta_incremental_load
SET previous_loaded_dt = CURRENT_TIMESTAMP,
	procedure_name = 'bl_cl.ce_counties_insertion()'	
WHERE source_table IN ('src_northwest_sales', 'src_iowalakes_sales')
	  AND target_table = 'ce_counties';  							

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