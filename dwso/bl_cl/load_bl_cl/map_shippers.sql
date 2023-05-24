-- DROP PROCEDURE IF EXISTS bl_cl.map_shippers_insertion();
CREATE OR REPLACE PROCEDURE bl_cl.map_shippers_insertion()

LANGUAGE plpgsql 
AS $$

DECLARE
  schema_name   TEXT := 'bl_cl';
  table_name    TEXT := 'map_shippers';
  command_name  TEXT := 'insert';
  rows_affected INTEGER := 0;
  error_type    TEXT := '';
  output_message TEXT := '';

BEGIN 

TRUNCATE bl_cl.map_shippers;
	
INSERT INTO bl_cl.map_shippers
SELECT		    shipper_src_id,
				'bl_cl' AS source_system,
				'map_shippers' AS source_table,
				name,
				rating,
				ship_base,
				ship_rate,
				contact_phone,
				contact_name,
				region_id,
				region_name,
				insert_dt,
				update_dt
FROM 
	(
	SELECT DISTINCT nw.shipper_id                  	AS shipper_src_id,
			        nw.shipper_name 	        	AS name,
			        nw.shipper_rating 				AS rating,
			        nw.ship_base,
			        nw.ship_rate,
			        nw.shipper_phone				AS contact_phone,
			   	    nw.shipper_contact_name 		AS contact_name,
			        nw.region_id,
			        nw.region_name,
					CURRENT_TIMESTAMP 				AS insert_dt,
					CURRENT_TIMESTAMP 				AS update_dt 
	FROM bl_3nf.incr_view_ce_shippers_no AS nw
	UNION ALL 
	SELECT DISTINCT il.shipper_id                  	AS shipper_src_id,
			        il.shipper_name 	        	AS name,
			        il.shipper_rating 				AS rating,
			        il.ship_base,
			        il.ship_rate,
			        il.shipper_phone				AS contact_phone,
			   	    il.shipper_contact_name 		AS contact_name,
			        il.region_id,
			        il.region_name,
					CURRENT_TIMESTAMP 				AS insert_dt,
					CURRENT_TIMESTAMP 				AS update_dt 
	FROM bl_3nf.incr_view_ce_shippers_io AS il
	) q 
GROUP BY shipper_src_id, 
	     source_system,
	     source_table,	
	     name,
	     rating,
		 ship_base,
		 ship_rate,
		 contact_phone,
		 contact_name,
		 region_id,
		 region_name,
		 insert_dt,
		 update_dt;
		 
GET DIAGNOSTICS rows_affected = row_count;

UPDATE bl_cl.prm_mta_incremental_load
SET previous_loaded_dt = CURRENT_TIMESTAMP,
	procedure_name = 'bl_cl.map_shippers_insertion()'	
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