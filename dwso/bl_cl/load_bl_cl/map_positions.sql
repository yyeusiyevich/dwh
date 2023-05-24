-- DROP PROCEDURE IF EXISTS bl_cl.map_positions_insertion();
CREATE OR REPLACE PROCEDURE bl_cl.map_positions_insertion()

LANGUAGE plpgsql 
AS $$

DECLARE
  schema_name   TEXT := 'bl_cl';
  table_name    TEXT := 'map_positions';
  command_name  TEXT := 'insert';
  rows_affected INTEGER := 0;
  error_type    TEXT := '';
  output_message TEXT := '';

BEGIN 
	
EXECUTE 'TRUNCATE bl_cl.map_positions;';		

INSERT INTO bl_cl.map_positions
SELECT DISTINCT position_src_id, 
		        'bl_cl' AS source_system, 
		        'map_positions' AS source_table,
		        position_name, 
		        insert_dt, 
		        update_dt  
FROM 
	(
	SELECT DISTINCT nw.position_id               AS position_src_id,
			        nw.position_name             AS position_name,
					CURRENT_TIMESTAMP AS insert_dt,
					CURRENT_TIMESTAMP AS update_dt 
	FROM bl_3nf.incr_view_ce_positions_no AS nw
	UNION ALL 
	SELECT DISTINCT il.position_id               AS position_src_id,
			        il.position_name             AS position_name,
					CURRENT_TIMESTAMP AS insert_dt,
					CURRENT_TIMESTAMP AS update_dt 
	FROM bl_3nf.incr_view_ce_positions_io AS il
	) q 
GROUP BY position_src_id, 
	     source_system,
	     source_table ,	
	     position_name, 
		 insert_dt, 
		 update_dt;
		 
GET DIAGNOSTICS rows_affected = row_count;

UPDATE bl_cl.prm_mta_incremental_load
SET previous_loaded_dt = CURRENT_TIMESTAMP,
	procedure_name = 'bl_cl.map_positions_insertion()'	
WHERE source_table IN ('src_northwest_sales', 'src_iowalakes_sales')
	  AND target_table = 'ce_positions';   

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