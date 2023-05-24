-- DROP PROCEDURE IF EXISTS bl_cl.map_transaction_types_insertion();
CREATE OR REPLACE PROCEDURE bl_cl.map_transaction_types_insertion()

LANGUAGE plpgsql 
AS $$

DECLARE
  schema_name   TEXT := 'bl_cl';
  table_name    TEXT := 'map_transaction_types';
  command_name  TEXT := 'insert';
  rows_affected INTEGER := 0;
  error_type    TEXT := '';
  output_message TEXT := '';

BEGIN 
	
TRUNCATE bl_cl.map_transaction_types;		

INSERT INTO bl_cl.map_transaction_types
SELECT DISTINCT type_src_id, 
		        'bl_cl' AS source_system, 
		        'map_transaction_types' AS source_table,
		        type_name, 
		        insert_dt, 
		        update_dt  
FROM 
	(
	SELECT DISTINCT nw.transaction_type_id  AS type_src_id,
			        nw.transaction_type     AS type_name,
					CURRENT_TIMESTAMP AS insert_dt,
					CURRENT_TIMESTAMP AS update_dt 
	FROM bl_3nf.incr_view_ce_transaction_types_no AS nw
	UNION ALL 
	SELECT DISTINCT il.transaction_type_id  AS type_src_id,
			        il.transaction_type     AS type_name,
					CURRENT_TIMESTAMP AS insert_dt,
					CURRENT_TIMESTAMP AS update_dt 
	FROM bl_3nf.incr_view_ce_transaction_types_io AS il
	) q 
GROUP BY type_src_id, 
	     source_system,
	     source_table,	
	     type_name, 
		 insert_dt, 
		 update_dt;
		 
GET DIAGNOSTICS rows_affected = row_count;

UPDATE bl_cl.prm_mta_incremental_load
SET previous_loaded_dt = CURRENT_TIMESTAMP,
	procedure_name = 'bl_cl.map_transaction_types_insertion()'	
WHERE source_table IN ('src_northwest_sales', 'src_iowalakes_sales')
	  AND target_table = 'ce_transaction_types';    

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