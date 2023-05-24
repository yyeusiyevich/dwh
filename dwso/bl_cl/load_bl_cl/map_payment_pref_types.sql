-- DROP PROCEDURE IF EXISTS bl_cl.map_payment_pref_types_insertion();
CREATE OR REPLACE PROCEDURE bl_cl.map_payment_pref_types_insertion()

LANGUAGE plpgsql 
AS $$

DECLARE
  schema_name   TEXT := 'bl_cl';
  table_name    TEXT := 'map_payment_pref_types';
  command_name  TEXT := 'insert';
  rows_affected INTEGER := 0;
  error_type    TEXT := '';
  output_message TEXT := '';

BEGIN 

EXECUTE 'TRUNCATE bl_cl.map_payment_pref_types;';	
	
INSERT INTO bl_cl.map_payment_pref_types
SELECT DISTINCT payment_pref_type_src_id, 
		        'bl_cl' AS source_system, 
		        'map_payment_pref_types' AS source_table,
		        payment_pref_type, 
		        insert_dt, 
		        update_dt  
FROM 
	(
	SELECT DISTINCT nw.payment_type_pref_id         AS payment_pref_type_src_id,
			        nw.payment_type_pref            AS payment_pref_type,
					CURRENT_TIMESTAMP AS insert_dt,
					CURRENT_TIMESTAMP AS update_dt 
	FROM bl_3nf.incr_view_ce_payment_pref_types_no AS nw
	UNION ALL 
	SELECT DISTINCT il.payment_type_pref_id         AS payment_pref_type_src_id,
			        il.payment_type_pref            AS payment_pref_type,
					CURRENT_TIMESTAMP AS insert_dt,
					CURRENT_TIMESTAMP AS update_dt 
	FROM bl_3nf.incr_view_ce_payment_pref_types_io AS il
	) q 
GROUP BY payment_pref_type_src_id, 
	     source_system,
	     source_table ,	
	     payment_pref_type, 
		 insert_dt, 
		 update_dt;
		 
GET DIAGNOSTICS rows_affected = row_count;

UPDATE bl_cl.prm_mta_incremental_load
SET previous_loaded_dt = CURRENT_TIMESTAMP,
	procedure_name = 'bl_cl.map_payment_pref_types_insertion()'	
WHERE source_table IN ('src_northwest_sales', 'src_iowalakes_sales')
	  AND target_table = 'ce_payment_pref_types';   

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