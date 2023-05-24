-- DROP PROCEDURE IF EXISTS bl_cl.map_tax_indicators_insertion();
CREATE OR REPLACE PROCEDURE bl_cl.map_tax_indicators_insertion()

LANGUAGE plpgsql 
AS $$

DECLARE
  schema_name   TEXT := 'bl_cl';
  table_name    TEXT := 'map_tax_indicators';
  command_name  TEXT := 'insert';
  rows_affected INTEGER := 0;
  error_type    TEXT := '';
  output_message TEXT := '';

BEGIN 
	
TRUNCATE bl_cl.map_tax_indicators;	

INSERT INTO bl_cl.map_tax_indicators
SELECT DISTINCT tax_indicator_src_id, 
		        'bl_cl' AS source_system, 
		        'map_tax_indicators' AS source_table,
		        tax_type, 
		        insert_dt, 
		        update_dt  
FROM 
	(
	SELECT DISTINCT nw.tax_indicator_id    AS tax_indicator_src_id,
			        nw.tax_info            AS tax_type,
					CURRENT_TIMESTAMP AS insert_dt,
					CURRENT_TIMESTAMP AS update_dt 
	FROM bl_3nf.incr_view_ce_tax_indicators_no AS nw
	UNION ALL 
	SELECT DISTINCT il.tax_indicator_id    AS tax_indicator_src_id,
			        il.tax_info            AS tax_type,
					CURRENT_TIMESTAMP AS insert_dt,
					CURRENT_TIMESTAMP AS update_dt 
	FROM bl_3nf.incr_view_ce_tax_indicators_io AS il
	) q 
GROUP BY tax_indicator_src_id, 
	     source_system,
	     source_table,	
	     tax_type, 
		 insert_dt, 
		 update_dt;
		 
GET DIAGNOSTICS rows_affected = row_count;

UPDATE bl_cl.prm_mta_incremental_load
SET previous_loaded_dt = CURRENT_TIMESTAMP,
	procedure_name = 'bl_cl.map_tax_indicators_insertion()'	
WHERE source_table IN ('src_northwest_sales', 'src_iowalakes_sales')
	  AND target_table = 'ce_tax_indicators';    

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