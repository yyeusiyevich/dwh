-- DROP PROCEDURE IF EXISTS bl_cl.map_subcategories_insertion();
CREATE OR REPLACE PROCEDURE bl_cl.map_subcategories_insertion()

LANGUAGE plpgsql 
AS $$

DECLARE
  schema_name   TEXT := 'bl_cl';
  table_name    TEXT := 'map_subcategories';
  command_name  TEXT := 'insert';
  rows_affected INTEGER := 0;
  error_type    TEXT := '';
  output_message TEXT := '';

BEGIN 
	
TRUNCATE bl_cl.map_subcategories;

INSERT INTO bl_cl.map_subcategories
SELECT DISTINCT subcategory_src_id, 
		        'bl_cl' AS source_system, 
		        'map_subcategories' AS source_table,
		        subcategory_name, 
		        product_category_src_id,
		        insert_dt, 
		        update_dt  
FROM 
	(
	SELECT DISTINCT nw.subcategory_id               AS subcategory_src_id,
			        nw.subcategory_name             AS subcategory_name ,
			        COALESCE(nw.category_id, 'N/A') AS product_category_src_id , 
					CURRENT_TIMESTAMP AS insert_dt,
					CURRENT_TIMESTAMP AS update_dt 
	FROM bl_3nf.incr_view_ce_product_subcategories_no AS nw
	UNION ALL 
	SELECT DISTINCT il.subcategory_id                AS subcategory_src_id,
			        il.subcategory_name              AS subcategory_name ,
			        COALESCE(il.category_id, 'N/A')  AS product_category_src_id , 
					CURRENT_TIMESTAMP AS insert_dt,
					CURRENT_TIMESTAMP AS update_dt 
	FROM bl_3nf.incr_view_ce_product_subcategories_io AS il
	) q 
GROUP BY subcategory_src_id, 
	     source_system,
	     source_table ,	
	     subcategory_name, 
		 product_category_src_id,
		 insert_dt, 
		 update_dt;
		 
GET DIAGNOSTICS rows_affected = row_count;

UPDATE bl_cl.prm_mta_incremental_load
SET previous_loaded_dt = CURRENT_TIMESTAMP,
	procedure_name = 'bl_cl.map_subcategories_insertion()'	
WHERE source_table IN ('src_northwest_sales', 'src_iowalakes_sales')
	  AND target_table = 'ce_product_subcategories';    

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