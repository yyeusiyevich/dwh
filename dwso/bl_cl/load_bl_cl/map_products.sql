-- DROP PROCEDURE IF EXISTS bl_cl.map_products_insertion();
CREATE OR REPLACE PROCEDURE bl_cl.map_products_insertion()

LANGUAGE plpgsql 
AS $$

DECLARE
  schema_name   TEXT := 'bl_cl';
  table_name    TEXT := 'map_products';
  command_name  TEXT := 'insert';
  rows_affected INTEGER := 0;
  error_type    TEXT := '';
  output_message TEXT := '';

BEGIN 

TRUNCATE bl_cl.map_products;
	
INSERT INTO bl_cl.map_products
SELECT		    product_src_id, 
		        'bl_cl' AS source_system, 
		        'map_products' AS source_table,
		        product_desc, 
		        product_category_src_id,
		        pack,
		        bottle_volume,
            	safety_stock_lvl,
            	reorder_point,
             	on_sale,
		        insert_dt, 
		        update_dt  
FROM 
	(
	SELECT DISTINCT nw.item_id                  	AS product_src_id,
			        nw.item_description         	AS product_desc,
			        COALESCE(nw.category_id, 'N/A') AS product_category_src_id, 
			        pack,
			        bottle_volume_ml				AS bottle_volume,
			        safety_stock_lvl,
			        reorder_point,
			        product_on_sale					AS on_sale,
					CURRENT_TIMESTAMP 				AS insert_dt,
					CURRENT_TIMESTAMP 				AS update_dt 
	FROM bl_3nf.incr_view_ce_products_no AS nw
	UNION ALL 
	SELECT DISTINCT il.item_id                		AS product_src_id,
			        il.item_description       		AS product_desc,
			        COALESCE(il.category_id, 'N/A') AS product_category_src_id, 
			        pack,
			        bottle_volume_ml				AS bottle_volume,
			        safety_stock_lvl,
			        reorder_point,
			        product_on_sale					AS on_sale,
					CURRENT_TIMESTAMP 				AS insert_dt,
					CURRENT_TIMESTAMP 				AS update_dt 
	FROM bl_3nf.incr_view_ce_products_io AS il
	) q 
GROUP BY product_src_id, 
	     source_system,
	     source_table,	
	     product_desc, 
		 product_category_src_id,
		 pack,
		 bottle_volume,
         safety_stock_lvl,
         reorder_point,
         on_sale,	
		 insert_dt, 
		 update_dt;
		 
GET DIAGNOSTICS rows_affected = row_count;

UPDATE bl_cl.prm_mta_incremental_load
SET previous_loaded_dt = CURRENT_TIMESTAMP,
	procedure_name = 'bl_cl.map_products_insertion()'	
WHERE source_table IN ('src_northwest_sales', 'src_iowalakes_sales')
	  AND target_table = 'ce_products';    

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