CREATE OR REPLACE PROCEDURE bl_cl.ce_products_insertion()

LANGUAGE plpgsql 
AS $$

DECLARE
  schema_name   TEXT := 'bl_3nf';
  table_name    TEXT := 'ce_products';
  command_name  TEXT := 'insert';
  rows_affected INTEGER := 0;
  error_type    TEXT := '';
  output_message TEXT := '';

BEGIN 

WITH new_rows AS  (
     SELECT COALESCE(mp.product_src_id, 'N/A') 		AS product_src_id,
			COALESCE(mp.product_desc, 'N/A')   		AS product_desc,    
			mp.source_system 				   		AS source_system,
			mp.source_table 				   		AS source_entity,
			COALESCE(nf.product_category_id, -1) 	AS product_category_id, 			
			COALESCE(mp.pack::INT, -1) AS pack,
			COALESCE(mp.bottle_volume::INT, -1) 	AS bottle_volume,
			COALESCE(mp.safety_stock_lvl::INT, -1)  AS safety_stock_lvl,
			COALESCE(mp.reorder_point::INT, -1) 	AS reorder_point,
			'1900-01-01'::DATE						AS start_date,
			'9999-12-31'::DATE						AS end_date,
			'Y' 									AS is_active,
			COALESCE(mp.on_sale, 'N/A') 			AS on_sale,			
 			CURRENT_TIMESTAMP						AS insert_dt,
			CURRENT_TIMESTAMP						AS update_dt 
		FROM bl_cl.map_products AS mp
		LEFT OUTER JOIN bl_3nf.ce_product_categories nf
		ON mp.product_category_src_id = nf.product_category_src_id 
     	WHERE NOT EXISTS (SELECT 1
                       	  FROM   bl_3nf.ce_products AS prod
                       	  WHERE  UPPER(mp.product_src_id) = UPPER(prod.product_src_id) AND
	                        	 mp.source_system = prod.source_system AND
	                          	 mp.source_table = prod.source_entity AND
	                             UPPER(mp.product_desc) = UPPER(prod.product_desc) AND
	                             nf.product_category_id = prod.category_id AND
	                             mp.pack::INT = prod.pack AND
	                             mp.bottle_volume::INT = prod.bottle_volume AND
	                             mp.safety_stock_lvl::INT = prod.safety_stock_lvl AND
	                             mp.reorder_point::INT = prod.reorder_point AND
	                             UPPER(mp.on_sale) = UPPER(prod.on_sale) AND
	                             UPPER(prod.is_active) = 'Y'
                        )
	 								),								
	  upd AS (
      UPDATE bl_3nf.ce_products AS target
             SET is_active = 'N',
             end_date = CURRENT_TIMESTAMP,
             update_dt = CURRENT_TIMESTAMP
      FROM new_rows
      WHERE UPPER(new_rows.product_src_id) = UPPER(target.product_src_id) AND
            UPPER(target.is_active) = 'Y'
     		 )
INSERT INTO bl_3nf.ce_products
            (product_id,
             product_src_id,
             product_desc,
             source_system,
             source_entity,
             category_id,
             pack,
             bottle_volume,
             safety_stock_lvl,
             reorder_point,
             start_date,
             end_date,
             is_active,
             on_sale,
             insert_dt, 
             update_dt)    
SELECT CASE WHEN EXISTS (SELECT 1 
                         FROM bl_3nf.ce_products AS pd 
                         WHERE pd.product_src_id = new_rows.product_src_id) 
            THEN (SELECT pd.product_id 
                  FROM bl_3nf.ce_products AS pd 
                  WHERE pd.product_src_id = new_rows.product_src_id) 
       		ELSE NEXTVAL('bl_3nf.ce_employees_seq')
	   END,
	   product_src_id,
       product_desc,
       source_system,
       source_entity,
       product_category_id,
       pack,
       bottle_volume,
       safety_stock_lvl,
       reorder_point,
	   CASE WHEN EXISTS (SELECT 1
                         FROM bl_3nf.ce_products AS pd, new_rows
                         WHERE  UPPER(new_rows.product_src_id) = UPPER(pd.product_src_id) AND
                                UPPER(new_rows.source_system) = UPPER(pd.source_system) AND
                                UPPER(new_rows.source_entity) = UPPER(pd.source_entity)) 
        THEN CURRENT_DATE
        ELSE '1900-01-01'::DATE 
        END,
        end_date,
        is_active,
        on_sale,
        insert_dt, 
        update_dt
FROM    new_rows
ON CONFLICT ON CONSTRAINT prod_pk DO NOTHING;     		 

GET DIAGNOSTICS rows_affected = row_count;

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