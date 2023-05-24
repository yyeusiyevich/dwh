-- DROP PROCEDURE IF EXISTS bl_cl.wrk_products_insertion();
CREATE OR REPLACE PROCEDURE bl_cl.wrk_products_insertion()

LANGUAGE plpgsql 
AS $$

DECLARE
  schema_name   TEXT := 'bl_cl';
  table_name    TEXT := 'wrk_products';
  command_name  TEXT := 'insert';
  rows_affected INTEGER := 0;
  error_type    TEXT := '';
  output_message TEXT := '';
 
  last_load_date TIMESTAMP;

BEGIN 
	
TRUNCATE bl_cl.wrk_products;

SELECT COALESCE(MAX(update_dt), '1900-01-01'::TIMESTAMP) INTO last_load_date FROM bl_dim.dim_products_scd;

INSERT INTO bl_cl.wrk_products (
				product_id,
				product_src_id,
				product_desc,
				source_system,
				source_entity,
				category_name,
				subcategory_name,
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
SELECT  product_id,
	    product_src_id,
	    product_desc,
	   'bl_3nf' AS source_system,
	   'ce_products' AS source_entity,
	    cat.category_name,
    	STRING_AGG(DISTINCT subcat.subcategory_name, ', ') AS subcategory_name,
		pack,
		bottle_volume,
		safety_stock_lvl,
		reorder_point,
		start_date,
		end_date,
		is_active,
		on_sale,
	    CURRENT_TIMESTAMP,
	    CURRENT_TIMESTAMP
FROM bl_3nf.ce_products AS prod
LEFT OUTER JOIN bl_3nf.ce_product_categories AS cat ON prod.category_id = cat.product_category_id 
LEFT OUTER JOIN bl_3nf.ce_product_subcategories AS subcat ON subcat.category_id = cat.product_category_id
WHERE GREATEST(prod.update_dt, cat.update_dt, subcat.update_dt) > last_load_date
GROUP BY
    prod.product_id,
    prod.product_src_id,
    prod.product_desc, 
    cat.category_name,
    prod.pack,
    prod.bottle_volume,
    prod.safety_stock_lvl,
    prod.reorder_point,
    prod.start_date,
    prod.end_date,
    prod.is_active,
    prod.on_sale;
		 
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