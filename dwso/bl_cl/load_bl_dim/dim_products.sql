-- DROP PROCEDURE IF EXISTS bl_cl.dim_products_scd_insertion();
CREATE OR REPLACE PROCEDURE bl_cl.dim_products_scd_insertion()

LANGUAGE plpgsql 
AS $$

DECLARE
  schema_name   TEXT := 'bl_dim';
  table_name    TEXT := 'dim_products_scd';
  command_name  TEXT := 'insert';
  rows_affected INTEGER := 0;
  error_type    TEXT := '';
  output_message TEXT := '';

BEGIN 
	
WITH inserted AS (
    SELECT
        product_id,
        'bl_cl' AS source_system,
        'wrk_products' AS source_entity,
	    COALESCE(product_desc, 'N/A') AS product_desc,
	    COALESCE(category_name, 'N/A') AS category_name,
	    COALESCE(subcategory_name, 'N/A') AS subcategory_name,
	    COALESCE(pack, -1) AS pack,
	    COALESCE(bottle_volume, -1) AS bottle_volume,
	    COALESCE(safety_stock_lvl, -1) AS safety_stock_lvl,
	    COALESCE(reorder_point, -1) AS reorder_point,
	    start_date,
	    end_date,
	    is_active,
	    COALESCE(on_sale, 'N/A') AS on_sale
    FROM bl_cl.wrk_products
    WHERE product_id != -1
    ),
    upd AS (
      UPDATE bl_dim.dim_products_scd AS target
             SET is_active = 'N',
             end_date = CURRENT_TIMESTAMP,
             update_dt = CURRENT_TIMESTAMP
      FROM inserted
      WHERE inserted.product_id = target.product_id AND
            UPPER(target.is_active) = 'Y')
    INSERT INTO bl_dim.dim_products_scd
    (
        product_surr_id,
        product_id,
        source_system,
        source_entity,
        product_desc,
        category_name,
        subcategory_name,
        pack,
        bottle_volume_ml,
        safety_stock_lvl,
        reorder_point,
        start_date,
        end_date,
        is_active,
        product_on_sale,
        insert_dt,
        update_dt
    )
    SELECT
    
        NEXTVAL('bl_dim.dim_products_seq'),
        product_id,
        source_system,
        source_entity,
        product_desc,
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
        CURRENT_TIMESTAMP,
        CURRENT_TIMESTAMP
   FROM inserted
   WHERE UPPER(is_active) = 'Y';

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