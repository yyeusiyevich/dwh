INSERT INTO bl_3nf.ce_product_categories
            (product_category_id,
             product_category_src_id,
             source_system,
             source_entity,
             category_name,
             insert_dt, 
             update_dt)
VALUES (-1, 'N/A', 'MANUAL', 'MANUAL', 'N/A', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
ON CONFLICT ON CONSTRAINT ce_product_categories_pkey DO NOTHING;  
