INSERT INTO bl_3nf.ce_product_subcategories
            (product_subcategory_id,
             product_subcategory_src_id,
             source_system,
             source_entity,
             category_id,
             subcategory_name,
             insert_dt, 
             update_dt)
VALUES (-1, -1, 'MANUAL', 'MANUAL', -1, 'N/A', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
ON CONFLICT ON CONSTRAINT ce_product_subcategories_pkey DO NOTHING;  