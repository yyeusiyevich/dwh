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
VALUES(-1, -1, 'N/A', 'MANUAL', 'MANUAL', -1, -1, -1, -1, -1, '9999-12-31', '9999-12-31', 'N', 'N', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)         
ON CONFLICT ON CONSTRAINT ce_products_pkey DO NOTHING;  