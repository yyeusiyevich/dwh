INSERT INTO bl_3nf.ce_regions
            (region_id,
             region_src_id,
             source_system,
             source_entity,
             region_name,
             insert_dt, 
             update_dt)             
VALUES(-1, -1, 'MANUAL', 'MANUAL', 'N/A', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
ON CONFLICT ON CONSTRAINT ce_regions_pkey DO NOTHING;   