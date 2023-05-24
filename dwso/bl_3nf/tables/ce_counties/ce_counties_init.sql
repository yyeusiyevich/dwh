INSERT INTO bl_3nf.ce_counties
            (county_id,
             county_src_id,
             source_system,
             source_entity,
             region_id,
             county_code,
             county_name,
             insert_dt, 
             update_dt)           
VALUES(-1, -1, 'MANUAL', 'MANUAL', -1, 'N/A', 'N/A', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
ON CONFLICT ON CONSTRAINT ce_counties_pkey DO NOTHING;   