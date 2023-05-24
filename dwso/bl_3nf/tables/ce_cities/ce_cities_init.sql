INSERT INTO bl_3nf.ce_cities
            (city_id,
             city_src_id,
             source_system,
             source_entity,
             county_id,
             city_name,
             insert_dt, 
             update_dt)        
VALUES(-1, -1, 'MANUAL', 'MANUAL', -1, 'N/A', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
ON CONFLICT ON CONSTRAINT ce_cities_pkey DO NOTHING;   