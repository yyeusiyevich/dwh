INSERT INTO bl_3nf.ce_locations 
            (location_id,
             location_src_id,
             source_system,
             source_entity,
             address,
             postal_code,
             city_id,             
      		 insert_dt,
      		 update_dt)
VALUES(-1, -1, 'MANUAL', 'MANUAL', 'N/A', 'N/A', -1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
ON CONFLICT ON CONSTRAINT ce_locations_pkey DO NOTHING;   