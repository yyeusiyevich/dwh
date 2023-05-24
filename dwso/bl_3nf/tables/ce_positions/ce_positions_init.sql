INSERT INTO bl_3nf.ce_positions
            (position_id,
             position_src_id,
             source_system,
             source_entity,
             position_name, 
             insert_dt, 
             update_dt)
VALUES (-1, -1, 'MANUAL', 'MANUAL', 'N/A', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
ON CONFLICT ON CONSTRAINT ce_positions_pkey DO NOTHING;  