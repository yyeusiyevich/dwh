INSERT INTO bl_3nf.ce_transaction_types
            (type_id,
             type_src_id,
             source_system,
             source_entity,
             type_name,
             insert_dt, 
             update_dt)           
VALUES (-1, -1, 'MANUAL', 'MANUAL', 'N/A', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
ON CONFLICT ON CONSTRAINT ce_transaction_types_pkey DO NOTHING;  