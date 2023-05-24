INSERT INTO bl_3nf.ce_transaction_statuses
            (status_id,
             status_src_id,
             source_system,
             source_entity,
             status_name,
             insert_dt, 
             update_dt)             
VALUES (-1, -1, 'MANUAL', 'MANUAL', 'N/A', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
ON CONFLICT ON CONSTRAINT ce_transaction_statuses_pkey DO NOTHING;  