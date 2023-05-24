INSERT INTO bl_3nf.ce_payment_pref_types
            (payment_type_pref_id,
             payment_type_pref_src_id,
             source_system,
             source_entity,
             payment_type_pref, 
             insert_dt, 
             update_dt)
VALUES (-1, -1, 'MANUAL', 'MANUAL', 'N/A', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
ON CONFLICT ON CONSTRAINT ce_payment_pref_types_pkey DO NOTHING;   