INSERT INTO bl_3nf.ce_tax_indicators
            (tax_indicator_id,
             tax_indicator_src_id,
             source_system,
             source_entity,
             tax_type,
             insert_dt, 
             update_dt)             
VALUES (-1, -1, 'MANUAL', 'MANUAL', 'N/A', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
ON CONFLICT ON CONSTRAINT ce_tax_indicators_pkey DO NOTHING;   