INSERT INTO bl_3nf.ce_vendors 
            (vendor_id,
             vendor_src_id,
             source_system,
             source_entity,
             name,
             start_date,
             end_date,
             is_active,
             rating,
             size,
      		 contact_phone,
      		 contact_name,
      		 homepage,
             insert_dt, 
             update_dt)      		 
VALUES(-1, -1, 'MANUAL', 'MANUAL', 'N/A', '9999-12-31', '9999-12-31', 'N', -1, 'N/A', 'N/A', 'N/A', 'N/A', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
ON CONFLICT ON CONSTRAINT ce_vendors_pkey DO NOTHING;  