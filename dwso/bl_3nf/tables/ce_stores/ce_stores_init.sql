INSERT INTO bl_3nf.ce_stores 
            (store_id,
             store_src_id,
             source_system,
             source_entity,
             name,
             start_date,
             end_date,             
             is_active,
      		 contact_phone,
      		 contact_email,
      		 location_id,
      		 payment_type_pref_id,
      		 curr_emp_profile_id,
      		 opt_in_flag,
      		 membership_flag,
      		 insert_dt,
      		 update_dt)
VALUES(-1, -1, 'MANUAL', 'MANUAL', 'N/A', '9999-12-31', '9999-12-31', 'N', 'N/A', 'N/A', -1, -1, -1, 'N', 'N', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
ON CONFLICT ON CONSTRAINT ce_stores_pkey DO NOTHING;  