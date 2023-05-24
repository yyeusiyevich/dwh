INSERT INTO bl_3nf.ce_shippers 
            (shipper_id,
             shipper_src_id,
             source_system,
             source_entity,
             name,
             start_date,
             end_date,             
             is_active,
             rating,
             ship_base,
             ship_rate,
      		 contact_phone,
      		 contact_name,
      		 curr_county_id,
      		 historical_county_id,
      		 insert_dt,
      		 update_dt)
VALUES(-1, -1, 'MANUAL', 'MANUAL', 'N/A', '9999-12-31', '9999-12-31', 'N', -1, -1, -1, 'N/A', 'N/A', -1, -1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
ON CONFLICT ON CONSTRAINT ce_shippers_pkey DO NOTHING;  
	