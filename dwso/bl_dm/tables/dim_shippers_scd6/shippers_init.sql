INSERT INTO bl_dim.dim_shippers (
  shipper_surr_id,
  shipper_id,
  name,
  start_date,
  end_date,
  is_active,
  rating,
  ship_base,
  ship_rate,
  contact_phone,
  contact_name,
  current_region,
  historic_region,
  insert_dt,
  update_dt
)
VALUES (-1, -1, 'N/A', '01/01/1991'::DATE, '01/01/1991'::DATE, 'N/A', -1, -1, -1, 'N/A',
		'N/A', 'N/A', 'N/A', '01/01/1991'::DATE, '01/01/1991'::DATE)
ON CONFLICT ON CONSTRAINT dim_shippers_pkey DO NOTHING;