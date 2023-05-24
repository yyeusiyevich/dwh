INSERT INTO bl_dim.dim_vendors (
  vendor_surr_id,
  vendor_id,
  name,
  start_date,
  end_date,
  is_active,
  contact_phone,
  contact_name,
  rating,
  size,
  homepage,
  insert_dt,
  update_dt
)
VALUES (-1, -1, 'N/A', '01/01/1900'::DATE, '01/01/1900'::DATE, 'N/A', 'N/A', 'N/A', -1, 'N/A', 
		'N/A', '01/01/1900'::DATE, '01/01/1900'::DATE)
ON CONFLICT ON CONSTRAINT dim_vendors_pkey DO NOTHING;