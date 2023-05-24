INSERT INTO bl_dim.dim_stores (
  store_surr_id,
  store_id,
  name,
  start_date,
  end_date,
  is_active,
  contact_phone,
  contact_email,
  region,
  county_code,
  county_name,
  postal_code,
  city,
  address,
  payment_type_pref,
  membership_flag,
  opt_in_flag,
  curr_emp_first_name,
  curr_emp_last_name,
  curr_emp_full_name,
  curr_emp_phone,
  curr_emp_email,
  insert_dt,
  update_dt
)
VALUES (-1, -1, 'N/A', '01/01/1991'::DATE, '01/01/1991'::DATE, 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A',
		 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', '01/01/1991'::DATE, '01/01/1991'::DATE)
ON CONFLICT ON CONSTRAINT dim_stores_pkey DO NOTHING;