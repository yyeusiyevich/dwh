INSERT INTO bl_dim.dim_employees (
  employee_surr_id,
  employee_id,
  first_name,
  last_name,
  gender,
  dob,
  hire_date,
  start_date,
  end_date,
  is_active,
  phone,
  email,
  postal_code,
  city,
  address,
  position_name,
  vacation_hours,
  sick_leave_hours,
  insert_dt,
  update_dt
)
VALUES (-1, -1, 'N/A', 'N/A', 'N/A', '01/01/1991'::DATE, '01/01/1991'::DATE, '01/01/1991'::DATE, '01/01/1991'::DATE,
		'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', -1, -1, '01/01/1991'::DATE, '01/01/1991'::DATE)
ON CONFLICT ON CONSTRAINT dim_employees_pkey DO NOTHING;