INSERT INTO bl_dim.dim_junk (
  junk_surr_id,
  transaction_status,
  transaction_type,
  tax_type,
  insert_dt,
  update_dt
)
VALUES (-1, 'N/A', 'N/A', 'N/A', '01/01/1991'::DATE, '01/01/1991'::DATE)
ON CONFLICT ON CONSTRAINT dim_junk_pkey DO NOTHING;