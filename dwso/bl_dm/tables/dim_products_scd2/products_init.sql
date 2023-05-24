INSERT INTO bl_dim.dim_products (
  product_surr_id,
  product_id,
  produc_desc,
  category_name,
  subcategory_name,
  pack,
  bottle_volume_ml,
  safety_stock_lvl,
  reorder_point,
  start_date,
  end_date,
  is_active,
  product_on_sale,
  insert_dt,
  update_dt
)
VALUES (-1, -1, 'N/A', 'N/A', 'N/A', -1, -1, -1, -1, '01/01/1991'::DATE, '01/01/1991'::DATE, 'N/A', 'N/A', '01/01/1991'::DATE,
	    '01/01/1991'::DATE)
ON CONFLICT ON CONSTRAINT dim_products_pkey DO NOTHING;