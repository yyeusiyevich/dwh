CREATE OR REPLACE FUNCTION bl_cl.missing_rows()
   RETURNS TABLE (table_name 	  TEXT,
				  rows_difference INTEGER)
AS $$
DECLARE 
	
	src_nw_ce_sales INTEGER;
	src_il_ce_sales INTEGER;
	ce_sales_src_nw INTEGER;
	ce_sales_src_il INTEGER;
    bl_3nf_dim		INTEGER;
    dim_bl_3nf		INTEGER;

BEGIN

SELECT COUNT(*) INTO src_nw_ce_sales FROM 
(
  SELECT TRIM(vendor_name),
  		 TRIM(invoice_and_item_number),
         TRIM(store_name),
         TRIM(emp_first_name),
         TRIM(emp_last_name),
         TRIM(payment_type_pref) AS payment_name, 
         TRIM(item_description),
         date::DATE AS transaction_date,
         state_bottle_retail::NUMERIC * bottles_sold::INT AS transaction_amount
  FROM sa_northwest_sales.src_northwest_sales
  EXCEPT 
  SELECT cv.name,
  		 css.transaction_src_id,
  		 cs.name,
  		 TRIM(ce.first_name),
  		 TRIM(ce.last_name),
  		 cp.payment_type_pref, 
  		 cpr.product_desc,
  		 css.event_date,
  		 css.state_bottle_retail * css.quantity_sold
  FROM bl_3nf.ce_sales css
  LEFT JOIN bl_3nf.ce_vendors cv USING (vendor_id)
  LEFT JOIN bl_3nf.ce_stores cs USING (store_id)
  LEFT JOIN bl_3nf.ce_employees ce USING (employee_id)
  LEFT JOIN bl_3nf.ce_payment_pref_types cp USING (payment_type_pref_id)
  LEFT JOIN bl_3nf.ce_products cpr USING (product_id)
  LEFT JOIN bl_3nf.ce_shippers AS sh USING (shipper_id)
  WHERE css.source_system = 'sa_northwest_sales'
) c;

SELECT COUNT(*) INTO src_il_ce_sales FROM 
(
  SELECT TRIM(vendor_name) AS vendor_name,
  		 TRIM(invoice_and_item_number),
         TRIM(store_name) AS store_name,
         TRIM(emp_first_name) AS employee_first_name,
         TRIM(emp_last_name) AS employee_last_name,
         TRIM(payment_type_pref) AS payment_name, 
         TRIM(item_description) AS product_name,
         date::DATE AS transaction_date,
         state_bottle_retail::NUMERIC * bottles_sold::INT AS transaction_amount
  FROM sa_iowalakes_sales.src_iowalakes_sales 
  EXCEPT 
  SELECT cv.name, 
  		 css.transaction_src_id,
  		 cs.name,
  		 TRIM(ce.first_name),
  		 TRIM(ce.last_name),
  		 cp.payment_type_pref, 
  		 cpr.product_desc,
  		 css.event_date, 
  		 css.state_bottle_retail * css.quantity_sold
  FROM bl_3nf.ce_sales css
  LEFT JOIN bl_3nf.ce_shippers AS sh USING (shipper_id)
  LEFT JOIN bl_3nf.ce_vendors cv USING (vendor_id)
  LEFT JOIN bl_3nf.ce_stores cs USING (store_id)
  LEFT JOIN bl_3nf.ce_employees ce USING (employee_id)
  LEFT JOIN bl_3nf.ce_payment_pref_types cp USING (payment_type_pref_id)
  LEFT JOIN bl_3nf.ce_products cpr USING (product_id)
  WHERE cs.source_system = 'sa_iowalakes_sales'
) c;

SELECT COUNT(*) INTO ce_sales_src_nw FROM
(
  SELECT cv.name, 
  		 css.transaction_src_id,
  		 cs.name,
  		 TRIM(ce.first_name),
  		 TRIM(ce.last_name),
  		 cp.payment_type_pref, 
  		 cpr.product_desc,
  		 css.event_date, 
  		 css.state_bottle_retail * css.quantity_sold
  FROM bl_3nf.ce_sales css
  LEFT JOIN bl_3nf.ce_shippers AS sh USING (shipper_id)
  LEFT JOIN bl_3nf.ce_vendors cv USING (vendor_id)
  LEFT JOIN bl_3nf.ce_stores cs USING (store_id)
  LEFT JOIN bl_3nf.ce_employees ce USING (employee_id)
  LEFT JOIN bl_3nf.ce_payment_pref_types cp USING (payment_type_pref_id)
  LEFT JOIN bl_3nf.ce_products cpr USING (product_id)
  WHERE cs.source_system = 'sa_northwest_sales'
  EXCEPT 
  SELECT TRIM(vendor_name) AS vendor_name,
  		 TRIM(invoice_and_item_number),
         TRIM(store_name) AS store_name,
         TRIM(emp_first_name) AS employee_first_name,
         TRIM(emp_last_name) AS employee_last_name,
         TRIM(payment_type_pref) AS payment_name, 
         TRIM(item_description) AS product_name,
         date::DATE AS transaction_date,
         state_bottle_retail::NUMERIC * bottles_sold::INT AS transaction_amount
  FROM sa_northwest_sales.src_northwest_sales
  ) q;
  
 SELECT COUNT(*) INTO ce_sales_src_il FROM
(
 SELECT  cv.name, 
  		 css.transaction_src_id,
  		 cs.name,
  		 TRIM(ce.first_name),
  		 TRIM(ce.last_name),
  		 cp.payment_type_pref, 
  		 cpr.product_desc,
  		 css.event_date, 
  		 css.state_bottle_retail * css.quantity_sold
  FROM bl_3nf.ce_sales css
  LEFT JOIN bl_3nf.ce_shippers AS sh USING (shipper_id)
  LEFT JOIN bl_3nf.ce_vendors cv USING (vendor_id)
  LEFT JOIN bl_3nf.ce_stores cs USING (store_id)
  LEFT JOIN bl_3nf.ce_employees ce USING (employee_id)
  LEFT JOIN bl_3nf.ce_payment_pref_types cp USING (payment_type_pref_id)
  LEFT JOIN bl_3nf.ce_products cpr USING (product_id)
  WHERE cs.source_system = 'sa_iowalakes_sales'
  EXCEPT 
  SELECT TRIM(vendor_name) AS vendor_name,
  		 TRIM(invoice_and_item_number),
         TRIM(store_name) AS store_name,
         TRIM(emp_first_name) AS employee_first_name,
         TRIM(emp_last_name) AS employee_last_name,
         TRIM(payment_type_pref) AS payment_name, 
         TRIM(item_description) AS product_name,
         date::DATE AS transaction_date,
         state_bottle_retail::NUMERIC * bottles_sold::INT AS transaction_amount
  FROM sa_iowalakes_sales.src_iowalakes_sales
  ) q;
 
 
SELECT COUNT(*) INTO bl_3nf_dim FROM 
(
  SELECT css.transaction_id,
  		 cv.name, 
  		 sh.name,
  		 cs.name,
  		 cp.payment_type_pref, 
  		 ce.first_name,
  		 ce.last_name,
  		 cpr.product_desc,
  		 css.event_date,
  		 tr_st.status_name,
  		 tr_tp.type_name,
  		 tax_i.tax_type
  FROM bl_3nf.ce_sales css
  LEFT JOIN bl_3nf.ce_shippers AS sh USING (shipper_id)
  LEFT JOIN bl_3nf.ce_vendors cv USING (vendor_id)
  LEFT JOIN bl_3nf.ce_stores cs USING (store_id)
  LEFT JOIN bl_3nf.ce_employees ce USING (employee_id)
  LEFT JOIN bl_3nf.ce_payment_pref_types cp USING (payment_type_pref_id)
  LEFT JOIN bl_3nf.ce_products cpr USING (product_id)
  LEFT JOIN bl_3nf.ce_transaction_statuses tr_st USING (status_id)
  LEFT JOIN bl_3nf.ce_transaction_types AS tr_tp USING (type_id)
  LEFT JOIN bl_3nf.ce_tax_indicators AS tax_i USING (tax_indicator_id) 
  EXCEPT 
  SELECT fct.transaction_id, 
  		 cv.name, 
  		 sh.name,
  		 cs.name,
  		 cs.payment_type_pref,
  		 ce.first_name,
  		 ce.last_name,
  		 cpr.product_desc,
  		 fct.event_date,
  		 jk.transaction_status,
  		 jk.transaction_type,
  		 jk.tax_type
  FROM bl_dim.fct_sales fct
  LEFT JOIN bl_dim.dim_shippers_scd AS sh USING (shipper_surr_id)
  LEFT JOIN bl_dim.dim_vendors_scd cv USING (vendor_surr_id)
  LEFT JOIN bl_dim.dim_stores_scd cs USING (store_surr_id)
  LEFT JOIN bl_dim.dim_employees_scd ce USING (employee_surr_id)
  LEFT JOIN bl_dim.dim_junk jk USING (junk_surr_id)
  LEFT JOIN bl_dim.dim_products_scd cpr USING (product_surr_id)
) c;

SELECT COUNT(*) INTO dim_bl_3nf FROM 
(
SELECT fct.transaction_id, 
  		 cv.name, 
  		 sh.name,
  		 cs.name,
  		 cs.payment_type_pref,
  		 ce.first_name,
  		 ce.last_name,
  		 cpr.product_desc,
  		 fct.event_date,
  		 jk.transaction_status,
  		 jk.transaction_type,
  		 jk.tax_type
  FROM bl_dim.fct_sales fct
  LEFT JOIN bl_dim.dim_shippers_scd AS sh USING (shipper_surr_id)
  LEFT JOIN bl_dim.dim_vendors_scd cv USING (vendor_surr_id)
  LEFT JOIN bl_dim.dim_stores_scd cs USING (store_surr_id)
  LEFT JOIN bl_dim.dim_employees_scd ce USING (employee_surr_id)
  LEFT JOIN bl_dim.dim_junk jk USING (junk_surr_id)
  LEFT JOIN bl_dim.dim_products_scd cpr USING (product_surr_id)
  WHERE fct.transaction_id != -1
  EXCEPT
  SELECT 	 css.transaction_id,
	  		 cv.name, 
	  		 sh.name,
	  		 cs.name,
	  		 cp.payment_type_pref, 
	  		 ce.first_name,
	  		 ce.last_name,
	  		 cpr.product_desc,
	  		 css.event_date,
	  		 tr_st.status_name,
	  		 tr_tp.type_name,
	  		 tax_i.tax_type
  FROM bl_3nf.ce_sales css
  LEFT JOIN bl_3nf.ce_shippers AS sh USING (shipper_id)
  LEFT JOIN bl_3nf.ce_vendors cv USING (vendor_id)
  LEFT JOIN bl_3nf.ce_stores cs USING (store_id)
  LEFT JOIN bl_3nf.ce_employees ce USING (employee_id)
  LEFT JOIN bl_3nf.ce_payment_pref_types cp USING (payment_type_pref_id)
  LEFT JOIN bl_3nf.ce_products cpr USING (product_id)
  LEFT JOIN bl_3nf.ce_transaction_statuses tr_st USING (status_id)
  LEFT JOIN bl_3nf.ce_transaction_types AS tr_tp USING (type_id)
  LEFT JOIN bl_3nf.ce_tax_indicators AS tax_i USING (tax_indicator_id) 
) c;
  
RETURN query 
SELECT 'src_nortwest_sales to ce_sales', src_nw_ce_sales
UNION 
SELECT 'src_iowalakes_sales to ce_sales', src_il_ce_sales
UNION
SELECT 'ce_sales to src_nortwest_sales', ce_sales_src_nw
UNION 
SELECT 'ce_sales to src_iowalakes_sales', ce_sales_src_il
UNION
SELECT 'ce_sales to fct_sales', bl_3nf_dim
UNION
SELECT 'fct_sales to ce_sales', dim_bl_3nf;

END; 
$$ LANGUAGE plpgsql;