--------------------------------------------
--_______________PREPARATIONS_______________
--------------------------------------------

-- change the data source in external tables --

DROP FOREIGN TABLE IF EXISTS sa_northwest_sales.ext_northwest_sales;
DROP FOREIGN TABLE IF EXISTS sa_iowalakes_sales.ext_iowalakes_sales;

-- recreate external tables (source)
CALL bl_cl.ext_creation();






--------------------------------------------
--_______________SA LOAD_______________-----
--------------------------------------------

-- load sa (incremental)
CALL bl_cl.sa_iowalakes_load();
CALL bl_cl.sa_northwest_load();











--------------------------------------------
--_______________BL3NF LOAD_______________--
--------------------------------------------

-- bl_cl insertion (mapping tables)
CALL bl_cl.map_categories_insertion();
CALL bl_cl.map_subcategories_insertion();
CALL bl_cl.map_positions_insertion();
CALL bl_cl.map_payment_pref_types_insertion();
CALL bl_cl.map_tax_indicators_insertion();
CALL bl_cl.map_transaction_statuses_insertion();
CALL bl_cl.map_transaction_types_insertion();
CALL bl_cl.map_products_insertion(); 
CALL bl_cl.map_curr_employees_insertion();
CALL bl_cl.map_shippers_insertion();

-- bl3nf data insertion
CALL bl_cl.ce_product_categories_insertion();
CALL bl_cl.ce_product_subcategories_insertion();

CALL bl_cl.ce_positions_insertion();
CALL bl_cl.ce_payment_pref_types_insertion();

CALL bl_cl.ce_tax_indicators_insertion();
CALL bl_cl.ce_transaction_statuses_insertion();
CALL bl_cl.ce_transaction_types_insertion();

CALL bl_cl.ce_regions_insertion();
CALL bl_cl.ce_counties_insertion();
CALL bl_cl.ce_cities_insertion();
CALL bl_cl.ce_locations_insertion();

-- scd insertion
CALL bl_cl.ce_employees_insertion();
CALL bl_cl.ce_products_insertion();
CALL bl_cl.ce_shippers_insertion();
CALL bl_cl.ce_vendors_insertion();
CALL bl_cl.ce_curr_emp_profiles_insertion(); 
COMMIT;
CALL bl_cl.ce_stores_insertion();















--------------------------------------------
--_______________BL3NF FCT LOAD_____________
--------------------------------------------

-- load fct table 
CALL bl_cl.ce_sales_insertion();





























--------------------------------------------
--_______________BLDIM LOAD_____________----
--------------------------------------------

-- working tables and dim insertion
CALL bl_cl.wrk_shippers_insertion();
CALL bl_cl.dim_shippers_scd_insertion();

CALL bl_cl.wrk_employees_insertion();
CALL bl_cl.dim_employees_scd_insertion();

CALL bl_cl.wrk_products_insertion();
CALL bl_cl.dim_products_scd_insertion();

CALL bl_cl.dim_vendors_scd_insertion();

CALL bl_cl.wrk_junk_insertion();
CALL bl_cl.dim_junk_insertion();

CALL bl_cl.wrk_stores_insertion(); 
CALL bl_cl.dim_stores_scd_insertion();








------------------------------------------------
--_______________BLDIM FCT LOAD_____________----
------------------------------------------------

-- load fct table (partitioning load)
CALL bl_cl.fct_sales_insertion('partition');






















