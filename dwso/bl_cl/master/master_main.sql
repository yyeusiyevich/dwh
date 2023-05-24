--------------------------------------------------
--___________CREATE MAIN STRUCTURES___________----
--------------------------------------------------

-- create bl_cl structure
CALL bl_cl.bl_cl_schema_creation();
CALL bl_cl.prm_mta_insertion();

-- create ext\sa tables
CALL bl_cl.ext_creation();
CALL bl_cl.sa_creation();

-- create bl_3nf layer and its structure
CALL bl_cl.bl_3nf_schema();

-- create bl_dm layer structure
CALL bl_cl.bl_dim_schema();








--------------------------------------------------
--___________CREATE USER___________---------------
--------------------------------------------------

-- REVOKE ALL PRIVILEGES ON DATABASE dwh FROM bl_cl;
-- REVOKE EXECUTE ON ALL ROUTINES IN SCHEMA bl_cl FROM bl_cl;

CALL bl_cl.user_creation('bl_cl');
GRANT CONNECT ON DATABASE dwh TO bl_cl;

-- grant privileges
GRANT pg_read_all_data TO bl_cl;
GRANT pg_write_all_data TO bl_cl;
GRANT EXECUTE ON ALL ROUTINES IN SCHEMA bl_cl TO bl_cl;













--------------------------------------------------
--___________LOAD SA DATA___________--------------
--------------------------------------------------

SET max_parallel_workers = 32;
SET max_parallel_workers_per_gather = 24;

-- load in sa tables
CALL bl_cl.sa_iowalakes_load();
CALL bl_cl.sa_northwest_load();














--------------------------------------------------
--___________VIEWS___________---------------------
--------------------------------------------------

-- create all views in bl_3nf
CALL generate_views();

























--------------------------------------------------
--___________MAPPING INSERTION___________---------
--------------------------------------------------
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
ANALYZE;
















--------------------------------------------------
--___________BL3NF LAYER___________---------------
--------------------------------------------------
-- bl3nf initial insertion
CALL bl_cl.bl3nf_init_load();

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
ANALYZE;
-- bl_3nf scd insertion
CALL bl_cl.ce_employees_insertion();
CALL bl_cl.ce_products_insertion();
CALL bl_cl.ce_shippers_insertion();
CALL bl_cl.ce_curr_emp_profiles_insertion(); 
CALL bl_cl.ce_vendors_insertion();
COMMIT;
CALL bl_cl.ce_stores_insertion();
ANALYZE;





--------------------------------------------------
--___________3NF FACT TABLE___________------------
--------------------------------------------------
-- load fct table 
CALL bl_cl.ce_sales_insertion();
ANALYZE;

























--------------------------------------------------
--___________DOUBLE CHECK___________--------------
--------------------------------------------------
-- check function for 3nf layer
SELECT * FROM bl_cl.load_check('bl_3nf');

























--------------------------------------------------
--___________DIM LAYER___________-----------------
--------------------------------------------------
-- bldim initial insertion
CALL bl_cl.bldim_init_load();

-- working tables and dim insertion
CALL bl_cl.wrk_shippers_insertion();
CALL bl_cl.dim_shippers_scd_insertion();

CALL bl_cl.wrk_employees_insertion();
CALL bl_cl.dim_employees_scd_insertion();

CALL bl_cl.wrk_stores_insertion(); 
CALL bl_cl.dim_stores_scd_insertion();

CALL bl_cl.wrk_products_insertion();
CALL bl_cl.dim_products_scd_insertion();

CALL bl_cl.wrk_junk_insertion();
CALL bl_cl.dim_junk_insertion();

CALL bl_cl.dim_dates_insertion();

CALL bl_cl.dim_vendors_scd_insertion();
ANALYZE;







--------------------------------------------------
--___________FACT INSERTION___________------------
--------------------------------------------------
-- create fct partitions
SELECT * FROM bl_cl.partitions_creation();

-- load fct table (full load)
CALL bl_cl.fct_sales_insertion('last load date');























--------------------------------------------------
--___________TESTS PART___________----------------
--------------------------------------------------

-- duplicates check, type one test (src_id, source_system and source entity triplet)
SELECT * FROM bl_cl.duplicated('bl_3nf');
SELECT * FROM bl_cl.duplicated('bl_dim');

-- all rows presented check, type two test
SELECT * FROM bl_cl.missing_rows();

































--------------------------------------------------
--___________ANALYTICAL PART___________-----------
--------------------------------------------------

SELECT * FROM bl_cl.low_sales_notification();










































--___________INCREMENTAL_LOAD_DEMO___________

---- scd type 6 test (demo)