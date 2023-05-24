-- DROP PROCEDURE IF EXISTS bl_cl.fct_sales_insertion();
CREATE OR REPLACE PROCEDURE bl_cl.fct_sales_insertion(load_type TEXT)

LANGUAGE plpgsql 
AS $$

DECLARE
  schema_name   TEXT := 'bl_dim';
  table_name    TEXT := 'fct_sales';
  command_name  TEXT := 'insert';
  rows_affected INTEGER := 0;
  temp_rows_affected INTEGER;
  error_type    TEXT := '';
  output_message TEXT := '';
  
  partitioning_year TEXT;
  partitioning_month TEXT;
 
  data_insertion TEXT;
 
  last_load_date TIMESTAMP;
 
  fday_prev DATE;
  fday_curr DATE;
  fday_next DATE;
 
  name_prev TEXT;
  name_curr TEXT;
  name_prev_month TEXT;
  name_curr_month TEXT;
 
  year_prev INTEGER;
  month_prev INTEGER;
  year_curr INTEGER;
  month_curr INTEGER;

BEGIN 
	
	partitioning_year := '
    CREATE TABLE IF NOT EXISTS bl_dim.fct_sales_partition_%s
    PARTITION OF bl_dim.fct_sales
    FOR VALUES FROM (%L) TO (%L)
    PARTITION BY RANGE (event_date);';
   
    partitioning_month := '
    CREATE TABLE IF NOT EXISTS bl_dim.fct_sales_partition_%s_%s
    PARTITION OF bl_dim.fct_sales_partition_%s
    FOR VALUES FROM (%L) TO (%L);';
                   
    data_insertion = 'INSERT INTO bl_dim.%I
					(transaction_id,
					 source_system,
					 source_entity,
					 junk_surr_id,
					 shipper_surr_id,
					 date_id,
					 store_surr_id,
					 employee_surr_id,
					 vendor_surr_id,
					 product_surr_id,
					 event_date,
					 quantity_sold,
					 total_amount,
					 volume_sold_liters,
					 volume_sold_gallons,
					 state_bottle_cost,
					 state_bottle_retail,
					 insert_dt)
		SELECT  transaction_id,
				source_system,
				source_entity,
				junk_surr_id,
				shipper_surr_id,
				date_id,
				store_surr_id,
			    employee_surr_id,
			    vendor_surr_id,
				product_surr_id,
				event_date,
				quantity_sold,
				total_amount,
				volume_sold_liters,
				volume_sold_gallons,
				state_bottle_cost,
				state_bottle_retail,
				CURRENT_TIMESTAMP
		FROM %s;';
               
    IF load_type ILIKE '%partition%'
	THEN 
		RAISE NOTICE '__________INCREMENTAL LOAD INTO THE FACT TABLE (PARTITIONING STRAREGY)__________';
    
    SELECT first_day_of_month - '1 month'::INTERVAL
    INTO fday_prev
    FROM bl_dim.dim_dates
    WHERE full_date = CURRENT_DATE;
   
    SELECT first_day_of_month + '1 month'::INTERVAL
    INTO fday_next
    FROM bl_dim.dim_dates
    WHERE full_date = CURRENT_DATE;
   
    SELECT first_day_of_month
    INTO fday_curr
    FROM bl_dim.dim_dates
    WHERE full_date = CURRENT_DATE;
   
    year_prev := EXTRACT(YEAR FROM fday_prev)::INTEGER;
	month_prev := EXTRACT(MONTH FROM fday_prev)::INTEGER;
	year_curr := EXTRACT(YEAR FROM fday_curr)::INTEGER;
	month_curr := EXTRACT(MONTH FROM fday_curr)::INTEGER;
   
    CREATE TEMP TABLE omnibus ON COMMIT DROP AS
    SELECT  	nf.transaction_id,
				'bl_3nf' AS source_system,
				'ce_sales' AS source_entity,
				jk.junk_surr_id,
				sh.shipper_surr_id,
				dt.date_id,
				st.store_surr_id,
			    emp.employee_surr_id,
			    vd.vendor_surr_id,
				pd.product_surr_id,
				event_date,
				quantity_sold,
				total_amount,
				volume_sold_liters,
				volume_sold_gallons,
				state_bottle_cost,
				state_bottle_retail,
				CURRENT_TIMESTAMP
		FROM bl_3nf.ce_sales AS nf
		LEFT OUTER JOIN bl_dim.dim_dates 	  	 AS dt 	ON dt.full_date = nf.event_date 
		LEFT OUTER JOIN bl_dim.dim_stores_scd 	 AS st 	ON st.store_id = nf.store_id 
														AND event_date  >= st.start_date 
													  	AND event_date  < st.end_date
		LEFT OUTER JOIN bl_dim.dim_products_scd  AS pd  ON pd.product_id = nf.product_id 
													  	AND event_date  >= pd.start_date 
													  	AND event_date  < pd.end_date
		LEFT OUTER JOIN bl_dim.dim_vendors_scd 	 AS vd 	ON vd.vendor_id = nf.vendor_id  
														AND event_date  >= vd.start_date 
														AND event_date  < vd.end_date	
		LEFT OUTER JOIN bl_dim.dim_shippers_scd  AS sh  ON sh.shipper_id = nf.shipper_id  
														AND event_date  >= sh.start_date 
														AND event_date  < sh.end_date
		LEFT OUTER JOIN bl_dim.dim_employees_scd AS emp ON emp.employee_id = nf.employee_id  
														AND event_date  >= emp.start_date 
														AND event_date  < emp.end_date		
		LEFT OUTER JOIN bl_cl.wrk_junk 			 AS wkjk ON wkjk.tax_indicator_id = nf.tax_indicator_id 
												  		 AND wkjk.status_id = nf.status_id 
												  		 AND wkjk.type_id = nf.type_id 
		LEFT OUTER JOIN bl_dim.dim_junk 		 AS jk  ON jk.transaction_status = wkjk.status_name 
											 			AND jk.transaction_type = wkjk.type_name 
											 			AND jk.tax_type = wkjk.tax_type;										
											 			
	   CREATE TEMP TABLE prev_month ON COMMIT DROP AS
	   SELECT * FROM omnibus
	   WHERE event_date >= fday_prev AND event_date < fday_curr;		
	  
	   CREATE TEMP TABLE curr_month ON COMMIT DROP AS
	   SELECT * FROM omnibus
	   WHERE event_date >= fday_curr AND event_date < fday_next;		  
    
      /* CREATE TEMP TABLE curr_month ON COMMIT DROP AS
       SELECT  	nf.transaction_id,
				'bl_3nf' AS source_system,
				'ce_sales' AS source_entity,
				jk.junk_surr_id,
				sh.shipper_surr_id,
				dt.date_id,
				st.store_surr_id,
			    emp.employee_surr_id,
			    vd.vendor_surr_id,
				pd.product_surr_id,
				event_date,
				quantity_sold,
				total_amount,
				volume_sold_liters,
				volume_sold_gallons,
				state_bottle_cost,
				state_bottle_retail,
				CURRENT_TIMESTAMP
		FROM bl_3nf.ce_sales AS nf
		LEFT OUTER JOIN bl_dim.dim_dates 	  	 AS dt 	ON dt.full_date = nf.event_date 
		LEFT OUTER JOIN bl_dim.dim_stores_scd 	 AS st 	ON st.store_id = nf.store_id 
														AND event_date  >= st.start_date 
													  	AND event_date  < st.end_date
		LEFT OUTER JOIN bl_dim.dim_products_scd  AS pd  ON pd.product_id = nf.product_id 
													  	AND event_date  >= pd.start_date 
													  	AND event_date  < pd.end_date
		LEFT OUTER JOIN bl_dim.dim_vendors_scd 	 AS vd 	ON vd.vendor_id = nf.vendor_id  
														AND event_date  >= vd.start_date 
														AND event_date  < vd.end_date	
		LEFT OUTER JOIN bl_dim.dim_shippers_scd  AS sh  ON sh.shipper_id = nf.shipper_id  
														AND event_date  >= sh.start_date 
														AND event_date  < sh.end_date
		LEFT OUTER JOIN bl_dim.dim_employees_scd AS emp ON emp.employee_id = nf.employee_id  
														AND event_date  >= emp.start_date 
														AND event_date  < emp.end_date		
		LEFT OUTER JOIN bl_cl.wrk_junk 			 AS wkjk ON wkjk.tax_indicator_id = nf.tax_indicator_id 
												  		 AND wkjk.status_id = nf.status_id 
												  		 AND wkjk.type_id = nf.type_id 
		LEFT OUTER JOIN bl_dim.dim_junk 		 AS jk  ON jk.transaction_status = wkjk.status_name 
											 			AND jk.transaction_type = wkjk.type_name 
											 			AND jk.tax_type = wkjk.tax_type
    	WHERE event_date >= fday_curr AND event_date < fday_next; */

    -- create partitions for year
    IF year_prev != year_curr THEN
    
	    EXECUTE format(partitioning_year, 
	   				   LPAD(year_prev::text, 4, '0'), 
	   				   TO_CHAR(DATE_TRUNC('YEAR', fday_prev), 'yyyy-mm-dd'), 
	   				   TO_CHAR(DATE_TRUNC('YEAR', fday_prev) + INTERVAL '1 year', 'yyyy-mm-dd'));
   				  
   	END IF;			  
   
    EXECUTE format(partitioning_year, 
    			   LPAD(year_curr::text, 4, '0'), 
				   TO_CHAR(DATE_TRUNC('YEAR', fday_curr), 'yyyy-mm-dd'), 
   				   TO_CHAR(DATE_TRUNC('YEAR', fday_curr) + INTERVAL '1 year', 'yyyy-mm-dd'));
   				  
     -- create partitions for previous month   				  
	EXECUTE format(partitioning_month, 
				   LPAD(year_prev::text, 4, '0'), 
				   LPAD(month_prev::text, 2, '0'), 
				   LPAD(year_prev::text, 4, '0'), 
				   TO_CHAR(fday_prev, 'yyyy-mm-dd'), 
				   TO_CHAR(fday_prev + INTERVAL '1 month', 'yyyy-mm-dd'));
				  
     -- create partitions for current month
   	EXECUTE format(partitioning_month, 
   				   LPAD(year_curr::text, 4, '0'), 
   				   LPAD(month_curr::text, 2, '0'), 
   				   LPAD(year_curr::text, 4, '0'), 
   				   TO_CHAR(fday_curr, 'yyyy-mm-dd'), 
   				   TO_CHAR(fday_curr + INTERVAL '1 month', 'yyyy-mm-dd'));

	-- prepare partition names for previous and current years
	name_prev := 'fct_sales_partition_' || LPAD(year_prev::text, 4, '0');
	name_curr := 'fct_sales_partition_' || LPAD(year_curr::text, 4, '0');

	-- patition name for months
	name_prev_month := 'fct_sales_partition_' || LPAD(year_prev::text, 4, '0') || '_' || LPAD(month_prev::text, 2, '0');
	name_curr_month := 'fct_sales_partition_' || LPAD(year_curr::text, 4, '0') || '_' || LPAD(month_curr::text, 2, '0');
	
	-- detach year partition and data insertion
	IF year_prev != year_curr THEN
		
		EXECUTE format('ALTER TABLE bl_dim.fct_sales DETACH PARTITION bl_dim.%I;', name_prev);
		EXECUTE format(data_insertion, name_prev, 'prev_month');
	
		GET DIAGNOSTICS temp_rows_affected = row_count;
		rows_affected := rows_affected + temp_rows_affected;
	
	END IF;
		
		EXECUTE format('ALTER TABLE bl_dim.fct_sales DETACH PARTITION bl_dim.%I;', name_curr);
	
		EXECUTE format('TRUNCATE TABLE bl_dim.%I;', name_prev_month);
		EXECUTE format('TRUNCATE TABLE bl_dim.%I;', name_curr_month);
	
		EXECUTE format(data_insertion, name_curr, 'curr_month');
	
	GET DIAGNOSTICS temp_rows_affected  = row_count;
	rows_affected := rows_affected + temp_rows_affected;
	
		EXECUTE format(data_insertion, name_curr, 'prev_month');
   
	GET DIAGNOSTICS temp_rows_affected  = row_count;
	rows_affected := rows_affected + temp_rows_affected;

	RAISE NOTICE '% row(s) inserted', rows_affected;
	output_message = 'Success';

	CALL bl_cl.logging_insertion(schema_name, 
								 table_name,
								 command_name,
								 rows_affected,
								 error_type,
								 output_message);
								
	-- attach partitions								
	IF year_prev != year_curr THEN
	
		EXECUTE format('ALTER TABLE bl_dim.fct_sales ATTACH PARTITION bl_dim.%I FOR VALUES FROM (%L) TO (%L);',
		                   name_prev, 
		                   TO_CHAR(DATE_TRUNC('YEAR', fday_prev), 'yyyy-mm-dd'), 
		                   TO_CHAR(DATE_TRUNC('YEAR', fday_prev) + INTERVAL '1 year', 'yyyy-mm-dd'));
		                  
	END IF;		                  
	                  
	EXECUTE format('ALTER TABLE bl_dim.fct_sales ATTACH PARTITION bl_dim.%I FOR VALUES FROM (%L) TO (%L);',
	                   name_curr, 
	                   TO_CHAR(DATE_TRUNC('YEAR', fday_curr), 'yyyy-mm-dd'), 
	                   TO_CHAR(DATE_TRUNC('YEAR', fday_curr) + INTERVAL '1 year', 'yyyy-mm-dd'));

    END IF;
	   
   IF load_type ILIKE '%last%'
	THEN 
		RAISE NOTICE '__________INCREMENTAL LOAD INTO THE FACT TABLE (LAST LOAD DATE)__________';
	
		SELECT COALESCE(MAX(insert_dt), '1900-01-01'::TIMESTAMP) 
			   INTO last_load_date 
			   FROM bl_dim.fct_sales;
			  
		INSERT INTO bl_dim.fct_sales
					(transaction_id,
					 source_system,
					 source_entity,
					 junk_surr_id,
					 shipper_surr_id,
					 date_id,
					 store_surr_id,
					 employee_surr_id,
					 vendor_surr_id,
					 product_surr_id,
					 event_date,
					 quantity_sold,
					 total_amount,
					 volume_sold_liters,
					 volume_sold_gallons,
					 state_bottle_cost,
					 state_bottle_retail,
					 insert_dt)
		SELECT  nf.transaction_id,
				'bl_3nf',
				'ce_sales',
				jk.junk_surr_id,
				sh.shipper_surr_id,
				dt.date_id,
				st.store_surr_id,
			    emp.employee_surr_id,
			    vd.vendor_surr_id,
				pd.product_surr_id,
				event_date,
				quantity_sold,
				total_amount,
				volume_sold_liters,
				volume_sold_gallons,
				state_bottle_cost,
				state_bottle_retail,
				CURRENT_TIMESTAMP
		FROM bl_3nf.ce_sales AS nf
		LEFT OUTER JOIN bl_dim.dim_dates 	  	 AS dt 	ON dt.full_date = nf.event_date 
		LEFT OUTER JOIN bl_dim.dim_stores_scd 	 AS st 	ON st.store_id = nf.store_id 
														AND dt.full_date  >= st.start_date 
													  	AND dt.full_date  < st.end_date
		LEFT OUTER JOIN bl_dim.dim_products_scd  AS pd  ON pd.product_id = nf.product_id 
													  	AND dt.full_date  >= st.start_date 
													  	AND dt.full_date  < st.end_date
		LEFT OUTER JOIN bl_dim.dim_vendors_scd 	 AS vd 	ON vd.vendor_id = nf.vendor_id  
														AND dt.full_date  >= st.start_date 
														AND dt.full_date  < st.end_date	
		LEFT OUTER JOIN bl_dim.dim_shippers_scd  AS sh  ON sh.shipper_id = nf.shipper_id  
														AND dt.full_date  >= st.start_date 
														AND dt.full_date  < st.end_date
		LEFT OUTER JOIN bl_dim.dim_employees_scd AS emp ON emp.employee_id = nf.employee_id  
														AND dt.full_date  >= st.start_date 
														AND dt.full_date  < st.end_date		
		LEFT OUTER JOIN bl_cl.wrk_junk 			 AS wkjk ON wkjk.tax_indicator_id = nf.tax_indicator_id 
												  		 AND wkjk.status_id = nf.status_id 
												  		 AND wkjk.type_id = nf.type_id 
		LEFT OUTER JOIN bl_dim.dim_junk 		 AS jk  ON jk.transaction_status = wkjk.status_name 
											 			AND jk.transaction_type = wkjk.type_name 
											 			AND jk.tax_type = wkjk.tax_type 
		WHERE nf.insert_dt > last_load_date;
	
	GET DIAGNOSTICS rows_affected = row_count;

	RAISE NOTICE '% row(s) inserted', rows_affected;
	output_message = 'Success';

	CALL bl_cl.logging_insertion(schema_name, 
								 table_name,
								 command_name,
								 rows_affected,
								 error_type,
								 output_message);
								
	END IF;
										
	EXCEPTION
	WHEN OTHERS THEN
	     GET stacked DIAGNOSTICS error_type = pg_exception_context,
	                             output_message = message_text;
	
	RAISE NOTICE 'Error: % %', error_type, output_message;
	
	CALL bl_cl.logging_insertion(schema_name, 
								 table_name,
								 command_name,
								 rows_affected,
								 error_type,
								 output_message);
								
END;
$$;
