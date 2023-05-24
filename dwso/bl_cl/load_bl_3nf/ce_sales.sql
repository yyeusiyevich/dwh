CREATE OR REPLACE PROCEDURE bl_cl.ce_sales_insertion()

LANGUAGE plpgsql 
AS $$

DECLARE
  schema_name   TEXT := 'bl_3nf';
  table_name    TEXT := 'ce_sales';
  command_name  TEXT := 'insert';
  rows_affected INTEGER := 0;
  error_type    TEXT := '';
  output_message TEXT := '';
 
  last_load_date TIMESTAMP;

BEGIN 
	
SELECT COALESCE(MAX(insert_dt), '1900-01-01'::TIMESTAMP) 
			   INTO last_load_date 
			   FROM bl_3nf.ce_sales;	
 			
INSERT INTO bl_3nf.ce_sales AS ce
	SELECT  NEXTVAL('bl_3nf.ce_sales_seq') AS transaction_id,
	        uni.transaction_src_id,
            uni.source_system,
            uni.source_entity,
            COALESCE(nf1.status_id, -1) AS transaction_status_id, 
            COALESCE(nf2.type_id, -1) AS transaction_type_id,
            COALESCE(nf8.tax_indicator_id, -1) AS tax_indicator_id,
            COALESCE(nf3.shipper_id, -1) AS shipper_id,
            COALESCE(uni.event_date, '9999-12-31') AS event_date,
            COALESCE(nf4.store_id, -1) AS store_id,
            COALESCE(nf5.employee_id, -1) AS employee_id,
            COALESCE(nf6.vendor_id, -1) AS vendor_id,
            COALESCE(nf7.product_id, -1) AS product_id,
            uni.quantity_sold,
            uni.total_amount,
            uni.volume_sold_liters,
            uni.volume_sold_gallons,
            uni.state_bottle_cost,
            uni.state_bottle_retail,
            uni.insert_dt
    FROM
        (SELECT COALESCE(invoice_and_item_number, 'N/A') AS transaction_src_id,
			    'sa_northwest_sales' AS source_system,
		        'src_northwest_sales' AS source_entity,
			    transaction_status_id,
			    transaction_type_id,
			    tax_indicator_id,
			    shipper_id,
			    date::DATE AS event_date,
			    store_number AS store_id,
			    employee_id,
			    vendor_number AS vendor_id,
			    item_id AS product_id,
			    bottles_sold::INTEGER AS quantity_sold,
			    sale_dollars::NUMERIC AS total_amount,
			    volume_sold_liters::NUMERIC,
			    volume_sold_gallons::NUMERIC,
			    state_bottle_cost::NUMERIC, 
			    state_bottle_retail::NUMERIC,
			    CURRENT_TIMESTAMP AS insert_dt
            FROM bl_3nf.incr_view_ce_sales_no
            UNION ALL
            SELECT COALESCE(invoice_and_item_number, 'N/A') AS transaction_src_id,
			    'sa_iowalakes_sales' AS source_system,
		        'src_iowalakes_sales' AS source_entity,
			    transaction_status_id,
			    transaction_type_id,
			    tax_indicator_id,
			    shipper_id,
			    date::DATE AS event_date,
			    store_number AS store_id,
			    employee_id,
			    vendor_number AS vendor_id,
			    item_id AS product_id,
			    bottles_sold::INTEGER AS quantity_sold,
			    sale_dollars::NUMERIC AS total_amount,
			    volume_sold_liters::NUMERIC,
			    volume_sold_gallons::NUMERIC, 
			    state_bottle_cost::NUMERIC, 
			    state_bottle_retail::NUMERIC,
			    CURRENT_TIMESTAMP AS insert_dt
            FROM bl_3nf.incr_view_ce_sales_io 
										) AS uni
LEFT OUTER JOIN bl_3nf.ce_transaction_statuses    AS nf1 ON  uni.transaction_status_id = nf1.status_src_id
LEFT OUTER JOIN bl_3nf.ce_transaction_types       AS nf2 ON  uni.transaction_type_id = nf2.type_src_id
LEFT OUTER JOIN bl_3nf.ce_tax_indicators          AS nf8 ON  uni.tax_indicator_id = nf8.tax_indicator_src_id         									    
LEFT OUTER JOIN bl_3nf.ce_stores  		          AS nf4 ON  uni.store_id = nf4.store_src_id AND
												  uni.source_system = nf4.source_system AND
     										      uni.source_entity = nf4.source_entity AND
												  uni.event_date >= nf4.start_date AND
												  uni.event_date < nf4.end_date      
LEFT OUTER JOIN bl_3nf.ce_employees  		      AS nf5 ON  uni.employee_id = nf5.employee_src_id AND
												  uni.source_system = nf5.source_system AND
     										      uni.source_entity = nf5.source_entity AND
												  uni.event_date >= nf5.start_date AND
												  uni.event_date < nf5.end_date  
LEFT OUTER JOIN bl_3nf.ce_vendors  		          AS nf6 ON  uni.vendor_id = nf6.vendor_src_id AND
												  uni.source_system = nf6.source_system AND
     										      uni.source_entity = nf6.source_entity AND
												  uni.event_date >= nf6.start_date AND
												  uni.event_date < nf6.end_date      
LEFT OUTER JOIN bl_3nf.ce_products  		      AS nf7 ON  uni.product_id = nf7.product_src_id AND 
												  uni.event_date >= nf7.start_date AND
												  uni.event_date < nf7.end_date 
LEFT OUTER JOIN bl_3nf.ce_shippers 		          AS nf3 ON  uni.shipper_id = nf3.shipper_src_id AND
												  uni.event_date >= nf3.start_date AND
												  uni.event_date < nf3.end_date     												  
LEFT OUTER JOIN bl_3nf.ce_sales 				  AS nf9 ON  uni.transaction_src_id = nf9.transaction_src_id AND
												  uni.source_system = nf9.source_system AND
     										      uni.source_entity = nf9.source_entity
												  WHERE nf9.transaction_id IS NULL
						  						  AND uni.insert_dt > last_load_date;										  
              
GET DIAGNOSTICS rows_affected = row_count;

UPDATE bl_cl.prm_mta_incremental_load
SET previous_loaded_dt = CURRENT_TIMESTAMP,
	procedure_name = 'bl_cl.ce_sales_insertion()'	
WHERE source_table IN ('src_northwest_sales', 'src_iowalakes_sales')
	  AND target_table = 'ce_sales';    

RAISE NOTICE '% row(s) inserted', rows_affected;
output_message = 'Success';

CALL bl_cl.logging_insertion(schema_name, 
							 table_name,
							 command_name,
							 rows_affected,
							 error_type,
							 output_message);

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