-- DROP PROCEDURE IF EXISTS bl_cl.wrk_shippers_insertion();
CREATE OR REPLACE PROCEDURE bl_cl.wrk_shippers_insertion()

LANGUAGE plpgsql 
AS $$

DECLARE
  schema_name   TEXT := 'bl_cl';
  table_name    TEXT := 'wrk_shippers';
  command_name  TEXT := 'insert';
  rows_affected INTEGER := 0;
  error_type    TEXT := '';
  output_message TEXT := '';
 
  last_load_date TIMESTAMP; 

BEGIN 
	
TRUNCATE bl_cl.wrk_shippers;

SELECT COALESCE(MAX(update_dt), '1900-01-01'::TIMESTAMP) INTO last_load_date FROM bl_dim.dim_shippers_scd;

INSERT INTO bl_cl.wrk_shippers (
				    shipper_id,
				    shipper_src_id,
				    source_system,
				    source_entity,
				    name,
				    start_date,
				    end_date,
				    is_active,
				    rating,
				    ship_base,
				    ship_rate,
				    contact_phone,
				    contact_name,
				    current_region,
				    historic_region,
				    insert_dt,
				    update_dt)
SELECT shipper_id,
	   shipper_src_id,
	   'bl_3nf' AS source_system,
	   'ce_shippers' AS source_entity,
	   name,
	   start_date,
	   end_date,
	   is_active,
	   rating,
	   ship_base,
	   ship_rate,
	   contact_phone,
	   contact_name,
	   reg1.region_name,
	   reg2.region_name,
	   CURRENT_TIMESTAMP,
	   CURRENT_TIMESTAMP
FROM bl_3nf.ce_shippers AS sh
LEFT OUTER JOIN bl_3nf.ce_regions AS reg1 ON sh.curr_region_id = reg1.region_id 
LEFT OUTER JOIN bl_3nf.ce_regions AS reg2 ON sh.historical_region_id = reg2.region_id
WHERE GREATEST(sh.update_dt, reg1.update_dt) > last_load_date;										
		 
GET DIAGNOSTICS rows_affected = row_count;

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