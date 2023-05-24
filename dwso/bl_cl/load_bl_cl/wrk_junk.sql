-- DROP PROCEDURE IF EXISTS bl_cl.wrk_junk_insertion();
CREATE OR REPLACE PROCEDURE bl_cl.wrk_junk_insertion()

LANGUAGE plpgsql 
AS $$

DECLARE
  schema_name   TEXT := 'bl_cl';
  table_name    TEXT := 'wrk_junk';
  command_name  TEXT := 'insert';
  rows_affected INTEGER := 0;
  error_type    TEXT := '';
  output_message TEXT := '';

BEGIN 
	
TRUNCATE bl_cl.wrk_junk;

INSERT INTO bl_cl.wrk_junk (
					source_system,
					source_entity,
				    tax_indicator_id,
				    tax_type,
				    status_id,
				    status_name,
				    type_id,
				    type_name,
				    insert_dt,
				    update_dt)
SELECT 'bl_3nf' AS source_system,
	   '-' AS source_entity,
	   tax_indicator_id,
	   tax_type,
       status_id,
	   status_name,
	   type_id,
	   type_name,
	   CURRENT_TIMESTAMP,
	   CURRENT_TIMESTAMP
FROM bl_3nf.ce_tax_indicators AS tax
	CROSS JOIN bl_3nf.ce_transaction_statuses AS st
	CROSS JOIN bl_3nf.ce_transaction_types AS tp 
	WHERE tax.tax_indicator_id != -1 AND 
		  st.status_id != -1 AND 
		  tp.type_id != -1;
		 
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