-- DROP PROCEDURE IF EXISTS bl_cl.dim_junk_insertion();
CREATE OR REPLACE PROCEDURE bl_cl.dim_junk_insertion()

LANGUAGE plpgsql 
AS $$

DECLARE
  schema_name   TEXT := 'bl_dim';
  table_name    TEXT := 'dim_junk';
  command_name  TEXT := 'insert';
  rows_affected INTEGER := 0;
  error_type    TEXT := '';
  output_message TEXT := '';

BEGIN 
	
 MERGE INTO bl_dim.dim_junk AS jk
USING (
    SELECT 
	    COALESCE(st.status_name, 'N/A') AS status_name,
	    COALESCE(tp.type_name, 'N/A') AS type_name,
	    COALESCE(tax.tax_type, 'N/A') AS tax_type
    FROM bl_3nf.ce_tax_indicators AS tax
	CROSS JOIN bl_3nf.ce_transaction_statuses AS st
	CROSS JOIN bl_3nf.ce_transaction_types AS tp 
	WHERE tax.tax_indicator_id != -1 AND 
		  st.status_id != -1 AND 
		  tp.type_id != -1
) src ON  jk.transaction_status = src.status_name
      AND jk.transaction_type = src.type_name
      AND jk.tax_type = src.tax_type
WHEN NOT MATCHED THEN
    INSERT (
        junk_surr_id,
		source_system,
		source_entity,
		transaction_status,
		transaction_type,
		tax_type,
		insert_dt,
		update_dt
    )
    VALUES (
        NEXTVAL('bl_dim.dim_junk_seq'),
        'bl_3nf',
        'MANUAL',
		src.status_name,
        src.type_name,
        src.tax_type,
        CURRENT_TIMESTAMP,
        CURRENT_TIMESTAMP
    );

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