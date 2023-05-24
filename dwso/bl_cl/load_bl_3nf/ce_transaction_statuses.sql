-- DROP PROCEDURE IF EXISTS bl_cl.ce_transaction_statuses_insertion();
CREATE OR REPLACE PROCEDURE bl_cl.ce_transaction_statuses_insertion()

LANGUAGE plpgsql 
AS $$

DECLARE
  schema_name   TEXT := 'bl_3nf';
  table_name    TEXT := 'ce_transaction_statuses';
  command_name  TEXT := 'insert';
  rows_affected INTEGER := 0;
  error_type    TEXT := '';
  output_message TEXT := '';

BEGIN 
	
WITH inserted_data (status_id,
                    source_system,
                    source_entity,
                    status_name,
                    insert_dt,
                    update_dt) AS (
		SELECT  COALESCE(mp.status_src_id, 'N/A'),
				mp.source_system AS source_system,
				mp.source_table AS source_table,
				COALESCE(mp.status_name, 'N/A'), 
				CURRENT_TIMESTAMP,
				CURRENT_TIMESTAMP
		FROM bl_cl.map_transaction_statuses AS mp  
                                  )
MERGE INTO bl_3nf.ce_transaction_statuses AS nf
USING inserted_data ON UPPER(nf.status_src_id) = UPPER(COALESCE(inserted_data.status_id, 'N/A')) AND
                       nf.source_system = inserted_data.source_system AND
                       nf.source_entity = inserted_data.source_entity	
WHEN MATCHED AND UPPER(inserted_data.status_name) != UPPER(nf.status_name)
THEN
        UPDATE
        SET status_name = inserted_data.status_name,
            update_dt = CURRENT_TIMESTAMP
WHEN NOT MATCHED 
THEN
        INSERT
            (status_id,
             status_src_id,
             source_system,
             source_entity,
             status_name,
             insert_dt, 
             update_dt)  
         VALUES 
         	(NEXTVAL('bl_3nf.ce_transaction_statuses_seq'),
         	inserted_data.status_id, 
            inserted_data.source_system, 
            inserted_data.source_entity, 
            inserted_data.status_name, 
            inserted_data.insert_dt, 
            inserted_data.update_dt);  
           
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