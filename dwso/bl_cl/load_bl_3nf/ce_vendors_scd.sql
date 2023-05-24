CREATE OR REPLACE PROCEDURE bl_cl.ce_vendors_insertion()

LANGUAGE plpgsql 
AS $$

DECLARE
  schema_name   TEXT := 'bl_3nf';
  table_name    TEXT := 'ce_vendors';
  command_name  TEXT := 'insert';
  rows_affected INTEGER := 0;
  error_type    TEXT := '';
  output_message TEXT := '';

BEGIN 

WITH inserted_data (vendor_id,
                    source_system,
                    source_entity,
                    name,
                    start_date,
                    end_date,
                    is_active,
                    rating,
                    size,
      		        contact_phone,
      		        contact_name,
      		        homepage,
                    insert_dt, 
                    update_dt) AS (
     SELECT DISTINCT COALESCE(vendor_number, 'N/A'),
			         'sa_northwest_sales',
			         'src_northwest_sales',
			         COALESCE(vendor_name, 'N/A'),
				     '1900-01-01'::DATE,
			         '9999-12-31'::DATE,
			         'Y',
			         COALESCE(vendor_rating::NUMERIC, -1),
			         COALESCE(vendor_size, 'N/A'),      			        
			         COALESCE(vendor_contact, 'N/A'),
			         COALESCE(vendor_contact_name, 'N/A'), 
			         COALESCE(vendor_web, 'N/A'),
			         CURRENT_TIMESTAMP,
				     CURRENT_TIMESTAMP 
     FROM bl_3nf.incr_view_ce_vendors_no AS src
	 UNION ALL
     SELECT DISTINCT COALESCE(vendor_number, 'N/A'),
			         'sa_iowalakes_sales',
			         'src_iowalakes_sales',
			         COALESCE(vendor_name, 'N/A'),
				     '1900-01-01'::DATE,
			         '9999-12-31'::DATE,
			         'Y',
			         COALESCE(vendor_rating::NUMERIC, -1),
			         COALESCE(vendor_size, 'N/A'),      			        
			         COALESCE(vendor_contact, 'N/A'),
			         COALESCE(vendor_contact_name, 'N/A'), 
			         COALESCE(vendor_web, 'N/A'),    
			         CURRENT_TIMESTAMP,
				     CURRENT_TIMESTAMP 
     FROM bl_3nf.incr_view_ce_vendors_io AS src
	                               ),
	 new_rows AS (
	 SELECT *
     FROM inserted_data AS ins
     WHERE NOT EXISTS (SELECT 1
                       FROM  bl_3nf.ce_vendors AS vnd
                       WHERE UPPER(ins.vendor_id) = UPPER(vnd.vendor_src_id) AND
						     UPPER(ins.source_system) = UPPER(vnd.source_system) AND
						     UPPER(ins.source_entity) = UPPER(vnd.source_entity) AND
						     UPPER(ins.name) = UPPER(vnd.name) AND
						     ins.rating::DECIMAL(2, 1) = vnd.rating AND
						     UPPER(ins.size) = UPPER(vnd.size) AND
						     UPPER(ins.contact_phone) = UPPER(vnd.contact_phone) AND
						     UPPER(ins.contact_name) = UPPER(vnd.contact_name) AND
						     UPPER(ins.homepage) = UPPER(vnd.homepage) AND
						     UPPER(vnd.is_active) = 'Y'
                        )
	 								),								
	  upd AS (
      UPDATE bl_3nf.ce_vendors AS target
             SET is_active = 'N',
             end_date = CURRENT_TIMESTAMP,
             update_dt = CURRENT_TIMESTAMP
      FROM new_rows
      WHERE UPPER(new_rows.vendor_id) = UPPER(target.vendor_src_id) AND
            UPPER(target.is_active) = 'Y'
     		 )
INSERT INTO bl_3nf.ce_vendors
            (vendor_id,
             vendor_src_id,
             source_system,
             source_entity,
             name,
             start_date,
             end_date,
             is_active,
             rating,
             size,
      		 contact_phone,
      		 contact_name,
      		 homepage,
             insert_dt, 
             update_dt)    
SELECT CASE WHEN EXISTS (SELECT 1 
                         FROM bl_3nf.ce_vendors AS vd 
                         WHERE vd.vendor_src_id = new_rows.vendor_id) 
            THEN (SELECT vd.vendor_id 
                  FROM bl_3nf.ce_vendors AS vd 
                  WHERE vd.vendor_src_id = new_rows.vendor_id) 
       		ELSE NEXTVAL('bl_3nf.ce_vendors_seq')
       END,
       vendor_id,
       source_system,
       source_entity,
       name,
	   CASE WHEN EXISTS (SELECT 1
                         FROM bl_3nf.ce_vendors AS vd, new_rows
                         WHERE  UPPER(new_rows.vendor_id) = UPPER(vd.vendor_src_id))
            THEN CURRENT_DATE
            ELSE '1900-01-01'::DATE 
       END,
       end_date,
       is_active,
       rating,
       size,
       contact_phone,
       contact_name,
       homepage,
       insert_dt, 
       update_dt
FROM   new_rows
ON CONFLICT ON CONSTRAINT vendors_pk DO NOTHING;     		 

GET DIAGNOSTICS rows_affected = row_count;

UPDATE bl_cl.prm_mta_incremental_load
SET previous_loaded_dt = CURRENT_TIMESTAMP,
	procedure_name = 'bl_cl.ce_vendors_insertion()'	
WHERE source_table IN ('src_northwest_sales', 'src_iowalakes_sales')
	  AND target_table = 'ce_vendors';    

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