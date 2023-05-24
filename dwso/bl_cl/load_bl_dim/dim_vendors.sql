-- DROP PROCEDURE IF EXISTS bl_cl.dim_vendors_scd_insertion();
CREATE OR REPLACE PROCEDURE bl_cl.dim_vendors_scd_insertion()

LANGUAGE plpgsql 
AS $$

DECLARE
  schema_name   TEXT := 'bl_dim';
  table_name    TEXT := 'dim_vendors_scd';
  command_name  TEXT := 'insert';
  rows_affected INTEGER := 0;
  error_type    TEXT := '';
  output_message TEXT := '';
 
  last_load_date TIMESTAMP;

BEGIN 
	
SELECT COALESCE(MAX(update_dt), '1900-01-01'::TIMESTAMP) INTO last_load_date FROM bl_dim.dim_vendors_scd;	
	
WITH inserted AS (
    SELECT 
        vendor_id,
        'bl_3nf' AS source_system,
        'ce_vendors' AS source_entity,
	    COALESCE(name, 'N/A') AS name,
	    start_date,
	    end_date,
	    is_active,
	    COALESCE(rating, -1) AS rating,
	    COALESCE(size, 'N/A') AS size,
	    COALESCE(contact_phone, 'N/A') AS contact_phone,
	    COALESCE(contact_name, 'N/A') AS contact_name,
	    COALESCE(homepage, 'N/A') AS homepage
    FROM bl_3nf.ce_vendors AS vd
    WHERE vendor_id != -1 AND
    	  GREATEST(vd.update_dt) > last_load_date
),
	 upd AS (
      UPDATE bl_dim.dim_vendors_scd AS target
             SET is_active = 'N',
             end_date = CURRENT_TIMESTAMP,
             update_dt = CURRENT_TIMESTAMP
      FROM inserted
      WHERE inserted.vendor_id = target.vendor_id AND
            UPPER(target.is_active) = 'Y'
     		 )
INSERT INTO bl_dim.dim_vendors_scd
	(
        vendor_surr_id,
        vendor_id,
        source_system,
        source_entity,
        name,
        start_date,
        end_date,
        is_active,
        contact_phone,
        contact_name,
        rating,
        size,
        homepage,
        insert_dt,
        update_dt
    )
SELECT 
        NEXTVAL('bl_dim.dim_vendors_seq'),
        vendor_id,
        source_system,
        source_entity,
        name,
        start_date,
        end_date,
        is_active,
        contact_phone,
        contact_name,
        rating,
        size,
        homepage,
        CURRENT_TIMESTAMP,
        CURRENT_TIMESTAMP
FROM inserted
WHERE UPPER(is_active) = 'Y' AND 
	  NOT EXISTS (SELECT 1
	 			  FROM bl_dim.dim_vendors_scd AS vd
	 			  WHERE inserted.vendor_id = vd.vendor_id AND
	 			 		inserted.source_system = vd.source_system AND
	 			 		inserted.source_entity = vd.source_entity AND
	 			 		inserted.name = vd.name AND
	 					inserted.contact_phone = vd.contact_phone AND
	 					inserted.contact_name = vd.contact_name AND
	 					inserted.rating = vd.rating AND
	 					inserted.size = vd.size AND
						inserted.homepage = vd.homepage);
		 
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