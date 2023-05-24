-- DROP PROCEDURE IF EXISTS bl_cl.dim_shippers_scd_insertion();
CREATE OR REPLACE PROCEDURE bl_cl.dim_shippers_scd_insertion()

LANGUAGE plpgsql 
AS $$

DECLARE
  schema_name   TEXT := 'bl_dim';
  table_name    TEXT := 'dim_shippers_scd';
  command_name  TEXT := 'insert';
  rows_affected INTEGER := 0;
  error_type    TEXT := '';
  output_message TEXT := '';

BEGIN 
	
WITH inserted AS (
    SELECT
        shipper_id,
        'bl_cl' AS source_system,
        'wrk_shippers' AS source_entity,
	    COALESCE(name, 'N/A') AS name,
	    start_date,
	    end_date,
	    is_active,
	    COALESCE(rating, -1) AS rating,
	    COALESCE(ship_base, -1) AS ship_base,
	    COALESCE(ship_rate, -1) AS ship_rate,
	    COALESCE(contact_phone, 'N/A') AS contact_phone,
	    COALESCE(contact_name, 'N/A') AS contact_name,
	    COALESCE(current_region, 'N/A') AS current_region,
	    COALESCE(historic_region, 'N/A') AS historic_region
    FROM bl_cl.wrk_shippers AS wrk
    WHERE shipper_id != -1
),
     upd_active AS (
     UPDATE bl_dim.dim_shippers_scd AS sh
             SET is_active = 'N',
	             end_date = CURRENT_TIMESTAMP,
	             update_dt = CURRENT_TIMESTAMP,
                 current_region = inserted.current_region
     FROM  inserted
     WHERE inserted.shipper_id = sh.shipper_id AND
           sh.is_active = 'Y'
     		 ),
     ups_inactive AS (
     UPDATE bl_dim.dim_shippers_scd AS sh
             SET current_region = inserted.current_region,
             	 update_dt = CURRENT_TIMESTAMP
     FROM  inserted
     WHERE inserted.shipper_id = sh.shipper_id AND
     	   sh.current_region != inserted.current_region AND
           sh.is_active = 'N')
INSERT INTO bl_dim.dim_shippers_scd
    (
        shipper_surr_id,
        shipper_id,
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
        update_dt
    )
SELECT
        NEXTVAL('bl_dim.dim_shippers_seq'),
        shipper_id,
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
        CURRENT_TIMESTAMP,
        CURRENT_TIMESTAMP
FROM inserted
WHERE UPPER(is_active) = 'Y';

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