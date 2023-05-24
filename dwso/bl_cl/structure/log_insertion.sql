CREATE OR REPLACE PROCEDURE bl_cl.logging_insertion(schema_name TEXT,
                                           		    table_name TEXT,
                                           			command_name TEXT,
                                           			rows_affected INTEGER,
                                           			error_type TEXT,
                                           			output_message TEXT)

LANGUAGE plpgsql 
AS $$

BEGIN

INSERT INTO bl_cl.logging
            (user_name,
             event_time,
             schema_name,
             table_name,
             command_name,
             rows_affected,
             error_type,
             output_message)
VALUES      (CURRENT_USER,
              CURRENT_TIMESTAMP,
              schema_name,
              table_name,
              command_name,
              rows_affected,
              error_type,
              output_message); 

END;
$$;