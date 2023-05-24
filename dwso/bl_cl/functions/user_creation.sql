-- DROP PROCEDURE IF EXISTS bl_cl.user_creation();
CREATE OR REPLACE PROCEDURE bl_cl.user_creation(_user TEXT)

LANGUAGE plpgsql 
AS $$

DECLARE
  schema_name   TEXT := '-';
  table_name    TEXT := '-';
  command_name  TEXT := 'user creation';
  rows_affected INTEGER := 0;
  error_type    TEXT := '';
  output_message TEXT := '';

BEGIN 

IF EXISTS (SELECT rolname
           FROM pg_catalog.pg_roles
           WHERE UPPER(rolname) = UPPER(_user))
THEN
	RAISE NOTICE 'User ''%'' already exists.', _user;
ELSE
    EXECUTE format('CREATE USER %s', _user);
    RAISE NOTICE 'User created';
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