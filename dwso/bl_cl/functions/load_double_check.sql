DROP TYPE resulting_table CASCADE;

CREATE TYPE resulting_table AS
(
    table_name 		 TEXT,
    procedure_name   TEXT,
    first_run_count  INT,
    second_run_count INT,
    initial_count	 INT
);

CREATE OR REPLACE FUNCTION bl_cl.load_check(_schema_name TEXT)
    RETURNS SETOF resulting_table
LANGUAGE plpgsql
AS
$$
DECLARE
    _table_name       TEXT;
    _procedure_name   TEXT;
    _first_run_count  INTEGER;
    _second_run_count INTEGER;
    _initial_count	  INTEGER;
   
BEGIN
	RAISE  NOTICE '__________CHECK HAS BEEN STARTED__________';
    FOR _table_name IN
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = _schema_name AND table_type = 'BASE TABLE'
    LOOP
        RAISE NOTICE 'Table Name: % - Starting', _table_name;
        _procedure_name := format(_table_name || '_insertion');
       BEGIN
        EXECUTE format('SELECT COUNT(*) FROM %I.%I;', _schema_name, _table_name)
            INTO _initial_count;
        RAISE NOTICE 'Initial Count: %', _initial_count;
        EXECUTE format('CALL bl_cl.%I();', _procedure_name);
        EXECUTE format('SELECT COUNT(*) FROM %I.%I;', _schema_name, _table_name)
            INTO _first_run_count;
        RAISE NOTICE 'Table Name: % - First Run, Row Count: %', _table_name, _first_run_count;
        EXECUTE format('CALL bl_cl.%I();', _procedure_name);
        EXECUTE format('SELECT COUNT(*) FROM %I.%I;', _schema_name, _table_name)
            INTO _second_run_count;
        RAISE NOTICE 'Table Name: % - Second Run, Row Count: %', _table_name, _second_run_count;
        RETURN NEXT (_table_name, _procedure_name, _initial_count, _first_run_count, _second_run_count);
    EXCEPTION
        WHEN OTHERS THEN
            RAISE NOTICE 'Procedure not found for table: %', _table_name;
            CONTINUE; 
    END;
    END LOOP;
	RAISE NOTICE '__________CHECK HAS BEEN COMPLETED__________';
END;
$$;