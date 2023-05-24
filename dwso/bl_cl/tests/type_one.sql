DROP TYPE duplicated_table CASCADE;

CREATE TYPE duplicated_table AS
(
    table_name 		 TEXT,
    duplicates_cnt   INT
);

-- DROP FUNCTION bl_cl.duplicated(schema_name TEXT);
CREATE OR REPLACE FUNCTION bl_cl.duplicated(schema_name TEXT)
    RETURNS SETOF duplicated_table
AS $$
DECLARE 
    tbl TEXT;
    src_id_col TEXT;
    query TEXT;
    _table_name TEXT;
    _duplicates_cnt INTEGER;
BEGIN
	
    FOR tbl, src_id_col IN (
        SELECT table_name, column_name
        FROM information_schema.columns
        WHERE table_schema = schema_name 
        	  AND table_name != 'dim_dates'
        	  AND table_name NOT LIKE '%fct_sales%'
        	  AND (
		            (schema_name = 'bl_3nf' AND column_name LIKE '%_src_id') OR 
		            (schema_name = 'bl_dim' AND column_name LIKE '%_id' AND column_name NOT LIKE '%_surr_id')
          		   )
    ) LOOP
        query := format('SELECT COALESCE(count(*),0) 
						 FROM (SELECT COUNT(*) 
							   FROM %I.%I 
							   GROUP BY %I, source_system, source_entity 
							   HAVING count(*)>1) c', schema_name, tbl, src_id_col);
        EXECUTE query INTO _duplicates_cnt;
        _table_name := tbl;
        RETURN NEXT (_table_name, _duplicates_cnt);
    END LOOP;
    RETURN;
END;
$$ LANGUAGE plpgsql;