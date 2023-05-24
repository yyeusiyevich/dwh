CREATE OR REPLACE PROCEDURE generate_views()
LANGUAGE plpgsql
AS $$
DECLARE
  current_table TEXT;
  source_schema TEXT;
  view_name TEXT;
  source_table TEXT;
BEGIN
 FOR current_table IN (SELECT c.relname AS table_name
                        FROM pg_catalog.pg_class c
                        LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                        WHERE n.nspname = 'bl_3nf' AND c.relkind = 'r')
  LOOP
    FOR source_schema, source_table IN (SELECT UNNEST(ARRAY['sa_northwest_sales', 'sa_iowalakes_sales']), 
    										   UNNEST(ARRAY['src_northwest_sales', 'src_iowalakes_sales']))
    LOOP
      view_name := 'incr_view_' || current_table || '_' || SUBSTRING(source_schema from 4 for 2);
      
      EXECUTE format('CREATE OR REPLACE VIEW bl_3nf.%I AS 
					  SELECT * FROM %I.%I 
					  WHERE insert_dt > (SELECT previous_loaded_dt 
										 FROM bl_cl.prm_mta_incremental_load 
										 WHERE source_table = %L AND target_table = %L)', 
			   view_name, 
			   source_schema, 
			   source_table, 
			   source_table, 
			   current_table);
    END LOOP;
  END LOOP;
END;
$$;