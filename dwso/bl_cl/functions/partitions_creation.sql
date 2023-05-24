CREATE OR REPLACE FUNCTION bl_cl.partitions_creation()
RETURNS VOID AS $$
DECLARE
    min_year INTEGER;
    max_year INTEGER;
    year INTEGER;
    month INTEGER;
BEGIN
    SELECT EXTRACT(YEAR FROM MIN(event_date)), EXTRACT(YEAR FROM MAX(event_date))
    INTO min_year, max_year
    FROM bl_3nf.ce_sales;
   
    FOR year IN min_year..max_year LOOP
        EXECUTE FORMAT('
            CREATE TABLE IF NOT EXISTS bl_dim.fct_sales_partition_%s
            PARTITION OF bl_dim.fct_sales
            FOR VALUES FROM (%L) TO (%L)
            PARTITION BY RANGE (event_date);
        ', LPAD(year::text, 4, '0'),
           DATE(year::text || '-01-01'), 
           DATE((year + 1)::text || '-01-01'));
          
        FOR month IN 1..12 LOOP
            EXECUTE FORMAT('
                CREATE TABLE IF NOT EXISTS bl_dim.fct_sales_partition_%s_%s
                PARTITION OF bl_dim.fct_sales_partition_%s
                FOR VALUES FROM (%L) TO (%L);
            ', LPAD(year::text, 4, '0'), LPAD(month::text, 2, '0'), LPAD(year::text, 4, '0'),
               DATE(year::text || '-' || month::text || '-01'), 
               DATE((year + (month / 12))::integer::text || '-' || ((month % 12) + 1)::text || '-01'));
        END LOOP;
    END LOOP;
    RAISE NOTICE 'Partition creation complete.';
END;
$$ LANGUAGE plpgsql;