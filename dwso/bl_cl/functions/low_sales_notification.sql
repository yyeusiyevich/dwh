-- DROP FUNCTION bl_cl.low_sales_notification();
CREATE OR REPLACE FUNCTION bl_cl.low_sales_notification()

RETURNS VOID AS $$

DECLARE

    rec_store RECORD;
    completed_transactions INTEGER;
    cur_stores CURSOR FOR SELECT store_surr_id, name, curr_emp_full_name, contact_phone, contact_email
   						  FROM bl_dim.dim_stores_scd
   						  WHERE store_surr_id != -1;
   	max_date DATE;
    start_date DATE;
    end_date DATE;  
   
BEGIN
	SELECT MAX(event_date) INTO max_date FROM bl_dim.fct_sales;

    start_date := DATE_TRUNC('month', max_date) - INTERVAL '1 month';
    end_date := DATE_TRUNC('month', max_date);

    OPEN cur_stores;
    LOOP
        FETCH cur_stores INTO rec_store;
        EXIT WHEN NOT FOUND;
        SELECT COUNT(*)
        INTO completed_transactions
        FROM bl_dim.fct_sales AS fct
        INNER JOIN bl_dim.dim_junk AS jk ON jk.junk_surr_id = fct.junk_surr_id 
        WHERE store_surr_id = rec_store.store_surr_id 
        	  AND UPPER(jk.transaction_status) = 'COMPLETED'
        	  AND event_date >= start_date
        	  AND event_date < end_date;

        IF completed_transactions < 300 THEN
            RAISE NOTICE 'The number of orders in the store % are below threshold this month. Send an email to %: %, %.', 
                         rec_store.name, rec_store.curr_emp_full_name, rec_store.contact_phone, rec_store.contact_email;
        END IF;
    END LOOP;
    CLOSE cur_stores;
END;
$$ LANGUAGE plpgsql;


