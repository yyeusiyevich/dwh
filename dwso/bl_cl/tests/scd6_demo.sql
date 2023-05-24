-- take a shipper from the first region
SELECT *
FROM bl_3nf.ce_shippers AS sh
INNER JOIN bl_3nf.ce_regions AS reg ON sh.curr_region_id = reg.region_id 
WHERE TRIM(sh.name) = 'Rapid Transport Solutions';

-- the shipper changes his region
INSERT INTO sa_iowalakes_sales.src_iowalakes_sales 
(shipper_id,
 shipper_name,
 shipper_rating,
 ship_base,
 ship_rate,
 shipper_phone,
 shipper_contact_name,
 region_id,
 region_name, 
 insert_dt)
VALUES ('554934',  
		'Rapid Transport Solutions', 
		'5.3', 
		'32', 
		'3', 
		'555-458-3072',	
		'Samantha Mckinney', 
		'2', 
		'iowalakes', 
		CURRENT_TIMESTAMP + '1 day' :: INTERVAL);
		
-- update shippers info
CALL bl_cl.map_shippers_insertion();

SELECT * FROM bl_cl.map_shippers;

-- run scd
CALL bl_cl.ce_shippers_insertion();

SELECT * FROM bl_3nf.ce_shippers;

-- check further
CALL bl_cl.wrk_shippers_insertion();

SELECT * FROM bl_cl.wrk_shippers;

-- run dim scd
CALL bl_cl.dim_shippers_insertion();

SELECT * FROM bl_dim.dim_shippers_scd;