--________________________ MAPPING: ADDRESSES _______________________________________
/* Map table for addresses is adopted for changes according to SCD1
 * (changes in address descr or zip code)*/ 

--TRUNCATE bl_cl.map_addresses;

CREATE OR REPLACE PROCEDURE bl_cl.prc_load_map_addresses()
LANGUAGE plpgsql 
AS $$
DECLARE 
	max_id INT;
	rows_before INT;
	rows_after INT;
	schema_n VARCHAR(10) := 'bl_cl';
	prcd VARCHAR(50) := 'prc_load_map_addresses';
	time_start TIMESTAMP;
	time_end TIMESTAMP;
	run_time_sec NUMERIC; -- seconds (e.g. 0.6489)
	er_flag CHAR(1) := 'N'; -- will be changed if exception
	er_code VARCHAR(15);
	er_msg VARCHAR(300);
BEGIN 
	time_start := CLOCK_TIMESTAMP();
	
	SELECT COALESCE(MAX(address_id), 0) 
	INTO max_id
	FROM bl_cl.map_addresses;
	
	SELECT COUNT(*) 
	INTO rows_before
	FROM bl_cl.map_addresses;

	BEGIN
	WITH src AS (
		SELECT 
			address_id AS src_adr_id,
			street AS src_str_name,
			city AS src_city_name, 
			state AS state,
			postal_code AS zip,
			'src_pharm_offline_sales' AS tab, 
			'sa_offline' AS syst
		FROM sa_offline.src_pharm_offline_sales ofl
		WHERE address_id IS NOT NULL
		GROUP BY address_id, street, city, state, postal_code
		UNION ALL
		SELECT 
			cust_address_id,
			cust_street_name,
			cust_city, 
			cust_state,
			cust_postal_code,
			'src_pharm_online_sales' AS tab, 
			'sa_online' AS syst
		FROM sa_online.src_pharm_online_sales onl
		WHERE cust_address_id IS NOT NULL
		GROUP BY cust_address_id, cust_street_name, cust_city, cust_state, cust_postal_code
			)

	MERGE INTO bl_cl.map_addresses mp
	USING (
		SELECT 
				/* Combination of street name, zip and city should be mapped as NK. Street name isn't unique. 
				 * Several cities may have same streets. Conformed city name should be used 
				 * from mapping cities table, because in sources cities are written differently. */
			DENSE_RANK() OVER (ORDER BY addr, city_name, zip) + max_id AS adr_id,
			addr, 
			src_addr, 
			src_addr_id, 
			src_city, 
			zip, 
			tab,
			syst
		FROM (
			SELECT 
				/* Perform street names transformation (conform 'street' postfix)*/
				UPPER(REGEXP_REPLACE(src.src_str_name, 'str\D*\s*$', 'street')) AS addr,
				COALESCE(mp.city_name, src.src_city_name) AS city_name,
				src.src_str_name AS src_addr, 
				src.src_adr_id AS src_addr_id,
				src.src_city_name AS src_city,
				CASE
					-- clean dirty not correct written zips
					WHEN LENGTH(COALESCE(REGEXP_REPLACE(src.zip, '\D', '', 'g'), 'n.a.')) > 5 THEN 'n.a.'
					ELSE COALESCE(REGEXP_REPLACE(src.zip, '\D', '', 'g'), 'n.a.') END AS zip,
				src.tab,
				src.syst
			FROM src 
			LEFT OUTER JOIN bl_cl.map_cities_by_states mp
			ON src.src_city_name = mp.city_src_name
			AND src.state = mp.state_src_name
			AND src.tab = mp.source_table 
			AND src.syst = mp.source_system
			)
	) upd

	ON mp.address_src_id = upd.src_addr_id
	AND mp.source_table = upd.tab
	AND mp.source_system = upd.syst
	
	WHEN MATCHED AND (
	mp.address_descr != upd.addr OR 
	mp.zip != upd.zip
	) THEN UPDATE SET 
		address_descr = upd.addr,
		zip = upd.zip
		
	WHEN NOT MATCHED 
		THEN INSERT VALUES(
		upd.adr_id, 
		upd.addr, 
		upd.src_addr, 
		upd.src_addr_id, 
		upd.src_city, 
		upd.zip, 
		upd.tab, 
		upd.syst
		);
	
	-- exception during load that should be logged (if any)
	EXCEPTION WHEN OTHERS THEN
		er_flag := 'Y';
		er_code := SQLSTATE::VARCHAR(15);
		er_msg := SQLERRM::VARCHAR(300);
	END;

	SELECT COUNT(*) 
	INTO rows_after
	FROM bl_cl.map_addresses;

	time_end := CLOCK_TIMESTAMP();
	run_time_sec := EXTRACT (EPOCH FROM (time_end - time_start));

	CALL bl_cl.prc_load_logs(
			prcd,
			schema_n,
			time_start,
			time_end,
			run_time_sec,
			(rows_after - rows_before),
			0,
			er_flag,
			er_code,
			er_msg
			);
		
	-- other exceptions that can occur during last 4 actions
	EXCEPTION WHEN OTHERS THEN
		RAISE NOTICE 'Error during %: %', prcd, SQLERRM;
	COMMIT; 
END; $$;	

--SELECT * FROM bl_cl.map_addresses order by address_descr, city;
--CALL bl_cl.prc_load_map_addresses();
--SELECT * FROM bl_cl.logs;

COMMIT;
