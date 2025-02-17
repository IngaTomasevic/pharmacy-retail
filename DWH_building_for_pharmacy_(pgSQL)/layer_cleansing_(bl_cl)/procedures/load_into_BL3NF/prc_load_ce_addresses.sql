-- _____________________________________________ CE_ADDRESSES LOAD PROCEDURE ____________________________________
/* Addresses are  SCD1 */

CREATE OR REPLACE PROCEDURE bl_cl.prc_load_ce_addresses()
LANGUAGE plpgsql
AS $$
DECLARE
	rows_ins_before INT;
	rows_ins_after INT;
	rows_upd_before INT;
	rows_upd_after INT;
	schema_n VARCHAR(10) := 'bl_3nf';
	prcd VARCHAR(50) := 'prc_load_ce_addresses';
	time_start TIMESTAMP;
	time_end TIMESTAMP;
	time_run NUMERIC; -- seconds (e.g. 0.6489)
	er_flag CHAR(1) := 'N'; -- will be changed if exception
	er_code VARCHAR(15);
	er_msg VARCHAR(300);
BEGIN
	time_start := CLOCK_TIMESTAMP();

	SELECT f.count_ins, f.count_upd
	INTO rows_ins_before, rows_upd_before
	FROM bl_cl.fn_current_ins_upd_rows_count('bl_3nf.ce_addresses') f;

	BEGIN
	WITH src AS (
		SELECT
			-- if address id isn't present, id becomes street_name
			COALESCE(address_id, street) AS src_addr_id,
			street AS src_addr_name,
			postal_code AS zip,
			city AS src_city,
			state AS src_state,
			'src_pharm_offline_sales' AS tab,
			'sa_offline' AS syst
		FROM sa_offline.src_pharm_offline_sales
		WHERE COALESCE(address_id, street) IS NOT NULL -- filter empty addresses
		GROUP BY address_id, street, postal_code, city, state
		UNION ALL
		SELECT
			COALESCE(cust_address_id, cust_street_name) AS src_addr_id,
			cust_street_name AS src_addr_name,
			cust_postal_code AS zip,
			cust_city AS src_city,
			cust_state AS src_state,
			'src_pharm_online_sales' AS tab,
			'sa_online' AS syst
		FROM sa_online.src_pharm_online_sales
		WHERE COALESCE(cust_address_id, cust_street_name) IS NOT NULL
		GROUP BY cust_address_id, cust_street_name, cust_postal_code, cust_city, cust_state
		)
	MERGE INTO bl_3nf.ce_addresses ce
	USING(
		SELECT *
		FROM (
		SELECT
			-- source id becomes first not null value: either from map table, or from source
			COALESCE(mp.address_id::VARCHAR, src.src_addr_id) AS nk,
			CASE WHEN mp.address_id IS NOT NULL THEN 'bl_cl' ELSE src.syst END AS source_system,
			CASE WHEN mp.address_id IS NOT NULL THEN 'map_addresses' ELSE src.tab END AS source_table,
			COALESCE(mp.address_descr, UPPER(src.src_addr_name), 'n.a.') AS address,
			COALESCE(c.city_id, -1) AS city_id,
			COALESCE(mp.zip, 'n.a.') AS zip
		FROM src
		LEFT OUTER JOIN bl_cl.map_addresses mp
		ON mp.address_src_id = src.src_addr_id
		AND mp.source_table = src.tab
		AND mp.source_system = src.syst
	
		-- cities are mapped with states they belong to!
		LEFT OUTER JOIN bl_cl.map_cities_by_states mp2
		ON mp2.city_src_id = src.src_city
		AND mp2.state_src_id = src.src_state
		AND mp2.source_table = src.tab
		AND mp2.source_system = src.syst
	
		LEFT OUTER JOIN bl_3nf.ce_cities c
		ON c.city_src_id = COALESCE(mp2.city_id::VARCHAR, src_city)
		AND c.source_table = CASE
			WHEN mp2.city_id IS NOT NULL THEN 'map_cities_by_states'
			ELSE src.syst END
		AND c.source_system = CASE
			WHEN mp2.city_id IS NOT NULL THEN 'bl_cl'
			ELSE src.syst END
				)
		GROUP BY nk, source_system, source_table, address, city_id, zip
		ORDER BY nk::INT -- just for beauty, for huge project is extra and not necessary
	) upd
	
	ON ce.address_src_id = upd.nk
	AND ce.source_system = upd.source_system
	AND ce.source_table = upd.source_table
	
	WHEN MATCHED AND ce.address_descr != upd.address
		THEN UPDATE SET 
		address_descr = upd.address,
		ta_update_dt = CURRENT_DATE
		
	WHEN NOT MATCHED 
		THEN INSERT VALUES(
		NEXTVAL('bl_3nf.bl_3nf_seq_address_id'),
		upd.nk, 
		upd.source_system, 
		upd.source_table, 
		upd.address, 
		upd.city_id, 
		upd.zip, 
		CURRENT_DATE, 
		CURRENT_DATE
		);	
	
	-- exception during load that should be logged (if any)
	EXCEPTION WHEN OTHERS THEN
		er_flag := 'Y';
		er_code := SQLSTATE::VARCHAR(15);
		er_msg := SQLERRM::VARCHAR(300);
	END ;

	time_end := CLOCK_TIMESTAMP();
	time_run := EXTRACT (EPOCH FROM (time_end - time_start));

	/* Since new inserted rows have the same update_dt as current_date,
	 * to extract count of only updated rows we must substract difference
	 * of inserted rows (before-after) from count of updated rows after. */
	SELECT f.count_ins, f.count_upd - (f.count_ins - rows_ins_before)
	INTO rows_ins_after, rows_upd_after
	FROM bl_cl.fn_current_ins_upd_rows_count('bl_3nf.ce_addresses') f;

	CALL bl_cl.prc_load_logs(
			prcd,
			schema_n,
			time_start,
			time_end,
			time_run,
			(rows_ins_after - rows_ins_before),
			(rows_upd_after - rows_upd_before),
			er_flag,
			er_code,
			er_msg
			);
		
	-- exception that can occur during last 4 actions
	EXCEPTION WHEN OTHERS THEN
		RAISE NOTICE 'Error in %, %: %', prcd, SQLSTATE, SQLERRM;
	COMMIT;
END; $$;


--DELETE FROM  bl_3nf.ce_addresses WHERE address_id != -1;
--ALTER SEQUENCE bl_3nf.bl_3nf_seq_address_id RESTART WITH 1
--SELECT * FROM bl_3nf.ce_addresses ORDER BY address_id;
--CALL bl_cl.prc_load_ce_addresses();
--SELECT * FROM bl_cl.logs;

COMMIT;
