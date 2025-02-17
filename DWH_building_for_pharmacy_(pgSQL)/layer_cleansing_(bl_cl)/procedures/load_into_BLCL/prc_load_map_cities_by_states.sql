--________________________ MAPPING: CITIES BY STATES ________________________________
/* Deduplicate cities and conform them with accordance to look up table (city names).
 * Different states have cities with sama names. So, cities must be mapped by states they belong to*/

--TRUNCATE bl_cl.map_cities_by_states;

CREATE OR REPLACE PROCEDURE bl_cl.prc_load_map_cities_by_states()
LANGUAGE plpgsql
AS $$
DECLARE
	max_id INT;
	rows_before INT;
	rows_after INT;
	schema_n VARCHAR(10) := 'bl_cl';
	prcd VARCHAR(50) := 'prc_load_map_cities_by_states';
	time_start TIMESTAMP;
	time_end TIMESTAMP;
	run_time_sec NUMERIC; -- seconds (e.g. 0.6489)
	er_flag CHAR(1) := 'N'; -- will be changed if exception
	er_code VARCHAR(15);
	er_msg VARCHAR(300);
BEGIN
	time_start := CLOCK_TIMESTAMP();

	SELECT COALESCE(MAX(city_id), 0)
	INTO max_id
	FROM bl_cl.map_cities_by_states;

	SELECT COUNT(*)
	INTO rows_before
	FROM bl_cl.map_cities_by_states;

	BEGIN
	WITH src AS (
		SELECT
			cust_city AS city_src_name,
			cust_state AS state_src_name,
			'src_pharm_online_sales' AS tab,
			'sa_online' AS syst
		FROM sa_online.src_pharm_online_sales onl
		WHERE cust_city IS NOT NULL
		AND cust_state IS NOT NULL
		GROUP BY cust_city, cust_state
		UNION ALL
		SELECT
			city AS city_src_name,
			state AS state_src_name,
			'src_pharm_offline_sales',
			'sa_offline'
		FROM sa_offline.src_pharm_offline_sales ofl
		WHERE city IS NOT NULL
		AND state IS NOT NULL
		GROUP BY city, state
		)

	MERGE INTO bl_cl.map_cities_by_states mp
	USING (
		SELECT
			/* Coalesce for selecting first not NULL entity after LEFT join. E.g. if city isn't loocked up (was written in source incorrect),
			 * instead of getting NULL from look up table get city from source directly. */
			DENSE_RANK() OVER (ORDER BY INITCAP(COALESCE(lkp.city_name_lkp, src.city_src_name)), INITCAP(COALESCE(mp.state_name, src.state_src_name))) + max_id	AS city_id,
			INITCAP(COALESCE(lkp.city_name_lkp, src.city_src_name)) AS city_name,
			src.city_src_name AS city_src_name,
			src.city_src_name AS city_src_id,
			COALESCE(mp.state_id, -1) AS state_id,
			INITCAP(COALESCE(mp.state_name, src.state_src_name)) AS state_name,
			src.state_src_name AS state_src_name,
			src.state_src_name AS state_src_id,
			src.tab AS source_table,
			src.syst AS source_system
		FROM  src
		LEFT OUTER JOIN bl_cl.lkp_cities lkp
		ON lkp.city_name_src = src.city_src_name
		LEFT OUTER JOIN bl_cl.map_states mp
		ON mp.state_src_name = src.state_src_name
		AND mp.source_table = src.tab
		AND mp.source_system = src.syst
	) upd 
	 
	ON upd.city_src_id = mp.city_src_id
	AND upd.state_src_id = mp.state_src_id
	AND upd.source_table = mp.source_table
	AND upd.source_system = mp.source_system
	
	WHEN MATCHED 
		THEN DO NOTHING 
		
	WHEN NOT MATCHED 
		THEN INSERT VALUES(
		upd.city_id,
		upd.city_name,
		upd.city_src_name,
		upd.city_src_id,
		upd.state_id,
		upd.state_name,
		upd.state_src_name,
		upd.state_src_id,
		upd.source_table,
		upd.source_system
		);


	-- exception during load that should be logged (if any)
	EXCEPTION WHEN OTHERS THEN
		er_flag := 'Y';
		er_code := SQLSTATE::VARCHAR(15);
		er_msg := SQLERRM::VARCHAR(300);
	END ;

	SELECT COUNT(*)
	INTO rows_after
	FROM bl_cl.map_cities_by_states;

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

	-- other exceptions that can occur during last 3 actions
	EXCEPTION WHEN OTHERS THEN
		RAISE NOTICE 'Error during %: %', prcd, SQLERRM;
	COMMIT;
END; $$;

--SELECT * FROM bl_cl.map_cities_by_states ORDER BY 2;
--CALL bl_cl.prc_load_map_cities_by_states();
--SELECT * FROM bl_cl.logs;

COMMIT;
