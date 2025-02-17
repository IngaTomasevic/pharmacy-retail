--________________________ LOOKUP (CLEANSING): CITIES _____________________________
/* Bring cities to appropriate names format (clean). They are written differently with 
 * a lot of dirty words ('cty', 'city', 'co', 'cnt' and others). */ 

--TRUNCATE bl_cl.lkp_cities;

CREATE OR REPLACE PROCEDURE bl_cl.prc_load_lkp_cities()
LANGUAGE plpgsql 
AS $$
DECLARE 
	rows_before INT;
	rows_after INT;
	schema_n VARCHAR(10) := 'bl_cl';
	prcd VARCHAR(50) := 'prc_load_lkp_cities';
	time_start TIMESTAMP;
	time_end TIMESTAMP;
	run_time_sec NUMERIC; -- seconds (e.g. 0.6489)
	er_flag CHAR(1) := 'N'; -- will be changed if exception
	er_code VARCHAR(15);
	er_msg VARCHAR(300);
BEGIN 
	time_start := CLOCK_TIMESTAMP();
	
	SELECT COUNT(*)
	INTO rows_before
	FROM bl_cl.lkp_cities;

	BEGIN
	MERGE INTO bl_cl.lkp_cities lkp
	USING (
		SELECT 
			un.city AS crs_city_name, 
			INITCAP(REGEXP_REPLACE(
				REGEXP_REPLACE(
					REGEXP_REPLACE(
						REGEXP_REPLACE(
							REGEXP_REPLACE(
								REGEXP_REPLACE(
									REGEXP_REPLACE(
										REGEXP_REPLACE(
											REGEXP_REPLACE(TRIM(un.city), 
											'\s{2,}|-|\r+| cty | city ', ' ', 'g'), 
										' west$', '', 'g'), 
									'( cty| city| cty cty| co| county| city co| city county| cty county)$', ' city', 'g'), 
								'^s ', 'san ', 'g'), 
							'^cty ', 'city '), 
						'(nort |nrth )^', 'north ', 'g'), 
					'( prk| pk)$', ' park', 'g'), 
				'new york$|nyc', 'new york city', 'g'), 
			'^w ', 'west ', 'g')) AS lkp_city_name
		FROM (
			SELECT cust_city AS city
			FROM sa_online.src_pharm_online_sales onl
			UNION ALL
			SELECT city
			FROM sa_offline.src_pharm_offline_sales ofl
			) un
		GROUP BY un.city
		) upd
	
	ON lkp.city_name_src = upd.crs_city_name
	AND lkp.city_name_lkp = upd.lkp_city_name
	
	WHEN MATCHED 
		THEN DO NOTHING 
		
	WHEN NOT MATCHED 
		THEN INSERT VALUES(
		upd.crs_city_name, 
		upd.lkp_city_name
		);
	

	-- exception during load that should be logged (if any)
	EXCEPTION WHEN OTHERS THEN
		er_flag := 'Y';
		er_code := SQLSTATE::VARCHAR(15);
		er_msg := SQLERRM::VARCHAR(300);
	END ;

	SELECT COUNT(*)
	INTO rows_after
	FROM bl_cl.lkp_cities;

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

--CALL bl_cl.prc_load_lkp_cities();
--SELECT * FROM bl_cl.lkp_cities order by 2;
--SELECT * FROM bl_cl.logs;

COMMIT; 
