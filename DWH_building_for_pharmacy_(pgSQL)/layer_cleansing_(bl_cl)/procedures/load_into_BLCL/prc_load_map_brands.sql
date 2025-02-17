--____________________________ MAPPING: BRANDS _______________________________________
/* Procedure adopted for changing brands according to SCD1 (changes in brand name) */

--TRUNCATE bl_cl.map_brands;

CREATE OR REPLACE PROCEDURE bl_cl.prc_load_map_brands()
LANGUAGE plpgsql 
AS $$
DECLARE 
	max_id INT;
	rows_before INT;
	rows_after INT;
	schema_n VARCHAR(10) := 'bl_cl';
	prcd VARCHAR(50) := 'prc_load_map_brands';
	time_start TIMESTAMP;
	time_end TIMESTAMP;
	run_time_sec NUMERIC; -- seconds (e.g. 0.6489)
	er_flag CHAR(1) := 'N'; -- will be changed if exception
	er_code VARCHAR(15);
	er_msg VARCHAR(300);
BEGIN 
	time_start := CLOCK_TIMESTAMP();
	
	SELECT COALESCE(MAX(brand_id), 0)  
	INTO max_id
	FROM bl_cl.map_brands;
	
	SELECT COUNT(*)
	INTO rows_before
	FROM bl_cl.map_brands;

	BEGIN
	WITH src AS (
		SELECT 
			brand_id,
			brand AS brand_name, 
			'src_pharm_online_sales' AS tab, 
			'sa_online' AS syst
		FROM sa_online.src_pharm_online_sales onl
		WHERE COALESCE(brand_id, brand) IS NOT NULL
		GROUP BY brand_id, brand
		UNION ALL
		SELECT 
			brand_id, 
			brand_name, 
			'src_pharm_offline_sales', 
			'sa_offline'
		FROM sa_offline.src_pharm_offline_sales ofl
		WHERE COALESCE(brand_id, brand_name) IS NOT NULL
		GROUP BY brand_id, brand_name
		)

		MERGE INTO bl_cl.map_brands mp
		USING (
			SELECT 
				DENSE_RANK() OVER (ORDER BY INITCAP(src.brand_name)) + max_id AS brand_id,
				INITCAP(src.brand_name) AS brand_name,
				src.brand_name AS brand_src_name, 
				src.brand_id AS brand_src_id,
				src.tab,
				src.syst
			FROM  src 
			ORDER BY brand_name -- order is extra, not necessary, but just for beauty in the project
			) upd 
			
		ON mp.brand_src_id = upd.brand_src_id
		AND mp.source_table = upd.tab
		AND mp.source_system = upd.syst

		WHEN MATCHED AND INITCAP(mp.brand_name) != upd.brand_name
			THEN UPDATE SET brand_name = upd.brand_name
			
		WHEN NOT MATCHED 
			THEN INSERT VALUES (upd.brand_id, upd.brand_name, upd.brand_src_name, upd.brand_src_id, upd.tab, upd.syst);
	
		
	-- exception during load that should be logged (if any)
	EXCEPTION WHEN OTHERS THEN
		er_flag := 'Y';
		er_code := SQLSTATE::VARCHAR(15);
		er_msg := SQLERRM::VARCHAR(300);
	END ;

	SELECT COUNT(*)
	INTO rows_after
	FROM bl_cl.map_brands;

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

--CALL bl_cl.prc_load_map_brands();
--SELECT * FROM bl_cl.map_brands ORDER BY 1;
--SELECT * FROM bl_cl.logs;

COMMIT; 
