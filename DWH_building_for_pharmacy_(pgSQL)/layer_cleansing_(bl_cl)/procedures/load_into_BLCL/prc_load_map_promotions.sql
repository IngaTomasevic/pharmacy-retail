--________________________ MAPPING: PROMOTIONS _______________________________________
/* Procedure adopted for changing according to SCD1 (changes in name) */

--TRUNCATE bl_cl.map_promotions;

CREATE OR REPLACE PROCEDURE bl_cl.prc_load_map_promotions()
LANGUAGE plpgsql 
AS $$
DECLARE 
	max_id INT;
	rows_before INT;
	rows_after INT;
	schema_n VARCHAR(10) := 'bl_cl';
	prcd VARCHAR(50) := 'prc_load_map_promotions';
	time_start TIMESTAMP;
	time_end TIMESTAMP;
	run_time_sec NUMERIC; -- seconds (e.g. 0.6489)
	er_flag CHAR(1) := 'N'; -- will be changed if exception
	er_code VARCHAR(15);
	er_msg VARCHAR(300);
BEGIN 
	time_start := CLOCK_TIMESTAMP();
	
	SELECT COALESCE(MAX(promotion_id), 0) 
	INTO max_id
	FROM bl_cl.map_promotions;
	
	SELECT COUNT(*)
	INTO rows_before
	FROM bl_cl.map_promotions;

	BEGIN
	WITH src AS (
		SELECT 
			promotion_id AS promo_id,
			promotion AS promo,
			'src_pharm_offline_sales' AS tab, 
			'sa_offline' AS syst
		FROM sa_offline.src_pharm_offline_sales ofl
		GROUP BY promotion_id, promotion
		UNION ALL
		SELECT 
			promo_id AS promo_id,
			promo AS promo,
			'src_pharm_online_sales' AS tab, 
			'sa_online' AS syst
		FROM sa_online.src_pharm_online_sales onl
		GROUP BY promo_id, promo
			)

		MERGE INTO bl_cl.map_promotions mp
		USING (
			SELECT 
				DENSE_RANK() OVER (ORDER BY UPPER(TRIM(src.promo, '"“”'))) + max_id AS id,
				UPPER(TRIM(src.promo, '"“”')) AS promotion_name,
				src.promo AS promotion_src_name, 
				src.promo_id AS promotion_src_id,
				src.tab,
				src.syst
			FROM src 
			ORDER BY UPPER(TRIM(src.promo, '"“”')) -- order is extra, not necessary, but just for beauty in the project
		) upd 
		
		ON mp.promotion_src_id = upd.promotion_src_id
		AND mp.source_table = upd.tab
		AND mp.source_system = upd.syst
		
		WHEN MATCHED AND mp.promotion_name != upd.promotion_name
			THEN UPDATE SET promotion_name = upd.promotion_name
			
		WHEN NOT MATCHED 
			THEN INSERT VALUES(
			upd.id, 
			upd.promotion_name, 
			upd.promotion_src_name, 
			upd.promotion_src_id, 
			upd.tab,
			upd.syst
			);

	-- exception during load that should be logged (if any)
	EXCEPTION WHEN OTHERS THEN
		er_flag := 'Y';
		er_code := SQLSTATE::VARCHAR(15);
		er_msg := SQLERRM::VARCHAR(300);
	END ;

	SELECT COUNT(*)
	INTO rows_after
	FROM bl_cl.map_promotions;

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

--CALL bl_cl.prc_load_map_promotions();
--SELECT * FROM bl_cl.map_promotions ORDER BY 1;
--SELECT * FROM bl_cl.logs;

COMMIT;
