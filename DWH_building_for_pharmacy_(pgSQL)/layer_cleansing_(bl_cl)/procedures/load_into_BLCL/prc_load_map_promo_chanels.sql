--________________________ MAPPING: PROMO CHANNELS ___________________________________
/* Procedure adopted for changing according to SCD1 (changes in name) */

--TRUNCATE bl_cl.map_promo_chanels;

CREATE OR REPLACE PROCEDURE bl_cl.prc_load_map_promo_chanels()
LANGUAGE plpgsql 
AS $$
DECLARE 
	max_id INT;
	rows_before INT;
	rows_after INT;
	schema_n VARCHAR(10) := 'bl_cl';
	prcd VARCHAR(50) := 'prc_load_map_promo_chanels';
	time_start TIMESTAMP;
	time_end TIMESTAMP;
	run_time_sec NUMERIC; -- seconds (e.g. 0.6489)
	er_flag CHAR(1) := 'N'; -- will be changed if exception
	er_code VARCHAR(15);
	er_msg VARCHAR(300);
BEGIN 
	time_start := CLOCK_TIMESTAMP();
	
	SELECT COALESCE(MAX(promo_channel_id), 0) 
	INTO max_id
	FROM bl_cl.map_promo_chanels;
	
	SELECT COUNT(*)
	INTO rows_before
	FROM bl_cl.map_promo_chanels;

	BEGIN
	WITH src AS (
		SELECT 
			promotion_channel_id AS chan_id,
			promotion_channel AS chan_name,
			'src_pharm_offline_sales' AS tab, 
			'sa_offline' AS syst
		FROM sa_offline.src_pharm_offline_sales ofl
		GROUP BY promotion_channel_id, promotion_channel
		UNION ALL
		SELECT 
			promo_distr_id AS chan_id,
			promo_distr AS chan_name,
			'src_pharm_online_sales' AS tab, 
			'sa_online' AS syst
		FROM sa_online.src_pharm_online_sales onl
		GROUP BY promo_distr_id, promo_distr
			)

	MERGE INTO bl_cl.map_promo_chanels mp
	USING (
		SELECT 
			/* during sources profiling it was realized that NULLs in names means 'NO DISCOUNT' channel, 
			thus, should be replaced WITH meaningful 'NO DISCOUNT' channel name */
			DENSE_RANK() OVER (ORDER BY COALESCE(UPPER(src.chan_name), 'NO DISCOUNT')) + max_id AS promo_channel_id,
			COALESCE(UPPER(src.chan_name), 'NO DISCOUNT') AS promo_channel_name,
			COALESCE(src.chan_name, 'n.a.') AS promo_channel_src_name, 
			src.chan_id AS promo_channel_src_id,
			src.tab,
			src.syst
		FROM src 
		ORDER BY src.chan_name -- order is extra, not necessary, but just for beauty in the project
	) upd
	
	ON mp.promo_channel_src_id = upd.promo_channel_src_id
	AND mp.source_table = upd.tab
	AND mp.source_system = upd.syst
	
	WHEN MATCHED AND mp.promo_channel_name != upd.promo_channel_name
		THEN UPDATE SET promo_channel_name = upd.promo_channel_name
		
	WHEN NOT MATCHED
		THEN INSERT VALUES(
		upd.promo_channel_id, 
		upd.promo_channel_name, 
		upd.promo_channel_src_name, 
		upd.promo_channel_src_id, 
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
	FROM bl_cl.map_promo_chanels;

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

--CALL bl_cl.prc_load_map_promo_chanels();
--SELECT * FROM bl_cl.map_promo_chanels ORDER BY 1;
--SELECT * FROM bl_cl.logs;

COMMIT;


