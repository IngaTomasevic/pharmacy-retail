--________________________ MAPPING: PROD CATEGORIES ________________________________
/* Procedure adopted for changing according to SCD1 (changes in name to be precise) */

--TRUNCATE bl_cl.map_prod_categories;

CREATE OR REPLACE PROCEDURE bl_cl.prc_load_map_prod_categories()
LANGUAGE plpgsql 
AS $$
DECLARE 
	max_id INT;
	rows_before INT;
	rows_after INT;
	schema_n VARCHAR(10) := 'bl_cl';
	prcd VARCHAR(50) := 'prc_load_map_prod_categories';
	time_start TIMESTAMP;
	time_end TIMESTAMP;
	run_time_sec NUMERIC; -- seconds (e.g. 0.6489)
	er_flag CHAR(1) := 'N'; -- will be changed if exception
	er_code VARCHAR(15);
	er_msg VARCHAR(300);
BEGIN 
	time_start := CLOCK_TIMESTAMP();
	
	SELECT COALESCE(MAX(prod_category_id), 0) 
	INTO max_id
	FROM bl_cl.map_prod_categories;
	
	SELECT COUNT(*)
	INTO rows_before
	FROM bl_cl.map_prod_categories;

	BEGIN
	WITH src AS (
		SELECT 
			cat_id,
			category, 
			'src_pharm_online_sales' AS tab, 
			'sa_online' AS syst
		FROM sa_online.src_pharm_online_sales onl
		WHERE category IS NOT NULL
		GROUP BY cat_id, category
		UNION ALL
		SELECT 
			class_id , 
			class_name, 
			'src_pharm_offline_sales', 
			'sa_offline'
		FROM sa_offline.src_pharm_offline_sales ofl
		WHERE class_name IS NOT NULL
		GROUP BY class_id, class_name
		)
		
		MERGE INTO bl_cl.map_prod_categories mp
		USING (
			SELECT 
				DENSE_RANK() OVER (ORDER BY INITCAP(src.category)) + max_id AS prod_category_id,
				INITCAP(src.category) AS prod_category_name,
				src.category AS prod_category_src_name,
				src.cat_id AS prod_category_src_id, 
				src.tab,
				src.syst
			FROM  src 
			ORDER BY prod_category_name -- order is extra, not necessary, but just for beauty in the project
		) upd 
		
		ON mp.prod_category_src_id = upd.prod_category_src_id
		AND mp.source_table = upd.tab
		AND mp.source_system = upd.syst

		WHEN MATCHED AND mp.prod_category_name != upd.prod_category_name
			THEN UPDATE SET prod_category_name = upd.prod_category_name
			
		WHEN NOT MATCHED 
			THEN INSERT VALUES (
			upd.prod_category_id, 
			upd.prod_category_name, 
			upd.prod_category_src_name, 
			upd.prod_category_src_id, 
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
	FROM bl_cl.map_prod_categories;

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

--CALL bl_cl.prc_load_map_prod_categories();
--SELECT * FROM bl_cl.map_prod_categories ORDER BY prod_category_id;
--SELECT * FROM bl_cl.logs;

COMMIT;
