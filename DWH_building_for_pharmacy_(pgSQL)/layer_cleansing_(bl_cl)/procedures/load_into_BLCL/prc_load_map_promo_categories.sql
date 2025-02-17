--________________________ MAPPING: PROMO CATEGORIES _______________________________________
/* Procedure adopted for changing according to SCD1 (changes in name) */

/* Both sources have differently names category with 0 discount ('none discount', 'zero discount'),
 * Map promo categories and then UPDATE them (uniform) manually: 'zero' and 'none' into single discount */


--TRUNCATE bl_cl.map_promo_categories;

CREATE OR REPLACE PROCEDURE bl_cl.prc_load_map_promo_categories()
LANGUAGE plpgsql 
AS $$
DECLARE 
	max_id INT;
	rows_before INT;
	rows_after INT;
	schema_n VARCHAR(10) := 'bl_cl';
	prcd VARCHAR(50) := 'prc_load_map_promo_categories';
	time_start TIMESTAMP;
	time_end TIMESTAMP;
	run_time_sec NUMERIC; -- seconds (e.g. 0.6489)
	er_flag CHAR(1) := 'N'; -- will be changed if exception
	er_code VARCHAR(15);
	er_msg VARCHAR(300);
BEGIN 
	time_start := CLOCK_TIMESTAMP();
	
	SELECT COALESCE(MAX(promo_category_id), 0) 
	INTO max_id
	FROM bl_cl.map_promo_categories;
	
	SELECT COUNT(*)
	INTO rows_before
	FROM bl_cl.map_promo_categories;

	BEGIN
	WITH src AS (
		SELECT 
			promotion_subcategory_id AS promo_subcat_id,
			promotion_subcategory AS promo_subcat_name,
			'src_pharm_offline_sales' AS tab, 
			'sa_offline' AS syst
		FROM sa_offline.src_pharm_offline_sales ofl
		GROUP BY promotion_subcategory_id, promotion_subcategory
		UNION ALL
		SELECT 
			promo_type_id AS promo_subcat_id,
			promo_type AS promo_subcat_name,
			'src_pharm_online_sales' AS tab, 
			'sa_online' AS syst
		FROM sa_online.src_pharm_online_sales onl
		GROUP BY promo_type_id, promo_type
			)
			
		MERGE INTO bl_cl.map_promo_categories mp 
		USING (
			SELECT 
				DENSE_RANK() OVER (ORDER BY INITCAP(src.promo_subcat_name)) + max_id AS id,
				INITCAP(src.promo_subcat_name) AS promo_category_name,
				src.promo_subcat_name AS promo_category_src_name, 
				src.promo_subcat_id AS promo_category_src_id,
				src.tab,
				src.syst
			FROM src 
			ORDER BY src.promo_subcat_name
		) upd

		ON mp.promo_category_src_id = upd.promo_category_src_id
		AND mp.source_table = upd.tab
		AND mp.source_system = upd.syst
		
		WHEN MATCHED AND mp.promo_category_name != upd.promo_category_name
			THEN UPDATE SET promo_category_name = upd.promo_category_name
			
		WHEN NOT MATCHED
			THEN INSERT VALUES (
			upd.id, 
			upd.promo_category_name, 
			upd.promo_category_src_name, 
			upd.promo_category_src_id, 
			upd.tab,
			upd.syst
			);
		
	-- conform manually diferrently named zero discount promotion
	UPDATE bl_cl.map_promo_categories
	SET promo_category_id = mp.promo_category_id, 
	promo_category_name = mp.promo_category_name
	FROM (
			SELECT promo_category_id, promo_category_name
			FROM bl_cl.map_promo_categories 
			WHERE UPPER(promo_category_name) = 'ZERO DISCOUNT'
			) mp
	WHERE UPPER(bl_cl.map_promo_categories.promo_category_name) = 'NONE DISCOUNT' OR
	bl_cl.map_promo_categories.promo_category_name IS NULL;


	-- exception during load that should be logged (if any)
	EXCEPTION WHEN OTHERS THEN
		er_flag := 'Y';
		er_code := SQLSTATE::VARCHAR(15);
		er_msg := SQLERRM::VARCHAR(300);
	END;

	SELECT COUNT(*)
	INTO rows_after
	FROM bl_cl.map_promo_categories;

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
		
	-- other exceptions that can occur during last 4 actions - show wich procedure failed
	EXCEPTION WHEN OTHERS THEN
		RAISE NOTICE 'Error during %: %', prcd, SQLERRM;
	COMMIT; 
END; $$;


--CALL bl_cl.prc_load_map_promo_categories();
--SELECT * FROM bl_cl.map_promo_categories ORDER BY 1;
--SELECT * FROM bl_cl.logs;

COMMIT; 

